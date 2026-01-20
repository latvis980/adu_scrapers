# operators/custom_scrapers/prorus.py
"""
ProRus Custom Scraper - HTTP Pattern Approach (Simplified)
Scrapes architecture news from ProRus.ru (Russian architecture magazine)

Site: https://prorus.ru/projects/
Strategy: Extract links from article-item-title divs

Pattern Analysis:
- Article structure: <div class="article-item-title"><a href="/projects/article-slug/">Title</a></div>
- Article URLs: /projects/article-slug/ (transliterated Russian, e.g., /projects/launzh-bar-g-lounge-v-tolyatti/)
- Non-article URLs: URLs with Cyrillic letters are tag pages, not articles

Architecture (Simplified):
- Custom scraper discovers article URLs from projects page (no article page visits)
- Article tracker handles new/seen filtering (with TEST_MODE support)
- Main pipeline handles: content scraping, hero image extraction (og:image), AI filtering

On first run: All homepage articles marked as seen
On subsequent runs: Only new articles returned for processing

Usage:
    scraper = ProRusScraper()
    articles = await scraper.fetch_articles()
    await scraper.close()
"""

import asyncio
import re
from typing import Optional, List, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from operators.custom_scraper_base import BaseCustomScraper, custom_scraper_registry
from storage.article_tracker import ArticleTracker


class ProRusScraper(BaseCustomScraper):
    """
    HTTP pattern-based custom scraper for ProRus.
    Extracts article URLs from article-item-title divs.
    """

    source_id = "prorus"
    source_name = "ProRus"
    base_url = "https://prorus.ru/projects/"

    # Configuration
    MAX_NEW_ARTICLES = 10

    # Pattern to detect Cyrillic characters (tag pages use Cyrillic URLs)
    CYRILLIC_PATTERN = re.compile(r'[\u0400-\u04FF]')

    def __init__(self):
        """Initialize scraper with article tracker."""
        super().__init__()
        self.tracker: Optional[ArticleTracker] = None

    async def _ensure_tracker(self):
        """Ensure article tracker is connected."""
        if not self.tracker:
            self.tracker = ArticleTracker()
            await self.tracker.connect()

    def _is_valid_article_url(self, href: str) -> bool:
        """
        Check if URL is a valid article URL.

        Valid articles:
        - Start with /projects/
        - Have content after /projects/
        - Use transliterated (Latin) characters, not Cyrillic
        - End with /

        Args:
            href: URL path to check

        Returns:
            True if valid article URL
        """
        # Must start with /projects/
        if not href.startswith('/projects/'):
            return False

        # Must have content after /projects/
        slug = href.replace('/projects/', '').strip('/')
        if not slug or len(slug) < 3:
            return False

        # Exclude URLs with Cyrillic characters (these are tag pages)
        if self.CYRILLIC_PATTERN.search(href):
            return False

        return True

    def _extract_articles_from_html(self, html: str) -> List[Tuple[str, str]]:
        """
        Extract article URLs and titles from HTML.

        Looks for pattern: <div class="article-item-title"><a href="...">Title</a></div>

        Args:
            html: Page HTML content

        Returns:
            List of tuples: (url, title) - deduplicated
        """
        soup = BeautifulSoup(html, 'html.parser')
        seen_urls: set[str] = set()
        articles: List[Tuple[str, str]] = []

        # Find all article-item-title divs
        title_divs = soup.find_all('div', class_='article-item-title')

        for div in title_divs:
            # Find the link inside
            link = div.find('a', href=True)
            if not link:
                continue

            href = link.get('href', '')

            # Check if valid article URL
            if not self._is_valid_article_url(href):
                continue

            # Build full URL
            full_url = urljoin("https://prorus.ru", href)

            # Ensure trailing slash
            if not full_url.endswith('/'):
                full_url = full_url + '/'

            # DEDUPLICATION: Skip if already seen
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)

            # Get title from link text
            title = link.get_text(strip=True)

            # If no title, use slug
            if not title or len(title) < 3:
                slug = href.strip('/').split('/')[-1]
                title = slug.replace('-', ' ').title()

            # Clean title
            title = ' '.join(title.split())[:200]

            if title:
                articles.append((full_url, title))

        return articles

    async def fetch_articles(self, hours: int = 24) -> list[dict]:
        """
        Fetch new articles from ProRus.

        Workflow:
        1. Load projects page
        2. Extract all article links from article-item-title divs
        3. Check database for new URLs
        4. Return minimal article dicts for new URLs
        5. Main pipeline handles: content, hero image (og:image), dates

        Args:
            hours: Ignored (we use database tracking instead)

        Returns:
            List of minimal article dicts
        """
        print(f"[{self.source_id}] Starting HTTP pattern scraping...")

        await self._ensure_tracker()

        try:
            page = await self._create_page()

            try:
                # ============================================================
                # Step 1: Load Projects Page
                # ============================================================
                print(f"[{self.source_id}] Loading projects page...")
                await page.goto(self.base_url, timeout=self.timeout, wait_until="networkidle")
                await page.wait_for_timeout(2000)

                # Get page HTML
                html = await page.content()

                # ============================================================
                # Step 2: Extract Article Links
                # ============================================================
                extracted = self._extract_articles_from_html(html)
                print(f"[{self.source_id}] Found {len(extracted)} article links")

                if not extracted:
                    print(f"[{self.source_id}] No articles found")
                    return []

                # ============================================================
                # Step 3: Check Database for New URLs
                # ============================================================
                if not self.tracker:
                    raise RuntimeError("Article tracker not initialized")

                all_urls = [url for url, _ in extracted]

                # Use filter_new_articles to get only new URLs
                new_urls = await self.tracker.filter_new_articles(self.source_id, all_urls)

                # Build lookup for titles
                url_to_title = {url: title for url, title in extracted}

                print(f"[{self.source_id}] Database check:")
                print(f"   Total extracted: {len(extracted)}")
                print(f"   Already seen: {len(extracted) - len(new_urls)}")
                print(f"   New articles: {len(new_urls)}")

                # ============================================================
                # Step 4: Mark All URLs as Seen
                # ============================================================
                await self.tracker.mark_as_seen(self.source_id, all_urls)

                if not new_urls:
                    print(f"[{self.source_id}] No new articles to process")
                    return []

                # ============================================================
                # Step 5: Create Minimal Article Dicts
                # ============================================================
                new_articles: list[dict] = []

                for url in new_urls[:self.MAX_NEW_ARTICLES]:
                    title = url_to_title.get(url, url.strip('/').split('/')[-1].replace('-', ' ').title())

                    # Create minimal article dict
                    # Main pipeline will extract: content, hero image (og:image), date
                    article = self._create_minimal_article_dict(
                        title=title,
                        link=url,
                        published=None  # Will be extracted by main pipeline
                    )

                    if self._validate_article(article):
                        new_articles.append(article)
                        print(f"[{self.source_id}]    Added: {title[:50]}...")

                # Final Summary
                print(f"\n[{self.source_id}] Processing Summary:")
                print(f"   Articles found: {len(extracted)}")
                print(f"   New articles: {len(new_urls)}")
                print(f"   Returning to pipeline: {len(new_articles)}")

                return new_articles

            finally:
                await page.close()

        except Exception as e:
            print(f"[{self.source_id}] Error in scraping: {e}")
            import traceback
            traceback.print_exc()
            return []

    async def close(self):
        """Close browser and tracker connections."""
        await super().close()

        if self.tracker:
            await self.tracker.close()
            self.tracker = None


# Register this scraper
custom_scraper_registry.register(ProRusScraper)


# =============================================================================
# Standalone Test
# =============================================================================

async def test_prorus_scraper():
    """Test the HTTP pattern scraper."""
    print("=" * 60)
    print("Testing ProRus HTTP Pattern Scraper")
    print("=" * 60)

    # Show TEST_MODE status
    from storage.article_tracker import ArticleTracker
    print(f"\nTEST_MODE: {ArticleTracker.TEST_MODE}")
    if ArticleTracker.TEST_MODE:
        print("   All articles will appear as 'new' (ignoring database)")
    else:
        print("   Normal mode - filtering seen articles")

    scraper = ProRusScraper()

    try:
        # Test connection
        print("\n1. Testing connection...")
        connected = await scraper.test_connection()

        if not connected:
            print("   Connection failed")
            return

        # Show tracker stats
        print("\n2. Checking tracker stats...")
        await scraper._ensure_tracker()

        if scraper.tracker:
            stats = await scraper.tracker.get_stats(source_id="prorus")
            print(f"   Total articles in database: {stats['total_articles']}")
            if stats['oldest_seen']:
                print(f"   Oldest: {stats['oldest_seen']}")
            if stats['newest_seen']:
                print(f"   Newest: {stats['newest_seen']}")

        # Fetch new articles
        print("\n3. Running HTTP pattern scraping...")
        articles = await scraper.fetch_articles(hours=24)

        print(f"\n   Found {len(articles)} NEW articles")

        # Display articles
        if articles:
            print("\n4. New articles:")
            for i, article in enumerate(articles, 1):
                print(f"\n   --- Article {i} ---")
                print(f"   Title: {article['title'][:60]}...")
                print(f"   Link: {article['link']}")
        else:
            print("\n4. No new articles (all previously seen)")

        print("\n" + "=" * 60)
        print("Test complete!")
        print("=" * 60)

    finally:
        await scraper.close()


if __name__ == "__main__":
    asyncio.run(test_prorus_scraper())