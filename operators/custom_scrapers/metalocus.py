# operators/custom_scrapers/metalocus.py
"""
Metalocus Custom Scraper - HTTP Pattern Approach (Simplified)
Scrapes architecture news from Metalocus (Spanish architecture magazine)

Site: https://www.metalocus.es/en
Strategy: Extract links matching /en/news/* pattern from HTML

Pattern Analysis:
- Article URLs: /en/news/article-slug (e.g., /en/news/dynamic-spaces-cuizhu-foreign-language-school-studio-link-arc)
- Non-article URLs: /en/architecture/, /en/art/, /en/design/, etc. (category pages)

Architecture (Simplified):
- Custom scraper discovers article URLs from homepage (no article page visits)
- Article tracker handles new/seen filtering (with TEST_MODE support)
- Main pipeline handles: content scraping, hero image extraction (og:image), AI filtering

On first run: All homepage articles marked as seen
On subsequent runs: Only new articles returned for processing

Usage:
    scraper = MetalocusScraper()
    articles = await scraper.fetch_articles()
    await scraper.close()
"""

import asyncio
import re
from typing import Optional, List, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from operators.custom_scraper_base import BaseCustomScraper, custom_scraper_registry
from storage.article_tracker import ArticleTracker


class MetalocusScraper(BaseCustomScraper):
    """
    HTTP pattern-based custom scraper for Metalocus.
    Extracts article URLs matching /en/news/* pattern from homepage.
    """

    source_id = "metalocus"
    source_name = "Metalocus"
    base_url = "https://www.metalocus.es/en"

    # Configuration
    MAX_NEW_ARTICLES = 10

    # URL pattern for articles: /en/news/article-slug
    ARTICLE_PATTERN = re.compile(r'^/en/news/[a-z0-9-]+$', re.IGNORECASE)

    # URL patterns to exclude (not articles)
    EXCLUDED_PATTERNS = [
        '/en/news$',       # News category page itself
        '/en/news/$',
        '/page/',
        '/user/',
        '/search/',
        '#',
        'javascript:',
    ]

    def __init__(self):
        """Initialize scraper with article tracker."""
        super().__init__()
        self.tracker: Optional[ArticleTracker] = None

    async def _ensure_tracker(self):
        """Ensure article tracker is connected."""
        if not self.tracker:
            self.tracker = ArticleTracker()
            await self.tracker.connect()

    def _is_valid_article_url(self, path: str) -> bool:
        """
        Check if URL path is a valid article URL.

        Valid articles match: /en/news/article-slug

        Args:
            path: URL path to check

        Returns:
            True if valid article URL
        """
        # Check excluded patterns
        path_lower = path.lower()
        for pattern in self.EXCLUDED_PATTERNS:
            if pattern in path_lower or path_lower == pattern.rstrip('/'):
                return False

        # Must match article pattern
        if not self.ARTICLE_PATTERN.match(path):
            return False

        return True

    def _extract_articles_from_html(self, html: str) -> List[Tuple[str, str]]:
        """
        Extract article URLs and titles from HTML with deduplication.

        Args:
            html: Page HTML content

        Returns:
            List of tuples: (url, title) - deduplicated
        """
        soup = BeautifulSoup(html, 'html.parser')
        seen_urls: set[str] = set()
        articles: List[Tuple[str, str]] = []

        # Find all links
        all_links = soup.find_all('a', href=True)

        for link in all_links:
            href = link.get('href', '')

            # Handle relative URLs
            if href.startswith('/'):
                path = href
            elif href.startswith('https://www.metalocus.es'):
                parsed = urlparse(href)
                path = parsed.path
            else:
                continue

            # Remove query params and fragments
            path = path.split('?')[0].split('#')[0]

            # Check if valid article
            if not self._is_valid_article_url(path):
                continue

            # Build full URL
            full_url = f"https://www.metalocus.es{path}"

            # DEDUPLICATION: Skip if already seen
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)

            # Get title from link text
            title = link.get_text(strip=True)

            # If title is empty or too short, try to find from parent
            if not title or len(title) < 5:
                parent = link.find_parent(['article', 'div', 'li'])
                if parent:
                    # Look for heading
                    heading = parent.find(['h1', 'h2', 'h3', 'h4'])
                    if heading:
                        title = heading.get_text(strip=True)

            # If still no title, use URL slug
            if not title or len(title) < 3:
                slug = path.split('/')[-1]
                title = slug.replace('-', ' ').title()

            # Clean title
            title = ' '.join(title.split())[:200]

            if title:
                articles.append((full_url, title))

        return articles

    async def fetch_articles(self, hours: int = 24) -> list[dict]:
        """
        Fetch new articles from Metalocus.

        Workflow:
        1. Load English homepage
        2. Extract all article links matching /en/news/* (with deduplication)
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
                # Step 1: Load Homepage
                # ============================================================
                print(f"[{self.source_id}] Loading English homepage...")
                await page.goto(self.base_url, timeout=self.timeout, wait_until="networkidle")
                await page.wait_for_timeout(2000)

                # Get page HTML
                html = await page.content()

                # ============================================================
                # Step 2: Extract Article Links (with deduplication)
                # ============================================================
                extracted = self._extract_articles_from_html(html)
                print(f"[{self.source_id}] Found {len(extracted)} unique article links")

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
                    title = url_to_title.get(url, url.split('/')[-1].replace('-', ' ').title())

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
custom_scraper_registry.register(MetalocusScraper)


# =============================================================================
# Standalone Test
# =============================================================================

async def test_metalocus_scraper():
    """Test the HTTP pattern scraper."""
    print("=" * 60)
    print("Testing Metalocus HTTP Pattern Scraper")
    print("=" * 60)

    # Show TEST_MODE status
    from storage.article_tracker import ArticleTracker
    print(f"\nTEST_MODE: {ArticleTracker.TEST_MODE}")
    if ArticleTracker.TEST_MODE:
        print("   All articles will appear as 'new' (ignoring database)")
    else:
        print("   Normal mode - filtering seen articles")

    scraper = MetalocusScraper()

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
            stats = await scraper.tracker.get_stats(source_id="metalocus")
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
    asyncio.run(test_metalocus_scraper())