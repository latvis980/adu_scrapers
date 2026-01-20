# operators/custom_scrapers/domus.py
"""
Domus Custom Scraper - Simplified URL Discovery
Scrapes architecture news from Domus (Italian architecture magazine)

Site: https://www.domusweb.it/en.html
Strategy: Extract links matching /en/architecture/ and /en/news/ patterns

Architecture (Simplified):
- Custom scraper ONLY discovers article URLs from homepage
- Article tracker handles new/seen filtering (with TEST_MODE support)
- Main pipeline handles: content scraping, date extraction, AI filtering

Workflow:
1. Fetch page HTML
2. Extract all links matching article patterns
3. Use article tracker to filter new URLs (respects TEST_MODE)
4. Return minimal article dicts for main pipeline

Usage:
    scraper = DomusScraper()
    articles = await scraper.fetch_articles()
    await scraper.close()
"""

import asyncio
import re
from typing import Optional, List
from urllib.parse import urljoin

from operators.custom_scraper_base import BaseCustomScraper, custom_scraper_registry
from storage.article_tracker import ArticleTracker


class DomusScraper(BaseCustomScraper):
    """
    Simplified custom scraper for Domus.
    Only discovers article URLs - main pipeline handles the rest.
    """

    source_id = "domus"
    source_name = "Domus"
    base_url = "https://www.domusweb.it/en.html"

    # Configuration
    MAX_NEW_ARTICLES = 10

    # URL patterns for English articles (architecture and news sections)
    # Matches: /en/architecture/2026/01/16/article-name.html
    # Matches: /en/news/2026/01/15/article-name.html
    # Excludes: /en/architecture.html (section index pages)
    ARTICLE_PATTERNS = [
        re.compile(r'/en/architecture/\d{4}/\d{2}/\d{2}/[^"\'>\s]+\.html'),
        re.compile(r'/en/news/\d{4}/\d{2}/\d{2}/[^"\'>\s]+\.html'),
        re.compile(r'/en/design/\d{4}/\d{2}/\d{2}/[^"\'>\s]+\.html'),
        re.compile(r'/en/art/\d{4}/\d{2}/\d{2}/[^"\'>\s]+\.html'),
        re.compile(r'/en/interiors/\d{4}/\d{2}/\d{2}/[^"\'>\s]+\.html'),
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

    def _extract_article_links(self, html: str) -> List[str]:
        """
        Extract all potential article links from HTML.

        Finds links matching /en/architecture|news|design|art|interiors/YYYY/MM/DD/*.html patterns.

        Args:
            html: Page HTML content

        Returns:
            List of unique URLs (absolute)
        """
        urls: set[str] = set()

        for pattern in self.ARTICLE_PATTERNS:
            matches = pattern.findall(html)
            for path in matches:
                full_url = urljoin("https://www.domusweb.it", path)
                urls.add(full_url)

        return list(urls)

    async def fetch_articles(self, hours: int = 24) -> list[dict]:
        """
        Fetch new articles from Domus.

        Simplified workflow:
        1. Load page and extract all article links
        2. Use article tracker to filter new URLs (respects TEST_MODE)
        3. Return minimal article dicts for main pipeline

        Note: Date extraction and content scraping handled by main pipeline.

        Args:
            hours: Ignored (we use database tracking instead)

        Returns:
            List of minimal article dicts for main pipeline
        """
        print(f"[{self.source_id}] Starting HTML pattern scraping...")

        await self._ensure_tracker()

        try:
            page = await self._create_page()

            try:
                # ============================================================
                # Step 1: Load Page and Extract Links
                # ============================================================
                print(f"[{self.source_id}] Loading homepage...")
                await page.goto(self.base_url, timeout=self.timeout, wait_until="networkidle")
                await page.wait_for_timeout(2000)

                # Get page HTML
                html = await page.content()

                # Extract all potential article links
                all_links = self._extract_article_links(html)
                print(f"[{self.source_id}] Found {len(all_links)} links matching article patterns")

                if not all_links:
                    print(f"[{self.source_id}] No links found")
                    return []

                # ============================================================
                # Step 2: Filter New URLs via Database
                # ============================================================
                if not self.tracker:
                    raise RuntimeError("Article tracker not initialized")

                new_urls = await self.tracker.filter_new_articles(self.source_id, all_links)

                print(f"[{self.source_id}] URL breakdown:")
                print(f"   Total found: {len(all_links)}")
                print(f"   Previously seen: {len(all_links) - len(new_urls)}")
                print(f"   New to process: {len(new_urls)}")

                if not new_urls:
                    print(f"[{self.source_id}] No new articles to process")
                    return []

                # Limit to max new articles
                urls_to_process = new_urls[:self.MAX_NEW_ARTICLES]

                # ============================================================
                # Step 3: Create Minimal Article Dicts
                # ============================================================
                # Main pipeline will handle: content scraping, date extraction, AI filtering
                new_articles: list[dict] = []

                for url in urls_to_process:
                    # Extract title from URL for initial display
                    # e.g., /en/architecture/2026/01/16/stadio-populous-prince-moulay-abdellah-rabat-messico.html
                    url_title = url.split("/")[-1].replace("-", " ").replace(".html", "")

                    article = self._create_minimal_article_dict(
                        title=url_title,  # Will be replaced by main pipeline
                        link=url,
                        published=None  # Will be extracted by main pipeline
                    )

                    if self._validate_article(article):
                        new_articles.append(article)

                # ============================================================
                # Step 4: Mark URLs as Seen and Finalize
                # ============================================================
                # Mark all discovered article URLs as seen
                await self.tracker.mark_as_seen(self.source_id, all_links)

                # Final Summary
                print(f"\n[{self.source_id}] Processing Summary:")
                print(f"   Links found: {len(all_links)}")
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
custom_scraper_registry.register(DomusScraper)


# =============================================================================
# Standalone Test
# =============================================================================

async def test_domus_scraper():
    """Test the HTML pattern scraper."""
    print("=" * 60)
    print("Testing Domus HTML Pattern Scraper")
    print("=" * 60)

    # Show TEST_MODE status
    from storage.article_tracker import ArticleTracker
    print(f"\nTEST_MODE: {ArticleTracker.TEST_MODE}")
    if ArticleTracker.TEST_MODE:
        print("   All articles will appear as 'new' (ignoring database)")
    else:
        print("   Normal mode - filtering seen articles")

    scraper = DomusScraper()

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
            stats = await scraper.tracker.get_stats(source_id="domus")
            print(f"   Total articles in database: {stats['total_articles']}")
            if stats['oldest_seen']:
                print(f"   Oldest: {stats['oldest_seen']}")
            if stats['newest_seen']:
                print(f"   Newest: {stats['newest_seen']}")

        # Fetch new articles
        print("\n3. Running HTML pattern scraping...")
        articles = await scraper.fetch_articles(hours=24)

        print(f"\n   Found {len(articles)} NEW articles")

        # Display articles
        if articles:
            print("\n4. New articles:")
            for i, article in enumerate(articles, 1):
                print(f"\n   --- Article {i} ---")
                print(f"   Title: {article['title'][:60]}...")
                print(f"   Link: {article['link']}")
                print(f"   Published: {article.get('published', 'No date (will be extracted by pipeline)')}")
        else:
            print("\n4. No new articles (all previously seen)")

        print("\n" + "=" * 60)
        print("Test complete!")
        print("=" * 60)

    finally:
        await scraper.close()


if __name__ == "__main__":
    asyncio.run(test_domus_scraper())