# operators/custom_scrapers/bauwelt.py
"""
Bauwelt Custom Scraper - Simplified URL Discovery
Scrapes architecture news from Bauwelt (German architecture magazine)

Site: https://www.bauwelt.de/rubriken/bauten/standard_index_2073531.html
Strategy: Extract links matching /rubriken/bauten/ pattern, use AI to filter real articles

Architecture (Simplified):
- Custom scraper ONLY discovers article URLs from homepage
- Article tracker handles new/seen filtering (with TEST_MODE support)
- Main pipeline handles: content scraping, date extraction, AI filtering

Workflow:
1. Fetch page HTML
2. Extract all links matching /rubriken/bauten/ pattern
3. Use AI to filter: keep article links, exclude index/category pages
4. Use article tracker to filter new URLs (respects TEST_MODE)
5. Return minimal article dicts for main pipeline

Usage:
    scraper = BauweltScraper()
    articles = await scraper.fetch_articles()
    await scraper.close()
"""

import asyncio
import re
import os
from typing import Optional, List
from urllib.parse import urljoin

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage
from pydantic import SecretStr

from operators.custom_scraper_base import BaseCustomScraper, custom_scraper_registry
from storage.article_tracker import ArticleTracker


class BauweltScraper(BaseCustomScraper):
    """
    Simplified custom scraper for Bauwelt.
    Only discovers article URLs - main pipeline handles the rest.
    """

    source_id = "bauwelt"
    source_name = "Bauwelt"
    base_url = "https://www.bauwelt.de/rubriken/bauten/standard_index_2073531.html"

    # Configuration
    MAX_NEW_ARTICLES = 10

    # URL pattern for buildings section
    ARTICLE_PATTERN = re.compile(r'/rubriken/bauten/[^"\'>\s]+\.html')

    def __init__(self):
        """Initialize scraper with article tracker and LLM."""
        super().__init__()
        self.tracker: Optional[ArticleTracker] = None
        self.llm: Optional[ChatOpenAI] = None

    async def _ensure_tracker(self):
        """Ensure article tracker is connected."""
        if not self.tracker:
            self.tracker = ArticleTracker()
            await self.tracker.connect()

    def _ensure_llm(self):
        """Ensure LLM is initialized."""
        if not self.llm:
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                raise ValueError("OPENAI_API_KEY not set")

            self.llm = ChatOpenAI(
                model="gpt-4o-mini",
                api_key=SecretStr(api_key),
                temperature=0.1
            )
            print(f"[{self.source_id}] LLM initialized")

    def _extract_article_links(self, html: str) -> List[str]:
        """
        Extract all potential article links from HTML.

        Finds links matching /rubriken/bauten/*.html pattern.

        Args:
            html: Page HTML content

        Returns:
            List of unique URLs (absolute)
        """
        # Find all matching hrefs
        matches = self.ARTICLE_PATTERN.findall(html)

        # Convert to absolute URLs and deduplicate
        urls: set[str] = set()
        for path in matches:
            full_url = urljoin("https://www.bauwelt.de", path)
            urls.add(full_url)

        return list(urls)

    async def _filter_article_urls_with_ai(self, urls: List[str]) -> List[str]:
        """
        Use AI to filter real article URLs from index/category pages.

        Args:
            urls: List of URLs to filter

        Returns:
            List of valid article URLs
        """
        self._ensure_llm()

        if not urls or not self.llm:
            return []

        # Create prompt for AI filtering
        urls_text = "\n".join([f"{i+1}. {url}" for i, url in enumerate(urls)])

        prompt = f"""Analyze these URLs from Bauwelt (German architecture magazine) and identify which are REAL ARTICLE pages.

URLs:
{urls_text}

REAL ARTICLES have:
- Descriptive names with project/location/architect: /rubriken/bauten/Jenaplansschule-am-Hartwege-Weimar-4330561.html
- End with a numeric ID before .html

EXCLUDE (not articles):
- Index pages with "standard_index" in URL
- Category/navigation pages

Respond with ONLY the numbers of real articles, comma-separated.
Example: 1, 3, 5, 7

If no real articles found, respond: NONE

Do not use any emoji in your response."""

        try:
            response = self.llm.invoke([HumanMessage(content=prompt)])
            response_text = str(response.content).strip()

            if response_text.upper() == "NONE":
                return []

            # Parse response - extract numbers
            numbers = re.findall(r'\d+', response_text)

            # Convert to URLs (1-indexed in prompt)
            valid_urls: List[str] = []
            for num_str in numbers:
                idx = int(num_str) - 1
                if 0 <= idx < len(urls):
                    valid_urls.append(urls[idx])

            return valid_urls

        except Exception as e:
            print(f"[{self.source_id}] AI filtering error: {e}")
            return []

    async def fetch_articles(self, hours: int = 24) -> list[dict]:
        """
        Fetch new articles from Bauwelt buildings section.

        Simplified workflow:
        1. Load page and extract all /rubriken/bauten/ links
        2. Use AI to filter real articles from index pages
        3. Use article tracker to filter new URLs (respects TEST_MODE)
        4. Return minimal article dicts for main pipeline

        Note: Date extraction and content scraping handled by main pipeline.

        Args:
            hours: Ignored (we use database tracking instead)

        Returns:
            List of minimal article dicts for main pipeline
        """
        print(f"[{self.source_id}] Starting HTML pattern scraping...")

        await self._ensure_tracker()
        self._ensure_llm()

        try:
            page = await self._create_page()

            try:
                # ============================================================
                # Step 1: Load Page and Extract Links
                # ============================================================
                print(f"[{self.source_id}] Loading buildings section...")
                await page.goto(self.base_url, timeout=self.timeout, wait_until="networkidle")
                await page.wait_for_timeout(2000)

                # Get page HTML
                html = await page.content()

                # Extract all potential article links
                all_links = self._extract_article_links(html)
                print(f"[{self.source_id}] Found {len(all_links)} links matching /rubriken/bauten/ pattern")

                if not all_links:
                    print(f"[{self.source_id}] No links found")
                    return []

                # ============================================================
                # Step 2: AI Filter - Real Articles vs Index Pages
                # ============================================================
                print(f"[{self.source_id}] Filtering with AI...")
                article_urls = await self._filter_article_urls_with_ai(all_links)

                print(f"[{self.source_id}] AI identified {len(article_urls)} real articles")

                if not article_urls:
                    print(f"[{self.source_id}] No real articles found after AI filtering")
                    return []

                # ============================================================
                # Step 3: Filter New URLs via Article Tracker
                # ============================================================
                if not self.tracker:
                    raise RuntimeError("Article tracker not initialized")

                new_urls = await self.tracker.filter_new_articles(self.source_id, article_urls)

                print(f"[{self.source_id}] {len(new_urls)} new articles (not in database)")

                if not new_urls:
                    print(f"[{self.source_id}] No new articles to process")
                    return []

                # Limit to max new articles
                urls_to_process = new_urls[:self.MAX_NEW_ARTICLES]

                # ============================================================
                # Step 4: Create Minimal Article Dicts
                # ============================================================
                # Main pipeline will handle: content scraping, date extraction, AI filtering
                new_articles: list[dict] = []

                for url in urls_to_process:
                    # Extract title from URL for initial display
                    url_title = url.split("/")[-1].replace("-", " ").replace(".html", "")
                    # Remove trailing numbers (article IDs)
                    url_title = re.sub(r'\s+\d+$', '', url_title)

                    article = self._create_minimal_article_dict(
                        title=url_title,  # Will be replaced by main pipeline
                        link=url,
                        published=None  # Will be extracted by main pipeline
                    )

                    if self._validate_article(article):
                        new_articles.append(article)

                # ============================================================
                # Step 5: Mark URLs as Seen and Finalize
                # ============================================================
                await self.tracker.mark_as_seen(self.source_id, article_urls)

                # Final Summary
                print(f"\n[{self.source_id}] Processing Summary:")
                print(f"   Links found: {len(all_links)}")
                print(f"   Real articles (AI filter): {len(article_urls)}")
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
custom_scraper_registry.register(BauweltScraper)


# =============================================================================
# Standalone Test
# =============================================================================

async def test_bauwelt_scraper():
    """Test the HTML pattern scraper."""
    print("=" * 60)
    print("Testing Bauwelt HTML Pattern Scraper")
    print("=" * 60)

    # Show TEST_MODE status
    from storage.article_tracker import ArticleTracker
    print(f"\nTEST_MODE: {ArticleTracker.TEST_MODE}")
    if ArticleTracker.TEST_MODE:
        print("   All articles will appear as 'new' (ignoring database)")
    else:
        print("   Normal mode - filtering seen articles")

    scraper = BauweltScraper()

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
            stats = await scraper.tracker.get_stats(source_id="bauwelt")
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
    asyncio.run(test_bauwelt_scraper())