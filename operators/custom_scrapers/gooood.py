# operators/custom_scrapers/gooood.py
"""
Gooood Custom Scraper - Simplified URL Discovery
Scrapes architecture news from Gooood.cn (Chinese architecture magazine)

Site: https://www.gooood.cn/category/type/architecture
Strategy: Extract links matching /*.htm pattern from article grid

Pattern Analysis:
- Article links: /article-name.htm (e.g., /cloud-11-by-snohetta-a49.htm)
- Date visible in article cards: YYYY-MM-DD format
- Hero images: First large image in article (og:image often missing on Chinese sites)

Architecture (Simplified):
- Custom scraper discovers article URLs, titles, dates, and hero images from homepage
- Article tracker handles new/seen filtering (with TEST_MODE support)
- Main pipeline handles: content scraping, AI filtering, summarization

Workflow:
1. Fetch architecture category page HTML
2. Extract all article links matching *.htm pattern
3. Parse dates from article cards (YYYY-MM-DD format)
4. Extract hero image URLs from article cards
5. Use article tracker to filter new URLs
6. Return article dicts for main pipeline

Usage:
    scraper = GoooodScraper()
    articles = await scraper.fetch_articles()
    await scraper.close()
"""

import asyncio
import re
from typing import Optional, List, Tuple
from datetime import datetime, timezone
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from operators.custom_scraper_base import BaseCustomScraper, custom_scraper_registry
from storage.article_tracker import ArticleTracker


class GoooodScraper(BaseCustomScraper):
    """
    Simplified custom scraper for Gooood.
    Extracts article URLs, dates, and hero images from homepage grid.
    """

    source_id = "gooood"
    source_name = "Gooood"
    base_url = "https://www.gooood.cn/category/type/architecture"

    # Configuration
    MAX_ARTICLE_AGE_DAYS = 14
    MAX_NEW_ARTICLES = 10

    # URL pattern for articles (*.htm but not category pages)
    # Matches: /cloud-11-by-snohetta-a49.htm
    # Excludes: /category/*, /tag/*, /company/*
    ARTICLE_PATTERN = re.compile(r'href=["\'](/[a-z0-9-]+\.htm)["\']', re.IGNORECASE)

    # Excluded URL patterns (not articles)
    EXCLUDED_PATTERNS = [
        '/category/',
        '/tag/',
        '/company/',
        '/submissions',
        '/aboutus',
        '/filter/',
        '/country/',
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

    def _is_valid_article_url(self, url: str) -> bool:
        """
        Check if URL is a valid article (not a category or section page).

        Args:
            url: URL path to check

        Returns:
            True if valid article URL
        """
        url_lower = url.lower()

        # Check excluded patterns
        for pattern in self.EXCLUDED_PATTERNS:
            if pattern in url_lower:
                return False

        # Must end with .htm
        if not url_lower.endswith('.htm'):
            return False

        return True

    def _extract_articles_from_html(self, html: str) -> List[Tuple[str, str, Optional[str], Optional[str]]]:
        """
        Extract article data from homepage HTML.

        Parses the article grid to extract:
        - URL
        - Title
        - Date (YYYY-MM-DD format visible in cards)
        - Hero image URL

        Args:
            html: Page HTML content

        Returns:
            List of tuples: (url, title, date, image_url) - date and image_url can be None
        """
        soup = BeautifulSoup(html, 'html.parser')
        articles: List[Tuple[str, str, Optional[str], Optional[str]]] = []
        seen_urls: set[str] = set()

        # Find all links - Gooood uses a grid layout with article links
        # The structure shows links like: href="/cloud-11-by-snohetta-a49.htm"
        all_links = soup.find_all('a', href=True)

        for link in all_links:
            href = link.get('href', '')

            # Check if it's a valid article URL
            if not href.endswith('.htm'):
                continue

            if not self._is_valid_article_url(href):
                continue

            # Build full URL
            full_url = urljoin("https://www.gooood.cn", href)

            # Skip duplicates
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)

            # Get title from link text
            title = link.get_text(strip=True)

            # If title is empty, try to find it from parent or nearby h2/h3
            if not title or len(title) < 5:
                parent = link.find_parent(['div', 'article', 'li'])
                if parent:
                    # Look for heading
                    heading = parent.find(['h1', 'h2', 'h3', 'h4'])
                    if heading:
                        title = heading.get_text(strip=True)

            # Skip if still no title
            if not title or len(title) < 3:
                # Use URL slug as fallback
                title = href.replace('.htm', '').replace('-', ' ').strip('/')
                if not title:
                    continue

            # Find date - look for YYYY-MM-DD pattern in parent container
            date_str = None
            parent = link.find_parent(['div', 'article', 'li'])
            if parent:
                parent_text = parent.get_text()
                # Look for date pattern: 2026-01-19
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', parent_text)
                if date_match:
                    date_str = date_match.group(1)

            # Find hero image - look for img in same container
            image_url = None
            if parent:
                img = parent.find('img')
                if img:
                    # Try various image source attributes
                    image_url = (
                        img.get('src') or
                        img.get('data-src') or
                        img.get('data-lazy-src') or
                        img.get('data-original')
                    )
                    # Resolve relative URL
                    if image_url and not image_url.startswith('http'):
                        image_url = urljoin("https://www.gooood.cn", image_url)

                    # Skip placeholder images
                    if image_url and 'placeholder' in image_url.lower():
                        image_url = None

            # Clean title
            title = ' '.join(title.split())[:200]

            articles.append((full_url, title, date_str, image_url))

        return articles

    def _parse_date_string(self, date_str: str) -> Optional[str]:
        """
        Parse date string to ISO format.

        Args:
            date_str: Date string like "2026-01-19"

        Returns:
            ISO format date string or None
        """
        if not date_str:
            return None

        try:
            # Parse YYYY-MM-DD format
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except ValueError:
            return None

    async def fetch_articles(self, hours: int = 24) -> list[dict]:
        """
        Fetch new articles from Gooood architecture section.

        Workflow:
        1. Load architecture category page
        2. Extract article URLs, titles, dates, and hero images from grid
        3. Filter by date (within MAX_ARTICLE_AGE_DAYS)
        4. Use article tracker to filter new URLs
        5. Return article dicts for main pipeline

        Args:
            hours: Ignored (we use database tracking instead)

        Returns:
            List of article dicts for main pipeline
        """
        print(f"[{self.source_id}] Starting HTML pattern scraping...")

        await self._ensure_tracker()

        try:
            page = await self._create_page()

            try:
                # ============================================================
                # Step 1: Load Page and Extract Articles
                # ============================================================
                print(f"[{self.source_id}] Loading architecture category page...")
                await page.goto(self.base_url, timeout=self.timeout, wait_until="networkidle")
                await page.wait_for_timeout(2000)

                # Get page HTML
                html = await page.content()

                # Extract all article data from grid
                extracted = self._extract_articles_from_html(html)
                print(f"[{self.source_id}] Found {len(extracted)} articles in grid")

                if not extracted:
                    print(f"[{self.source_id}] No articles found")
                    return []

                # Get just the URLs for tracking
                all_urls = [url for url, _, _, _ in extracted]

                # ============================================================
                # Step 2: Filter by Date
                # ============================================================
                date_filtered: List[Tuple[str, str, Optional[str], Optional[str]]] = []
                skipped_old = 0
                skipped_no_date = 0

                current_date = datetime.now(timezone.utc)

                for url, title, date_str, image_url in extracted:
                    if not date_str:
                        # Include articles without dates (will be filtered later if needed)
                        date_filtered.append((url, title, None, image_url))
                        skipped_no_date += 1
                        continue

                    published = self._parse_date_string(date_str)
                    if published:
                        article_date = datetime.fromisoformat(published.replace('Z', '+00:00'))
                        days_old = (current_date - article_date).days

                        if days_old > self.MAX_ARTICLE_AGE_DAYS:
                            skipped_old += 1
                            continue

                    date_filtered.append((url, title, published, image_url))

                print(f"[{self.source_id}] Date filtering:")
                print(f"   Skipped (too old): {skipped_old}")
                print(f"   No date found: {skipped_no_date}")
                print(f"   Passed filter: {len(date_filtered)}")

                if not date_filtered:
                    print(f"[{self.source_id}] No recent articles found")
                    return []

                # ============================================================
                # Step 3: Filter New URLs via Database
                # ============================================================
                if not self.tracker:
                    raise RuntimeError("Article tracker not initialized")

                filtered_urls = [url for url, _, _, _ in date_filtered]
                new_urls = await self.tracker.filter_new_articles(self.source_id, filtered_urls)

                print(f"[{self.source_id}] Database filtering:")
                print(f"   Total after date filter: {len(date_filtered)}")
                print(f"   Previously seen: {len(date_filtered) - len(new_urls)}")
                print(f"   New to process: {len(new_urls)}")

                if not new_urls:
                    print(f"[{self.source_id}] No new articles to process")
                    # Still mark all as seen
                    await self.tracker.mark_as_seen(self.source_id, all_urls)
                    return []

                # ============================================================
                # Step 4: Create Article Dicts and Download Hero Images
                # ============================================================
                new_articles: list[dict] = []
                new_url_set = set(new_urls)
                images_saved = 0

                for url, title, published, image_url in date_filtered:
                    if url not in new_url_set:
                        continue

                    if len(new_articles) >= self.MAX_NEW_ARTICLES:
                        break

                    print(f"\n[{self.source_id}] Processing: {title[:50]}...")

                    # Create article dict
                    article = {
                        "title": self._clean_text(title),
                        "link": url,
                        "guid": url,
                        "published": published,
                        "source_id": self.source_id,
                        "source_name": self.source_name,
                        "custom_scraped": True,
                        "description": "",
                        "full_content": "",
                        "hero_image": None,
                    }

                    # Download and save hero image to R2 if found
                    if image_url:
                        hero_image = await self._download_and_save_hero_image(
                            page=page,
                            image_url=image_url,
                            article=article
                        )
                        if hero_image:
                            article["hero_image"] = hero_image
                            if hero_image.get("r2_path"):
                                images_saved += 1

                    if self._validate_article(article):
                        new_articles.append(article)
                        print(f"[{self.source_id}]    Added to results")

                # ============================================================
                # Step 5: Mark URLs as Seen and Finalize
                # ============================================================
                await self.tracker.mark_as_seen(self.source_id, all_urls)

                # Final Summary
                print(f"\n[{self.source_id}] Processing Summary:")
                print(f"   Articles in grid: {len(extracted)}")
                print(f"   After date filter: {len(date_filtered)}")
                print(f"   New articles: {len(new_urls)}")
                print(f"   Hero images saved to R2: {images_saved}")
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
custom_scraper_registry.register(GoooodScraper)


# =============================================================================
# Standalone Test
# =============================================================================

async def test_gooood_scraper():
    """Test the HTML pattern scraper."""
    print("=" * 60)
    print("Testing Gooood HTML Pattern Scraper")
    print("=" * 60)

    # Show TEST_MODE status
    from storage.article_tracker import ArticleTracker
    print(f"\nTEST_MODE: {ArticleTracker.TEST_MODE}")
    if ArticleTracker.TEST_MODE:
        print("   All articles will appear as 'new' (ignoring database)")
    else:
        print("   Normal mode - filtering seen articles")

    scraper = GoooodScraper()

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
            stats = await scraper.tracker.get_stats(source_id="gooood")
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
                print(f"   Published: {article.get('published', 'No date')}")
                print(f"   Hero Image: {'Yes' if article.get('hero_image') else 'No'}")
        else:
            print("\n4. No new articles (all previously seen)")

        print("\n" + "=" * 60)
        print("Test complete!")
        print("=" * 60)

    finally:
        await scraper.close()


if __name__ == "__main__":
    asyncio.run(test_gooood_scraper())