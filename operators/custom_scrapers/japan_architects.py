# operators/custom_scrapers/japan_architects.py
"""
Japan Architects Custom Scraper - HTML Pattern Approach
Scrapes architecture news from Japan-Architects.com

Site: https://www.japan-architects.com/ja
Strategy: Extract links matching /ja/architecture-news/ pattern + AI date extraction

Pattern Analysis:
- Article links: /ja/architecture-news/category/article-name
- Date format in HTML: DD.MM.YYYY (e.g., "28.12.2025") in span with author

HTML Structure:
<div class="grid-item ... news-panel">
    <div class="title ...">
        <a href="/ja/architecture-news/...">Article Title</a>
    </div>
    <span> Author Name | DD.MM.YYYY </span>
</div>

Requirements:
- User-Agent header required to avoid 403
- Uses cloudscraper as fallback for anti-bot protection

Usage:
    scraper = JapanArchitectsScraper()
    articles = await scraper.fetch_articles()
    await scraper.close()
"""

import asyncio
import re
import os
from typing import Optional, List, Tuple, Any
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage
from pydantic import SecretStr

from operators.custom_scraper_base import BaseCustomScraper, custom_scraper_registry
from storage.article_tracker import ArticleTracker

# Try to import cloudscraper for fallback
try:
    import cloudscraper as cloudscraper_module
    CLOUDSCRAPER_AVAILABLE = True
except ImportError:
    cloudscraper_module = None
    CLOUDSCRAPER_AVAILABLE = False
    print("[japan_architects] cloudscraper not installed - fallback disabled")


class JapanArchitectsScraper(BaseCustomScraper):
    """
    HTML pattern-based custom scraper for Japan Architects.
    Extracts article links from HTML, uses AI for date extraction.
    """

    source_id = "japan_architects"
    source_name = "Japan Architects"
    base_url = "https://www.japan-architects.com/ja"

    # Configuration
    MAX_ARTICLE_AGE_DAYS = 14
    MAX_NEW_ARTICLES = 15
    SCRAPER_TIMEOUT = 30000  # 30 seconds for this site

    # URL pattern for architecture news
    ARTICLE_PATTERN = re.compile(r'/ja/architecture-news/[^"\'>\s]+')

    # User agent for requests
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )

    def __init__(self):
        """Initialize scraper with article tracker."""
        super().__init__()
        self.tracker: Optional[ArticleTracker] = None
        self.llm: Optional[ChatOpenAI] = None

    async def _ensure_tracker(self):
        """Ensure article tracker is connected."""
        if not self.tracker:
            self.tracker = ArticleTracker()
            await self.tracker.connect()

    def _ensure_llm(self):
        """Ensure LLM is initialized for date extraction."""
        if not self.llm:
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                raise ValueError("OPENAI_API_KEY not set")

            self.llm = ChatOpenAI(
                model="gpt-4o-mini",
                api_key=SecretStr(api_key),
                temperature=0.1
            )
            print(f"[{self.source_id}] LLM initialized for date extraction")

    def _extract_articles_from_html(self, html: str) -> List[Tuple[str, str, str]]:
        """
        Extract article URLs, titles, and surrounding HTML blocks for date extraction.

        Args:
            html: Page HTML content

        Returns:
            List of tuples: (url, title, html_block)
        """
        soup = BeautifulSoup(html, 'html.parser')
        articles: List[Tuple[str, str, str]] = []
        seen_urls: set[str] = set()

        # Find all news panel blocks
        news_panels = soup.find_all('div', class_='news-panel')

        if not news_panels:
            # Fallback: look for grid-item blocks
            news_panels = soup.find_all('div', class_='grid-item')

        print(f"[{self.source_id}] Found {len(news_panels)} news panels")

        for panel in news_panels:
            try:
                # Find article link (href starting with /ja/architecture-news/)
                article_link = None
                title = None

                # Look for links in the title div first
                title_div = panel.find('div', class_='title')
                if title_div:
                    link = title_div.find('a', href=self.ARTICLE_PATTERN)
                    if link:
                        article_link = link.get('href')
                        title = link.get_text(strip=True)

                # Fallback: find any matching link
                if not article_link:
                    for link in panel.find_all('a', href=True):
                        href = link.get('href', '')
                        if self.ARTICLE_PATTERN.match(href):
                            article_link = href
                            link_text = link.get_text(strip=True)
                            if link_text and not title:
                                title = link_text
                            break

                if not article_link:
                    continue

                # Make URL absolute
                full_url = urljoin("https://www.japan-architects.com", article_link)

                # Skip if already seen
                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)

                # Use URL slug as title if no title found
                if not title:
                    slug = article_link.rstrip('/').split('/')[-1]
                    title = slug.replace('-', ' ').title()

                # Get the HTML block for date extraction
                html_block = str(panel)

                articles.append((full_url, title, html_block))

            except Exception as e:
                print(f"[{self.source_id}] Error parsing panel: {e}")
                continue

        return articles

    def _extract_date_with_ai(self, html_block: str, title: str) -> Optional[str]:
        """
        Use AI to extract date from HTML block.

        The date is typically in format: "Author Name | DD.MM.YYYY"

        Args:
            html_block: HTML content of the article block
            title: Article title for context

        Returns:
            ISO format date string or None
        """
        self._ensure_llm()

        if not self.llm:
            return None

        # Clean up HTML for AI
        soup = BeautifulSoup(html_block, 'html.parser')
        text_content = soup.get_text(separator=' ', strip=True)

        prompt = f"""Extract the publication date from this article block.

Article title: {title}

HTML block text:
{text_content[:1000]}

The date format is typically DD.MM.YYYY (European format), appearing after the author name with a "|" separator.
For example: "Akio Nakasa | 28.12.2025"

Today's date is: {datetime.now().strftime("%d.%m.%Y")}

Respond with ONLY the date in ISO format (YYYY-MM-DD) or NONE if no date found.
Do not use emoji."""

        try:
            response = self.llm.invoke([HumanMessage(content=prompt)])
            response_text = str(response.content).strip()

            if response_text.upper() == "NONE" or "NONE" in response_text.upper():
                return None

            # Extract ISO format date
            iso_pattern = r'(\d{4}-\d{2}-\d{2})'
            match = re.search(iso_pattern, response_text)

            if match:
                date_str = match.group(1)
                # Validate it's a real date
                try:
                    dt = datetime.strptime(date_str, '%Y-%m-%d')
                    dt = dt.replace(tzinfo=timezone.utc)
                    return dt.isoformat()
                except ValueError:
                    return None

            return None

        except Exception as e:
            print(f"[{self.source_id}] AI date extraction error: {e}")
            return None

    def _is_within_age_limit(self, date_iso: Optional[str]) -> bool:
        """Check if article date is within MAX_ARTICLE_AGE_DAYS."""
        if not date_iso:
            return True  # Include if no date

        try:
            article_date = datetime.fromisoformat(date_iso.replace('Z', '+00:00'))
            cutoff = datetime.now(timezone.utc) - timedelta(days=self.MAX_ARTICLE_AGE_DAYS)
            return article_date >= cutoff
        except Exception:
            return True

    def _fetch_with_cloudscraper(self) -> Optional[str]:
        """
        Fallback method using cloudscraper to bypass anti-bot protection.

        Returns:
            HTML content or None if failed
        """
        if not CLOUDSCRAPER_AVAILABLE or cloudscraper_module is None:
            print(f"[{self.source_id}] cloudscraper not available")
            return None

        print(f"[{self.source_id}] Trying cloudscraper fallback...")

        try:
            scraper = cloudscraper_module.create_scraper(
                browser={
                    'browser': 'chrome',
                    'platform': 'darwin',
                    'mobile': False
                },
                delay=5
            )

            # Set headers
            headers = {
                'User-Agent': self.USER_AGENT,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5,ja;q=0.3',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }

            response = scraper.get(
                self.base_url,
                headers=headers,
                timeout=30
            )

            if response.status_code == 200:
                print(f"[{self.source_id}] cloudscraper success - got {len(response.text)} bytes")
                return response.text
            else:
                print(f"[{self.source_id}] cloudscraper failed with status {response.status_code}")
                return None

        except Exception as e:
            print(f"[{self.source_id}] cloudscraper error: {e}")
            return None

    async def fetch_articles(self, hours: int = 24) -> list[dict]:
        """
        Fetch new articles from Japan Architects.

        Workflow:
        1. Load homepage with User-Agent header (try browser first, cloudscraper fallback)
        2. Extract all /ja/architecture-news/ links + HTML blocks
        3. Check database for new URLs
        4. For new articles: use AI to extract date from HTML block
        5. Filter by date (within MAX_ARTICLE_AGE_DAYS)
        6. Return minimal article dicts for main pipeline

        Args:
            hours: Ignored (we use database tracking instead)

        Returns:
            List of article dicts for main pipeline
        """
        print(f"\n[{self.source_id}] Starting HTML pattern scraping...")
        print(f"   URL: {self.base_url}")

        await self._ensure_tracker()
        self._ensure_llm()

        html: Optional[str] = None
        browser_success = False

        # ============================================================
        # Step 1: Load Homepage (Browser first, cloudscraper fallback)
        # ============================================================
        try:
            page = await self._create_page()

            # Set comprehensive headers
            await page.set_extra_http_headers({
                "User-Agent": self.USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5,ja;q=0.3",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            })

            try:
                print(f"[{self.source_id}] Loading homepage with browser...")

                # Use domcontentloaded instead of networkidle (faster, avoids timeout)
                await page.goto(
                    self.base_url,
                    timeout=self.SCRAPER_TIMEOUT,
                    wait_until="domcontentloaded"
                )

                # Wait for content to render
                await page.wait_for_timeout(3000)

                # Get page HTML
                html = await page.content()
                browser_success = True
                print(f"[{self.source_id}] Browser success - got {len(html)} bytes")

            except Exception as browser_error:
                print(f"[{self.source_id}] Browser failed: {browser_error}")

            finally:
                await page.close()

        except Exception as e:
            print(f"[{self.source_id}] Browser initialization failed: {e}")

        # Try cloudscraper fallback if browser failed
        if not browser_success or not html:
            html = self._fetch_with_cloudscraper()

        # Check if we have HTML content
        if not html:
            print(f"[{self.source_id}] Failed to fetch page with both browser and cloudscraper")
            return []

        try:
            # ============================================================
            # Step 2: Extract Articles from HTML
            # ============================================================
            print(f"[{self.source_id}] Extracting articles from HTML...")
            extracted = self._extract_articles_from_html(html)

            print(f"[{self.source_id}] Found {len(extracted)} articles matching /ja/architecture-news/ pattern")

            if not extracted:
                print(f"[{self.source_id}] No articles found")
                return []

            # ============================================================
            # Step 3: Check Database for New URLs
            # ============================================================
            if not self.tracker:
                raise RuntimeError("Article tracker not initialized")

            # Get all URLs for tracking
            all_urls = [url for url, _, _ in extracted]

            # Build lookup for title and html_block by URL
            url_to_data: dict[str, Tuple[str, str]] = {
                url: (title, html_block) for url, title, html_block in extracted
            }

            # Use filter_new_articles to get only new URLs
            new_urls = await self.tracker.filter_new_articles(self.source_id, all_urls)

            print(f"[{self.source_id}] Database check:")
            print(f"   Total extracted: {len(extracted)}")
            print(f"   Already seen: {len(extracted) - len(new_urls)}")
            print(f"   New articles: {len(new_urls)}")

            # Mark all URLs as seen
            await self.tracker.mark_as_seen(self.source_id, all_urls)

            if not new_urls:
                print(f"[{self.source_id}] No new articles to process")
                return []

            # ============================================================
            # Step 4: Extract Dates and Build Results
            # ============================================================
            print(f"\n[{self.source_id}] Extracting dates with AI...")
            new_articles: list[dict] = []
            skipped_old = 0

            for url in new_urls[:self.MAX_NEW_ARTICLES]:
                title, html_block = url_to_data[url]
                print(f"\n   Processing: {title[:50]}...")

                # Extract date using AI
                date_iso = self._extract_date_with_ai(html_block, title)

                if date_iso:
                    print(f"      Date: {date_iso[:10]}")
                else:
                    print(f"      Date: Not found")

                # Check date limit
                if not self._is_within_age_limit(date_iso):
                    print(f"      Skipped (too old)")
                    skipped_old += 1
                    continue

                # Build article dict
                article = {
                    'title': title,
                    'link': url,
                    'source_id': self.source_id,
                }

                if date_iso:
                    article['published'] = date_iso

                new_articles.append(article)

                print(f"      Added")

            # ============================================================
            # Step 5: Final Summary
            # ============================================================
            print(f"\n[{self.source_id}] Processing Summary:")
            print(f"   Articles found: {len(extracted)}")
            print(f"   New articles: {len(new_urls)}")
            print(f"   Skipped (too old): {skipped_old}")
            print(f"   Successfully scraped: {len(new_articles)}")

            return new_articles

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
custom_scraper_registry.register(JapanArchitectsScraper)


# =============================================================================
# Standalone Test
# =============================================================================

async def test_japan_architects_scraper():
    """Test the HTML pattern scraper."""
    print("=" * 60)
    print("Testing Japan Architects HTML Pattern Scraper")
    print("=" * 60)

    scraper = JapanArchitectsScraper()

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
            stats = await scraper.tracker.get_stats(source_id="japan_architects")
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
        else:
            print("\n4. No new articles (all previously seen)")

        print("\n" + "=" * 60)
        print("Test complete!")
        print("=" * 60)

    finally:
        await scraper.close()


if __name__ == "__main__":
    asyncio.run(test_japan_architects_scraper())