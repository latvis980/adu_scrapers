# operators/custom_scraper_base.py
"""
Base Custom Scraper Infrastructure
Provides common functionality for custom site scrapers (sites without working RSS feeds).

Architecture:
    - BaseCustomScraper: Abstract base class with common methods
    - Site-specific scrapers inherit and implement fetch_articles()
    - Consistent output format matching RSS fetcher

Usage:
    from operators.custom_scrapers.landezine import LandezineScraper
    scraper = LandezineScraper()
    articles = await scraper.fetch_articles(hours=24)
"""

import asyncio
import os
import re
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Optional, Any, Tuple
from urllib.parse import urljoin, urlparse
from html import unescape

from storage.r2 import R2Storage
import os as os_module

from playwright.async_api import (
    async_playwright,
    Browser,
    Page,
    TimeoutError as PlaywrightTimeoutError
)

class BaseCustomScraper(ABC):
    """
    Abstract base class for custom site scrapers.

    Each source-specific scraper inherits from this and implements:
    - source_id: str
    - source_name: str
    - base_url: str
    - fetch_articles(hours) -> list[dict]
    """

    # Subclasses must define these
    source_id: str
    source_name: str
    base_url: str

    def __init__(self):
        """Initialize the custom scraper."""
        if not all([self.source_id, self.source_name, self.base_url]):
            raise ValueError(
                f"{self.__class__.__name__} must define source_id, source_name, and base_url"
            )

        # Browser settings
        self.browser: Optional[Browser] = None
        self.playwright = None
        self.timeout = 20000  # 20 seconds

        # User-Agent to avoid blocks
        self.user_agent = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )

        print(f"[{self.source_id}] Custom scraper initialized")

    @abstractmethod
    async def fetch_articles(self, hours: int = 24) -> list[dict]:
        """
        Fetch articles from the last N hours.

        Must return list of dicts with structure:
        {
            "title": str,
            "link": str,
            "description": str,
            "published": str (ISO format),
            "guid": str,
            "source_id": str,
            "source_name": str,
            "custom_scraped": True,
            "hero_image": {
                "url": str,
                "width": int or None,
                "height": int or None,
                "source": "scraper"
            } or None
        }

        Args:
            hours: How many hours back to look for articles

        Returns:
            List of article dicts
        """
        pass

    # =========================================================================
    # Browser Management
    # =========================================================================

    async def _initialize_browser(self):
        """Initialize Playwright browser if needed."""
        if self.browser:
            return

        try:
            self.playwright = await async_playwright().start()

            # Railway Browserless configuration (exact pattern from scraper.py)
            browserless_endpoint = (
                os.getenv('BROWSER_PLAYWRIGHT_ENDPOINT_PRIVATE') or
                os.getenv('BROWSER_PLAYWRIGHT_ENDPOINT') or
                os.getenv('BROWSERLESS_URL')
            )
            browserless_token = os.getenv('BROWSER_TOKEN')

            if browserless_endpoint:
                # Connect to Railway Browserless
                connect_url = browserless_endpoint
                if browserless_token and 'token=' not in connect_url:
                    separator = '&' if '?' in connect_url else '?'
                    connect_url = f"{connect_url}{separator}token={browserless_token}"

                self.browser = await self.playwright.chromium.connect(
                    connect_url,
                    timeout=25000
                )
                print(f"[{self.source_id}] Connected to Railway Browserless")
            else:
                # Local Playwright fallback
                self.browser = await self.playwright.chromium.launch(
                    headless=True,
                    args=[
                        "--no-sandbox",
                        "--disable-setuid-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-gpu",
                    ]
                )
                print(f"[{self.source_id}] Using local Playwright")

        except Exception as e:
            print(f"[{self.source_id}] Browser init failed: {e}")
            raise

    async def _create_page(self) -> Page:
        """Create a new browser page with proper configuration."""
        await self._initialize_browser()

        if not self.browser:
            raise RuntimeError("Browser not initialized")

        page = await self.browser.new_page(
            user_agent=self.user_agent,
            viewport={"width": 1280, "height": 800}
        )

        # Block unnecessary resources for speed
        await page.route("**/*", self._block_resources)

        return page

    async def _block_resources(self, route):
        """Block ads, trackers, and unnecessary resources."""
        request = route.request
        resource_type = request.resource_type
        url = request.url.lower()

        # Block by resource type
        blocked_types = ['font', 'media', 'websocket', 'manifest']
        if resource_type in blocked_types:
            await route.abort()
            return

        # Block known ad/tracking domains
        blocked_domains = [
            'google-analytics', 'googletagmanager', 'googlesyndication',
            'doubleclick', 'facebook.com', 'twitter.com',
            'adservice', 'advertising', 'analytics',
        ]

        if any(domain in url for domain in blocked_domains):
            await route.abort()
            return

        await route.continue_()

    async def close(self):
        """Clean shutdown of browser."""
        if self.browser:
            try:
                await self.browser.close()
            except Exception:
                pass

        if self.playwright:
            try:
                await self.playwright.stop()
            except Exception:
                pass

        print(f"[{self.source_id}] Browser closed")

    # =========================================================================
    # Common Helper Methods
    # =========================================================================

    def _parse_date_with_ai(self, article_text: str) -> str | None:
        """
        Use AI to extract publication date from article text.
        Works with any date format and language.

        Args:
            article_text: Article text containing publication date

        Returns:
            ISO format date string (YYYY-MM-DD) or None
        """
        from datetime import datetime
        from prompts.date_extractor import DATE_EXTRACTOR_PROMPT_TEMPLATE, parse_date_response
        from langchain_core.messages import HumanMessage

        if not article_text or len(article_text.strip()) < 10:
            return None

        # Get current date for context
        current_date = datetime.now().strftime("%B %d, %Y")

        try:
            # Truncate article text to first 2000 chars (dates are usually at top)
            text_sample = article_text[:2000]

            # Create prompt
            prompt_text = DATE_EXTRACTOR_PROMPT_TEMPLATE.format_messages(
                current_date=current_date,
                article_text=text_sample
            )

            # Call AI (using vision_model which is already initialized)
            response = self.vision_model.invoke(prompt_text)

            # Parse response
            date_str = parse_date_response(response.content)

            if date_str:
                # Convert to ISO format with timezone
                dt = datetime.strptime(date_str, '%Y-%m-%d')
                from datetime import timezone
                dt = dt.replace(tzinfo=timezone.utc)
                return dt.isoformat()

            return None

        except Exception as e:
            print(f"      ⚠️ Date extraction failed: {e}")
            return None

    def _is_within_timeframe(self, date_string: str, hours: int) -> bool:
        """
        Check if a date is within the specified timeframe.

        Args:
            date_string: ISO format date string
            hours: Hours to look back

        Returns:
            True if within timeframe, False otherwise
        """
        if not date_string:
            return True  # Include if no date (edge case)

        try:
            article_date = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
            cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
            return article_date >= cutoff
        except:
            return True  # Include if parsing fails

    def _clean_text(self, text: str) -> str:
        """
        Clean and normalize text content.

        Args:
            text: Raw text

        Returns:
            Cleaned text
        """
        if not text:
            return ""

        # Decode HTML entities
        text = unescape(text)

        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()

        return text

    def _resolve_url(self, url: str) -> str:
        """
        Resolve relative URLs to absolute.

        Args:
            url: URL (can be relative or absolute)

        Returns:
            Absolute URL
        """
        if not url:
            return ""

        if url.startswith('http'):
            return url

        if url.startswith('//'):
            return 'https:' + url

        return urljoin(self.base_url, url)

    def _extract_hero_image_from_html(self, html: str, base_url: str) -> Optional[dict]:
        """
        Extract hero image from HTML content.

        Looks for:
        - og:image meta tag
        - twitter:image meta tag
        - First large image

        Args:
            html: HTML content
            base_url: Base URL for resolving relative paths

        Returns:
            Dict with url, width, height or None
        """
        # Try og:image
        og_pattern = r'<meta\s+property=["\']og:image["\']\s+content=["\']([^"\']+)["\']'
        match = re.search(og_pattern, html, re.IGNORECASE)
        if match:
            url = self._resolve_url(match.group(1))
            return {
                "url": url,
                "width": None,
                "height": None,
                "source": "scraper"
            }

        # Try twitter:image
        twitter_pattern = r'<meta\s+name=["\']twitter:image["\']\s+content=["\']([^"\']+)["\']'
        match = re.search(twitter_pattern, html, re.IGNORECASE)
        if match:
            url = self._resolve_url(match.group(1))
            return {
                "url": url,
                "width": None,
                "height": None,
                "source": "scraper"
            }

        return None

    # =========================================================================
    # Validation
    # =========================================================================

    def _validate_article(self, article: dict) -> bool:
        """
        Validate that an article has required fields.

        Args:
            article: Article dict

        Returns:
            True if valid, False otherwise
        """
        required_fields = ['title', 'link', 'source_id', 'source_name']

        for field in required_fields:
            if not article.get(field):
                print(f"[{self.source_id}] Invalid article: missing {field}")
                return False

        return True

    # In operators/custom_scraper_base.py

    def _create_minimal_article_dict(
        self,
        title: str,
        link: str,
        published: Optional[str] = None
    ) -> dict:
        """
        Create a MINIMAL article dict for custom scrapers.

        These articles will be processed through main scraper.py pipeline
        for consistent hero image and content extraction.

        Args:
            title: Article title (from homepage)
            link: Article URL
            published: Publication date (ISO format) - extracted from article page

        Returns:
            Minimal article dict (will be enhanced by scraper.py)
        """
        return {
            "title": self._clean_text(title),
            "link": self._resolve_url(link),
            "guid": self._resolve_url(link),
            "published": published,
            "source_id": self.source_id,
            "source_name": self.source_name,
            "custom_scraped": True,
            # These will be filled by scraper.py:
            "description": "",
            "hero_image": None,
            "full_content": ""
        }

    # =========================================================================
    # Testing
    # =========================================================================

    async def test_connection(self) -> bool:
        """
        Test if the scraper can access the site.

        Returns:
            True if successful, False otherwise
        """
        try:
            page = await self._create_page()

            try:
                await page.goto(self.base_url, timeout=self.timeout)
                print(f"[{self.source_id}] Connection test: OK")
                return True
            finally:
                await page.close()

        except Exception as e:
            print(f"[{self.source_id}] Connection test failed: {e}")
            return False

    async def _download_and_save_hero_image(
        self,
        page,
        image_url: str,
        article: dict
    ) -> Optional[dict]:
        """
        Download hero image and save to R2 storage.

        This method handles the full flow:
        1. Download image bytes via Playwright
        2. Upload to R2 storage
        3. Return updated hero_image dict with r2_path and r2_url

        Args:
            page: Playwright page object (for downloading)
            image_url: URL of the image to download
            article: Article dict (needed for slug generation)

        Returns:
            Updated hero_image dict with r2_path/r2_url, or None if failed
        """
        if not image_url:
            return None

        try:
            # Create a new page for downloading the image
            context = page.context
            download_page = await context.new_page()

            try:
                # Download image
                response = await download_page.goto(image_url, timeout=15000)

                if not response or not response.ok:
                    print(f"[{self.source_id}]    Failed to download image: HTTP {response.status if response else 'no response'}")
                    return None

                image_bytes = await response.body()

                if not image_bytes or len(image_bytes) < 1000:
                    print(f"[{self.source_id}]    Image too small or empty: {len(image_bytes) if image_bytes else 0} bytes")
                    return None

                print(f"[{self.source_id}]    Downloaded image: {len(image_bytes)} bytes")

            finally:
                await download_page.close()

            # Initialize R2 and save
            from storage.r2 import R2Storage
            r2 = R2Storage()

            # Create hero_image dict for the article
            hero_image = {
                "url": image_url,
                "width": None,
                "height": None,
                "source": "custom_scraper"
            }

            # Temporarily add hero_image to article for save_hero_image
            article["hero_image"] = hero_image

            # Save to R2
            updated_hero = r2.save_hero_image(
                image_bytes=image_bytes,
                article=article,
                source=self.source_id
            )

            if updated_hero and updated_hero.get("r2_path"):
                print(f"[{self.source_id}]    Saved to R2: {updated_hero.get('r2_path')}")
                return updated_hero
            else:
                print(f"[{self.source_id}]    Failed to save to R2")
                return hero_image  # Return original without r2 info

        except Exception as e:
            print(f"[{self.source_id}]    Hero image error: {e}")
            return None


# =============================================================================
# Custom Scraper Registry
# =============================================================================

class CustomScraperRegistry:
    """
    Registry for managing custom scrapers.

    Usage:
        registry = CustomScraperRegistry()
        registry.register(LandezineScraper)

        scraper = registry.get("landezine")
        articles = await scraper.fetch_articles()
    """

    def __init__(self):
        self._scrapers: dict[str, type[BaseCustomScraper]] = {}

    def register(self, scraper_class: type[BaseCustomScraper]):
        """Register a custom scraper class."""
        if not issubclass(scraper_class, BaseCustomScraper):
            raise ValueError(f"{scraper_class} must inherit from BaseCustomScraper")

        # Get source_id from class
        source_id = getattr(scraper_class, 'source_id', None)
        if not source_id:
            raise ValueError(f"{scraper_class} must define source_id")

        self._scrapers[source_id] = scraper_class
        print(f"[Registry] Registered custom scraper: {source_id}")

    def get(self, source_id: str) -> Optional[BaseCustomScraper]:
        """Get a scraper instance by source_id."""
        scraper_class = self._scrapers.get(source_id)
        if scraper_class:
            return scraper_class()
        return None

    def has_scraper(self, source_id: str) -> bool:
        """Check if a scraper is registered."""
        return source_id in self._scrapers

    def list_scrapers(self) -> list[str]:
        """List all registered scraper source_ids."""
        return list(self._scrapers.keys())


# Global registry instance
custom_scraper_registry = CustomScraperRegistry()