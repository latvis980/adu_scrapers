# operators/custom_scrapers/archiposition.py
"""
Archiposition Custom Scraper - HTML Pattern Based

Scrapes www.archiposition.com project pages using URL pattern detection.

This scraper:
1. Loads the projects category page (/category/1675)
2. Finds all /items/{id} links via HTML parsing
3. Extracts hero images from the category grid (thumbnails)
4. Filters out section pages (e.g., /items/competition)
5. Tracks seen URLs in PostgreSQL database
6. Visits new article pages to extract dates only (images already from grid)
7. Downloads and saves hero images to R2 storage

URL Pattern: /items/{alphanumeric_id}
- Valid: /items/8def04b14c, /items/20260115074501
- Invalid: /items/competition, /items/spaceresearch (section pages)

FIXED: Now extracts hero images from category page grid (like Gooood)
       instead of trying to find og:image on article pages.
"""

import asyncio
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin
from bs4 import BeautifulSoup

from operators.custom_scraper_base import BaseCustomScraper, custom_scraper_registry
from storage.article_tracker import ArticleTracker


class ArchipositionScraper(BaseCustomScraper):
    """
    Archiposition custom scraper using HTML pattern detection.

    Finds articles by parsing HTML for /items/ links rather than
    relying on visual detection.
    """

    # Base configuration
    source_id = "archiposition"
    source_name = "Archiposition"
    base_url = "https://www.archiposition.com"

    # Projects category page (most reliable source)
    CATEGORY_URL = "https://www.archiposition.com/category/1675"

    # Pattern for valid article URLs
    # Matches: /items/8def04b14c, /items/20260115074501
    ARTICLE_PATTERN = re.compile(r'/items/([a-zA-Z0-9]+)')

    # Known section slugs to exclude (not articles)
    SECTION_SLUGS = {
        'competition', 'spaceresearch', 'customize', 'jobservice',
        '20180525080701', '20180530191342', '20180527092142',
        '20180528083806', '20180527092602'
    }

    # Maximum article age in days (for filtering old articles)
    MAX_ARTICLE_AGE_DAYS = 14

    # Maximum new articles to process per run
    MAX_NEW_ARTICLES = 10

    def __init__(self) -> None:
        """Initialize the scraper."""
        super().__init__()
        self.tracker: Optional[ArticleTracker] = None

    async def _ensure_tracker(self) -> None:
        """Ensure article tracker is initialized."""
        if self.tracker is None:
            self.tracker = ArticleTracker()
            await self.tracker.connect()

    def _is_valid_article_slug(self, slug: str) -> bool:
        """
        Check if a slug is a valid article (not a section page).

        Args:
            slug: The URL slug after /items/

        Returns:
            True if it's an article, False if it's a section page
        """
        # Skip known section slugs
        if slug.lower() in self.SECTION_SLUGS:
            return False

        # Valid articles have alphanumeric slugs, often:
        # - Short hex IDs like "8def04b14c"
        # - Timestamp-based like "20260115074501"
        # Section pages often have descriptive names

        # If it's a date-like slug (all digits, 14 chars), it's likely an article
        if slug.isdigit() and len(slug) >= 10:
            return True

        # If it's a short alphanumeric ID (8-12 chars), it's likely an article
        if len(slug) <= 12 and slug.isalnum():
            return True

        # If it contains only lowercase letters and is long, might be a section
        if slug.islower() and slug.isalpha() and len(slug) > 8:
            return False

        # Default to accepting it
        return True

    def _extract_article_links(self, html: str) -> List[Tuple[str, str, Optional[str]]]:
        """
        Extract all article links AND hero images from category page HTML.

        FIXED: Now also extracts thumbnail images from the grid cards.

        Archiposition structure:
        <a href="/items/8def04b14c">
            <img src="https://image.archiposition.com/2026/01/xxx.png?...">
            ...title text...
        </a>

        Args:
            html: Page HTML content

        Returns:
            List of tuples: (url, title, image_url) - image_url can be None
        """
        soup = BeautifulSoup(html, 'html.parser')
        articles: List[Tuple[str, str, Optional[str]]] = []
        seen_urls: set[str] = set()

        # Find all links with /items/ pattern
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            match = self.ARTICLE_PATTERN.search(href)

            if not match:
                continue

            slug = match.group(1)

            # Skip section URLs
            if not self._is_valid_article_slug(slug):
                continue

            # Build full URL
            if href.startswith('/'):
                full_url = urljoin("https://www.archiposition.com", href)
            else:
                full_url = href

            # Skip duplicates
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)

            # Get title from link text or nearby elements
            title = link.get_text(strip=True)

            # If link text is empty (image link), look for title in parent
            if not title or len(title) < 3:
                parent = link.find_parent(['div', 'article', 'li'])
                if parent:
                    # Look for heading or title class
                    title_elem = parent.find(['h1', 'h2', 'h3', 'h4', '.title', '.name'])
                    if title_elem:
                        title = title_elem.get_text(strip=True)
                    else:
                        # Get first substantial text
                        title = parent.get_text(strip=True)[:100]

            # Fallback: use slug as title
            if not title or len(title) < 3:
                title = slug

            # Clean up title (remove extra whitespace)
            title = ' '.join(title.split())[:150]

            # ============================================================
            # FIXED: Extract hero image from the link or its parent container
            # ============================================================
            image_url: Optional[str] = None

            # First, try to find img directly inside the link
            img = link.find('img')
            if img:
                image_url = (
                    img.get('src') or
                    img.get('data-src') or
                    img.get('data-lazy-src') or
                    img.get('data-original')
                )

            # If not found, look in parent container
            if not image_url:
                parent = link.find_parent(['div', 'article', 'li', 'figure'])
                if parent:
                    img = parent.find('img')
                    if img:
                        image_url = (
                            img.get('src') or
                            img.get('data-src') or
                            img.get('data-lazy-src') or
                            img.get('data-original')
                        )

            # Clean up image URL
            if image_url:
                # Skip placeholder/logo images
                if 'placeholder' in image_url.lower() or 'logo' in image_url.lower():
                    image_url = None
                # Skip staticimage (logos, icons)
                elif 'staticimage.archiposition.com' in image_url:
                    image_url = None
                # Make URL absolute
                elif not image_url.startswith('http'):
                    image_url = urljoin("https://www.archiposition.com", image_url)

                # Remove resize parameters to get full quality image
                # Original: https://image.archiposition.com/2026/01/xxx.png?x-oss-process=image/resize,m_fill,w_917,h_600
                # Clean:    https://image.archiposition.com/2026/01/xxx.png
                if image_url and '?x-oss-process=' in image_url:
                    image_url = image_url.split('?x-oss-process=')[0]

            articles.append((full_url, title, image_url))

        return articles

    async def _get_article_date(self, page: Any, url: str) -> Optional[str]:
        """
        Visit article page and extract publication date only.

        SIMPLIFIED: No longer extracts hero image (now from category grid).

        Args:
            page: Playwright page
            url: Article URL

        Returns:
            ISO date string or None
        """
        try:
            # Navigate to article page
            response = await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=15000
            )

            if not response or not response.ok:
                return None

            # Wait a bit for content to render
            await asyncio.sleep(0.5)

            # Extract date using JavaScript (raw string for regex)
            date_iso = await page.evaluate(r"""
                () => {
                    let dateStr = null;

                    // Pattern 1: YYYY.MM.DD HH:MM format (common in Chinese sites)
                    const datePattern1 = /(\d{4})\.(\d{1,2})\.(\d{1,2})\s*(\d{1,2}):(\d{2})/;

                    // Pattern 2: YYYY-MM-DD format
                    const datePattern2 = /(\d{4})-(\d{1,2})-(\d{1,2})/;

                    // Pattern 3: Chinese date format
                    const datePattern3 = /(\d{4})年(\d{1,2})月(\d{1,2})日/;

                    // Look for date in meta tags first
                    const metaSelectors = [
                        'meta[property="article:published_time"]',
                        'meta[name="pubdate"]',
                        'meta[name="publishdate"]',
                        'meta[itemprop="datePublished"]'
                    ];

                    for (const selector of metaSelectors) {
                        const meta = document.querySelector(selector);
                        if (meta && meta.content) {
                            try {
                                const d = new Date(meta.content);
                                if (!isNaN(d.getTime())) {
                                    return d.toISOString();
                                }
                            } catch (e) {}
                        }
                    }

                    // If no meta date, search in page text
                    const textContent = document.body.innerText;

                    let match = textContent.match(datePattern1);
                    if (match) {
                        const [_, y, m, d, h, min] = match;
                        const date = new Date(y, m - 1, d, h, min);
                        if (!isNaN(date.getTime())) {
                            return date.toISOString();
                        }
                    }

                    match = textContent.match(datePattern2);
                    if (match) {
                        const [_, y, m, d] = match;
                        const date = new Date(y, m - 1, d);
                        if (!isNaN(date.getTime())) {
                            return date.toISOString();
                        }
                    }

                    match = textContent.match(datePattern3);
                    if (match) {
                        const [_, y, m, d] = match;
                        const date = new Date(y, m - 1, d);
                        if (!isNaN(date.getTime())) {
                            return date.toISOString();
                        }
                    }

                    return null;
                }
            """)

            # Parse and validate the date
            if date_iso:
                try:
                    parsed = datetime.fromisoformat(date_iso.replace('Z', '+00:00'))
                    date_iso = parsed.isoformat()
                except ValueError:
                    # Try parsing common formats
                    for fmt in ['%Y-%m-%d', '%Y.%m.%d', '%Y/%m/%d']:
                        try:
                            parsed = datetime.strptime(date_iso[:10], fmt)
                            date_iso = parsed.isoformat()
                            break
                        except ValueError:
                            continue

            return date_iso

        except Exception as e:
            print(f"[{self.source_id}] Date extraction error for {url}: {e}")
            return None

    def _is_within_age_limit(self, date_iso: Optional[str]) -> bool:
        """Check if article date is within MAX_ARTICLE_AGE_DAYS."""
        if not date_iso:
            # If no date, assume it's recent enough
            return True

        try:
            article_date = datetime.fromisoformat(date_iso.replace('Z', '+00:00'))
            cutoff = datetime.now(timezone.utc) - timedelta(days=self.MAX_ARTICLE_AGE_DAYS)
            return article_date >= cutoff
        except Exception:
            return True

    async def _download_hero_image_http(
        self,
        image_url: str,
        article: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Download hero image using HTTP (not Playwright).

        This avoids the 'Please use browser.new_context()' error
        that occurs with Railway Browserless.

        NOTE: This method downloads the image bytes but does NOT save to R2.
        The main pipeline's save_candidate() handles R2 upload with consistent
        article naming (source_001, source_002, etc.).

        Args:
            image_url: URL of the image to download
            article: Article dict (for reference)

        Returns:
            hero_image dict with bytes included, or None if failed
        """
        if not image_url:
            return None

        image_bytes = None

        # Try aiohttp first
        try:
            import aiohttp

            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'Referer': 'https://www.archiposition.com/',
                'Origin': 'https://www.archiposition.com',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Sec-Fetch-Dest': 'image',
                'Sec-Fetch-Mode': 'no-cors',
                'Sec-Fetch-Site': 'same-site',
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(image_url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as response:
                    if response.status == 200:
                        image_bytes = await response.read()
                        print(f"[{self.source_id}]    Downloaded via aiohttp: {len(image_bytes)} bytes")
                    else:
                        print(f"[{self.source_id}]    aiohttp failed: HTTP {response.status}, trying cloudscraper...")

        except ImportError:
            print(f"[{self.source_id}]    aiohttp not installed, trying cloudscraper...")
        except Exception as e:
            print(f"[{self.source_id}]    aiohttp error: {e}, trying cloudscraper...")

        # Try cloudscraper if aiohttp failed
        if not image_bytes:
            try:
                import cloudscraper

                scraper = cloudscraper.create_scraper(
                    browser={
                        'browser': 'chrome',
                        'platform': 'darwin',
                        'mobile': False
                    }
                )

                response = scraper.get(
                    image_url,
                    headers={
                        'Referer': 'https://www.archiposition.com/',
                        'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                    },
                    timeout=15
                )

                if response.status_code == 200:
                    image_bytes = response.content
                    print(f"[{self.source_id}]    Downloaded via cloudscraper: {len(image_bytes)} bytes")
                else:
                    print(f"[{self.source_id}]    cloudscraper failed: HTTP {response.status_code}, trying urllib...")

            except ImportError:
                print(f"[{self.source_id}]    cloudscraper not installed, trying urllib...")
            except Exception as e:
                print(f"[{self.source_id}]    cloudscraper error: {e}, trying urllib...")

        # Try urllib as last resort
        if not image_bytes:
            try:
                import urllib.request

                headers = {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                    'Referer': 'https://www.archiposition.com/',
                }

                request = urllib.request.Request(image_url, headers=headers)

                with urllib.request.urlopen(request, timeout=15) as response:
                    image_bytes = response.read()
                    print(f"[{self.source_id}]    Downloaded via urllib: {len(image_bytes)} bytes")

            except Exception as e:
                print(f"[{self.source_id}]    urllib error: {e}")

        # Check if we got image bytes
        if not image_bytes or len(image_bytes) < 1000:
            print(f"[{self.source_id}]    Failed to download image from all methods")
            return None

        # Return hero_image dict with bytes (R2 save handled by main pipeline)
        hero_image = {
            "url": image_url,
            "width": None,
            "height": None,
            "source": "custom_scraper",
            "bytes": image_bytes,  # Include bytes for main pipeline's save_candidate()
        }

        print(f"[{self.source_id}]    Hero image ready for R2 upload ({len(image_bytes)} bytes)")
        return hero_image

    async def fetch_articles(self, hours: int = 24) -> List[Dict[str, Any]]:
        """
        Fetch new articles from Archiposition.

        Workflow:
        1. Load category page with User-Agent header
        2. Extract all /items/ links AND hero images from HTML grid
        3. Filter out known section URLs
        4. Check database for new URLs
        5. For new articles: visit page to get date only
        6. Filter by date (within MAX_ARTICLE_AGE_DAYS)
        7. Download and save hero images to R2
        8. Mark all URLs as seen
        """
        print(f"[{self.source_id}] Starting HTML pattern scraping...")

        # Initialize tracker
        await self._ensure_tracker()

        try:
            # Create page (this initializes browser automatically via _create_page)
            page = await self._create_page()

            try:
                # ============================================================
                # Step 1: Load Category Page
                # ============================================================
                print(f"[{self.source_id}] Loading category page...")

                await page.set_extra_http_headers({
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                })

                response = await page.goto(
                    self.CATEGORY_URL,
                    wait_until="domcontentloaded",
                    timeout=30000
                )

                if not response or not response.ok:
                    print(f"[{self.source_id}] Failed to load category page: {response.status if response else 'no response'}")
                    return []

                # Wait for content to load
                await asyncio.sleep(2)

                # ============================================================
                # Step 2: Extract Article Links AND Images from HTML Grid
                # ============================================================
                html = await page.content()
                extracted = self._extract_article_links(html)

                # Count how many have images
                with_images = sum(1 for _, _, img in extracted if img)
                print(f"[{self.source_id}] Found {len(extracted)} article links ({with_images} with images)")

                if not extracted:
                    print(f"[{self.source_id}] No articles found")
                    return []

                # ============================================================
                # Step 3: Filter Through Database
                # ============================================================
                # Build lookup dicts for title and image by URL
                url_to_title: Dict[str, str] = {url: title for url, title, _ in extracted}
                url_to_image: Dict[str, Optional[str]] = {url: img for url, _, img in extracted}

                # Get just URLs for tracker (List[str])
                all_urls: List[str] = [url for url, _, _ in extracted]

                print(f"[{self.source_id}] Database check:")

                # Ensure tracker is available
                if not self.tracker:
                    print(f"[{self.source_id}] Error: Tracker not initialized")
                    return []

                # Get new URLs from tracker (returns List[str])
                new_urls: List[str] = await self.tracker.filter_new_articles(self.source_id, all_urls)

                print(f"[{self.source_id}]    Total links: {len(all_urls)}")
                print(f"[{self.source_id}]    New articles: {len(new_urls)}")

                if not new_urls:
                    print(f"[{self.source_id}] No new articles to process")
                    # Still mark as seen
                    await self.tracker.mark_as_seen(self.source_id, all_urls)
                    return []

                # ============================================================
                # Step 4: Visit Each New Article for Date Only
                # ============================================================
                new_articles: List[Dict[str, Any]] = []
                skipped_old = 0
                images_saved = 0

                for url in new_urls[:self.MAX_NEW_ARTICLES]:
                    # Look up title and image from our dicts
                    title = url_to_title.get(url, url)
                    image_url = url_to_image.get(url)

                    print(f"[{self.source_id}] Processing: {title[:80]}...")

                    # Visit article page to get date only
                    date_iso = await self._get_article_date(page, url)

                    if date_iso:
                        print(f"[{self.source_id}]    Date: {date_iso[:10]}")

                    # Check date limit
                    if not self._is_within_age_limit(date_iso):
                        print(f"[{self.source_id}]    Skipped (too old)")
                        skipped_old += 1
                        continue

                    # Build article dict
                    article: Dict[str, Any] = {
                        'title': title,
                        'link': url,
                        'guid': url,
                        'source_id': self.source_id,
                        'source_name': self.source_name,
                        'custom_scraped': True,
                        'description': '',
                        'full_content': '',
                        'hero_image': None,
                    }

                    if date_iso:
                        article['published'] = date_iso

                    # Download and save hero image to R2 (image URL from grid!)
                    if image_url:
                        print(f"[{self.source_id}]    Hero image: {image_url[:60]}...")
                        hero_image = await self._download_hero_image_http(
                            image_url=image_url,
                            article=article
                        )
                        if hero_image:
                            article['hero_image'] = hero_image
                            if hero_image.get('r2_path'):
                                images_saved += 1
                    else:
                        print(f"[{self.source_id}]    No hero image in grid")

                    new_articles.append(article)
                    print(f"[{self.source_id}]    Added to results")

                    # Small delay between article page visits
                    await asyncio.sleep(0.5)

                # ============================================================
                # Step 5: Store All URLs and Finalize
                # ============================================================
                await self.tracker.mark_as_seen(self.source_id, all_urls)

                # Final Summary
                print(f"\n[{self.source_id}] Processing Summary:")
                print(f"   Articles found: {len(extracted)}")
                print(f"   New articles: {len(new_urls)}")
                print(f"   Skipped (too old): {skipped_old}")
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

    async def close(self) -> None:
        """Close browser and tracker connections."""
        await super().close()

        if self.tracker:
            await self.tracker.close()
            self.tracker = None


# Register this scraper
custom_scraper_registry.register(ArchipositionScraper)


# =============================================================================
# Standalone Test
# =============================================================================

async def test_archiposition_scraper() -> None:
    """Test the HTML pattern scraper."""
    print("=" * 60)
    print("Testing Archiposition HTML Pattern Scraper")
    print("=" * 60)

    # Show TEST_MODE status
    print(f"\nTEST_MODE: {ArticleTracker.TEST_MODE}")
    if ArticleTracker.TEST_MODE:
        print("   All articles will appear as 'new' (ignoring database)")
    else:
        print("   Normal mode - filtering seen articles")

    scraper = ArchipositionScraper()

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
            stats = await scraper.tracker.get_stats(source_id="archiposition")
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
                title = article.get('title', '')
                print(f"   Title: {title[:60]}...")
                print(f"   Link: {article['link']}")
                print(f"   Published: {article.get('published', 'No date')}")
                hero = article.get('hero_image')
                if hero:
                    r2_path = hero.get('r2_path', hero.get('url', 'No'))
                    print(f"   Hero Image: {r2_path[:50]}...")
                else:
                    print("   Hero Image: No")
        else:
            print("\n4. No new articles (all previously seen)")

        print("\n" + "=" * 60)
        print("Test complete!")
        print("=" * 60)

    finally:
        await scraper.close()


if __name__ == "__main__":
    asyncio.run(test_archiposition_scraper())