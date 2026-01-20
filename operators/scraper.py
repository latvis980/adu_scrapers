# operators/scraper.py
"""
ArchNews Article Scraper
Scrapes full article content from architecture news sites using Railway Browserless.

Features:
- Persistent browser sessions for efficiency
- Connection pooling (reuses browsers across URLs)
- Aggressive ad/tracker blocking for speed
- Hero image extraction from og:image meta tags
- Image downloading for R2 storage

Usage:
    from operators.scraper import ArticleScraper

    scraper = ArticleScraper()
    articles = await scraper.scrape_articles(article_list)
    await scraper.close()

Environment Variables (set in Railway):
    BROWSER_PLAYWRIGHT_ENDPOINT - Railway Browserless WebSocket URL
    BROWSER_TOKEN - Railway Browserless auth token (optional)
"""

import asyncio
import logging
import time
import re
import os
from typing import Dict, List, Any, Optional
from urllib.parse import urlparse, urljoin
from playwright.async_api import (
    async_playwright, 
    Browser, 
    BrowserContext, 
    Page, 
    TimeoutError as PlaywrightTimeoutError
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ArticleScraper:
    """
    Scrapes architecture news articles using Railway Browserless.
    Optimized for speed with persistent browser sessions.
    """

    def __init__(self, browser_pool_size: int = 2):
        """
        Initialize the article scraper.

        Args:
            browser_pool_size: Number of concurrent browsers (2-3 recommended)
        """
        # Browser pool settings
        self.browser_pool_size = browser_pool_size
        self.browser_pool: List[Browser] = []
        self.browser_contexts: List[BrowserContext] = []
        self.playwright = None
        self.session_active = False
        self._session_lock = asyncio.Lock()

        # Railway Browserless configuration
        self.browserless_endpoint = (
            os.getenv('BROWSER_PLAYWRIGHT_ENDPOINT_PRIVATE') or
            os.getenv('BROWSER_PLAYWRIGHT_ENDPOINT') or
            os.getenv('BROWSERLESS_URL')
        )
        self.browserless_token = os.getenv('BROWSER_TOKEN')

        # Timeout settings (in milliseconds)
        self.default_timeout = 20000  # 20 seconds
        self.browser_launch_timeout = 25000  # 25 seconds

        # Domain-specific timeouts for slower sites
        self.domain_timeouts = {
            'archdaily.com': 25000,
            'dezeen.com': 25000,
            'designboom.com': 20000,
            'architizer.com': 20000,
            'archpaper.com': 18000,
        }

        # Performance settings
        self.load_wait_time = 2.0  # Seconds to wait after page load
        self.interaction_delay = 0.3

        # Statistics tracking
        self.stats = {
            "total_scraped": 0,
            "successful": 0,
            "failed": 0,
            "browser_reuses": 0,
            "total_time": 0.0,
            "images_extracted": 0,
            "hero_images_found": 0,
        }

        # Log configuration
        logger.info("🏛️ ArchNews Article Scraper initialized")
        logger.info(f"   Browser pool size: {self.browser_pool_size}")
        logger.info(f"   Browserless: {'✓ ' + self._get_endpoint_display() if self.browserless_endpoint else '✗ Local mode'}")

    def _get_endpoint_display(self) -> str:
        """Get safe display string for endpoint (hide sensitive parts)."""
        if not self.browserless_endpoint:
            return "not configured"
        parsed = urlparse(self.browserless_endpoint)
        return f"{parsed.scheme}://{parsed.netloc}/..."

    # =========================================================================
    # Browser Pool Management
    # =========================================================================

    async def _initialize_browser_pool(self):
        """Initialize persistent browser pool."""
        if self.session_active:
            return

        async with self._session_lock:
            if self.session_active:
                return

            logger.info("🚀 Initializing browser pool...")
            self.playwright = await async_playwright().start()

            for i in range(self.browser_pool_size):
                try:
                    browser = await self._create_browser(f"browser-{i}")
                    if browser:
                        self.browser_pool.append(browser)
                        context = await self._create_context(browser)
                        self.browser_contexts.append(context)
                        logger.info(f"   ✅ Browser {i + 1}/{self.browser_pool_size} ready")
                except Exception as e:
                    logger.error(f"   ❌ Browser {i + 1} failed: {e}")

            if self.browser_pool:
                self.session_active = True
                logger.info(f"🎯 Browser pool ready: {len(self.browser_pool)}/{self.browser_pool_size}")
            else:
                raise RuntimeError("Failed to initialize any browsers")

    async def _create_browser(self, browser_id: str) -> Optional[Browser]:
        """Create a single browser connection."""
        try:
            if self.browserless_endpoint:
                # Connect to Railway Browserless
                connect_url = self.browserless_endpoint
                if self.browserless_token and 'token=' not in connect_url:
                    separator = '&' if '?' in connect_url else '?'
                    connect_url = f"{connect_url}{separator}token={self.browserless_token}"

                browser = await self.playwright.chromium.connect(
                    connect_url,
                    timeout=self.browser_launch_timeout
                )
                logger.info(f"   🚂 {browser_id} connected to Railway Browserless")
                return browser
            else:
                # Local Playwright fallback
                browser = await self.playwright.chromium.launch(
                    headless=True,
                    args=[
                        "--no-sandbox",
                        "--disable-setuid-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-gpu",
                    ]
                )
                logger.info(f"   💻 {browser_id} running locally")
                return browser

        except Exception as e:
            logger.error(f"   ❌ Failed to create {browser_id}: {e}")
            return None

    async def _create_context(self, browser: Browser) -> BrowserContext:
        """Create browser context with optimized settings."""
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            java_script_enabled=True,
            ignore_https_errors=True,
        )
        return context

    async def _reconnect_browser(self, index: int) -> bool:
        """Reconnect a failed browser in the pool."""
        try:
            logger.info(f"🔄 Reconnecting browser-{index}...")

            # Close old browser if exists
            if index < len(self.browser_pool) and self.browser_pool[index]:
                try:
                    await self.browser_pool[index].close()
                except:
                    pass

            # Create new browser
            browser = await self._create_browser(f"browser-{index}")
            if browser:
                context = await self._create_context(browser)
                self.browser_pool[index] = browser
                self.browser_contexts[index] = context
                logger.info(f"   ✅ Browser-{index} reconnected")
                return True
            return False
        except Exception as e:
            logger.error(f"   ❌ Reconnection failed: {e}")
            return False

    # =========================================================================
    # Main Scraping Methods
    # =========================================================================

    async def scrape_articles(self, articles: List[Dict]) -> List[Dict]:
        """
        Scrape full content for a list of articles.

        Args:
            articles: List of article dicts with 'link' key

        Returns:
            Same list with added 'full_content', 'images', 'hero_image', 'scrape_success' keys
        """
        if not articles:
            logger.warning("📭 No articles to scrape")
            return []

        logger.info(f"🔍 Scraping {len(articles)} articles...")
        start_time = time.time()

        # Initialize browser pool
        await self._initialize_browser_pool()

        # Create tasks with semaphore for concurrency control
        semaphore = asyncio.Semaphore(self.browser_pool_size)
        tasks = []

        for i, article in enumerate(articles):
            browser_index = i % len(self.browser_pool)
            task = self._scrape_with_semaphore(semaphore, article, browser_index)
            tasks.append(task)

        # Execute all tasks
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        scraped_articles = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"❌ Article {i} failed: {result}")
                article = articles[i].copy()
                article.update({
                    "full_content": "",
                    "images": [],
                    "hero_image": None,
                    "scrape_success": False,
                    "scrape_error": str(result)
                })
                scraped_articles.append(article)
            else:
                scraped_articles.append(result)

        # Update statistics
        total_time = time.time() - start_time
        self.stats["total_scraped"] += len(articles)
        self.stats["successful"] += sum(1 for a in scraped_articles if a.get("scrape_success"))
        self.stats["failed"] += sum(1 for a in scraped_articles if not a.get("scrape_success"))
        self.stats["total_time"] += total_time
        self.stats["browser_reuses"] += max(0, len(articles) - self.browser_pool_size)

        success_count = sum(1 for a in scraped_articles if a.get("scrape_success"))
        hero_count = sum(1 for a in scraped_articles if a.get("hero_image"))
        logger.info(f"✅ Scraping complete: {success_count}/{len(articles)} successful, {hero_count} hero images in {total_time:.1f}s")

        return scraped_articles

    async def _scrape_with_semaphore(
        self, 
        semaphore: asyncio.Semaphore, 
        article: Dict, 
        browser_index: int
    ) -> Dict:
        """Scrape article with semaphore for concurrency control."""
        async with semaphore:
            return await self._scrape_single_article(article, browser_index)

    async def _scrape_single_article(self, article: Dict, browser_index: int) -> Dict:
        """
        Scrape a single article URL.

        Args:
            article: Article dict with 'link' key
            browser_index: Which browser from pool to use

        Returns:
            Article dict with scraped content added
        """
        url = article.get("link", "")
        if not url:
            article["scrape_success"] = False
            article["scrape_error"] = "No URL provided"
            return article

        start_time = time.time()
        result = article.copy()

        # Get browser from pool
        if browser_index >= len(self.browser_pool):
            browser_index = 0

        context = self.browser_contexts[browser_index]

        try:
            logger.info(f"🌐 Scraping: {url[:60]}...")

            # Create new page
            page = await context.new_page()

            try:
                # Configure page (block ads, etc.)
                await self._configure_page(page)

                # Get timeout for this domain
                domain = urlparse(url).netloc.lower()
                timeout = self.domain_timeouts.get(
                    domain.replace('www.', ''), 
                    self.default_timeout
                )

                # Navigate to page
                await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
                await asyncio.sleep(self.load_wait_time)

                # Dismiss popups/overlays
                await self._dismiss_overlays(page)

                # Extract hero image (og:image) - do this FIRST before any DOM manipulation
                hero_image = await self._extract_hero_image(page, url)

                # Extract content
                content = await self._extract_article_content(page, url)

                # Extract all images (for reference)
                images = await self._extract_images(page, url)

                if content and len(content.strip()) > 100:
                    processing_time = time.time() - start_time

                    result.update({
                        "full_content": content,
                        "images": images,
                        "image_count": len(images),
                        "hero_image": hero_image,
                        "scrape_success": True,
                        "scrape_time": processing_time,
                        "content_length": len(content),
                    })

                    if hero_image:
                        self.stats["hero_images_found"] += 1

                    logger.info(f"   ✅ Success: {len(content)} chars, {len(images)} images, hero: {'✓' if hero_image else '✗'} in {processing_time:.1f}s")
                else:
                    result.update({
                        "full_content": "",
                        "images": [],
                        "hero_image": hero_image,  # Still save hero image even if content extraction failed
                        "scrape_success": False,
                        "scrape_error": "Content too short or empty"
                    })
                    logger.warning(f"   ⚠️ Low content: {url[:40]}...")

            finally:
                await page.close()

        except PlaywrightTimeoutError:
            result.update({
                "full_content": "",
                "images": [],
                "hero_image": None,
                "scrape_success": False,
                "scrape_error": "Timeout"
            })
            logger.warning(f"   ⏱️ Timeout: {url[:40]}...")

        except Exception as e:
            result.update({
                "full_content": "",
                "images": [],
                "hero_image": None,
                "scrape_success": False,
                "scrape_error": str(e)
            })
            logger.error(f"   ❌ Error: {e}")

        return result

    # =========================================================================
    # Page Configuration & Optimization
    # =========================================================================

    async def _configure_page(self, page: Page):
        """Configure page with optimizations and ad blocking."""
        try:
            # Set headers
            await page.set_extra_http_headers({
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
            })

            # Block unnecessary resources
            await page.route("**/*", self._block_resources)

            # Inject helper scripts
            await page.add_init_script("""
                // Disable animations for faster rendering
                document.addEventListener('DOMContentLoaded', () => {
                    const style = document.createElement('style');
                    style.textContent = `
                        *, *::before, *::after {
                            animation: none !important;
                            transition: none !important;
                        }
                    `;
                    document.head.appendChild(style);
                });

                // Block popups
                window.alert = window.confirm = window.prompt = () => {};
                window.open = () => null;
            """)

        except Exception as e:
            logger.warning(f"Page config warning: {e}")

    async def _block_resources(self, route):
        """Block ads, trackers, and unnecessary resources."""
        request = route.request
        resource_type = request.resource_type
        url = request.url.lower()

        # Block by resource type (keep images for extraction)
        blocked_types = ['media', 'font', 'websocket', 'manifest']
        if resource_type in blocked_types:
            await route.abort()
            return

        # Block known ad/tracking domains
        blocked_domains = [
            'google-analytics', 'googletagmanager', 'googlesyndication',
            'doubleclick', 'facebook.com', 'facebook.net',
            'twitter.com', 'amazon-adsystem', 'adsystem',
            'adservice', 'advertising', 'analytics',
            'hotjar', 'mixpanel', 'segment.io',
            'optimizely', 'crazyegg', 'mouseflow',
        ]

        if any(domain in url for domain in blocked_domains):
            await route.abort()
            return

        # Allow everything else
        await route.continue_()

    async def _dismiss_overlays(self, page: Page):
        """Dismiss cookie banners, popups, and overlays."""
        dismiss_selectors = [
            # Cookie consent
            'button[id*="cookie"]',
            'button[class*="cookie"]',
            'button[id*="consent"]',
            'button[class*="consent"]',
            '[class*="cookie"] button',
            '[class*="gdpr"] button',

            # Generic close/accept buttons
            'button:has-text("Accept")',
            'button:has-text("Accept All")',
            'button:has-text("Got it")',
            'button:has-text("OK")',
            'button:has-text("Close")',
            '[aria-label="Close"]',
            '.modal-close',
            '.popup-close',
        ]

        for selector in dismiss_selectors:
            try:
                element = page.locator(selector).first
                if await element.is_visible(timeout=500):
                    await element.click(timeout=1000)
                    await asyncio.sleep(0.2)
                    break
            except:
                continue

    # =========================================================================
    # Hero Image Extraction (og:image)
    # =========================================================================

    async def _extract_hero_image(self, page: Page, base_url: str) -> Optional[Dict]:
        """
        Extract hero image from Open Graph meta tags.
        This is the image used for social sharing (Telegram, Twitter, Facebook).

        Priority:
            1. og:image (Open Graph - most common)
            2. twitter:image (Twitter Cards)
            3. First large image in article

        Args:
            page: Playwright page object
            base_url: Article URL for resolving relative paths

        Returns:
            Dict with 'url', 'width', 'height', 'alt' or None
        """
        try:
            hero_data = await page.evaluate("""
                (baseUrl) => {
                    // Helper to resolve relative URLs
                    function resolveUrl(url) {
                        if (!url) return null;
                        if (url.startsWith('http')) return url;
                        if (url.startsWith('//')) return 'https:' + url;
                        try {
                            return new URL(url, baseUrl).href;
                        } catch {
                            return null;
                        }
                    }

                    // Try og:image first (most reliable for social sharing)
                    const ogImage = document.querySelector('meta[property="og:image"]');
                    if (ogImage) {
                        const url = resolveUrl(ogImage.content);
                        if (url) {
                            // Try to get dimensions from og:image:width/height
                            const ogWidth = document.querySelector('meta[property="og:image:width"]');
                            const ogHeight = document.querySelector('meta[property="og:image:height"]');
                            const ogAlt = document.querySelector('meta[property="og:image:alt"]');

                            return {
                                url: url,
                                width: ogWidth ? parseInt(ogWidth.content) : null,
                                height: ogHeight ? parseInt(ogHeight.content) : null,
                                alt: ogAlt ? ogAlt.content : '',
                                source: 'og:image'
                            };
                        }
                    }

                    // Try twitter:image
                    const twitterImage = document.querySelector('meta[name="twitter:image"]') ||
                                         document.querySelector('meta[property="twitter:image"]');
                    if (twitterImage) {
                        const url = resolveUrl(twitterImage.content);
                        if (url) {
                            return {
                                url: url,
                                width: null,
                                height: null,
                                alt: '',
                                source: 'twitter:image'
                            };
                        }
                    }

                    // Fallback: try to find schema.org image
                    const schemaImage = document.querySelector('meta[itemprop="image"]');
                    if (schemaImage) {
                        const url = resolveUrl(schemaImage.content);
                        if (url) {
                            return {
                                url: url,
                                width: null,
                                height: null,
                                alt: '',
                                source: 'schema.org'
                            };
                        }
                    }

                    // Last resort: link rel="image_src"
                    const linkImage = document.querySelector('link[rel="image_src"]');
                    if (linkImage) {
                        const url = resolveUrl(linkImage.href);
                        if (url) {
                            return {
                                url: url,
                                width: null,
                                height: null,
                                alt: '',
                                source: 'link:image_src'
                            };
                        }
                    }

                    return null;
                }
            """, base_url)

            if hero_data and hero_data.get('url'):
                logger.info(f"   🖼️ Hero image found via {hero_data.get('source', 'unknown')}")
                return hero_data

            return None

        except Exception as e:
            logger.warning(f"Hero image extraction error: {e}")
            return None

    async def download_hero_image(self, hero_image: Dict, context: BrowserContext = None) -> Optional[bytes]:
        """
        Download hero image bytes for storage.

        Args:
            hero_image: Dict with 'url' key
            context: Browser context to use (optional)

        Returns:
            Image bytes or None if failed
        """
        if not hero_image or not hero_image.get('url'):
            return None

        url = hero_image['url']

        try:
            # Use provided context or create new one
            if context is None:
                if not self.browser_contexts:
                    logger.warning("No browser context available for image download")
                    return None
                context = self.browser_contexts[0]

            # Create a new page for downloading
            page = await context.new_page()

            try:
                # Fetch the image
                response = await page.goto(url, timeout=15000)

                if response and response.ok:
                    image_bytes = await response.body()
                    logger.info(f"   📥 Downloaded hero image: {len(image_bytes)} bytes")
                    return image_bytes
                else:
                    logger.warning(f"   ⚠️ Failed to download hero image: HTTP {response.status if response else 'no response'}")
                    return None

            finally:
                await page.close()

        except Exception as e:
            logger.error(f"   ❌ Hero image download error: {e}")
            return None

    # =========================================================================
    # Content Extraction
    # =========================================================================

    async def _extract_article_content(self, page: Page, url: str) -> str:
        """
        Extract main article content from page.
        Uses site-specific selectors when available.
        """
        domain = urlparse(url).netloc.lower().replace('www.', '')

        # Site-specific extraction
        site_selectors = {
            'archdaily.com': [
                'article.afd-char-gallery',
                '.afd-gallery-container',
                'article[class*="article"]',
                '.article-content',
            ],
            'dezeen.com': [
                '.article-content',
                'article .entry-content',
                '.dezeen-content',
            ],
            'designboom.com': [
                '.article-content',
                '.entry-content',
                'article .content',
            ],
        }

        # Get selectors for this site, or use generic ones
        selectors = site_selectors.get(domain, []) + [
            'article',
            '[role="article"]',
            '.article-content',
            '.article-body',
            '.post-content',
            '.entry-content',
            'main',
            '[role="main"]',
            '.content',
        ]

        try:
            content = await page.evaluate("""
                (selectors) => {
                    // Try each selector
                    for (const selector of selectors) {
                        const element = document.querySelector(selector);
                        if (element) {
                            // Clone to avoid modifying original
                            const clone = element.cloneNode(true);

                            // Remove unwanted elements
                            const removeSelectors = [
                                'script', 'style', 'nav', 'header', 'footer',
                                'aside', '.ad', '.ads', '.advertisement',
                                '.social-share', '.related-posts', '.comments',
                                '.newsletter', '.sidebar', '[role="complementary"]',
                                '.breadcrumb', '.pagination', '.author-bio'
                            ];

                            removeSelectors.forEach(sel => {
                                clone.querySelectorAll(sel).forEach(el => el.remove());
                            });

                            const text = clone.innerText || clone.textContent || '';
                            if (text.trim().length > 200) {
                                return text.trim();
                            }
                        }
                    }

                    // Fallback: get body text
                    const body = document.body.cloneNode(true);
                    ['script', 'style', 'nav', 'header', 'footer', 'aside']
                        .forEach(tag => body.querySelectorAll(tag).forEach(el => el.remove()));

                    return (body.innerText || body.textContent || '').trim().substring(0, 10000);
                }
            """, selectors)

            # Clean up content
            return self._clean_content(content) if content else ""

        except Exception as e:
            logger.warning(f"Content extraction error: {e}")
            try:
                # Fallback
                text = await page.inner_text('body')
                return self._clean_content(text[:5000]) if text else ""
            except:
                return ""

    def _clean_content(self, content: str) -> str:
        """Clean extracted content."""
        if not content:
            return ""

        # Remove excessive whitespace
        content = re.sub(r'\n\s*\n+', '\n\n', content)
        content = re.sub(r'[ \t]+', ' ', content)

        # Remove common junk phrases
        junk_patterns = [
            r'cookie\s*(policy|consent|notice)',
            r'privacy\s*policy',
            r'terms\s*(of|and)\s*(use|service)',
            r'newsletter\s*sign\s*up',
            r'follow\s*us\s*on',
            r'share\s*(this|on)',
            r'advertisement',
            r'sponsored\s*content',
        ]

        for pattern in junk_patterns:
            content = re.sub(pattern, '', content, flags=re.IGNORECASE)

        return content.strip()

    # =========================================================================
    # Image Extraction (all images)
    # =========================================================================

    async def _extract_images(self, page: Page, base_url: str) -> List[Dict]:
        """
        Extract article images for Telegram posts.

        Returns:
            List of image dicts with 'url', 'alt', 'width', 'height'
        """
        try:
            images = await page.evaluate("""
                (baseUrl) => {
                    const images = [];
                    const seen = new Set();

                    // Selectors for article images (prioritized)
                    const selectors = [
                        'article img',
                        '.article-content img',
                        '.gallery img',
                        'main img',
                        '.post-content img',
                        '.entry-content img',
                    ];

                    // Collect images
                    for (const selector of selectors) {
                        document.querySelectorAll(selector).forEach(img => {
                            let src = img.src || img.dataset.src || img.dataset.lazySrc || '';

                            // Skip if no src or already seen
                            if (!src || seen.has(src)) return;

                            // Skip tiny images, icons, logos
                            const width = img.naturalWidth || img.width || 0;
                            const height = img.naturalHeight || img.height || 0;
                            if (width < 200 || height < 150) return;

                            // Skip common non-content images
                            const srcLower = src.toLowerCase();
                            if (srcLower.includes('logo') || 
                                srcLower.includes('icon') ||
                                srcLower.includes('avatar') ||
                                srcLower.includes('advertisement') ||
                                srcLower.includes('banner') ||
                                srcLower.includes('placeholder')) return;

                            seen.add(src);
                            images.push({
                                url: src,
                                alt: img.alt || '',
                                width: width,
                                height: height,
                            });
                        });
                    }

                    return images.slice(0, 10);  // Limit to 10 images
                }
            """, base_url)

            # Convert relative URLs to absolute
            for img in images:
                if img['url'] and not img['url'].startswith('http'):
                    img['url'] = urljoin(base_url, img['url'])

            self.stats["images_extracted"] += len(images)
            return images

        except Exception as e:
            logger.warning(f"Image extraction error: {e}")
            return []

    async def get_hero_image(self, page: Page, base_url: str) -> Optional[Dict]:
        """
        Get the main/hero image for Telegram post thumbnail.

        Returns:
            Single image dict or None
        """
        images = await self._extract_images(page, base_url)

        if not images:
            return None

        # Return the largest image (likely the hero)
        return max(images, key=lambda x: (x.get('width', 0) * x.get('height', 0)))

    # =========================================================================
    # Statistics & Cleanup
    # =========================================================================

    def get_stats(self) -> Dict[str, Any]:
        """Get scraping statistics."""
        stats = self.stats.copy()
        if stats["total_scraped"] > 0:
            stats["success_rate"] = (stats["successful"] / stats["total_scraped"]) * 100
            stats["avg_time"] = stats["total_time"] / stats["total_scraped"]
        return stats

    def print_stats(self):
        """Print scraping statistics."""
        stats = self.get_stats()
        if stats["total_scraped"] > 0:
            logger.info("=" * 50)
            logger.info("📊 SCRAPER STATISTICS")
            logger.info("=" * 50)
            logger.info(f"   Total scraped: {stats['total_scraped']}")
            logger.info(f"   Success rate: {stats.get('success_rate', 0):.1f}%")
            logger.info(f"   Browser reuses: {stats['browser_reuses']}")
            logger.info(f"   Images extracted: {stats['images_extracted']}")
            logger.info(f"   Hero images found: {stats['hero_images_found']}")
            logger.info(f"   Total time: {stats['total_time']:.1f}s")
            logger.info("=" * 50)

    async def close(self):
        """Clean shutdown of browser pool."""
        logger.info("🛑 Shutting down scraper...")

        # Close contexts
        for context in self.browser_contexts:
            try:
                await context.close()
            except:
                pass

        # Close browsers
        for browser in self.browser_pool:
            try:
                await browser.close()
            except:
                pass

        # Stop Playwright
        if self.playwright:
            try:
                await self.playwright.stop()
            except:
                pass

        self.session_active = False
        self.print_stats()
        logger.info("✅ Scraper shutdown complete")


# =============================================================================
# Standalone Test
# =============================================================================

async def test_scraper():
    """Test the scraper with a sample URL."""
    print("🧪 Testing Article Scraper...")

    scraper = ArticleScraper(browser_pool_size=1)

    try:
        test_articles = [
            {"link": "https://www.archdaily.com", "title": "Test Article"}
        ]

        results = await scraper.scrape_articles(test_articles)

        for article in results:
            print(f"\n📰 {article.get('title', 'No title')}")
            print(f"   Success: {article.get('scrape_success', False)}")
            print(f"   Content length: {len(article.get('full_content', ''))}")
            print(f"   Images: {len(article.get('images', []))}")
            print(f"   Hero image: {article.get('hero_image', {}).get('url', 'None')[:60] if article.get('hero_image') else 'None'}")

    finally:
        await scraper.close()


if __name__ == "__main__":
    asyncio.run(test_scraper())