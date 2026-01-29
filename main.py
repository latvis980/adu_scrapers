# main.py
"""
ADUmedia Custom Scrapers Pipeline

Dedicated pipeline for custom scraper sources (sites without working RSS feeds).
Runs independently from RSS pipeline.

Schedule: 18:30 Lisbon time (17:30 UTC in winter, 16:30 UTC in summer)

Pipeline:
    1. Run custom scrapers to discover new article URLs
    2. Scrape full article content (Browserless)
    3. AI content filtering (BEFORE summarization to save costs)
    4. Generate AI summaries (OpenAI) - only for filtered articles
    5. Save articles to R2 storage
    6. Record articles to Supabase (for cross-edition tracking)

Usage:
    python main.py                            # Run all custom scrapers
    python main.py --sources identity prorus  # Run specific scrapers
    python main.py --no-filter                # Skip AI filtering
    python main.py --list-sources             # Show available scrapers

Environment Variables (set in Railway):
    OPENAI_API_KEY              - OpenAI API key for GPT-4o-mini
    BROWSER_PLAYWRIGHT_ENDPOINT - Railway Browserless endpoint
    R2_ACCOUNT_ID               - Cloudflare R2 account ID
    R2_ACCESS_KEY_ID            - R2 access key
    R2_SECRET_ACCESS_KEY        - R2 secret key
    R2_BUCKET_NAME              - R2 bucket name
    SUPABASE_URL                - Supabase project URL (optional)
    SUPABASE_KEY                - Supabase API key (optional)
"""

import asyncio
import argparse
from datetime import datetime
from typing import Optional

from io import BytesIO
from PIL import Image
import aiohttp

# Import operators
from operators.scraper import ArticleScraper
from operators.monitor import create_llm, summarize_article

# Import storage
from storage.r2 import R2Storage

# Import database (optional - graceful degradation if not configured)
from database.connection import record_batch_to_db, test_connection as test_db_connection

# Import prompts and config
from prompts.summarize import SUMMARIZE_PROMPT_TEMPLATE
from prompts.filter import FILTER_PROMPT_TEMPLATE, parse_filter_response
from config.sources import (
    SOURCES,
    get_source_config,
    get_custom_scraper_ids,
)

# Import custom scrapers
from operators.custom_scrapers.identity import IdentityScraper
from operators.custom_scrapers.archiposition import ArchipositionScraper
from operators.custom_scrapers.prorus import ProRusScraper
from operators.custom_scrapers.bauwelt import BauweltScraper
from operators.custom_scrapers.gooood import GoooodScraper
from operators.custom_scrapers.japan_architects import JapanArchitectsScraper
from operators.custom_scrapers.domus import DomusScraper
from operators.custom_scrapers.metalocus import MetalocusScraper
from operators.custom_scrapers.metropolis import MetropolisScraper
from operators.custom_scrapers.world_landscape_architect import WorldLandscapeArchitectScraper
from operators.custom_scrapers.landscape_architecture_magazine import LandscapeArchitectureMagazineScraper

# Default configuration
DEFAULT_HOURS_LOOKBACK = 24

# Custom scraper registry - maps source_id to scraper class
CUSTOM_SCRAPER_MAP = {
    "identity": IdentityScraper,
    "archiposition": ArchipositionScraper,
    "prorus": ProRusScraper,
    "bauwelt": BauweltScraper,
    "gooood": GoooodScraper,
    "japan_architects": JapanArchitectsScraper,
    "domus": DomusScraper,
    "metalocus": MetalocusScraper,
    "metropolis": MetropolisScraper,
    "world_landscape_architect": WorldLandscapeArchitectScraper,
    "landscape_architecture_magazine": LandscapeArchitectureMagazineScraper,
}


# =============================================================================
# Command Line Arguments
# =============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="ADUmedia Custom Scrapers Pipeline"
    )

    parser.add_argument(
        "--sources",
        nargs="+",
        help="Specific source IDs to process (e.g., identity prorus)"
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=DEFAULT_HOURS_LOOKBACK,
        help=f"Hours to look back (default: {DEFAULT_HOURS_LOOKBACK})"
    )
    parser.add_argument(
        "--no-filter",
        action="store_true",
        help="Skip AI content filtering"
    )
    parser.add_argument(
        "--no-scrape",
        action="store_true",
        help="Skip Browserless content scraping (use minimal article data)"
    )
    parser.add_argument(
        "--list-sources",
        action="store_true",
        help="List all available custom scrapers and exit"
    )

    return parser.parse_args()


# =============================================================================
# Helper Functions
# =============================================================================

def filter_articles(articles: list, llm) -> tuple[list, list]:
    """
    Filter articles using AI - runs BEFORE summarization.

    Uses scraped full_content for better accuracy.

    Args:
        articles: List of articles with scraped content
        llm: LLM instance

    Returns:
        Tuple of (included_articles, excluded_articles)
    """
    print(f"\n[FILTER] AI content filtering {len(articles)} articles...")

    included = []
    excluded = []

    # Create the chain once
    filter_chain = FILTER_PROMPT_TEMPLATE | llm

    for i, article in enumerate(articles, 1):
        title = article.get("title", "No title")
        source_name = article.get("source_name", article.get("source_id", "Unknown"))
        print(f"   [{i}/{len(articles)}] [{source_name}] {title[:40]}...")

        try:
            # Use scraped full_content for filtering (most accurate)
            # Fall back to description if full_content not available
            content_for_filter = (
                article.get("full_content", "") or 
                article.get("content", "") or 
                article.get("description", "")
            )

            # Invoke the chain with proper parameters
            response = filter_chain.invoke({
                "title": title,
                "description": article.get("description", "")[:500],
                "content": content_for_filter[:1000]  # Use scraped content
            })

            result = parse_filter_response(response.content)

            if result.get("include", True):
                included.append(article)
                print(f"      [OK] Included")
            else:
                excluded.append(article)
                print(f"      [SKIP] Excluded: {result.get('reason', 'N/A')}")

        except Exception as e:
            print(f"      [WARN] Filter error: {e} - including by default")
            included.append(article)

    return included, excluded


def generate_summaries(articles: list, llm, prompt_template: str) -> list:
    """Generate AI summaries for articles."""
    print(f"\n[SUMMARY] Generating AI summaries for {len(articles)} articles...")

    for i, article in enumerate(articles, 1):
        title = article.get("title", "No title")
        source_name = article.get("source_name", article.get("source_id", "Unknown"))
        print(f"   [{i}/{len(articles)}] [{source_name}] {title[:40]}...")

        try:
            # summarize_article expects (article, llm, prompt_template)
            summarized = summarize_article(article, llm, prompt_template)
            article["headline"] = summarized.get("headline", "")
            article["ai_summary"] = summarized.get("ai_summary", "")
            article["tag"] = summarized.get("tag", "")
        except Exception as e:
            print(f"      [WARN] Error: {e}")
            article["headline"] = article.get("title", "")
            article["ai_summary"] = article.get("description", "")[:200] + "..."
            article["tag"] = ""

    return articles

def convert_webp_to_jpeg(image_bytes: bytes, quality: int = 85) -> tuple[bytes, str]:
    """
    Convert WebP image to JPEG format.

    Args:
        image_bytes: Original image bytes (any format)
        quality: JPEG quality (1-100, default 85)

    Returns:
        Tuple of (converted_bytes, content_type)
        If already JPEG or conversion fails, returns original bytes
    """
    try:
        # Open image from bytes
        img = Image.open(BytesIO(image_bytes))

        # Check if it's WebP or needs conversion
        original_format = img.format

        # If already JPEG, return as-is
        if original_format == 'JPEG':
            return image_bytes, 'image/jpeg'

        # Convert RGBA to RGB (WebP often has alpha channel)
        if img.mode in ('RGBA', 'LA', 'P'):
            # Create white background
            background = Image.new('RGB', img.size, (255, 255, 255))
            if img.mode == 'P':
                img = img.convert('RGBA')
            background.paste(img, mask=img.split()[-1] if img.mode in ('RGBA', 'LA') else None)
            img = background
        elif img.mode != 'RGB':
            img = img.convert('RGB')

        # Convert to JPEG
        output = BytesIO()
        img.save(output, format='JPEG', quality=quality, optimize=True)
        jpeg_bytes = output.getvalue()

        print(f"      [CONVERT] {original_format} -> JPEG ({len(image_bytes)} -> {len(jpeg_bytes)} bytes)")
        return jpeg_bytes, 'image/jpeg'

    except Exception as e:
        print(f"      [WARN] Image conversion failed: {e} - using original")
        return image_bytes, 'image/jpeg'  # Assume JPEG if conversion fails


async def download_hero_images(articles: list) -> list:
    """
    Download hero images for articles that have URLs but no bytes yet.
    Converts WebP and other formats to JPEG.

    Args:
        articles: List of articles with hero_image metadata

    Returns:
        Articles with hero_image.bytes populated and converted to JPEG
    """
    print(f"\n[IMAGES] Downloading and converting hero images...")

    downloaded = 0
    converted = 0
    failed = 0

    # Use aiohttp for direct image downloads
    timeout = aiohttp.ClientTimeout(total=15)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
    }

    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        for i, article in enumerate(articles, 1):
            hero = article.get("hero_image")
            if not hero or not hero.get("url"):
                continue

            # Skip if already has bytes (custom scraper downloaded it)
            if hero.get("bytes"):
                # Still convert if needed
                if len(hero["bytes"]) > 0:
                    converted_bytes, final_content_type = convert_webp_to_jpeg(hero["bytes"])
                    hero["bytes"] = converted_bytes
                    hero["content_type"] = final_content_type
                    converted += 1
                continue

            image_url = hero["url"]
            title = article.get("title", "No title")[:30]

            try:
                async with session.get(image_url) as response:
                    if response.status == 200:
                        image_bytes = await response.read()
                        original_content_type = response.headers.get("Content-Type", "image/jpeg")

                        # Convert WebP (and other formats) to JPEG
                        converted_bytes, final_content_type = convert_webp_to_jpeg(image_bytes)

                        # Store converted bytes in hero_image dict
                        hero["bytes"] = converted_bytes
                        hero["content_type"] = final_content_type
                        hero["original_format"] = original_content_type

                        downloaded += 1
                        print(f"   [{i}] [OK] {title}...")
                    else:
                        failed += 1
                        print(f"   [{i}] [FAIL] HTTP {response.status}: {title}...")

            except asyncio.TimeoutError:
                failed += 1
                print(f"   [{i}] [TIMEOUT] {title}...")
            except Exception as e:
                failed += 1
                print(f"   [{i}] [ERROR] {title}... {str(e)[:30]}")

    print(f"\n   [STATS] Downloaded: {downloaded}, Converted: {converted}, Failed: {failed}")
    return articles

def save_candidates_to_r2(articles: list, r2: R2Storage) -> list:
    """
    Save articles as editorial candidates to R2 storage.
    Also records to Supabase for cross-edition tracking.

    Args:
        articles: List of article dicts with ai_summary
        r2: R2Storage instance

    Returns:
        List of candidate info dicts (for manifest creation)
    """
    print("\n[R2] Saving candidates to R2 storage...")

    # Reset counters for this batch
    r2.reset_counters()

    candidates = []
    for article in articles:
        try:
            # Get hero image bytes if available
            image_bytes = None
            hero = article.get("hero_image")
            if hero and hero.get("bytes"):
                image_bytes = hero["bytes"]

            # save_candidate handles both JSON and image
            result = r2.save_candidate(
                article=article,
                image_bytes=image_bytes
            )

            # Store original article in result for DB recording
            result["article"] = article

            candidates.append(result)
            print(f"   [OK] Saved: {result.get('article_id', 'unknown')}")

        except Exception as e:
            print(f"   [ERROR] Saving {article.get('title', 'unknown')[:30]}: {e}")

    # Create/update manifest with all candidates
    if candidates:
        try:
            manifest_path = r2.save_manifest(candidates)
            print(f"   [MANIFEST] Saved: {manifest_path}")
        except Exception as e:
            print(f"   [WARN] Failed to save manifest: {e}")

    # =================================================================
    # NEW: Record to Supabase for cross-edition tracking
    # =================================================================
    print(f"\n[DB] Recording to Supabase...")
    db_result = record_batch_to_db(candidates, status="candidate")

    if db_result.get("db_available"):
        print(f"   [STATS] Recorded: {db_result['recorded']}, Skipped: {db_result['skipped']}, Failed: {db_result['failed']}")
    else:
        print(f"   [SKIP] Supabase not configured")

    return candidates


# =============================================================================
# Main Pipeline
# =============================================================================

async def run_pipeline(
    source_ids: Optional[list[str]] = None,
    hours: int = DEFAULT_HOURS_LOOKBACK,
    skip_scraping: bool = False,
    skip_filter: bool = False,
):
    """
    Run the custom scrapers pipeline.

    Args:
        source_ids: List of custom scraper IDs to run (None = all)
        hours: How many hours back to look
        skip_scraping: Skip Browserless content scraping
        skip_filter: Skip AI content filtering
    """
    # Get available custom scrapers
    available_scrapers = get_custom_scraper_ids()

    # Determine which scrapers to run
    if source_ids:
        # Validate provided sources are custom scrapers
        valid_sources = []
        for sid in source_ids:
            if sid in CUSTOM_SCRAPER_MAP:
                valid_sources.append(sid)
            else:
                print(f"[WARN] Skipping {sid}: not a valid custom scraper")
    else:
        # Run all available custom scrapers
        valid_sources = [s for s in available_scrapers if s in CUSTOM_SCRAPER_MAP]

    if not valid_sources:
        print("[ERROR] No valid custom scrapers to run. Exiting.")
        return

    # Log pipeline start
    print(f"\n{'=' * 60}")
    print("[START] ADUmedia Custom Scrapers Pipeline")
    print(f"{'=' * 60}")
    print(f"[DATE] {datetime.now().strftime('%B %d, %Y at %H:%M')}")
    print(f"[SCRAPERS] {len(valid_sources)}")
    print(f"   {', '.join(valid_sources)}")
    print(f"[LOOKBACK] {hours} hours")
    print(f"[FILTER] {'disabled' if skip_filter else 'enabled'}")
    print(f"[SCRAPING] {'disabled' if skip_scraping else 'enabled'}")
    print(f"{'=' * 60}")

    scraper = None
    r2 = None
    excluded_articles = []

    try:
        # Initialize R2 storage
        try:
            r2 = R2Storage()
            print("[OK] R2 storage connected")
        except Exception as e:
            print(f"[WARN] R2 not configured: {e}")
            r2 = None

        # Test Supabase connection (optional)
        if test_db_connection():
            print("[OK] Supabase connected")
        else:
            print("[INFO] Supabase not configured (articles won't be tracked in DB)")

        # =================================================================
        # Step 1: Run Custom Scrapers
        # =================================================================
        print("\n[STEP 1] Running custom scrapers...")

        all_articles = []

        for source_id in valid_sources:
            print(f"\n   [{source_id}] Starting...")
            try:
                scraper_class = CUSTOM_SCRAPER_MAP[source_id]
                custom_scraper = scraper_class()
                articles = await custom_scraper.fetch_articles(hours=hours)

                if articles:
                    all_articles.extend(articles)
                    print(f"   [{source_id}] Found {len(articles)} new articles")
                else:
                    print(f"   [{source_id}] No new articles")

                # Close the custom scraper
                await custom_scraper.close()

            except Exception as e:
                print(f"   [{source_id}] Error: {e}")

        articles = all_articles
        print(f"\n[TOTAL] Total new articles: {len(articles)}")

        if not articles:
            print("\n[EMPTY] No new articles found. Exiting.")
            return

        # =================================================================
        # Step 2: Scrape Full Content
        # =================================================================
        if not skip_scraping and articles:
            print("\n[STEP 2] Scraping full article content...")
            try:
                scraper = ArticleScraper()
                # ArticleScraper initializes browsers in scrape_articles
                articles = await scraper.scrape_articles(articles)
                print(f"   [STATS] Scraped {len(articles)} articles")
            except Exception as e:
                print(f"   [ERROR] Scraping failed: {e}")
                print("   Continuing with basic article data...")
        else:
            print("\n[STEP 2] Skipping content scraping (--no-scrape)")

        # =================================================================
        # Step 2.5: Download Hero Images (for articles that need it)
        # =================================================================
        if articles:
            print("\n[STEP 2.5] Downloading and converting hero images...")
            try:
                articles = await download_hero_images(articles)
            except Exception as e:
                print(f"   [ERROR] Image download failed: {e}")
                print("   Continuing without images...")

        # =================================================================
        # Step 3: AI Content Filtering (BEFORE summaries - saves API costs)
        # =================================================================
        if not skip_filter and articles:
            print("\n[STEP 3] AI content filtering...")
            try:
                llm = create_llm()
                articles, excluded_articles = filter_articles(articles, llm)

                print(f"\n   [STATS] Filtered: {len(articles)} included, {len(excluded_articles)} excluded")

                if not articles:
                    print("\n[EMPTY] All articles filtered out. Exiting.")
                    return

            except Exception as e:
                print(f"   [ERROR] AI filtering failed: {e}")
                print("   Continuing with all articles...")
        else:
            print("\n[STEP 3] Skipping AI filter (--no-filter)")

        # =================================================================
        # Step 4: Generate AI Summaries (only for filtered articles)
        # =================================================================
        print("\n[STEP 4] Generating AI summaries...")

        try:
            llm = create_llm()
            articles = generate_summaries(articles, llm, SUMMARIZE_PROMPT_TEMPLATE)
        except Exception as e:
            print(f"   [ERROR] AI summarization failed: {e}")
            for article in articles:
                if not article.get("ai_summary"):
                    article["headline"] = article.get("title", "")
                    article["ai_summary"] = article.get("description", "")[:200] + "..."
                    article["tag"] = ""

        # =================================================================
        # Step 5: Save to R2 Storage + Record to Supabase
        # =================================================================
        if r2:
            print("\n[STEP 5] Saving to R2 storage and recording to database...")
            save_candidates_to_r2(articles, r2)
        else:
            print("\n[STEP 5] Skipping R2 storage (not configured)")

        # =================================================================
        # Done
        # =================================================================
        print(f"\n{'=' * 60}")
        print("[DONE] Pipeline completed!")
        print(f"   Articles processed: {len(articles)}")
        print(f"   Articles excluded: {len(excluded_articles)}")
        print(f"{'=' * 60}")

    finally:
        if scraper:
            await scraper.close()


# =============================================================================
# Utility Functions
# =============================================================================

def list_available_scrapers():
    """List all available custom scrapers."""
    print("\n[LIST] Available Custom Scrapers")
    print("=" * 60)

    all_custom = get_custom_scraper_ids()

    print(f"\n{'Source ID':<35} {'Name':<25} {'Status':<12}")
    print("-" * 72)

    for source_id in all_custom:
        config = SOURCES.get(source_id, {})
        name = config.get("name", source_id)
        if source_id in CUSTOM_SCRAPER_MAP:
            status = "Ready"
        else:
            status = "Not implemented"
        print(f"{source_id:<35} {name:<25} {status:<12}")

    implemented_count = len([s for s in all_custom if s in CUSTOM_SCRAPER_MAP])
    print(f"\n[TOTAL] {len(all_custom)} configured, {implemented_count} implemented")
    print()


if __name__ == "__main__":
    args = parse_args()

    if args.list_sources:
        list_available_scrapers()
    else:
        asyncio.run(run_pipeline(
            source_ids=args.sources,
            hours=args.hours,
            skip_scraping=args.no_scrape,
            skip_filter=args.no_filter,
        ))