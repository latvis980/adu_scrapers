# main.py
"""
ADUmedia Custom Scrapers Pipeline

Dedicated pipeline for custom scraper sources (sites without working RSS feeds).
Runs independently from RSS pipeline.

Schedule: 18:30 Lisbon time (17:30 UTC in winter, 16:30 UTC in summer)

Pipeline:
    1. Run custom scrapers to discover new article URLs
    2. Scrape full article content (Browserless)
    3. AI content filtering
    4. Generate AI summaries (OpenAI)
    5. Save articles to R2 storage

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
    DATABASE_URL                - PostgreSQL connection string
"""

import asyncio
import argparse
from datetime import datetime
from typing import Optional

# Import operators
from operators.scraper import ArticleScraper
from operators.monitor import create_llm, summarize_article

# Import storage
from storage.r2 import R2Storage

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

def generate_summaries(articles: list, llm, prompt_template: str) -> list:
    """Generate AI summaries for articles."""
    print(f"\n Generating AI summaries for {len(articles)} articles...")

    for i, article in enumerate(articles, 1):
        title = article.get("title", "No title")
        source_name = article.get("source_name", article.get("source_id", "Unknown"))
        print(f"   [{i}/{len(articles)}] [{source_name}] {title[:40]}...")

        try:
            # summarize_article expects (article, llm, prompt_template)
            summarized = summarize_article(article, llm, prompt_template)
            article["ai_summary"] = summarized.get("ai_summary", "")
            article["tags"] = summarized.get("tags", [])
        except Exception as e:
            print(f"      Error: {e}")
            article["ai_summary"] = article.get("description", "")[:200] + "..."
            article["tags"] = []

    return articles


def save_candidates_to_r2(articles: list, r2: R2Storage) -> dict:
    """
    Save articles as editorial candidates to R2 storage.

    Args:
        articles: List of article dicts with ai_summary
        r2: R2Storage instance

    Returns:
        Dict with saved paths
    """
    print("\n Saving candidates to R2 storage...")

    # Reset counters for this batch
    r2.reset_counters()

    paths = {}
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

            paths[article["link"]] = result
            print(f"   Saved: {result.get('id', 'unknown')}")

        except Exception as e:
            print(f"   Error saving {article.get('title', 'unknown')[:30]}: {e}")

    return paths


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
                print(f" Skipping {sid}: not a valid custom scraper")
    else:
        # Run all available custom scrapers
        valid_sources = [s for s in available_scrapers if s in CUSTOM_SCRAPER_MAP]

    if not valid_sources:
        print(" No valid custom scrapers to run. Exiting.")
        return

    # Log pipeline start
    print(f"\n{'=' * 60}")
    print(" ADUmedia Custom Scrapers Pipeline")
    print(f"{'=' * 60}")
    print(f" {datetime.now().strftime('%B %d, %Y at %H:%M')}")
    print(f" Scrapers: {len(valid_sources)}")
    print(f"   {', '.join(valid_sources)}")
    print(f" Looking back: {hours} hours")
    print(f" Content filter: {'disabled' if skip_filter else 'enabled'}")
    print(f" Content scraping: {'disabled' if skip_scraping else 'enabled'}")
    print(f"{'=' * 60}")

    scraper = None
    r2 = None
    excluded_articles = []

    try:
        # Initialize R2 storage
        try:
            r2 = R2Storage()
            print(" R2 storage connected")
        except Exception as e:
            print(f" R2 not configured: {e}")
            r2 = None

        # =================================================================
        # Step 1: Run Custom Scrapers
        # =================================================================
        print("\n Step 1: Running custom scrapers...")

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
        print(f"\n Total new articles: {len(articles)}")

        if not articles:
            print("\n No new articles found. Exiting.")
            return

        # =================================================================
        # Step 2: Scrape Full Content
        # =================================================================
        if not skip_scraping and articles:
            print("\n Step 2: Scraping full article content...")
            try:
                scraper = ArticleScraper()
                # ArticleScraper initializes browsers in scrape_articles
                articles = await scraper.scrape_articles(articles)
                print(f"   Scraped {len(articles)} articles")
            except Exception as e:
                print(f"   Scraping failed: {e}")
                print("   Continuing with basic article data...")
        else:
            print("\n Step 2: Skipping content scraping (--no-scrape)")

        # =================================================================
        # Step 3: AI Content Filtering
        # =================================================================
        if not skip_filter and articles:
            print("\n Step 3: AI content filtering...")
            try:
                llm = create_llm()
                filtered = []

                for i, article in enumerate(articles, 1):
                    title = article.get("title", "No title")
                    print(f"   [{i}/{len(articles)}] {title[:50]}...")

                    prompt = FILTER_PROMPT_TEMPLATE.format(
                        title=title,
                        description=article.get("description", "")[:500],
                        content=article.get("full_content", article.get("content", ""))[:1000]
                    )

                    response = llm.invoke(prompt)
                    result = parse_filter_response(response.content)

                    if result.get("include", True):
                        filtered.append(article)
                        print("      Included")
                    else:
                        excluded_articles.append(article)
                        print(f"      Excluded: {result.get('reason', 'N/A')}")

                articles = filtered
                print(f"\n   Filtered: {len(articles)} included, {len(excluded_articles)} excluded")

                if not articles:
                    print("\n All articles filtered out. Exiting.")
                    return

            except Exception as e:
                print(f"   AI filtering failed: {e}")
                print("   Continuing with all articles...")
        else:
            print("\n Step 3: Skipping AI filter (--no-filter)")

        # =================================================================
        # Step 4: Generate AI Summaries
        # =================================================================
        print("\n Step 4: Generating AI summaries...")

        try:
            llm = create_llm()
            articles = generate_summaries(articles, llm, SUMMARIZE_PROMPT_TEMPLATE)
        except Exception as e:
            print(f"   AI summarization failed: {e}")
            for article in articles:
                if not article.get("ai_summary"):
                    article["ai_summary"] = article.get("description", "")[:200] + "..."
                    article["tags"] = []

        # =================================================================
        # Step 5: Save to R2 Storage
        # =================================================================
        if r2:
            save_candidates_to_r2(articles, r2)
        else:
            print("\n Step 5: Skipping R2 storage (not configured)")

        # =================================================================
        # Done
        # =================================================================
        print(f"\n{'=' * 60}")
        print(" Pipeline completed!")
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
    print("\n Available Custom Scrapers")
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
    print(f"\n Total: {len(all_custom)} configured, {implemented_count} implemented")
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