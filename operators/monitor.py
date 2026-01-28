# operators/monitor.py
"""
Architecture News Monitor
Collects news from multiple RSS feeds and summarizes with AI.

Usage:
    # As module (called from main.py):
    from operators.monitor import run_monitor, fetch_rss_feed

    # Single source:
    articles = await run_monitor(source_id="archdaily")

    # Multiple sources:
    articles = await run_multi_source_monitor(source_ids=["archdaily", "dezeen"])

    # All Tier 1 sources:
    articles = await run_tier1_monitor()

Environment Variables (set in Railway):
    OPENAI_API_KEY - OpenAI API key for GPT-4o-mini
"""

import os
import feedparser
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

from langchain_openai import ChatOpenAI

from prompts.summarize import SUMMARIZE_PROMPT_TEMPLATE
from prompts.summarize import parse_summary_response
from config.sources import (
    get_source_config,
    get_sources_by_tier,
    get_all_rss_sources,
)


# Configuration
HOURS_LOOKBACK = 24  # Collect articles from last N hours


def fetch_rss_feed(
    url: str, 
    hours: int = 24,
    source_id: Optional[str] = None
) -> list[dict]:
    """
    Fetch and parse RSS feed, return entries from last N hours.

    Args:
        url: RSS feed URL
        hours: Look back this many hours
        source_id: Optional source ID to attach to articles

    Returns:
        List of article dicts
    """
    print(f"📡 Fetching RSS feed: {url}")

    feed = feedparser.parse(url)

    # Check for errors
    if feed.bozo:
        print(f"⚠️ Feed warning: {feed.bozo_exception}")

    # Filter articles from specified time window
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
    recent_articles = []

    for entry in feed.entries:
        # Parse published date
        pub_date = None
        if hasattr(entry, 'published_parsed') and entry.published_parsed:
            pub_date = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
            pub_date = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)

        # Include if within time window (or if no date available)
        if pub_date is None or pub_date >= cutoff_time:
            article = {
                "title": entry.get("title", "No title"),
                "link": entry.get("link", ""),
                "description": entry.get("summary", ""),
                "published": pub_date.isoformat() if pub_date else None,
                "guid": entry.get("id", entry.get("link", "")),
                "source_id": source_id,
            }
            recent_articles.append(article)

    print(f"📰 Found {len(recent_articles)} articles from last {hours} hours")
    return recent_articles


def fetch_source(source_id: str, hours: int = 24) -> list[dict]:
    """
    Fetch articles from a configured source.

    Args:
        source_id: Source ID from sources registry
        hours: Look back this many hours

    Returns:
        List of article dicts with source_id attached
    """
    config = get_source_config(source_id)
    if not config:
        print(f"⚠️ Unknown source: {source_id}")
        return []

    rss_url = config.get("rss_url")
    if not rss_url:
        print(f"⚠️ No RSS URL for source: {source_id}")
        return []

    source_name = config.get("name", source_id)
    print(f"\n📡 Fetching {source_name}...")

    return fetch_rss_feed(rss_url, hours, source_id)


def create_llm():
    """Create and configure the LLM instance."""
    api_key = os.getenv("OPENAI_API_KEY")

    if not api_key:
        raise ValueError("OPENAI_API_KEY not set in environment")

    return ChatOpenAI(
        model="gpt-4o-mini",
        api_key=api_key,
        max_tokens=300,
        temperature=0.3  # Lower temperature for more consistent summaries
    )


def summarize_article(article: dict, llm, prompt_template) -> dict:
    """
    Generate AI summary for an article.

    Args:
        article: Article dict with title, description, link
        llm: LangChain LLM instance
        prompt_template: LangChain prompt template

    Returns:
        Article dict with added ai_summary and tags
    """
    from datetime import datetime

    print(f"🤖 Summarizing: {article['title'][:50]}...")

    # Get current date for temporal context
    current_date = datetime.now().strftime("%B %d, %Y")  # e.g., "January 15, 2026"

    # Create chain and invoke
    chain = prompt_template | llm

    response = chain.invoke({
        "title": article["title"],
        "description": article["description"],
        "url": article["link"],
        "current_date": current_date
    })

    # Parse response
    parsed = parse_summary_response(response.content)

    # Add to article
    article["headline"] = parsed["headline"]
    article["ai_summary"] = parsed["summary"]
    article["tag"] = parsed["tag"]

    return article


# =============================================================================
# Main Monitor Functions
# =============================================================================

async def run_monitor(
    source_id: str = "archdaily",
    hours: int = HOURS_LOOKBACK,
    skip_summary: bool = False
) -> list[dict]:
    """
    Monitor a single source - fetches RSS and optionally generates AI summaries.

    Args:
        source_id: Source ID from registry (e.g., 'archdaily', 'dezeen')
        hours: How many hours back to look for articles
        skip_summary: If True, skip AI summarization

    Returns:
        List of article dicts with ai_summary and tags added
    """
    # Fetch articles
    articles = fetch_source(source_id, hours)

    if not articles:
        print("📭 No new articles found")
        return []

    if skip_summary:
        return articles

    # Validate API key
    if not os.getenv("OPENAI_API_KEY"):
        raise ValueError("OPENAI_API_KEY not set in environment")

    # Initialize LLM
    print("🔧 Initializing AI (GPT-4o-mini)...")
    llm = create_llm()

    # Generate summaries
    print(f"📝 Generating summaries for {len(articles)} articles...")
    summarized_articles = []

    for article in articles:
        try:
            summarized = summarize_article(article, llm, SUMMARIZE_PROMPT_TEMPLATE)
            summarized_articles.append(summarized)
        except Exception as e:
            print(f"⚠️ Error summarizing '{article['title'][:30]}...': {e}")
            # Fallback: use original description
            article["headline"] = article["title"]
            article["ai_summary"] = article["description"][:200] + "..."
            article["tag"] = ""
            summarized_articles.append(article)

    print(f"✅ Summarized {len(summarized_articles)} articles")
    return summarized_articles


async def run_multi_source_monitor(
    source_ids: list[str],
    hours: int = HOURS_LOOKBACK,
    skip_summary: bool = False
) -> dict[str, list[dict]]:
    """
    Monitor multiple sources.

    Args:
        source_ids: List of source IDs to monitor
        hours: How many hours back to look for articles
        skip_summary: If True, skip AI summarization

    Returns:
        Dict mapping source_id to list of articles
    """
    results = {}

    for source_id in source_ids:
        try:
            articles = await run_monitor(source_id, hours, skip_summary)
            results[source_id] = articles
        except Exception as e:
            print(f"⚠️ Error monitoring {source_id}: {e}")
            results[source_id] = []

    # Summary
    total = sum(len(articles) for articles in results.values())
    print(f"\n📊 Total articles collected: {total} from {len(source_ids)} sources")

    return results


async def run_tier1_monitor(
    hours: int = HOURS_LOOKBACK,
    skip_summary: bool = False
) -> dict[str, list[dict]]:
    """
    Monitor all Tier 1 (primary) sources.

    Args:
        hours: How many hours back to look for articles
        skip_summary: If True, skip AI summarization

    Returns:
        Dict mapping source_id to list of articles
    """
    tier1_sources = get_sources_by_tier(1)
    source_ids = [s["id"] for s in tier1_sources]

    print(f"\n🏛️ Running Tier 1 Monitor ({len(source_ids)} sources)")
    print("=" * 50)

    return await run_multi_source_monitor(source_ids, hours, skip_summary)


async def run_tested_sources_monitor(
    hours: int = HOURS_LOOKBACK,
    skip_summary: bool = False
) -> dict[str, list[dict]]:
    """
    Monitor only tested and verified sources.

    Args:
        hours: How many hours back to look for articles
        skip_summary: If True, skip AI summarization

    Returns:
        Dict mapping source_id to list of articles
    """
    tested_sources = get_tested_sources()
    source_ids = [s["id"] for s in tested_sources]

    print(f"\n✓ Running Tested Sources Monitor ({len(source_ids)} sources)")
    print("=" * 50)

    return await run_multi_source_monitor(source_ids, hours, skip_summary)


# =============================================================================
# RSS Feed Testing
# =============================================================================

async def test_rss_feed(source_id: str) -> dict:
    """
    Test if an RSS feed is accessible and working.

    Args:
        source_id: Source ID to test

    Returns:
        Dict with test results
    """
    config = get_source_config(source_id)
    if not config:
        return {"source_id": source_id, "success": False, "error": "Unknown source"}

    rss_url = config.get("rss_url")
    if not rss_url:
        return {"source_id": source_id, "success": False, "error": "No RSS URL"}

    try:
        feed = feedparser.parse(rss_url)

        if feed.bozo and not feed.entries:
            return {
                "source_id": source_id,
                "success": False,
                "error": str(feed.bozo_exception),
                "url": rss_url,
            }

        return {
            "source_id": source_id,
            "success": True,
            "entries_count": len(feed.entries),
            "feed_title": feed.feed.get("title", "Unknown"),
            "url": rss_url,
        }

    except Exception as e:
        return {
            "source_id": source_id,
            "success": False,
            "error": str(e),
            "url": rss_url,
        }


async def test_all_feeds() -> list[dict]:
    """
    Test all configured RSS feeds.

    Returns:
        List of test results for each source
    """
    sources = get_all_rss_sources()
    results = []

    print("\n🧪 Testing All RSS Feeds")
    print("=" * 60)

    for source in sources:
        source_id = source["id"]
        result = await test_rss_feed(source_id)

        status = "✅" if result["success"] else "❌"
        print(f"{status} {source['name']}: ", end="")

        if result["success"]:
            print(f"{result['entries_count']} entries")
        else:
            print(f"Error - {result.get('error', 'Unknown')[:50]}")

        results.append(result)

        # Small delay to be nice to servers
        await asyncio.sleep(0.5)

    # Summary
    successful = sum(1 for r in results if r["success"])
    print("\n" + "=" * 60)
    print(f"📊 Results: {successful}/{len(results)} feeds working")

    return results


# =============================================================================
# Standalone Execution
# =============================================================================

async def main():
    """
    Standalone test - runs monitor and sends to Telegram.
    Use this for testing the monitor independently.
    """
    from telegram_bot import TelegramBot

    print("=" * 60)
    print("🏛️ Architecture News Monitor (Standalone Test)")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    # Check for test mode
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--test-feeds":
        await test_all_feeds()
        return

    # Validate required environment variables
    required_vars = ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHANNEL_ID", "OPENAI_API_KEY"]
    missing = [var for var in required_vars if not os.getenv(var)]

    if missing:
        print(f"❌ Missing environment variables: {', '.join(missing)}")
        print("Please set these in Railway dashboard.")
        return

    # Run monitor for tested sources only
    results = await run_tested_sources_monitor()

    # Combine all articles
    all_articles = []
    for source_id, articles in results.items():
        all_articles.extend(articles)

    if not all_articles:
        print("📭 No articles to send. Exiting.")
        return

    # Send to Telegram
    print("\n📱 Sending to Telegram...")
    try:
        bot = TelegramBot()
        results = await bot.send_digest(all_articles)

        print("=" * 60)
        print(f"✅ Complete! Sent {results['sent']} messages.")
        if results['failed'] > 0:
            print(f"⚠️ Failed: {results['failed']} messages")
        print("=" * 60)

    except Exception as e:
        print(f"❌ Telegram error: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())