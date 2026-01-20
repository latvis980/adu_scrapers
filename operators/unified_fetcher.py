# operators/unified_fetcher.py
"""
Unified News Fetcher
Combines RSS feeds and custom scrapers into a single interface.

Handles:
- RSS feeds (via RSSFetcher)
- Custom scrapers (via CustomScraper)
- Consistent output format
- Automatic source detection

Usage:
    from operators.unified_fetcher import UnifiedFetcher
    
    fetcher = UnifiedFetcher()
    articles = await fetcher.fetch_source("archdaily", hours=24)  # RSS
    
    # Or fetch all sources
    all_articles = await fetcher.fetch_all_sources(hours=24)
"""

import asyncio
from typing import List, Dict, Any, Optional

from operators.rss_fetcher import RSSFetcher
from operators.custom_scrapers import (
    has_custom_scraper,
    fetch_custom_source,
    list_custom_scrapers
)
from config.sources import get_source_config, get_all_rss_sources


class UnifiedFetcher:
    """
    Unified interface for fetching from both RSS and custom scrapers.
    
    Automatically detects whether to use RSS or custom scraper for each source.
    """
    
    def __init__(self):
        """Initialize the unified fetcher."""
        self.rss_fetcher = RSSFetcher()
        
        # Track which sources use which method
        self.custom_sources = set(list_custom_scrapers())
        
        print("[UnifiedFetcher] Initialized")
        print(f"   Custom scrapers: {len(self.custom_sources)}")
    
    async def fetch_source(
        self,
        source_id: str,
        hours: int = 24,
        max_articles: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch articles from a single source (RSS or custom).
        
        Args:
            source_id: Source identifier
            hours: How many hours back to look
            max_articles: Maximum articles to return
        
        Returns:
            List of article dicts
        """
        config = get_source_config(source_id)
        if not config:
            print(f"[UnifiedFetcher] Unknown source: {source_id}")
            return []
        
        # Check if custom scraper exists
        if has_custom_scraper(source_id):
            print(f"[UnifiedFetcher] Using custom scraper for: {source_id}")
            try:
                articles = await fetch_custom_source(source_id, hours)
                
                # Apply max_articles limit if specified
                if max_articles and len(articles) > max_articles:
                    articles = articles[:max_articles]
                
                return articles
            except Exception as e:
                print(f"[UnifiedFetcher] Custom scraper failed for {source_id}: {e}")
                return []
        
        # Otherwise use RSS
        rss_url = config.get("rss_url")
        if not rss_url:
            print(f"[UnifiedFetcher] No RSS URL for: {source_id}")
            return []
        
        print(f"[UnifiedFetcher] Using RSS for: {source_id}")
        return self.rss_fetcher.fetch_source(
            source_id,
            hours=hours,
            max_articles=max_articles
        )
    
    async def fetch_all_sources(
        self,
        hours: int = 24,
        source_ids: Optional[List[str]] = None,
        max_per_source: Optional[int] = None,
        include_custom: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Fetch articles from multiple sources.
        
        Args:
            hours: How many hours back to look
            source_ids: List of source IDs (None = all)
            max_per_source: Maximum articles per source
            include_custom: Whether to include custom scrapers
        
        Returns:
            Combined list of articles from all sources
        """
        all_articles: List[Dict[str, Any]] = []
        
        # Determine which sources to fetch
        if source_ids:
            sources_to_fetch = source_ids
        else:
            # Get all RSS sources
            rss_sources = [s["id"] for s in get_all_rss_sources()]
            
            # Add custom scrapers if enabled
            if include_custom:
                sources_to_fetch = list(set(rss_sources + list(self.custom_sources)))
            else:
                sources_to_fetch = rss_sources
        
        print(f"\n[UnifiedFetcher] Fetching {len(sources_to_fetch)} sources...")
        
        # Fetch from each source
        for source_id in sources_to_fetch:
            articles = await self.fetch_source(
                source_id,
                hours=hours,
                max_articles=max_per_source
            )
            all_articles.extend(articles)
        
        # Sort by publication date (newest first)
        all_articles.sort(
            key=lambda x: x.get("published") or "1970-01-01",
            reverse=True
        )
        
        print(f"[UnifiedFetcher] Total: {len(all_articles)} articles from {len(sources_to_fetch)} sources")
        return all_articles
    
    def get_fetch_method(self, source_id: str) -> str:
        """
        Get fetch method for a source.
        
        Args:
            source_id: Source identifier
        
        Returns:
            "custom" or "rss" or "unknown"
        """
        if has_custom_scraper(source_id):
            return "custom"
        
        config = get_source_config(source_id)
        if config and config.get("rss_url"):
            return "rss"
        
        return "unknown"
    
    def list_all_sources(self) -> Dict[str, List[str]]:
        """
        List all available sources by fetch method.
        
        Returns:
            Dict with "rss" and "custom" keys, each with list of source_ids
        """
        rss_sources = [s["id"] for s in get_all_rss_sources()]
        custom_sources = list(self.custom_sources)
        
        return {
            "rss": rss_sources,
            "custom": custom_sources,
            "total": len(set(rss_sources + custom_sources))
        }


# =============================================================================
# Convenience Functions
# =============================================================================

async def fetch_unified(
    source_id: str,
    hours: int = 24
) -> List[Dict[str, Any]]:
    """
    Quick function to fetch from a single source (auto-detect method).
    
    Args:
        source_id: Source identifier
        hours: Hours to look back
    
    Returns:
        List of article dicts
    """
    fetcher = UnifiedFetcher()
    return await fetcher.fetch_source(source_id, hours)


async def fetch_all_unified(
    hours: int = 24,
    sources: Optional[List[str]] = None,
    include_custom: bool = True
) -> List[Dict[str, Any]]:
    """
    Quick function to fetch from multiple sources.
    
    Args:
        hours: Hours to look back
        sources: List of source IDs (None = all)
        include_custom: Whether to include custom scrapers
    
    Returns:
        Combined list of articles
    """
    fetcher = UnifiedFetcher()
    return await fetcher.fetch_all_sources(hours, sources, include_custom=include_custom)


# =============================================================================
# Standalone Test
# =============================================================================

async def test_unified_fetcher():
    """Test the unified fetcher."""
    print("=" * 60)
    print("Unified Fetcher Test")
    print("=" * 60)
    
    fetcher = UnifiedFetcher()
    
    # Show available sources
    sources = fetcher.list_all_sources()
    print("\n📊 Available sources:")
    print(f"   RSS sources: {len(sources['rss'])}")
    print(f"   Custom scrapers: {len(sources['custom'])}")
    print(f"   Total unique: {sources['total']}")
    
    # Test custom scraper
    if sources['custom']:
        custom_source = sources['custom'][0]
        print(f"\n🔍 Testing custom scraper: {custom_source}")
        articles = await fetcher.fetch_source(custom_source, hours=24*7, max_articles=3)
        
        for i, article in enumerate(articles, 1):
            print(f"\n   Article {i}:")
            print(f"     Title: {article['title'][:50]}...")
            print(f"     Link: {article['link']}")
            print(f"     Published: {article.get('published', 'N/A')}")
    
    # Test RSS source
    if sources['rss']:
        rss_source = sources['rss'][0]
        print(f"\n📡 Testing RSS: {rss_source}")
        articles = await fetcher.fetch_source(rss_source, hours=24, max_articles=3)
        
        for i, article in enumerate(articles, 1):
            print(f"\n   Article {i}:")
            print(f"     Title: {article['title'][:50]}...")
            print(f"     Published: {article.get('published', 'N/A')}")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    asyncio.run(test_unified_fetcher())
