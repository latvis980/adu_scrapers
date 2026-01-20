# operators/custom_scrapers/__init__.py
"""
Custom Scrapers Package
Contains custom scrapers for sites without working RSS feeds.

Usage:
    from operators.custom_scrapers import get_custom_scraper, fetch_custom_source

    # Get a specific scraper
    scraper = get_custom_scraper("landezine")
    articles = await scraper.fetch_articles(hours=24)

    # Or use convenience function
    articles = await fetch_custom_source("landezine", hours=24)
"""

from operators.custom_scraper_base import (
    BaseCustomScraper,
    CustomScraperRegistry,
    custom_scraper_registry
)

# Import all custom scrapers to auto-register them
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

# Convenience functions
def get_custom_scraper(source_id: str) -> BaseCustomScraper:
    """
    Get a custom scraper instance by source_id.

    Args:
        source_id: Source identifier (e.g., 'landezine')

    Returns:
        Scraper instance

    Raises:
        ValueError: If scraper not found
    """
    scraper = custom_scraper_registry.get(source_id)
    if not scraper:
        raise ValueError(f"No custom scraper registered for: {source_id}")
    return scraper


def has_custom_scraper(source_id: str) -> bool:
    """Check if a custom scraper exists for a source."""
    return custom_scraper_registry.has_scraper(source_id)


def list_custom_scrapers() -> list[str]:
    """List all available custom scraper source_ids."""
    return custom_scraper_registry.list_scrapers()


async def fetch_custom_source(source_id: str, hours: int = 24) -> list[dict]:
    """
    Convenience function to fetch articles from a custom scraper.

    Args:
        source_id: Source identifier
        hours: Hours to look back

    Returns:
        List of article dicts
    """
    scraper = get_custom_scraper(source_id)
    try:
        articles = await scraper.fetch_articles(hours)
        return articles
    finally:
        await scraper.close()


# Export main classes and functions
__all__ = [
    'BaseCustomScraper',
    'CustomScraperRegistry',
    'custom_scraper_registry',
    'IdentityScraper',
    'ArchipositionScraper',
    'ProRusScraper',
    'BauweltScraper',
    'GoooodScraper',
    'JapanArchitectsScraper',
    'DomusScraper',
    'MetalocusScraper',
    'MetropolisScraper',
    'WorldLandscapeArchitectScraper',
    'LandscapeArchitectureMagazineScraper',
    'get_custom_scraper',
    'has_custom_scraper',
    'list_custom_scrapers',
    'fetch_custom_source',
]