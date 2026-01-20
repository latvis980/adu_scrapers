# config/sources.py
"""
News Source Registry - Custom Scrapers Pipeline
Configuration for custom scraper sources only (sites without working RSS feeds).

This is the custom scrapers version for the dedicated scraping service.
RSS feeds are handled by a separate service.

Organization:
    - Sources organized by region
    - All sources use custom scrapers (no RSS)

Usage:
    from config.sources import get_source_name, get_source_config, SOURCES
    from config.sources import get_custom_scraper_ids
"""

from urllib.parse import urlparse
from typing import Optional


# =============================================================================
# Custom Scraper Source Configuration
# =============================================================================

SOURCES = {
    # =========================================================================
    # Middle East
    # =========================================================================

    "identity": {
        "id": "identity",
        "name": "Identity Magazine",
        "domains": ["identity.ae", "www.identity.ae"],
        "tier": 2,
        "region": "middle_east",
        "custom_scraper": True,
    },

    # =========================================================================
    # Asia-Pacific
    # =========================================================================

    "archiposition": {
        "id": "archiposition",
        "name": "Archiposition",
        "domains": ["archiposition.com", "www.archiposition.com"],
        "tier": 2,
        "region": "asia_pacific",
        "custom_scraper": True,
    },
    "gooood": {
        "id": "gooood",
        "name": "Gooood",
        "domains": ["gooood.cn", "www.gooood.cn"],
        "tier": 2,
        "region": "asia_pacific",
        "custom_scraper": True,
    },
    "japan_architects": {
        "id": "japan_architects",
        "name": "Japan Architects",
        "domains": ["japan-architects.com", "www.japan-architects.com"],
        "tier": 2,
        "region": "asia_pacific",
        "custom_scraper": True,
    },

    # =========================================================================
    # Europe
    # =========================================================================

    "prorus": {
        "id": "prorus",
        "name": "ProRus",
        "domains": ["prorus.ru", "www.prorus.ru"],
        "tier": 2,
        "region": "europe",
        "custom_scraper": True,
    },
    "bauwelt": {
        "id": "bauwelt",
        "name": "Bauwelt",
        "domains": ["bauwelt.de", "www.bauwelt.de"],
        "tier": 2,
        "region": "europe",
        "custom_scraper": True,
    },
    "domus": {
        "id": "domus",
        "name": "Domus",
        "domains": ["domusweb.it", "www.domusweb.it"],
        "tier": 2,
        "region": "europe",
        "custom_scraper": True,
    },
    "metalocus": {
        "id": "metalocus",
        "name": "Metalocus",
        "domains": ["metalocus.es", "www.metalocus.es"],
        "tier": 2,
        "region": "europe",
        "custom_scraper": True,
    },

    # =========================================================================
    # North America
    # =========================================================================

    "metropolis": {
        "id": "metropolis",
        "name": "Metropolis",
        "domains": ["metropolismag.com", "www.metropolismag.com"],
        "tier": 2,
        "region": "north_america",
        "custom_scraper": True,
    },
    "landscape_architecture_magazine": {
        "id": "landscape_architecture_magazine",
        "name": "Landscape Architecture Magazine",
        "domains": ["landscapearchitecturemagazine.org"],
        "tier": 2,
        "region": "north_america",
        "category": "landscape",
        "custom_scraper": True,
    },

    # =========================================================================
    # International
    # =========================================================================

    "world_landscape_architect": {
        "id": "world_landscape_architect",
        "name": "World Landscape Architect",
        "domains": ["worldlandscapearchitect.com"],
        "tier": 2,
        "region": "international",
        "category": "landscape",
        "custom_scraper": True,
    },
}


# =============================================================================
# Build Lookup Tables
# =============================================================================

_DOMAIN_TO_SOURCE = {}
for source_id, config in SOURCES.items():
    for domain in config["domains"]:
        _DOMAIN_TO_SOURCE[domain.lower()] = source_id


# =============================================================================
# Core Functions
# =============================================================================

def get_source_id(url: str) -> Optional[str]:
    """Get source ID from URL."""
    if not url:
        return None
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        return _DOMAIN_TO_SOURCE.get(domain)
    except Exception:
        return None


def get_source_name(url: str) -> str:
    """Get display name for a source URL."""
    if not url:
        return "Source"

    source_id = get_source_id(url)

    if source_id and source_id in SOURCES:
        return SOURCES[source_id]["name"]

    # Fallback: clean up domain name
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower().replace("www.", "")
        parts = domain.split(".")
        if parts:
            return parts[0].capitalize()
    except Exception:
        pass

    return "Source"


def get_source_config(source_id: str) -> Optional[dict]:
    """Get full configuration for a source."""
    return SOURCES.get(source_id)


# =============================================================================
# Filtering Functions
# =============================================================================

def get_custom_scraper_ids() -> list[str]:
    """Get all source IDs that use custom scrapers."""
    return list(SOURCES.keys())


def get_sources_by_region(region: str) -> list[dict]:
    """Get all sources for a specific region."""
    result = []
    for source_id, config in SOURCES.items():
        if config.get("region") == region:
            result.append({"id": source_id, **config})
    return result


def get_all_source_ids() -> list[str]:
    """Get all source IDs."""
    return list(SOURCES.keys())


def is_custom_scraper(source_id: str) -> bool:
    """Check if a source uses custom scraper. Always True for this service."""
    return source_id in SOURCES


def get_source_stats() -> dict:
    """Get statistics about configured sources."""
    stats = {
        "total": len(SOURCES),
        "rss_sources": 0,
        "custom_scrapers": len(SOURCES),
        "by_region": {},
    }

    for config in SOURCES.values():
        region = config.get("region", "unknown")
        stats["by_region"][region] = stats["by_region"].get(region, 0) + 1

    return stats


# =============================================================================
# Test
# =============================================================================

if __name__ == "__main__":
    print("=" * 50)
    print("ADUmedia Custom Scraper Sources")
    print("=" * 50)

    stats = get_source_stats()
    print(f"\nTotal custom scrapers: {stats['total']}")

    print("\nBy Region:")
    for region, count in sorted(stats["by_region"].items()):
        print(f"  {region}: {count}")

    print("\nAll Custom Scrapers:")
    for source_id in get_custom_scraper_ids():
        config = SOURCES[source_id]
        print(f"  {source_id:35} [{config['region']}] {config['name']}")