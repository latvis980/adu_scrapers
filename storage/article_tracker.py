# storage/article_tracker.py
"""
Article Tracker - PostgreSQL Database for Custom Scrapers

Tracks seen article URLs to prevent reprocessing.
Simple URL-based tracking: store URLs when discovered, filter against them next run.

Database Schema:
    - articles table: stores seen URLs per source
    - Indexed by source_id and url for fast lookups

Usage:
    tracker = ArticleTracker()
    await tracker.connect()

    # URL tracking workflow  
    new_urls = await tracker.filter_new_articles(source_id, url_list)
    await tracker.mark_as_seen(source_id, url_list)
"""

import os
import asyncpg
from typing import Optional, List


class ArticleTracker:
    """PostgreSQL-based article URL tracking for custom scrapers."""

    # ========================================
    # TEST MODE - Set to True to ignore "seen" status
    # This makes all articles appear as "new" for testing
    # Set via environment variable: SCRAPER_TEST_MODE=true
    # ========================================
    TEST_MODE = os.getenv("SCRAPER_TEST_MODE", "").lower() == "true"

    def __init__(self, connection_url: Optional[str] = None):
        """
        Initialize article tracker.

        Args:
            connection_url: PostgreSQL connection URL (defaults to DATABASE_URL env var)
        """
        self.connection_url = connection_url or os.getenv("DATABASE_URL")

        if not self.connection_url:
            raise ValueError("DATABASE_URL environment variable not set")

        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        """Connect to PostgreSQL and initialize schema."""
        if self.pool:
            return

        # Create connection pool
        self.pool = await asyncpg.create_pool(
            self.connection_url,
            min_size=1,
            max_size=5,
            command_timeout=60
        )

        # Initialize schema
        await self._init_schema()

        # Show mode status
        if self.TEST_MODE:
            print("⚠️  Article tracker TEST MODE ENABLED - all articles will appear as 'new'")

        print("✅ Article tracker connected to PostgreSQL")

    async def _init_schema(self):
        """Create articles table if it doesn't exist."""
        if not self.pool:
            raise RuntimeError("Not connected to database")

        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS articles (
                    id SERIAL PRIMARY KEY,
                    source_id VARCHAR(100) NOT NULL,
                    url TEXT NOT NULL,
                    first_seen TIMESTAMP DEFAULT NOW(),
                    last_checked TIMESTAMP DEFAULT NOW(),
                    UNIQUE(source_id, url)
                )
            """)

            # Create index for fast lookups
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_articles_source_url 
                ON articles(source_id, url)
            """)

        print("✅ Article tracker schema initialized")

    # =========================================================================
    # URL Tracking - Core Methods
    # =========================================================================

    async def filter_new_articles(self, source_id: str, urls: List[str]) -> List[str]:
        """
        Filter list of URLs to only those not seen before.

        This is the main method for detecting new articles.
        Respects TEST_MODE: when enabled, returns all URLs as "new".

        Args:
            source_id: Source identifier (e.g., 'bauwelt')
            urls: List of article URLs found on homepage

        Returns:
            List of URLs not previously seen (new articles)
        """
        if not self.pool:
            raise RuntimeError("Not connected to database")

        if not urls:
            return []

        # TEST MODE: Return all URLs as "new" for testing
        if self.TEST_MODE:
            print(f"   ⚠️  TEST MODE: Returning ALL {len(urls)} URLs as 'new'")
            return urls

        async with self.pool.acquire() as conn:
            # Get all existing URLs for this source (batch lookup)
            rows = await conn.fetch("""
                SELECT url FROM articles
                WHERE source_id = $1 AND url = ANY($2)
            """, source_id, urls)

            seen_urls = set(row['url'] for row in rows)

            # Return URLs not in database
            new_urls = [url for url in urls if url not in seen_urls]

            print(f"   Database: {len(seen_urls)} seen, {len(new_urls)} new")

            return new_urls

    async def mark_as_seen(self, source_id: str, urls: List[str]) -> int:
        """
        Mark URLs as seen in the database.

        Call this after discovering URLs on homepage to track them
        for future runs.

        Args:
            source_id: Source identifier
            urls: List of article URLs to mark as seen

        Returns:
            Number of URLs marked as seen
        """
        if not self.pool:
            raise RuntimeError("Not connected to database")

        if not urls:
            return 0

        marked = 0

        async with self.pool.acquire() as conn:
            for url in urls:
                try:
                    await conn.execute("""
                        INSERT INTO articles (source_id, url)
                        VALUES ($1, $2)
                        ON CONFLICT (source_id, url) DO UPDATE
                        SET last_checked = NOW()
                    """, source_id, url)

                    marked += 1

                except Exception as e:
                    print(f"   ⚠️  Error marking URL as seen: {e}")
                    continue

        print(f"   Marked {marked} URLs as seen in database")
        return marked

    async def is_seen(self, source_id: str, url: str) -> bool:
        """
        Check if a single URL has been seen before.

        Args:
            source_id: Source identifier
            url: Article URL to check

        Returns:
            True if URL exists in database
        """
        if not self.pool:
            raise RuntimeError("Not connected to database")

        # TEST MODE: Always return False (not seen)
        if self.TEST_MODE:
            return False

        async with self.pool.acquire() as conn:
            exists = await conn.fetchval("""
                SELECT EXISTS(
                    SELECT 1 FROM articles
                    WHERE source_id = $1 AND url = $2
                )
            """, source_id, url)

            return bool(exists)

    # =========================================================================
    # Statistics
    # =========================================================================

    async def get_stats(self, source_id: Optional[str] = None) -> dict:
        """
        Get statistics about tracked articles.

        Args:
            source_id: Optional source to filter by (None = all sources)

        Returns:
            Dict with statistics
        """
        if not self.pool:
            raise RuntimeError("Not connected to database")

        async with self.pool.acquire() as conn:
            if source_id:
                count = await conn.fetchval("""
                    SELECT COUNT(*) FROM articles WHERE source_id = $1
                """, source_id)

                oldest = await conn.fetchval("""
                    SELECT first_seen FROM articles
                    WHERE source_id = $1
                    ORDER BY first_seen ASC LIMIT 1
                """, source_id)

                newest = await conn.fetchval("""
                    SELECT first_seen FROM articles
                    WHERE source_id = $1
                    ORDER BY first_seen DESC LIMIT 1
                """, source_id)
            else:
                count = await conn.fetchval("SELECT COUNT(*) FROM articles")
                oldest = await conn.fetchval("""
                    SELECT first_seen FROM articles
                    ORDER BY first_seen ASC LIMIT 1
                """)
                newest = await conn.fetchval("""
                    SELECT first_seen FROM articles
                    ORDER BY first_seen DESC LIMIT 1
                """)

            return {
                "total_articles": count or 0,
                "oldest_seen": oldest.isoformat() if oldest else None,
                "newest_seen": newest.isoformat() if newest else None,
            }

    async def get_source_counts(self) -> dict:
        """
        Get article counts per source.

        Returns:
            Dict mapping source_id to count
        """
        if not self.pool:
            raise RuntimeError("Not connected to database")

        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT source_id, COUNT(*) as count
                FROM articles
                GROUP BY source_id
                ORDER BY count DESC
            """)

            return {row['source_id']: row['count'] for row in rows}

    # =========================================================================
    # Maintenance
    # =========================================================================

    async def clear_source(self, source_id: str) -> int:
        """
        Clear all tracked articles for a source.
        Useful for resetting a scraper's state.

        Args:
            source_id: Source identifier

        Returns:
            Number of articles deleted
        """
        if not self.pool:
            raise RuntimeError("Not connected to database")

        async with self.pool.acquire() as conn:
            result = await conn.execute("""
                DELETE FROM articles WHERE source_id = $1
            """, source_id)

            # Extract count from result
            deleted = int(result.split()[-1])
            print(f"[{source_id}] Cleared {deleted} tracked URLs")
            return deleted

    async def clear_all(self) -> int:
        """
        Clear ALL tracked articles (all sources).
        Use with caution!

        Returns:
            Number of articles deleted
        """
        if not self.pool:
            raise RuntimeError("Not connected to database")

        async with self.pool.acquire() as conn:
            result = await conn.execute("DELETE FROM articles")
            deleted = int(result.split()[-1])
            print(f"⚠️  Cleared ALL {deleted} tracked URLs from database")
            return deleted

    # =========================================================================
    # Cleanup
    # =========================================================================

    async def close(self):
        """Close database connection pool."""
        if self.pool:
            await self.pool.close()
            self.pool = None
            print("✅ Article tracker disconnected")