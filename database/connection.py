# database/connection.py
"""
Supabase Connection Handler for ADUmedia RSS/Scrapers

Provides connection to Supabase database for recording articles
at fetch time (not just publish time).

Environment Variables:
    SUPABASE_URL    - Supabase project URL
    SUPABASE_KEY    - Supabase API key (anon or service role)
"""

import os
from typing import Optional
from datetime import date, datetime


# Try to import supabase, but don't fail if not installed
try:
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    Client = None


# Global client instance
_client: Optional[Client] = None


def get_supabase_client() -> Optional[Client]:
    """
    Get or create Supabase client instance.
    
    Returns:
        Supabase client or None if not configured
    """
    global _client
    
    if not SUPABASE_AVAILABLE:
        return None
    
    if _client is not None:
        return _client
    
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    
    if not url or not key:
        return None
    
    try:
        _client = create_client(url, key)
        print(f"[DB] Connected to Supabase")
        return _client
    except Exception as e:
        print(f"[DB] Failed to connect to Supabase: {e}")
        return None


def record_article_to_db(
    article: dict,
    r2_path: str,
    r2_image_path: Optional[str] = None,
    status: str = "fetched"
) -> Optional[str]:
    """
    Record an article to Supabase all_articles table.
    
    This is called immediately after saving to R2, so we have
    a database record of all fetched articles (not just published ones).
    
    Args:
        article: Article dict with title, link, source_id, etc.
        r2_path: Path to article JSON in R2
        r2_image_path: Path to image in R2 (if any)
        status: Initial status (default: 'fetched')
        
    Returns:
        UUID of created record, or None if failed/not configured
    """
    client = get_supabase_client()
    if not client:
        return None
    
    # Normalize URL for deduplication
    url = article.get("link", "").lower().strip().rstrip("/")
    if not url:
        print("[DB] Cannot record article without URL")
        return None
    
    # Check if already exists
    try:
        existing = client.table("all_articles")\
            .select("id")\
            .eq("article_url", url)\
            .limit(1)\
            .execute()
        
        if existing.data:
            # Already recorded, skip
            return existing.data[0]["id"]
    except Exception as e:
        print(f"[DB] Error checking existing article: {e}")
    
    # Parse published date
    published_date = None
    if article.get("published"):
        try:
            # Handle various date formats
            pub = article["published"]
            if isinstance(pub, str):
                # Try ISO format first
                if "T" in pub:
                    published_date = pub.split("T")[0]
                else:
                    published_date = pub[:10]  # YYYY-MM-DD
        except:
            pass
    
    # Build record
    data = {
        "article_url": url,
        "source_id": article.get("source_id", "unknown"),
        "source_name": article.get("source_name", ""),
        "original_title": article.get("title", "")[:500],
        "headline": article.get("headline", ""),
        "original_publish_date": published_date,
        "ai_summary": article.get("ai_summary", ""),
        "tags": article.get("tags", []),
        "r2_path": r2_path,
        "r2_image_path": r2_image_path,
        "fetch_date": date.today().isoformat(),
        "status": status,
    }
    
    try:
        result = client.table("all_articles")\
            .insert(data)\
            .execute()
        
        if result.data:
            article_id = result.data[0]["id"]
            return article_id
    except Exception as e:
        # Might be duplicate or other error
        print(f"[DB] Failed to record article: {e}")
    
    return None


def record_batch_to_db(
    candidates: list,
    status: str = "fetched"
) -> dict:
    """
    Record multiple articles to Supabase.
    
    Args:
        candidates: List of dicts from r2.save_candidate() with:
            - article_id: The R2 article ID
            - json_path: Path to JSON in R2
            - image_path: Path to image in R2
            - article: Original article dict (if available)
        status: Status to set for all articles
        
    Returns:
        Dict with recorded/skipped/failed counts
    """
    client = get_supabase_client()
    if not client:
        return {"recorded": 0, "skipped": 0, "failed": 0, "db_available": False}
    
    recorded = 0
    skipped = 0
    failed = 0
    
    for candidate in candidates:
        article = candidate.get("article", {})
        if not article:
            skipped += 1
            continue
        
        result = record_article_to_db(
            article=article,
            r2_path=candidate.get("json_path", ""),
            r2_image_path=candidate.get("image_path"),
            status=status
        )
        
        if result:
            recorded += 1
        else:
            failed += 1
    
    return {
        "recorded": recorded,
        "skipped": skipped,
        "failed": failed,
        "db_available": True
    }


def test_connection() -> bool:
    """
    Test database connection.
    
    Returns:
        True if connection successful
    """
    client = get_supabase_client()
    if not client:
        print("[DB] Supabase not configured (SUPABASE_URL/KEY not set)")
        return False
    
    try:
        # Try a simple query
        result = client.table("all_articles")\
            .select("id")\
            .limit(1)\
            .execute()
        
        print("[DB] Connection test successful")
        return True
        
    except Exception as e:
        print(f"[DB] Connection test failed: {e}")
        return False
