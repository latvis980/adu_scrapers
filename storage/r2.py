# storage/r2.py
"""
Cloudflare R2 Storage Module
Handles all interactions with Cloudflare R2 for storing scraped news data and images.

UNIFIED Folder Structure:
    bucket/
    └── 2026/
        └── January/
            └── Week-4/
                └── 2026-01-20/
                    ├── images/                    # Shared images (accessible by all services)
                    │   ├── archdaily_001.jpg
                    │   └── dezeen_002.jpg
                    │
                    ├── candidates/                # For editorial selection
                    │   ├── manifest.json
                    │   ├── archdaily_001.json
                    │   └── archdaily_002.json
                    │
                    ├── selected/                  # After editorial selection
                    │   └── digest.json
                    │
                    └── archive/                   # Sent/processed articles
                        ├── archdaily_001.json
                        └── dezeen_002.json

Note: Images are stored in a shared /images/ folder at the date level,
NOT inside candidates/ or archive/. This ensures Telegram can always
find images regardless of article status.
"""

import os
import json
import re
import hashlib
from datetime import datetime, date
from typing import Optional, Tuple, List, Dict
from urllib.parse import urlparse
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


class R2Storage:
    """Handles Cloudflare R2 storage operations."""

    def __init__(
        self,
        account_id: Optional[str] = None,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        bucket_name: Optional[str] = None,
        public_url: Optional[str] = None
    ):
        """
        Initialize R2 storage client.
        """
        self.account_id = account_id or os.getenv("R2_ACCOUNT_ID")
        self.access_key_id = access_key_id or os.getenv("R2_ACCESS_KEY_ID")
        self.secret_access_key = secret_access_key or os.getenv("R2_SECRET_ACCESS_KEY")
        self.bucket_name = bucket_name or os.getenv("R2_BUCKET_NAME")
        self.public_url = public_url or os.getenv("R2_PUBLIC_URL")

        # Validate required credentials
        missing: list[str] = []
        if not self.account_id:
            missing.append("R2_ACCOUNT_ID")
        if not self.access_key_id:
            missing.append("R2_ACCESS_KEY_ID")
        if not self.secret_access_key:
            missing.append("R2_SECRET_ACCESS_KEY")
        if not self.bucket_name:
            missing.append("R2_BUCKET_NAME")

        if missing:
            raise ValueError(f"Missing R2 credentials: {', '.join(missing)}")

        # Create S3 client configured for R2
        self.client = boto3.client(
            "s3",
            endpoint_url=f"https://{self.account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            config=Config(
                signature_version="s3v4",
                retries={"max_attempts": 3, "mode": "standard"}
            )
        )

        # Track article indices per source (for current session)
        self._source_counters: Dict[str, int] = {}

    # =========================================================================
    # Path Building Utilities
    # =========================================================================

    def _get_week_number(self, dt: date) -> int:
        """Get the week number within the month (1-5)."""
        first_day = dt.replace(day=1)
        day_of_month = dt.day
        first_weekday = first_day.weekday()
        adjusted_day = day_of_month + first_weekday
        week_number = (adjusted_day - 1) // 7 + 1
        return week_number

    def _get_base_path(self, target_date: Optional[date] = None) -> str:
        """
        Get base path for a date.

        Format: YYYY/MonthName/Week-N/YYYY-MM-DD
        """
        if target_date is None:
            target_date = date.today()

        year = target_date.year
        month_name = target_date.strftime("%B")
        week_num = self._get_week_number(target_date)
        date_str = target_date.strftime("%Y-%m-%d")

        return f"{year}/{month_name}/Week-{week_num}/{date_str}"

    def _build_candidate_path(
        self, 
        source_id: str, 
        index: int,
        target_date: Optional[date] = None
    ) -> str:
        """
        Build path for candidate article JSON.

        Format: YYYY/MonthName/Week-N/YYYY-MM-DD/candidates/source_NNN.json
        """
        base = self._get_base_path(target_date)
        return f"{base}/candidates/{source_id}_{index:03d}.json"

    def _build_image_path(
        self,
        source_id: str,
        index: int,
        extension: str = "jpg",
        target_date: Optional[date] = None
    ) -> str:
        """
        Build path for article hero image.

        IMPORTANT: Images are stored in shared /images/ folder at date level,
        NOT inside candidates/ or archive/. This ensures consistent access.

        Format: YYYY/MonthName/Week-N/YYYY-MM-DD/images/source_NNN.ext
        """
        base = self._get_base_path(target_date)
        return f"{base}/images/{source_id}_{index:03d}.{extension}"

    def _build_manifest_path(self, target_date: Optional[date] = None) -> str:
        """
        Build path for manifest file.

        Format: YYYY/MonthName/Week-N/YYYY-MM-DD/candidates/manifest.json
        """
        base = self._get_base_path(target_date)
        return f"{base}/candidates/manifest.json"

    def _build_selected_path(self, target_date: Optional[date] = None) -> str:
        """
        Build path for selected digest.

        Format: YYYY/MonthName/Week-N/YYYY-MM-DD/selected/digest.json
        """
        base = self._get_base_path(target_date)
        return f"{base}/selected/digest.json"

    def _build_archive_json_path(
        self,
        source_id: str,
        index: int,
        target_date: Optional[date] = None
    ) -> str:
        """
        Build path for archived article JSON (after sending to Telegram).

        Format: YYYY/MonthName/Week-N/YYYY-MM-DD/archive/source_NNN.json
        """
        base = self._get_base_path(target_date)
        return f"{base}/archive/{source_id}_{index:03d}.json"

    # =========================================================================
    # Slugify (Fixed for Chinese/Unicode)
    # =========================================================================

    def _slugify(self, text: str, max_length: int = 50) -> str:
        """
        Convert text to URL-safe slug.
        Handles Chinese and other non-ASCII characters by using a hash fallback.
        """
        if not text:
            return "untitled"

        # First, try to extract ASCII characters only
        slug = text.lower()

        # Keep only ASCII alphanumeric, spaces, and hyphens
        ascii_slug = re.sub(r'[^a-z0-9\s-]', '', slug)
        ascii_slug = re.sub(r'[-\s]+', '-', ascii_slug)
        ascii_slug = ascii_slug.strip('-')

        # If we got a reasonable ASCII slug (at least 5 chars), use it
        if len(ascii_slug) >= 5:
            if len(ascii_slug) > max_length:
                ascii_slug = ascii_slug[:max_length].rstrip('-')
            return ascii_slug

        # For non-ASCII text (like Chinese), generate a short hash
        text_hash = hashlib.md5(text.encode('utf-8')).hexdigest()[:8]

        if ascii_slug and len(ascii_slug) >= 2:
            # Combine any ASCII prefix with hash
            return f"{ascii_slug[:20]}-{text_hash}"
        else:
            # Pure non-ASCII text - use hash only
            return text_hash

    # =========================================================================
    # Image Utilities
    # =========================================================================

    def _get_image_extension(self, url: str, content_type: Optional[str] = None) -> str:
        """Determine image extension from URL or content type."""
        if content_type:
            mime_map = {
                'image/jpeg': 'jpg',
                'image/jpg': 'jpg',
                'image/png': 'png',
                'image/webp': 'webp',
                'image/gif': 'gif',
                'image/svg+xml': 'svg',
            }
            ext = mime_map.get(content_type.lower().split(';')[0])
            if ext:
                return ext

        parsed = urlparse(url)
        path = parsed.path.lower()

        for ext in ['jpg', 'jpeg', 'png', 'webp', 'gif', 'svg']:
            if path.endswith(f'.{ext}'):
                return 'jpg' if ext == 'jpeg' else ext

        return 'jpg'

    def _get_content_type(self, extension: str) -> str:
        """Get MIME type for file extension."""
        content_types = {
            'jpg': 'image/jpeg',
            'jpeg': 'image/jpeg',
            'png': 'image/png',
            'webp': 'image/webp',
            'gif': 'image/gif',
            'svg': 'image/svg+xml',
        }
        return content_types.get(extension, 'image/jpeg')

    # =========================================================================
    # Article Index Management
    # =========================================================================

    def _get_next_index(self, source_id: str) -> int:
        """
        Get the next available index for a source.
        Starts at 1 for each source.
        """
        if source_id not in self._source_counters:
            self._source_counters[source_id] = 0

        self._source_counters[source_id] += 1
        return self._source_counters[source_id]

    def reset_counters(self):
        """Reset all source counters (call at start of pipeline run)."""
        self._source_counters = {}

    def get_article_id(self, source_id: str, index: int) -> str:
        """Generate article ID from source and index."""
        return f"{source_id}_{index:03d}"

    # =========================================================================
    # Candidate Storage (for Editorial Selection)
    # =========================================================================

    def save_candidate(
        self,
        article: dict,
        image_bytes: Optional[bytes] = None,
        target_date: Optional[date] = None
    ) -> dict:
        """
        Save a single article as an editorial candidate.

        Saves:
        - Article JSON to candidates/ folder
        - Hero image to shared images/ folder (if provided)

        Args:
            article: Article dict with ai_summary, tag, etc.
            image_bytes: Optional hero image bytes
            target_date: Target date (defaults to today)

        Returns:
            Dict with saved paths and article_id
        """
        source_id = article.get("source_id", "unknown")
        index = self._get_next_index(source_id)
        article_id = self.get_article_id(source_id, index)

        if target_date is None:
            target_date = date.today()

        # Determine image info
        has_image = False
        image_path = None
        image_filename = None

        if image_bytes:
            hero = article.get("hero_image", {})
            extension = self._get_image_extension(
                hero.get("url", ""),
                None
            )
            image_filename = f"{article_id}.{extension}"
            # Use shared images folder (NOT inside candidates/)
            image_path = self._build_image_path(
                source_id, index, extension, target_date
            )
            has_image = True

        # Build candidate JSON
        candidate_data = {
            "id": article_id,
            "index": index,
            "source_id": source_id,
            "source_name": article.get("source_name", source_id),
            "title": article.get("title", ""),
            "link": article.get("link", ""),
            "published": article.get("published"),
            "headline": article.get("headline", ""),
            "ai_summary": article.get("ai_summary", ""),
            "tag": article.get("tag", ""),
            "image": {
                "filename": image_filename,
                "r2_path": image_path,
                "has_image": has_image,
                "original_url": article.get("hero_image", {}).get("url") if has_image else None,
            },
            "saved_at": datetime.now().isoformat(),
        }

        # Save article JSON to candidates/
        json_path = self._build_candidate_path(source_id, index, target_date)

        self.client.put_object(
            Bucket=self.bucket_name,
            Key=json_path,
            Body=json.dumps(candidate_data, indent=2, ensure_ascii=False),
            ContentType="application/json"
        )

        print(f"   [OK] Saved candidate: {article_id}")

        # Save image to shared images/ folder
        if image_bytes and image_path:
            content_type = self._get_content_type(
                image_path.split('.')[-1]
            )

            self.client.put_object(
                Bucket=self.bucket_name,
                Key=image_path,
                Body=image_bytes,
                ContentType=content_type,
                CacheControl="public, max-age=31536000"
            )

            print(f"   [OK] Saved image: {image_filename}")

        return {
            "article_id": article_id,
            "json_path": json_path,
            "image_path": image_path,
            "has_image": has_image,
        }

    def save_hero_image(
        self,
        image_bytes: bytes,
        article: dict,
        source: str,
        target_date: Optional[date] = None
    ) -> Optional[dict]:
        """
        Save hero image to R2 storage independently (for custom scrapers).

        This method is used by custom scrapers (archiposition, gooood) that
        extract and download hero images themselves, rather than relying
        on the main pipeline's browserless scraper.

        Args:
            image_bytes: Image bytes to save
            article: Article dict with hero_image info
            source: Source ID for naming
            target_date: Target date (defaults to today)

        Returns:
            Updated hero_image dict with r2_path and r2_url, or None if failed
        """
        if not image_bytes or len(image_bytes) < 1000:
            return None

        if target_date is None:
            target_date = date.today()

        # Get hero image info
        hero = article.get("hero_image", {})
        original_url = hero.get("url", "")

        # Determine extension
        extension = self._get_image_extension(original_url, None)

        # Get next index for this source
        index = self._get_next_index(source)
        article_id = self.get_article_id(source, index)

        # Build image path
        image_path = self._build_image_path(source, index, extension, target_date)
        image_filename = f"{article_id}.{extension}"

        try:
            # Upload to R2
            content_type = self._get_content_type(extension)

            self.client.put_object(
                Bucket=self.bucket_name,
                Key=image_path,
                Body=image_bytes,
                ContentType=content_type,
                CacheControl="public, max-age=31536000"
            )

            # Build public URL if available
            r2_url = None
            if self.public_url:
                r2_url = f"{self.public_url.rstrip('/')}/{image_path}"

            # Return updated hero_image dict
            updated_hero = {
                "url": original_url,
                "width": hero.get("width"),
                "height": hero.get("height"),
                "source": hero.get("source", "custom_scraper"),
                "r2_path": image_path,
                "r2_url": r2_url,
                "filename": image_filename,
                "bytes": image_bytes,  # Include bytes for main pipeline
            }

            return updated_hero

        except Exception as e:
            print(f"   [ERROR] Failed to save hero image: {e}")
            return None

    def save_manifest(
        self,
        candidates: List[dict],
        target_date: Optional[date] = None
    ) -> str:
        """
        Save manifest file with all candidates for the day.

        IMPORTANT: This MERGES with existing manifest, not overwrites.
        This allows both RSS and Scrapers services to add to the same manifest.

        Args:
            candidates: List of candidate info dicts from save_candidate()
            target_date: Target date (defaults to today)

        Returns:
            Path to manifest file
        """
        if target_date is None:
            target_date = date.today()

        # Try to load existing manifest first
        existing_manifest = self.get_manifest(target_date)

        # Get existing candidates (to merge with)
        existing_candidates = []
        existing_ids = set()
        if existing_manifest:
            existing_candidates = existing_manifest.get("candidates", [])
            existing_ids = {c["id"] for c in existing_candidates}
            print(f"   [INFO] Found existing manifest with {len(existing_candidates)} candidates")

        # Add new candidates (skip duplicates)
        new_count = 0
        for c in candidates:
            if c["article_id"] not in existing_ids:
                existing_candidates.append({
                    "id": c["article_id"],
                    "has_image": c["has_image"],
                    "json_path": c["json_path"],
                    "image_path": c.get("image_path"),
                })
                existing_ids.add(c["article_id"])
                new_count += 1

        # Group all candidates by source
        by_source: Dict[str, List[str]] = {}
        for c in existing_candidates:
            source_id = c["id"].rsplit("_", 1)[0]
            if source_id not in by_source:
                by_source[source_id] = []
            by_source[source_id].append(c["id"])

        # Build merged manifest
        manifest = {
            "date": target_date.isoformat(),
            "created_at": existing_manifest.get("created_at") if existing_manifest else datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "total_candidates": len(existing_candidates),
            "sources": {
                source_id: {
                    "count": len(ids),
                    "article_ids": sorted(ids)
                }
                for source_id, ids in by_source.items()
            },
            "candidates": existing_candidates
        }

        path = self._build_manifest_path(target_date)

        self.client.put_object(
            Bucket=self.bucket_name,
            Key=path,
            Body=json.dumps(manifest, indent=2, ensure_ascii=False),
            ContentType="application/json"
        )

        print(f"   [OK] Manifest updated: +{new_count} new, {len(existing_candidates)} total candidates")
        return path

    def get_manifest(self, target_date: Optional[date] = None) -> Optional[dict]:
        """
        Retrieve manifest for a given date.

        Args:
            target_date: Target date (defaults to today)

        Returns:
            Manifest dict or None if not found
        """
        path = self._build_manifest_path(target_date)

        try:
            response = self.client.get_object(
                Bucket=self.bucket_name,
                Key=path
            )
            content = response["Body"].read().decode("utf-8")
            return json.loads(content)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise

    def get_candidate(
        self,
        article_id: str,
        target_date: Optional[date] = None
    ) -> Optional[dict]:
        """
        Retrieve a single candidate article.

        Args:
            article_id: Article ID (e.g., "archdaily_001")
            target_date: Target date (defaults to today)

        Returns:
            Candidate dict or None if not found
        """
        parts = article_id.rsplit("_", 1)
        if len(parts) != 2:
            return None

        source_id = parts[0]
        try:
            index = int(parts[1])
        except ValueError:
            return None

        path = self._build_candidate_path(source_id, index, target_date)

        try:
            response = self.client.get_object(
                Bucket=self.bucket_name,
                Key=path
            )
            content = response["Body"].read().decode("utf-8")
            return json.loads(content)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise

    def get_all_candidates(
        self,
        target_date: Optional[date] = None
    ) -> List[dict]:
        """
        Retrieve all candidate articles for a given date.

        Args:
            target_date: Target date (defaults to today)

        Returns:
            List of candidate dicts
        """
        manifest = self.get_manifest(target_date)
        if not manifest:
            return []

        candidates = []
        for entry in manifest.get("candidates", []):
            article_id = entry.get("id")
            if article_id:
                candidate = self.get_candidate(article_id, target_date)
                if candidate:
                    candidates.append(candidate)

        return candidates

    # =========================================================================
    # Selected Digest
    # =========================================================================

    def save_selected_digest(
        self,
        selected_articles: List[dict],
        target_date: Optional[date] = None,
        metadata: Optional[dict] = None
    ) -> str:
        """
        Save the selected digest (after editorial selection).

        Args:
            selected_articles: List of selected candidate dicts
            target_date: Target date (defaults to today)
            metadata: Optional metadata dict

        Returns:
            Path to saved digest
        """
        if target_date is None:
            target_date = date.today()

        digest = {
            "date": target_date.isoformat(),
            "created_at": datetime.now().isoformat(),
            "article_count": len(selected_articles),
            "articles": selected_articles,
        }

        if metadata:
            digest["metadata"] = metadata

        path = self._build_selected_path(target_date)

        self.client.put_object(
            Bucket=self.bucket_name,
            Key=path,
            Body=json.dumps(digest, indent=2, ensure_ascii=False),
            ContentType="application/json"
        )

        print(f"   [OK] Saved digest: {len(selected_articles)} selected articles")
        return path

    def get_selected_digest(
        self,
        target_date: Optional[date] = None
    ) -> Optional[dict]:
        """
        Retrieve the selected digest for a given date.

        Args:
            target_date: Target date (defaults to today)

        Returns:
            Digest dict or None if not found
        """
        path = self._build_selected_path(target_date)

        try:
            response = self.client.get_object(
                Bucket=self.bucket_name,
                Key=path
            )
            content = response["Body"].read().decode("utf-8")
            return json.loads(content)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise

    # =========================================================================
    # Image Operations
    # =========================================================================

    def get_image(self, path: str) -> Optional[bytes]:
        """Retrieve an image from R2 storage."""
        try:
            response = self.client.get_object(
                Bucket=self.bucket_name,
                Key=path
            )
            return response["Body"].read()
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise

    def image_exists(self, path: str) -> bool:
        """Check if an image exists at the given path."""
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=path)
            return True
        except ClientError:
            return False

    def get_image_public_url(self, r2_path: str) -> Optional[str]:
        """
        Get public URL for an image.

        Args:
            r2_path: Path to image in R2

        Returns:
            Public URL or None if no public URL configured
        """
        if not self.public_url or not r2_path:
            return None
        return f"{self.public_url.rstrip('/')}/{r2_path}"

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def list_dates_with_content(self, year: int, month: int) -> List[date]:
        """
        List all dates that have content for a given month.

        Args:
            year: Year (e.g., 2026)
            month: Month number (1-12)

        Returns:
            List of dates with content
        """
        month_name = date(year, month, 1).strftime("%B")
        prefix = f"{year}/{month_name}/"

        dates_found = set()

        try:
            paginator = self.client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    # Extract date from path like "2026/January/Week-3/2026-01-20/..."
                    parts = key.split("/")
                    if len(parts) >= 4:
                        date_str = parts[3]  # e.g., "2026-01-20"
                        try:
                            d = date.fromisoformat(date_str)
                            dates_found.add(d)
                        except ValueError:
                            pass
        except ClientError:
            pass

        return sorted(dates_found)

    def test_connection(self) -> bool:
        """Test R2 connection and bucket access."""
        try:
            self.client.list_objects_v2(
                Bucket=self.bucket_name,
                MaxKeys=1
            )
            print(f"   [OK] R2 connected: bucket '{self.bucket_name}'")
            if self.public_url:
                print(f"   [OK] Public URL: {self.public_url}")
            return True
        except ClientError as e:
            print(f"   [ERROR] R2 connection failed: {e}")
            return False