# storage/r2.py
"""
Cloudflare R2 Storage Module
Handles all interactions with Cloudflare R2 for storing scraped news data and images.

NEW Folder Structure (for Editorial Selection):
    bucket/
    └── 2026/
        └── January/
            └── Week-4/
                └── 2026-01-20/
                    ├── candidates/                    # For editorial selection
                    │   ├── manifest.json              # Master index
                    │   ├── archdaily_001.json         # Individual articles
                    │   ├── archdaily_002.json
                    │   └── images/
                    │       ├── archdaily_001.jpg      # Matching images
                    │       └── archdaily_002.jpg
                    │
                    ├── selected/                      # After editorial selection
                    │   └── digest.json
                    │
                    └── archive/                       # Full source data
                        ├── archdaily.json
                        └── dezeen.json
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

    def _build_archive_path(self, source: str, target_date: Optional[date] = None) -> str:
        """
        Build path for archive storage (full source data).

        Format: YYYY/MonthName/Week-N/YYYY-MM-DD/archive/source.json
        """
        base = self._get_base_path(target_date)
        return f"{base}/archive/{source}.json"

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

    def _build_candidate_image_path(
        self,
        source_id: str,
        index: int,
        extension: str = "jpg",
        target_date: Optional[date] = None
    ) -> str:
        """
        Build path for candidate hero image.

        Format: YYYY/MonthName/Week-N/YYYY-MM-DD/candidates/images/source_NNN.ext
        """
        base = self._get_base_path(target_date)
        return f"{base}/candidates/images/{source_id}_{index:03d}.{extension}"

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

    # Legacy path builders (for backward compatibility)
    def _build_path(self, source: str, target_date: Optional[date] = None) -> str:
        """Legacy: Build path for source JSON (now uses archive)."""
        return self._build_archive_path(source, target_date)

    def _build_image_path(
        self, 
        source: str, 
        article_slug: str, 
        extension: str = "jpg",
        target_date: Optional[date] = None
    ) -> str:
        """
        Legacy: Build path for image using slug.

        Format: YYYY/MonthName/Week-N/YYYY-MM-DD/images/source-slug.ext
        """
        if target_date is None:
            target_date = date.today()

        base = self._get_base_path(target_date)
        clean_slug = self._slugify(article_slug)

        return f"{base}/images/{source}-{clean_slug}.{extension}"

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
    # NEW: Candidate Storage (for Editorial Selection)
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
        - Article JSON with summary, tags, metadata
        - Hero image (if provided) with matching filename

        Args:
            article: Article dict with ai_summary, tags, etc.
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
            image_path = self._build_candidate_image_path(
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
            "ai_summary": article.get("ai_summary", ""),
            "tags": article.get("tags", []),
            "image": {
                "filename": image_filename,
                "r2_path": image_path,
                "has_image": has_image,
                "original_url": article.get("hero_image", {}).get("url") if has_image else None,
            },
            "saved_at": datetime.now().isoformat(),
        }

        # Save article JSON
        json_path = self._build_candidate_path(source_id, index, target_date)

        self.client.put_object(
            Bucket=self.bucket_name,
            Key=json_path,
            Body=json.dumps(candidate_data, indent=2, ensure_ascii=False),
            ContentType="application/json"
        )

        print(f"   📄 Saved candidate: {article_id}")

        # Save image if provided
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

            print(f"   🖼️  Saved image: {image_filename}")

        return {
            "article_id": article_id,
            "json_path": json_path,
            "image_path": image_path,
            "has_image": has_image,
        }

    def save_manifest(
        self,
        candidates: List[dict],
        target_date: Optional[date] = None
    ) -> str:
        """
        Save manifest file with all candidates for the day.

        Args:
            candidates: List of candidate info dicts from save_candidate()
            target_date: Target date (defaults to today)

        Returns:
            Path to manifest file
        """
        if target_date is None:
            target_date = date.today()

        # Group by source
        by_source: Dict[str, List[str]] = {}
        for c in candidates:
            source_id = c["article_id"].rsplit("_", 1)[0]
            if source_id not in by_source:
                by_source[source_id] = []
            by_source[source_id].append(c["article_id"])

        # Build manifest
        manifest = {
            "date": target_date.isoformat(),
            "created_at": datetime.now().isoformat(),
            "total_candidates": len(candidates),
            "sources": {
                source_id: {
                    "count": len(ids),
                    "article_ids": sorted(ids)
                }
                for source_id, ids in by_source.items()
            },
            "candidates": [
                {
                    "id": c["article_id"],
                    "has_image": c["has_image"],
                    "json_path": c["json_path"],
                    "image_path": c.get("image_path"),
                }
                for c in candidates
            ]
        }

        # Save manifest
        path = self._build_manifest_path(target_date)

        self.client.put_object(
            Bucket=self.bucket_name,
            Key=path,
            Body=json.dumps(manifest, indent=2, ensure_ascii=False),
            ContentType="application/json"
        )

        print(f"   📋 Saved manifest: {len(candidates)} candidates")
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
        # Parse article_id to get source and index
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
    # NEW: Selected/Digest Storage
    # =========================================================================

    def save_selected_digest(
        self,
        selected_ids: List[str],
        target_date: Optional[date] = None,
        metadata: Optional[dict] = None
    ) -> str:
        """
        Save the editorial selection (selected article IDs).

        Args:
            selected_ids: List of article IDs that were selected
            target_date: Target date (defaults to today)
            metadata: Optional metadata about the selection

        Returns:
            Path to digest file
        """
        if target_date is None:
            target_date = date.today()

        # Load full candidate data for selected articles
        selected_articles = []
        for article_id in selected_ids:
            candidate = self.get_candidate(article_id, target_date)
            if candidate:
                selected_articles.append(candidate)

        digest = {
            "date": target_date.isoformat(),
            "selected_at": datetime.now().isoformat(),
            "total_selected": len(selected_articles),
            "selected_ids": selected_ids,
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

        print(f"   ✅ Saved digest: {len(selected_articles)} selected articles")
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
    # Legacy: Archive Storage (Full Source Data)
    # =========================================================================

    def save_articles(
        self, 
        articles: list[dict], 
        source: str,
        target_date: Optional[date] = None,
        metadata: Optional[dict] = None
    ) -> str:
        """Save articles to archive storage (grouped by source)."""
        path = self._build_archive_path(source, target_date)
        actual_date = target_date or date.today()

        data: dict = {
            "source": source,
            "date": actual_date.isoformat(),
            "fetched_at": datetime.now().isoformat(),
            "article_count": len(articles),
            "articles": articles
        }

        if metadata:
            data["metadata"] = metadata

        self.client.put_object(
            Bucket=self.bucket_name,
            Key=path,
            Body=json.dumps(data, indent=2, ensure_ascii=False),
            ContentType="application/json"
        )

        print(f"   📁 Archived {len(articles)} articles to: {path}")
        return path

    def get_articles(
        self, 
        source: str, 
        target_date: Optional[date] = None
    ) -> Optional[dict]:
        """Retrieve articles from archive storage."""
        path = self._build_archive_path(source, target_date)

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
    # Legacy: Image Storage
    # =========================================================================

    def save_image(
        self,
        image_bytes: bytes,
        source: str,
        article_slug: str,
        image_url: Optional[str] = None,
        content_type: Optional[str] = None,
        target_date: Optional[date] = None
    ) -> Tuple[str, Optional[str]]:
        """
        Save an image to R2 storage (legacy method using slug).
        """
        if not image_bytes:
            raise ValueError("No image data provided")

        extension = self._get_image_extension(image_url or "", content_type)
        path = self._build_image_path(source, article_slug, extension, target_date)
        upload_content_type = self._get_content_type(extension)

        self.client.put_object(
            Bucket=self.bucket_name,
            Key=path,
            Body=image_bytes,
            ContentType=upload_content_type,
            CacheControl="public, max-age=31536000"
        )

        public_url: Optional[str] = None
        if self.public_url:
            public_url = f"{self.public_url.rstrip('/')}/{path}"

        print(f"   🖼️ Saved image: {path} ({len(image_bytes)} bytes)")
        return path, public_url

    def save_hero_image(
        self,
        image_bytes: bytes,
        article: dict,
        source: str,
        target_date: Optional[date] = None
    ) -> Optional[dict]:
        """Save hero image for an article and return updated hero_image dict."""
        hero_image = article.get("hero_image")
        if not hero_image:
            return None

        slug = article.get("title", "")
        if not slug:
            url = article.get("link", "")
            if url:
                parsed = urlparse(url)
                slug = parsed.path.split("/")[-1] or parsed.path.split("/")[-2] or "article"

        try:
            path, public_url = self.save_image(
                image_bytes=image_bytes,
                source=source,
                article_slug=slug,
                image_url=hero_image.get("url"),
                target_date=target_date
            )

            hero_image["r2_path"] = path
            hero_image["r2_url"] = public_url
            hero_image["saved_at"] = datetime.now().isoformat()

            return hero_image

        except Exception as e:
            print(f"   ⚠️ Failed to save hero image: {e}")
            return hero_image

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

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def file_exists(self, source: str, target_date: Optional[date] = None) -> bool:
        """Check if a source file exists."""
        path = self._build_archive_path(source, target_date)

        try:
            self.client.head_object(Bucket=self.bucket_name, Key=path)
            return True
        except ClientError:
            return False

    def delete_file(self, source: str, target_date: Optional[date] = None) -> bool:
        """Delete a file from storage."""
        path = self._build_archive_path(source, target_date)

        try:
            self.client.delete_object(Bucket=self.bucket_name, Key=path)
            print(f"   🗑️ Deleted: {path}")
            return True
        except ClientError as e:
            print(f"   ❌ Delete failed: {e}")
            return False

    def test_connection(self) -> bool:
        """Test R2 connection and bucket access."""
        try:
            self.client.list_objects_v2(
                Bucket=self.bucket_name,
                MaxKeys=1
            )
            print(f"   ✅ R2 connected: bucket '{self.bucket_name}'")
            if self.public_url:
                print(f"   ✅ Public URL: {self.public_url}")
            return True
        except ClientError as e:
            print(f"   ❌ R2 connection failed: {e}")
            return False