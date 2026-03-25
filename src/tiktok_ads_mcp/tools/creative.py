"""Tool 5: tiktok_creative — Creative and asset management.

Supported action values:
  list_videos       — List videos in ad account asset library
  search_videos     — Search videos by keyword
  list_images       — List images in ad account asset library
  list_creatives    — List ad creatives (filterable by status, campaign_ids, adgroup_ids)
  upload_image      — Upload image asset
  upload_video      — Upload video asset (by URL)
  spark_authorize   — Generate/renew Spark Ad authorization code
  spark_status      — Check Spark Ad auth code status
  list_tt_posts     — List TikTok posts from a connected identity (for Spark Ads)
  generate_ad_text  — Generate ad text suggestions using TikTok's AI
"""

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..api.marketing_client import MarketingClient
from ..utils.confirmation import build_preview

logger = logging.getLogger(__name__)


class CreativeTool:
    """Consolidated creative and asset management tool."""

    def __init__(self, client: MarketingClient):
        self.client = client

    async def execute(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Route to the appropriate handler based on action."""
        action = arguments.get("action", "")
        if not action:
            return _error("action is required")

        handler = self._handlers.get(action)
        if not handler:
            valid = ", ".join(sorted(self._handlers.keys()))
            return _error(f"Unknown action '{action}'. Valid: {valid}")

        try:
            return await handler(self, arguments)
        except Exception as e:
            logger.error(f"creative({action}) failed: {e}")
            return _error(str(e))

    # ── Handlers ────────────────────────────────────────────────────────

    async def _list_videos(self, args: Dict[str, Any]) -> Dict[str, Any]:
        video_ids = args.get("video_ids")
        page = args.get("page", 1)
        page_size = args.get("page_size", 200)
        result = await self.client.get_videos(
            video_ids=video_ids, page=page, page_size=page_size,
        )
        items = result.get("data", {}).get("list", [])
        page_info = result.get("data", {}).get("page_info", {})
        return _success(items, page_info)

    async def _search_videos(self, args: Dict[str, Any]) -> Dict[str, Any]:
        keyword = args.get("keyword")
        page = args.get("page", 1)
        page_size = args.get("page_size", 200)
        result = await self.client.search_videos(
            keyword=keyword, page=page, page_size=page_size,
        )
        items = result.get("data", {}).get("list", [])
        page_info = result.get("data", {}).get("page_info", {})
        return _success(items, page_info)

    async def _list_creatives(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """List ads with their embedded creative details (TikTok has no standalone creative endpoint)."""
        page = args.get("page", 1)
        page_size = args.get("page_size", 200)
        status = args.get("status")
        campaign_ids = args.get("campaign_ids")
        adgroup_ids = args.get("adgroup_ids")
        result = await self.client.get_ad_creatives(
            status=status, campaign_ids=campaign_ids, adgroup_ids=adgroup_ids,
            page=page, page_size=page_size,
        )
        ads = result.get("data", {}).get("list", [])
        page_info = result.get("data", {}).get("page_info", {})
        # Extract creative-relevant fields from each ad
        creatives = []
        for ad in ads:
            creatives.append({
                "ad_id": ad.get("ad_id"),
                "ad_name": ad.get("ad_name"),
                "creative_type": ad.get("creative_type"),
                "ad_format": ad.get("ad_format"),
                "ad_text": ad.get("ad_text"),
                "ad_texts": ad.get("ad_texts"),
                "call_to_action": ad.get("call_to_action"),
                "display_name": ad.get("display_name"),
                "image_ids": ad.get("image_ids"),
                "video_id": ad.get("video_id"),
                "landing_page_url": ad.get("landing_page_url"),
                "profile_image_url": ad.get("profile_image_url"),
                "image_mode": ad.get("image_mode"),
                "status": ad.get("secondary_status"),
            })
        return _success(creatives, page_info)

    async def _list_images(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """List image assets in the ad account library."""
        image_ids = args.get("image_ids")
        page = args.get("page", 1)
        page_size = args.get("page_size", 200)
        result = await self.client.search_images(
            image_ids=image_ids, page=page, page_size=page_size,
        )
        items = result.get("data", {}).get("list", [])
        page_info = result.get("data", {}).get("page_info", {})
        return _success(items, page_info)

    async def _upload_image(self, args: Dict[str, Any]) -> Dict[str, Any]:
        image_url = args.get("image_url")
        image_path = args.get("image_path")
        confirm = args.get("confirm", False)

        if image_url:
            # URL-based upload
            if not confirm:
                return build_preview("upload", "image", {
                    "image_url": image_url,
                    "upload_type": "UPLOAD_BY_URL",
                })
            result = await self.client.upload_image_by_url(image_url)
            return _success(result.get("data", {}))

        if not image_path:
            return _error("image_url or image_path is required")

        if not os.path.exists(image_path):
            return _error(f"File not found: {image_path}")

        allowed = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
        ext = os.path.splitext(image_path)[1].lower()
        if ext not in allowed:
            return _error(f"Unsupported file type '{ext}'. Allowed: {', '.join(sorted(allowed))}")

        file_size = os.path.getsize(image_path)
        if file_size > 10 * 1024 * 1024:
            return _error(f"File too large ({file_size / 1024 / 1024:.1f}MB). Max 10MB.")

        if not confirm:
            return build_preview("upload", "image", {
                "file": os.path.basename(image_path),
                "size_mb": round(file_size / 1024 / 1024, 2),
            })

        result = await self.client.upload_image(image_path)
        return _success(result.get("data", {}))

    async def _upload_video(self, args: Dict[str, Any]) -> Dict[str, Any]:
        confirm = args.get("confirm", False)
        video_url = args.get("video_url")
        video_name = args.get("video_name")

        if not video_url:
            return _error("video_url is required")

        if not confirm:
            return build_preview("upload", "video", {
                "video_url": video_url,
                "video_name": video_name,
            })

        data = {
            "upload_type": "UPLOAD_BY_URL",
            "video_url": video_url,
        }
        if video_name:
            data["file_name"] = video_name

        result = await self.client.upload_video(data)
        return _success(result.get("data", {}))

    async def _spark_authorize(self, args: Dict[str, Any]) -> Dict[str, Any]:
        confirm = args.get("confirm", False)
        params = args.get("params", {})

        # Accept flat params (LLMs often pass fields at top level)
        _SPARK_FIELDS = {"tiktok_item_id", "authorized_days", "identity_id", "identity_type", "identity_authorized_bc_id"}
        for field in _SPARK_FIELDS:
            if field not in params and field in args:
                params[field] = args[field]

        if not params.get("tiktok_item_id"):
            return _error("tiktok_item_id is required")

        if not confirm:
            return build_preview("authorize", "Spark Ad", {
                "tiktok_item_id": params.get("tiktok_item_id"),
                "authorized_days": params.get("authorized_days", 30),
            })

        result = await self.client.authorize_spark_ad(params)
        return _success(result.get("data", {}))

    async def _spark_status(self, args: Dict[str, Any]) -> Dict[str, Any]:
        auth_codes = args.get("auth_codes", [])
        if not auth_codes:
            return _error("auth_codes list is required")

        result = await self.client.get_spark_ad_info(auth_codes)
        return _success(result.get("data", {}))

    async def _list_tt_posts(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """List TikTok posts/videos from a connected identity for Spark Ads.

        Uses identity/video/get/ with cursor-based pagination.
        Returns item_id, created_at, text, video_info, auth_info per post.
        Supports optional start_date/end_date filtering (decoded from item_id snowflake).
        When date filters are used, auto-paginates through all results.
        """
        identity_id = args.get("identity_id")
        if not identity_id:
            return _error(
                "identity_id is required. Use tiktok_entity_get with entity_type='identities' to find your connected TikTok account identity_id."
            )
        identity_type = args.get("identity_type", "BC_AUTH_TT")
        identity_authorized_bc_id = args.get("identity_authorized_bc_id")
        cursor = args.get("cursor")
        start_date = args.get("start_date")
        end_date = args.get("end_date")

        # Parse date filters
        start_dt = _parse_date(start_date) if start_date else None
        end_dt = _parse_date(end_date, end_of_day=True) if end_date else None

        date_filtering = start_dt is not None or end_dt is not None

        # When date filtering, auto-paginate to collect all matching posts.
        # Otherwise, return a single page and let the caller paginate.
        all_items: List[Dict[str, Any]] = []
        current_cursor = cursor
        stop = False

        while not stop:
            result = await self.client.get_tt_videos(
                identity_id=identity_id,
                identity_type=identity_type,
                identity_authorized_bc_id=identity_authorized_bc_id,
                cursor=current_cursor,
            )
            data = result.get("data", {})
            items = data.get("video_list", [])
            has_more = data.get("has_more", False)
            next_cursor = data.get("cursor")

            for item in items:
                created_at = _decode_item_timestamp(item["item_id"])
                item["created_at"] = created_at.strftime("%Y-%m-%d %H:%M:%S UTC")

                if date_filtering:
                    # Results are newest-first; if we've gone past start_date, stop
                    if start_dt and created_at < start_dt:
                        stop = True
                        break
                    if end_dt and created_at > end_dt:
                        continue
                all_items.append(item)

            if not date_filtering or not has_more or stop:
                break
            current_cursor = next_cursor

        response: Dict[str, Any] = {"success": True, "data": all_items}
        response["metadata"] = {
            "count": len(all_items),
        }
        if date_filtering:
            response["metadata"]["filtered"] = True
            if start_date:
                response["metadata"]["start_date"] = start_date
            if end_date:
                response["metadata"]["end_date"] = end_date
        else:
            response["metadata"]["has_more"] = has_more
            if next_cursor:
                response["metadata"]["next_cursor"] = next_cursor
        return response

    async def _generate_ad_text(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Generate ad text suggestions using TikTok's AI (not available for US/CA advertisers)."""
        adgroup_id = args.get("adgroup_id")
        if not adgroup_id:
            return _error("adgroup_id is required")
        language = args.get("language", "EN")
        num_results = args.get("num_results", 5)
        brand_name = args.get("brand_name")
        keywords = args.get("keywords")
        result = await self.client.generate_ad_text(
            adgroup_id=adgroup_id,
            language=language,
            num_results=num_results,
            brand_name=brand_name,
            keywords=keywords,
        )
        return _success(result.get("data", {}))

    # ── Handler dispatch table ──────────────────────────────────────────

    _handlers = {
        "list_videos": _list_videos,
        "search_videos": _search_videos,
        "list_images": _list_images,
        "list_creatives": _list_creatives,
        "upload_image": _upload_image,
        "upload_video": _upload_video,
        "spark_authorize": _spark_authorize,
        "spark_status": _spark_status,
        "list_tt_posts": _list_tt_posts,
        "generate_ad_text": _generate_ad_text,
    }


def _success(data: Any, page_info: Any = None) -> Dict[str, Any]:
    result: Dict[str, Any] = {"success": True, "data": data}
    if page_info:
        result["metadata"] = {
            "total_count": page_info.get("total_number", 0),
            "page": page_info.get("page", 1),
            "page_size": page_info.get("page_size", 0),
        }
    return result


def _error(message: str) -> Dict[str, Any]:
    return {"success": False, "error_message": message}


def _decode_item_timestamp(item_id: str) -> datetime:
    """Decode creation timestamp from a TikTok snowflake item_id (top 42 bits = ms since Unix epoch)."""
    return datetime.fromtimestamp((int(item_id) >> 22) / 1000, tz=timezone.utc)


def _parse_date(date_str: str, end_of_day: bool = False) -> Optional[datetime]:
    """Parse a YYYY-MM-DD string into a timezone-aware datetime."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if end_of_day:
            dt = dt.replace(hour=23, minute=59, second=59)
        return dt
    except ValueError:
        return None
