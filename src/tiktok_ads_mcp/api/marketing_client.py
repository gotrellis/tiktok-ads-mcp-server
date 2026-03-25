"""TikTok Marketing API client — wraps all Marketing API endpoints."""

import json
import logging
from typing import Any, Dict, List, Optional

from .base_client import BaseAPIClient

logger = logging.getLogger(__name__)


class MarketingClient(BaseAPIClient):
    """Full-featured client for the TikTok Marketing API (v1.3).

    Extends BaseAPIClient with typed methods for every endpoint category.
    Uses auto-pagination and rate limiting from the base class.
    """

    # ── Campaign endpoints ──────────────────────────────────────────────

    async def get_campaigns(
        self,
        status: Optional[str] = None,
        campaign_ids: Optional[List[str]] = None,
        page: int = 1,
        page_size: int = 200,
    ) -> Dict[str, Any]:
        filtering = {}
        if status:
            filtering["primary_status"] = status
        if campaign_ids:
            filtering["campaign_ids"] = campaign_ids
        params: Dict[str, Any] = {"page": page, "page_size": page_size}
        if filtering:
            params["filtering"] = filtering
        return await self.request("GET", "campaign/get/", params=params)

    async def get_campaign_details(self, campaign_id: str) -> Dict[str, Any]:
        params = {"filtering": {"campaign_ids": [campaign_id]}}
        return await self.request("GET", "campaign/get/", params=params)

    async def create_campaign(self, campaign_data: Dict[str, Any]) -> Dict[str, Any]:
        return await self.request("POST", "campaign/create/", data=campaign_data)

    async def update_campaign(self, campaign_data: Dict[str, Any]) -> Dict[str, Any]:
        return await self.request("POST", "campaign/update/", data=campaign_data)

    async def update_campaign_status(self, campaign_ids: List[str], status: str) -> Dict[str, Any]:
        return await self.request("POST", "campaign/status/update/", data={
            "campaign_ids": campaign_ids,
            "operation_status": status,
        })

    # ── Ad Group endpoints ──────────────────────────────────────────────

    async def get_adgroups(
        self,
        campaign_ids: Optional[List[str]] = None,
        adgroup_ids: Optional[List[str]] = None,
        status: Optional[str] = None,
        page: int = 1,
        page_size: int = 200,
    ) -> Dict[str, Any]:
        filtering = {}
        if campaign_ids:
            filtering["campaign_ids"] = campaign_ids
        if adgroup_ids:
            filtering["adgroup_ids"] = adgroup_ids
        if status:
            filtering["primary_status"] = status
        params: Dict[str, Any] = {"page": page, "page_size": page_size}
        if filtering:
            params["filtering"] = filtering
        return await self.request("GET", "adgroup/get/", params=params)

    async def get_adgroup_details(self, adgroup_id: str) -> Dict[str, Any]:
        params = {"filtering": {"adgroup_ids": [adgroup_id]}}
        return await self.request("GET", "adgroup/get/", params=params)

    async def create_adgroup(self, adgroup_data: Dict[str, Any]) -> Dict[str, Any]:
        return await self.request("POST", "adgroup/create/", data=adgroup_data)

    async def update_adgroup(self, adgroup_data: Dict[str, Any]) -> Dict[str, Any]:
        return await self.request("POST", "adgroup/update/", data=adgroup_data)

    async def update_adgroup_status(self, adgroup_ids: List[str], status: str) -> Dict[str, Any]:
        return await self.request("POST", "adgroup/status/update/", data={
            "adgroup_ids": adgroup_ids,
            "operation_status": status,
        })

    # ── Ad endpoints ────────────────────────────────────────────────────

    async def get_ads(
        self,
        campaign_ids: Optional[List[str]] = None,
        adgroup_ids: Optional[List[str]] = None,
        ad_ids: Optional[List[str]] = None,
        status: Optional[str] = None,
        page: int = 1,
        page_size: int = 200,
    ) -> Dict[str, Any]:
        filtering = {}
        if campaign_ids:
            filtering["campaign_ids"] = campaign_ids
        if adgroup_ids:
            filtering["adgroup_ids"] = adgroup_ids
        if ad_ids:
            filtering["ad_ids"] = ad_ids
        if status:
            filtering["primary_status"] = status
        params: Dict[str, Any] = {"page": page, "page_size": page_size}
        if filtering:
            params["filtering"] = filtering
        return await self.request("GET", "ad/get/", params=params)

    async def get_ad_details(self, ad_id: str) -> Dict[str, Any]:
        params = {"filtering": {"ad_ids": [ad_id]}}
        return await self.request("GET", "ad/get/", params=params)

    async def create_ad(self, ad_data: Dict[str, Any]) -> Dict[str, Any]:
        # TikTok ad/create/ requires adgroup_id at top level, creative fields in a "creatives" array
        creative = {k: v for k, v in ad_data.items() if k != "adgroup_id" and v is not None}
        payload = {"adgroup_id": ad_data["adgroup_id"], "creatives": [creative]}
        return await self.request("POST", "ad/create/", data=payload)

    async def update_ad(self, ad_data: Dict[str, Any]) -> Dict[str, Any]:
        # TikTok ad/update/ requires the full creative context even for partial updates.
        # Missing any of these fields causes 40002 errors. Auto-fetch from the existing ad.
        ad_id = ad_data.get("ad_id")
        _REQUIRED_CARRYFORWARD = [
            "ad_name", "adgroup_id", "ad_format", "call_to_action",
            # Spark Ad identity fields
            "identity_id", "identity_type", "identity_authorized_bc_id",
            "tiktok_item_id",
            # Objective-specific fields
            "page_id", "landing_page_url",
        ]
        if ad_id:
            existing = await self.get_ad_details(ad_id)
            ads = existing.get("data", {}).get("list", [])
            if ads:
                for field in _REQUIRED_CARRYFORWARD:
                    if not ad_data.get(field) and ads[0].get(field):
                        ad_data[field] = ads[0][field]

        creative = {k: v for k, v in ad_data.items() if k != "adgroup_id" and v is not None}
        payload = {"adgroup_id": ad_data.get("adgroup_id"), "creatives": [creative]}
        if not payload["adgroup_id"]:
            del payload["adgroup_id"]
        return await self.request("POST", "ad/update/", data=payload)

    async def update_ad_status(self, ad_ids: List[str], status: str) -> Dict[str, Any]:
        return await self.request("POST", "ad/status/update/", data={
            "ad_ids": ad_ids,
            "operation_status": status,
        })

    # ── Account / Identity endpoints ────────────────────────────────────

    async def get_advertiser_info(self) -> Dict[str, Any]:
        return await self.request("GET", "advertiser/info/", params={
            "advertiser_ids": [self.advertiser_id],
        })

    async def get_identities(self) -> Dict[str, Any]:
        return await self.request("GET", "identity/get/")

    # ── Pixel endpoints ─────────────────────────────────────────────────

    async def get_pixels(self, page: int = 1, page_size: int = 20) -> Dict[str, Any]:
        return await self.request("GET", "pixel/list/", params={
            "page": page, "page_size": min(page_size, 20),  # API max is 20
        })

    # ── Audience / DMP endpoints ────────────────────────────────────────

    async def get_custom_audiences(
        self,
        custom_audience_ids: Optional[List[str]] = None,
        page: int = 1,
        page_size: int = 100,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"page": page, "page_size": min(page_size, 100)}
        if custom_audience_ids:
            params["custom_audience_ids"] = custom_audience_ids
        return await self.request("GET", "dmp/custom_audience/list/", params=params)

    async def create_crm_audience(self, audience_data: Dict[str, Any]) -> Dict[str, Any]:
        return await self.request("POST", "dmp/custom_audience/create/", data=audience_data)

    async def create_lookalike_audience(self, audience_data: Dict[str, Any]) -> Dict[str, Any]:
        return await self.request("POST", "dmp/custom_audience/lookalike/create/", data=audience_data)

    async def create_rule_audience(self, audience_data: Dict[str, Any]) -> Dict[str, Any]:
        return await self.request("POST", "dmp/custom_audience/rule/create/", data=audience_data)

    async def delete_audience(self, custom_audience_ids: List[str]) -> Dict[str, Any]:
        return await self.request("POST", "dmp/custom_audience/delete/", data={
            "custom_audience_ids": custom_audience_ids,
        })

    async def estimate_audience_size(self, targeting: Dict[str, Any]) -> Dict[str, Any]:
        return await self.request("POST", "dmp/custom_audience/reach/estimate/", data=targeting)

    # ── Catalog / Product endpoints ─────────────────────────────────────

    async def get_catalogs(self, bc_id: Optional[str] = None, page: int = 1, page_size: int = 200) -> Dict[str, Any]:
        params: Dict[str, Any] = {"page": page, "page_size": page_size}
        if bc_id:
            params["bc_id"] = bc_id
        return await self.request("GET", "catalog/get/", params=params)

    async def get_catalog_products(
        self, catalog_id: str, page: int = 1, page_size: int = 200,
    ) -> Dict[str, Any]:
        return await self.request("GET", "catalog/product/get/", params={
            "catalog_id": catalog_id,
            "page": page, "page_size": page_size,
        })

    async def get_product_sets(
        self, catalog_id: str, page: int = 1, page_size: int = 200,
    ) -> Dict[str, Any]:
        return await self.request("GET", "catalog/set/get/", params={
            "catalog_id": catalog_id,
            "page": page, "page_size": page_size,
        })

    # ── Creative / Asset endpoints ──────────────────────────────────────

    async def get_ad_creatives(
        self,
        status: Optional[str] = None,
        campaign_ids: Optional[List[str]] = None,
        adgroup_ids: Optional[List[str]] = None,
        page: int = 1,
        page_size: int = 200,
    ) -> Dict[str, Any]:
        """List ads with their embedded creative details (no standalone creative endpoint exists)."""
        params: Dict[str, Any] = {"page": page, "page_size": page_size}
        filtering = {}
        if status:
            filtering["primary_status"] = status
        if campaign_ids:
            filtering["campaign_ids"] = campaign_ids
        if adgroup_ids:
            filtering["adgroup_ids"] = adgroup_ids
        if filtering:
            params["filtering"] = filtering
        return await self.request("GET", "ad/get/", params=params)

    async def get_videos(
        self, video_ids: Optional[List[str]] = None, page: int = 1, page_size: int = 200,
    ) -> Dict[str, Any]:
        """Get video info by IDs, or search/list all videos if no IDs given."""
        if video_ids:
            # info endpoint requires video_ids
            return await self.request("GET", "file/video/ad/info/", params={
                "video_ids": video_ids,
            })
        # No IDs specified — use search endpoint to list all
        return await self.request("GET", "file/video/ad/search/", params={
            "page": page, "page_size": page_size,
        })

    async def search_videos(
        self, keyword: Optional[str] = None, page: int = 1, page_size: int = 200,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"page": page, "page_size": page_size}
        filtering = {}
        if keyword:
            filtering["display_name"] = keyword
        if filtering:
            params["filtering"] = filtering
        return await self.request("GET", "file/video/ad/search/", params=params)

    async def search_images(
        self, image_ids: Optional[List[str]] = None, page: int = 1, page_size: int = 100,
    ) -> Dict[str, Any]:
        """Search/list image assets in the ad account library."""
        params: Dict[str, Any] = {"page": page, "page_size": min(page_size, 100)}
        filtering = {}
        if image_ids:
            filtering["image_ids"] = image_ids
        if filtering:
            params["filtering"] = filtering
        return await self.request("GET", "file/image/ad/search/", params=params)

    async def upload_video(self, video_data: Dict[str, Any]) -> Dict[str, Any]:
        return await self.request("POST", "file/video/ad/upload/", data=video_data)

    async def upload_image(self, image_path: str) -> Dict[str, Any]:
        with open(image_path, "rb") as f:
            files = {"image_file": f}
            data = {"upload_type": "UPLOAD_BY_FILE"}
            return await self.request("POST", "file/image/ad/upload/", data=data, files=files)

    async def upload_image_by_url(self, image_url: str) -> Dict[str, Any]:
        return await self.request("POST", "file/image/ad/upload/", data={
            "upload_type": "UPLOAD_BY_URL",
            "image_url": image_url,
        })

    # ── Spark Ads endpoints ─────────────────────────────────────────────

    async def authorize_spark_ad(self, tt_video_data: Dict[str, Any]) -> Dict[str, Any]:
        return await self.request("POST", "tt_video/authorize/", data=tt_video_data)

    async def get_spark_ad_info(self, auth_codes: List[str]) -> Dict[str, Any]:
        return await self.request("POST", "tt_video/info/", data={"auth_codes": auth_codes})

    async def get_tt_videos(
        self,
        identity_id: str,
        identity_type: str = "BC_AUTH_TT",
        identity_authorized_bc_id: Optional[str] = None,
        cursor: Optional[int] = None,
    ) -> Dict[str, Any]:
        """List TikTok posts/videos from a connected identity (via Business Center).

        Uses identity/video/get/ which returns item_id, text, video_info, auth_info
        for Spark Ad usage. Cursor-based pagination (pass cursor from previous response).
        """
        params: Dict[str, Any] = {
            "identity_id": identity_id,
            "identity_type": identity_type,
        }
        if identity_authorized_bc_id:
            params["identity_authorized_bc_id"] = identity_authorized_bc_id
        if cursor is not None:
            params["cursor"] = cursor
        return await self.request("GET", "identity/video/get/", params=params)

    # ── Lead Ads endpoints ───────────────────────────────────────────────

    async def get_lead_forms(self, page: int = 1, page_size: int = 100) -> Dict[str, Any]:
        """List lead generation forms for the advertiser."""
        return await self.request("GET", "tt_leadAds/form/list/", params={
            "page": page, "page_size": min(page_size, 100),
        })

    async def create_lead_download_task(self, form_id: str) -> Dict[str, Any]:
        """Create a task to download leads from a specific form."""
        return await self.request("POST", "page/lead/task/", data={
            "form_id": form_id,
        })

    async def download_lead_task(self, task_id: str) -> Dict[str, Any]:
        """Download leads from a completed download task."""
        return await self.request("GET", "page/lead/task/download/", params={
            "task_id": task_id,
        })

    # ── Targeting / Tools endpoints ─────────────────────────────────────

    async def get_interest_categories(
        self, version: int = 2, placements: Optional[List[str]] = None, language: str = "en",
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"version": version, "language": language}
        if placements:
            params["placements"] = placements
        return await self.request("GET", "tool/interest_category/", params=params)

    async def get_regions(
        self,
        placements: Optional[List[str]] = None,
        objective_type: str = "TRAFFIC",
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "placements": placements or ["PLACEMENT_TIKTOK"],
            "objective_type": objective_type,
        }
        return await self.request("GET", "tool/region/", params=params)

    async def get_action_categories(self) -> Dict[str, Any]:
        return await self.request("GET", "tool/action_category/")

    async def get_targeting_recommend(self, option_type: str, country_code: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {"type": option_type}
        if country_code:
            params["country_code"] = country_code
        return await self.request("GET", "tools/target_recommend/", params=params)

    # ── Reporting endpoints ─────────────────────────────────────────────

    async def get_report(
        self,
        report_type: str,
        data_level: str,
        dimensions: List[str],
        metrics: List[str],
        start_date: str,
        end_date: str,
        filtering: Optional[List[Dict[str, Any]]] = None,
        page: int = 1,
        page_size: int = 200,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "report_type": report_type,
            "data_level": data_level,
            "dimensions": dimensions,
            "metrics": metrics,
            "start_date": start_date,
            "end_date": end_date,
            "page": page,
            "page_size": page_size,
        }
        # enable_total_metrics only supported for BASIC and TT_SHOP reports
        if report_type == "BASIC":
            params["enable_total_metrics"] = True
        if filtering:
            params["filtering"] = filtering
        return await self.request("GET", "report/integrated/get/", params=params)

    async def get_report_all_pages(
        self,
        report_type: str,
        data_level: str,
        dimensions: List[str],
        metrics: List[str],
        start_date: str,
        end_date: str,
        filtering: Optional[List[Dict[str, Any]]] = None,
        page_size: int = 200,
    ) -> Dict[str, Any]:
        """Fetch all pages of a report."""
        params: Dict[str, Any] = {
            "report_type": report_type,
            "data_level": data_level,
            "dimensions": dimensions,
            "metrics": metrics,
            "start_date": start_date,
            "end_date": end_date,
        }
        # enable_total_metrics only supported for BASIC and TT_SHOP reports
        if report_type == "BASIC":
            params["enable_total_metrics"] = True
        if filtering:
            params["filtering"] = filtering
        return await self.request_all_pages("GET", "report/integrated/get/", params=params, page_size=page_size)

    async def create_report_task(
        self,
        report_type: str,
        data_level: str,
        dimensions: List[str],
        metrics: List[str],
        start_date: str,
        end_date: str,
        filtering: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        data = {
            "report_type": report_type,
            "data_level": data_level,
            "dimensions": dimensions,
            "metrics": metrics,
            "start_date": start_date,
            "end_date": end_date,
        }
        if filtering:
            data["filtering"] = filtering
        return await self.request("POST", "report/task/create/", data=data)

    async def check_report_task(self, task_id: str) -> Dict[str, Any]:
        return await self.request("GET", "report/task/check/", params={"task_id": task_id})

    async def download_report_task(self, task_id: str) -> Dict[str, Any]:
        return await self.request("GET", "report/task/download/", params={"task_id": task_id})

    # ── GMV Max / Smart+ endpoints ──────────────────────────────────────

    async def get_gmv_max_report(
        self,
        campaign_ids: List[str],
        metrics: List[str],
        start_date: str,
        end_date: str,
    ) -> Dict[str, Any]:
        return await self.request("GET", "smart_plus/gmv_max/report/get/", params={
            "campaign_ids": campaign_ids,
            "metrics": metrics,
            "start_date": start_date,
            "end_date": end_date,
        })

    async def create_smart_plus_campaign(self, campaign_data: Dict[str, Any]) -> Dict[str, Any]:
        return await self.request("POST", "smart_plus/campaign/create/", data=campaign_data)

    # ── Comment Management endpoints ─────────────────────────────────────

    async def list_comments(
        self,
        search_field: str,
        search_value: str,
        start_time: str,
        end_time: str,
        page: int = 1,
        page_size: int = 50,
        status: Optional[str] = None,
    ) -> Dict[str, Any]:
        """List comments on TikTok ads.

        Args:
            search_field: ADGROUP_ID or AD_ID
            search_value: The ID to search for
            start_time: Start date (YYYY-MM-DD)
            end_time: End date (YYYY-MM-DD)
        """
        params: Dict[str, Any] = {
            "search_field": search_field,
            "search_value": search_value,
            "start_time": start_time,
            "end_time": end_time,
            "page": page,
            "page_size": min(page_size, 50),
        }
        if status:
            params["status"] = status
        return await self.request("GET", "comment/list/", params=params)

    async def reply_comment(self, ad_id: str, comment_id: str, text: str) -> Dict[str, Any]:
        """Reply to a comment on a TikTok ad."""
        return await self.request("POST", "comment/reply/", data={
            "ad_id": ad_id,
            "comment_id": comment_id,
            "text": text,
        })

    async def hide_comments(self, ad_id: str, comment_ids: List[str], hidden: bool = True) -> Dict[str, Any]:
        """Hide or unhide comments on a TikTok ad."""
        return await self.request("POST", "comment/status/update/", data={
            "ad_id": ad_id,
            "comment_ids": comment_ids,
            "hidden": hidden,
        })

    # ── Event / Conversion Tracking endpoints ────────────────────────────

    async def track_event(
        self,
        pixel_code: str,
        event: str,
        event_id: Optional[str] = None,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Send a server-side conversion event via Events API."""
        data: Dict[str, Any] = {
            "pixel_code": pixel_code,
            "event": event,
        }
        if event_id:
            data["event_id"] = event_id
        if properties:
            data["properties"] = properties
        return await self.request("POST", "pixel/track/", data=data)

    # ── Pixel CRUD endpoints ─────────────────────────────────────────────

    async def create_pixel(self, pixel_name: str) -> Dict[str, Any]:
        """Create a new pixel."""
        return await self.request("POST", "pixel/create/", data={
            "pixel_name": pixel_name,
        })

    async def update_pixel(self, pixel_code: str, pixel_name: str) -> Dict[str, Any]:
        """Update a pixel's name."""
        return await self.request("POST", "pixel/update/", data={
            "pixel_code": pixel_code,
            "pixel_name": pixel_name,
        })

    # ── Business Center endpoints ────────────────────────────────────────

    async def get_bc_info(self, bc_id: str) -> Dict[str, Any]:
        """Get Business Center information."""
        return await self.request("GET", "bc/get/", params={"bc_id": bc_id})

    async def get_bc_assets(
        self, bc_id: str, asset_type: str = "ADVERTISER", page: int = 1, page_size: int = 50,
    ) -> Dict[str, Any]:
        """List assets (e.g. advertisers) under a Business Center."""
        return await self.request("GET", "bc/asset/get/", params={
            "bc_id": bc_id,
            "asset_type": asset_type,
            "page": page,
            "page_size": min(page_size, 50),
        })

    # ── Creative AI endpoints ────────────────────────────────────────────

    async def generate_ad_text(
        self,
        adgroup_id: str,
        language: str = "EN",
        num_results: int = 5,
        brand_name: Optional[str] = None,
        keywords: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Generate ad text suggestions using TikTok's AI (not available for US/CA advertisers)."""
        data: Dict[str, Any] = {
            "advertiser_id": self.advertiser_id,
            "adgroup_id": adgroup_id,
            "language": language.upper(),
            "number_of_results": min(num_results, 10),
        }
        if brand_name:
            data["brand_name"] = brand_name
        if keywords:
            data["keywords"] = keywords
        return await self.request("POST", "creative/smart_text/generate/", data=data)
