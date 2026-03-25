"""Tool 1: tiktok_entity_get — Read any TikTok Ads entity.

Supported entity_type values:
  campaigns, campaign_details, adgroups, adgroup_details, ads, ad_details,
  account_info, pixels, catalogs, catalog_products, product_sets,
  interest_categories, regions, action_categories, identities, audiences,
  lead_download_task, lead_download, bc_info, bc_assets
"""

import logging
from typing import Any, Dict, List, Optional

from ..api.marketing_client import MarketingClient
from ..cache.cache_manager import CacheManager

logger = logging.getLogger(__name__)

# Entity types that benefit from caching
CACHEABLE_ENTITIES = {
    "interest_categories",
    "regions",
    "action_categories",
    "account_info",
    "pixels",
}


class EntityGetTool:
    """Consolidated read tool for all TikTok Ads entities."""

    def __init__(self, client: MarketingClient, cache: CacheManager):
        self.client = client
        self.cache = cache

    async def execute(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Route to the appropriate handler based on entity_type."""
        entity_type = arguments.get("entity_type", "")
        if not entity_type:
            return _error("entity_type is required")

        handler = self._handlers.get(entity_type)
        if not handler:
            valid = ", ".join(sorted(self._handlers.keys()))
            return _error(f"Unknown entity_type '{entity_type}'. Valid: {valid}")

        try:
            return await handler(self, arguments)
        except Exception as e:
            logger.error(f"entity_get({entity_type}) failed: {e}")
            return _error(str(e))

    # ── Handlers ────────────────────────────────────────────────────────

    async def _get_campaigns(self, args: Dict[str, Any]) -> Dict[str, Any]:
        status = args.get("status")
        campaign_ids = args.get("campaign_ids")
        page = args.get("page", 1)
        page_size = args.get("page_size", 200)

        result = await self.client.get_campaigns(
            status=status, campaign_ids=campaign_ids,
            page=page, page_size=page_size,
        )
        items = result.get("data", {}).get("list", [])
        page_info = result.get("data", {}).get("page_info", {})
        return _success(items, page_info)

    async def _get_campaign_details(self, args: Dict[str, Any]) -> Dict[str, Any]:
        campaign_id = args.get("campaign_id")
        if not campaign_id:
            return _error("campaign_id is required for campaign_details")
        result = await self.client.get_campaign_details(campaign_id)
        items = result.get("data", {}).get("list", [])
        if not items:
            return _error(f"Campaign {campaign_id} not found")
        return _success(items[0])

    async def _get_adgroups(self, args: Dict[str, Any]) -> Dict[str, Any]:
        campaign_ids = args.get("campaign_ids")
        campaign_id = args.get("campaign_id")
        adgroup_ids = args.get("adgroup_ids")
        status = args.get("status")
        page = args.get("page", 1)
        page_size = args.get("page_size", 200)

        # Support both single campaign_id and list
        if campaign_id and not campaign_ids:
            campaign_ids = [campaign_id]

        result = await self.client.get_adgroups(
            campaign_ids=campaign_ids, adgroup_ids=adgroup_ids,
            status=status, page=page, page_size=page_size,
        )
        items = result.get("data", {}).get("list", [])
        page_info = result.get("data", {}).get("page_info", {})
        return _success(items, page_info)

    async def _get_adgroup_details(self, args: Dict[str, Any]) -> Dict[str, Any]:
        adgroup_id = args.get("adgroup_id")
        if not adgroup_id:
            return _error("adgroup_id is required for adgroup_details")
        result = await self.client.get_adgroup_details(adgroup_id)
        items = result.get("data", {}).get("list", [])
        if not items:
            return _error(f"Ad group {adgroup_id} not found")
        return _success(items[0])

    async def _get_ads(self, args: Dict[str, Any]) -> Dict[str, Any]:
        campaign_ids = args.get("campaign_ids")
        campaign_id = args.get("campaign_id")
        adgroup_ids = args.get("adgroup_ids")
        adgroup_id = args.get("adgroup_id")
        ad_ids = args.get("ad_ids")
        status = args.get("status")
        page = args.get("page", 1)
        page_size = args.get("page_size", 200)

        if campaign_id and not campaign_ids:
            campaign_ids = [campaign_id]
        if adgroup_id and not adgroup_ids:
            adgroup_ids = [adgroup_id]

        result = await self.client.get_ads(
            campaign_ids=campaign_ids, adgroup_ids=adgroup_ids,
            ad_ids=ad_ids, status=status, page=page, page_size=page_size,
        )
        items = result.get("data", {}).get("list", [])
        page_info = result.get("data", {}).get("page_info", {})
        return _success(items, page_info)

    async def _get_ad_details(self, args: Dict[str, Any]) -> Dict[str, Any]:
        ad_id = args.get("ad_id")
        if not ad_id:
            return _error("ad_id is required for ad_details")
        result = await self.client.get_ad_details(ad_id)
        items = result.get("data", {}).get("list", [])
        if not items:
            return _error(f"Ad {ad_id} not found")
        return _success(items[0])

    async def _get_account_info(self, args: Dict[str, Any]) -> Dict[str, Any]:
        cached = self.cache.get("account_info")
        if cached:
            return _success(cached, metadata={"cached": True})
        result = await self.client.get_advertiser_info()
        data = result.get("data", {})
        self.cache.set("account_info", data)
        return _success(data)

    async def _get_pixels(self, args: Dict[str, Any]) -> Dict[str, Any]:
        cached = self.cache.get("pixels")
        if cached:
            return _success(cached, metadata={"cached": True})
        result = await self.client.get_pixels()
        items = result.get("data", {}).get("list", [])
        self.cache.set("pixels", items)
        return _success(items)

    async def _get_catalogs(self, args: Dict[str, Any]) -> Dict[str, Any]:
        bc_id = args.get("bc_id")
        if not bc_id:
            return _error(
                "bc_id (Business Center ID) is required for catalogs",
                suggestion="Pass bc_id parameter. Find your BC ID in TikTok Business Center settings.",
            )
        result = await self.client.get_catalogs(bc_id=bc_id)
        items = result.get("data", {}).get("list", [])
        page_info = result.get("data", {}).get("page_info", {})
        return _success(items, page_info)

    async def _get_catalog_products(self, args: Dict[str, Any]) -> Dict[str, Any]:
        catalog_id = args.get("catalog_id")
        if not catalog_id:
            return _error("catalog_id is required for catalog_products")
        page = args.get("page", 1)
        page_size = args.get("page_size", 200)
        result = await self.client.get_catalog_products(catalog_id, page=page, page_size=page_size)
        items = result.get("data", {}).get("list", [])
        page_info = result.get("data", {}).get("page_info", {})
        return _success(items, page_info)

    async def _get_product_sets(self, args: Dict[str, Any]) -> Dict[str, Any]:
        catalog_id = args.get("catalog_id")
        if not catalog_id:
            return _error("catalog_id is required for product_sets")
        result = await self.client.get_product_sets(catalog_id)
        items = result.get("data", {}).get("list", [])
        page_info = result.get("data", {}).get("page_info", {})
        return _success(items, page_info)

    async def _get_interest_categories(self, args: Dict[str, Any]) -> Dict[str, Any]:
        cached = self.cache.get("interest_categories")
        if cached:
            return _success(cached, metadata={"cached": True})
        version = args.get("version", 2)
        result = await self.client.get_interest_categories(version=version)
        data = result.get("data", {})
        self.cache.set("interest_categories", data)
        return _success(data)

    async def _get_regions(self, args: Dict[str, Any]) -> Dict[str, Any]:
        placements = args.get("placements") or [args.get("placement", "PLACEMENT_TIKTOK")]
        objective_type = args.get("objective_type", "TRAFFIC")
        cache_key = f"locations_{'_'.join(placements)}_{objective_type}"
        cached = self.cache.get(cache_key)
        if cached:
            return _success(cached, metadata={"cached": True})
        result = await self.client.get_regions(placements=placements, objective_type=objective_type)
        data = result.get("data", {})
        self.cache.set(cache_key, data)
        return _success(data)

    async def _get_action_categories(self, args: Dict[str, Any]) -> Dict[str, Any]:
        cached = self.cache.get("action_categories")
        if cached:
            return _success(cached, metadata={"cached": True})
        result = await self.client.get_action_categories()
        data = result.get("data", {})
        self.cache.set("action_categories", data)
        return _success(data)

    async def _get_identities(self, args: Dict[str, Any]) -> Dict[str, Any]:
        result = await self.client.get_identities()
        # TikTok returns identities under "identity_list", not "list"
        items = result.get("data", {}).get("identity_list", [])
        return _success(items)

    async def _get_lead_forms(self, args: Dict[str, Any]) -> Dict[str, Any]:
        return _error(
            "lead_forms is not available via the advertiser-level Marketing API (v1.3). "
            "Lead forms are accessed via the Business Center API: use entity_type='bc_assets' "
            "with bc_id and asset_type='LEAD' instead.",
        )

    async def _create_lead_download_task(self, args: Dict[str, Any]) -> Dict[str, Any]:
        form_id = args.get("form_id")
        if not form_id:
            return _error("form_id is required for lead_download_task")
        result = await self.client.create_lead_download_task(form_id)
        return _success(result.get("data", {}))

    async def _download_leads(self, args: Dict[str, Any]) -> Dict[str, Any]:
        task_id = args.get("task_id")
        if not task_id:
            return _error("task_id is required for lead_download")
        result = await self.client.download_lead_task(task_id)
        return _success(result.get("data", {}))

    async def _get_bc_info(self, args: Dict[str, Any]) -> Dict[str, Any]:
        bc_id = args.get("bc_id")
        if not bc_id:
            return _error("bc_id is required for bc_info")
        result = await self.client.get_bc_info(bc_id)
        return _success(result.get("data", {}))

    async def _get_bc_assets(self, args: Dict[str, Any]) -> Dict[str, Any]:
        bc_id = args.get("bc_id")
        if not bc_id:
            return _error("bc_id is required for bc_assets")
        asset_type = args.get("asset_type", "ADVERTISER")
        page = args.get("page", 1)
        page_size = args.get("page_size", 100)
        result = await self.client.get_bc_assets(bc_id, asset_type=asset_type, page=page, page_size=page_size)
        items = result.get("data", {}).get("list", [])
        page_info = result.get("data", {}).get("page_info", {})
        return _success(items, page_info)

    async def _get_audiences(self, args: Dict[str, Any]) -> Dict[str, Any]:
        cached = self.cache.get("audiences")
        if cached:
            return _success(cached, metadata={"cached": True})
        result = await self.client.get_custom_audiences()
        items = result.get("data", {}).get("list", [])
        self.cache.set("audiences", items)
        return _success(items)

    # ── Handler dispatch table ──────────────────────────────────────────

    _handlers = {
        "campaigns": _get_campaigns,
        "campaign_details": _get_campaign_details,
        "adgroups": _get_adgroups,
        "adgroup_details": _get_adgroup_details,
        "ads": _get_ads,
        "ad_details": _get_ad_details,
        "account_info": _get_account_info,
        "pixels": _get_pixels,
        "catalogs": _get_catalogs,
        "catalog_products": _get_catalog_products,
        "product_sets": _get_product_sets,
        "interest_categories": _get_interest_categories,
        "regions": _get_regions,
        "action_categories": _get_action_categories,
        "identities": _get_identities,
        "audiences": _get_audiences,
        "lead_forms": _get_lead_forms,
        "lead_download_task": _create_lead_download_task,
        "lead_download": _download_leads,
        "bc_info": _get_bc_info,
        "bc_assets": _get_bc_assets,
    }


# ── Response helpers ────────────────────────────────────────────────────

def _success(data: Any, page_info: Any = None, metadata: dict | None = None) -> Dict[str, Any]:
    result: Dict[str, Any] = {"success": True, "data": data}
    if page_info:
        result["metadata"] = {
            "total_count": page_info.get("total_number", 0),
            "page": page_info.get("page", 1),
            "page_size": page_info.get("page_size", 0),
            **(metadata or {}),
        }
    elif metadata:
        result["metadata"] = metadata
    return result


def _error(message: str, suggestion: str = "") -> Dict[str, Any]:
    result: Dict[str, Any] = {"success": False, "error_message": message}
    if suggestion:
        result["suggestion"] = suggestion
    return result
