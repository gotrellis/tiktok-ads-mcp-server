"""Tool 4: tiktok_audience — Audience intelligence and management.

Supported action values:
  list              — List custom audiences
  estimate_reach    — Estimate audience size/reach for targeting parameters
  create_crm        — Create CRM/customer file audience
  create_lookalike  — Create Lookalike audience from seed audience
  create_engagement — Create engagement-based rule audience
  delete            — Delete custom audiences
"""

import logging
from typing import Any, Dict, List

from ..api.marketing_client import MarketingClient
from ..cache.cache_manager import CacheManager
from ..utils.confirmation import build_preview

logger = logging.getLogger(__name__)


class AudienceTool:
    """Consolidated audience intelligence and management tool."""

    def __init__(self, client: MarketingClient, cache: CacheManager):
        self.client = client
        self.cache = cache

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
            logger.error(f"audience({action}) failed: {e}")
            return _error(str(e))

    # ── Handlers ────────────────────────────────────────────────────────

    async def _list_audiences(self, args: Dict[str, Any]) -> Dict[str, Any]:
        page = args.get("page", 1)
        page_size = args.get("page_size", 200)
        custom_audience_ids = args.get("custom_audience_ids")
        result = await self.client.get_custom_audiences(
            custom_audience_ids=custom_audience_ids,
            page=page, page_size=page_size,
        )
        items = result.get("data", {}).get("list", [])
        page_info = result.get("data", {}).get("page_info", {})
        # Cache for other tools
        self.cache.set("audiences", items)
        return _success(items, page_info)

    async def _estimate_reach(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Provide targeting intelligence for audience planning.

        TikTok does not expose a reach estimation endpoint in Marketing API v1.3.
        Instead, this returns available regions and bid recommendations to help
        with audience planning. Use the TikTok Ads Manager UI for exact reach estimates.
        """
        objective_type = args.get("objective_type", "TRAFFIC")
        placements = args.get("placements", ["PLACEMENT_TIKTOK"])
        location_ids = args.get("location_ids")

        result_data: Dict[str, Any] = {
            "note": "TikTok Marketing API v1.3 does not expose a reach estimation endpoint. "
                    "Use TikTok Ads Manager UI for exact audience size/reach estimates. "
                    "Below is targeting intelligence to help with audience planning.",
        }

        # Get available regions
        try:
            regions = await self.client.get_regions(
                placements=placements,
                objective_type=objective_type,
            )
            region_list = regions.get("data", {}).get("region_info", [])
            # If location_ids specified, filter to those
            if location_ids:
                location_set = set(location_ids)
                region_list = [r for r in region_list if str(r.get("location_id")) in location_set]
            else:
                # Show only country-level regions by default
                region_list = [r for r in region_list if r.get("level") == "COUNTRY"]
            result_data["available_regions"] = region_list[:50]
            result_data["total_regions"] = len(regions.get("data", {}).get("region_info", []))
        except Exception as e:
            result_data["regions_error"] = str(e)

        # Get bid recommendation if targeting params provided
        if location_ids:
            try:
                from ..config import OBJECTIVE_TO_GOALS, GOAL_TO_BILLING
                goals = OBJECTIVE_TO_GOALS.get(objective_type, [])
                # Try each goal until one succeeds
                for goal in goals:
                    billing_events = GOAL_TO_BILLING.get(goal, ["OCPM"])
                    bid_data: Dict[str, Any] = {
                        "advertiser_id": self.client.advertiser_id,
                        "objective_type": objective_type,
                        "optimization_goal": goal,
                        "placements": placements,
                        "location_ids": location_ids,
                        "billing_event": billing_events[0],
                    }
                    if args.get("age_groups"):
                        bid_data["age_groups"] = args["age_groups"]
                    if args.get("gender"):
                        bid_data["gender"] = args["gender"]
                    try:
                        bid_result = await self.client.request("POST", "tool/bid/recommend/", data=bid_data)
                        if bid_result.get("code") == 0:
                            result_data["suggested_bid"] = bid_result.get("data", {})
                            result_data["bid_context"] = {
                                "objective_type": objective_type,
                                "optimization_goal": goal,
                                "billing_event": billing_events[0],
                            }
                            break
                    except Exception:
                        continue
            except Exception as e:
                result_data["bid_error"] = str(e)

        # Include targeting params used for reference
        result_data["targeting_params"] = {
            "placements": placements,
            "objective_type": objective_type,
            "location_ids": location_ids,
            "age_groups": args.get("age_groups"),
            "gender": args.get("gender"),
        }

        return _success(result_data)

    async def _create_crm(self, args: Dict[str, Any]) -> Dict[str, Any]:
        params = args.get("params", {})
        confirm = args.get("confirm", False)

        if not params.get("custom_audience_name"):
            return _error("custom_audience_name is required in params")

        if not confirm:
            return build_preview("create", "CRM audience", {
                "name": params.get("custom_audience_name"),
                "file_paths": params.get("file_paths", []),
            })

        result = await self.client.create_crm_audience(params)
        audience_id = result.get("data", {}).get("custom_audience_id")
        self.cache.invalidate("audiences")
        return _success({
            "custom_audience_id": audience_id,
            "message": f"CRM audience created: {audience_id}",
        })

    async def _create_lookalike(self, args: Dict[str, Any]) -> Dict[str, Any]:
        params = args.get("params", {})
        confirm = args.get("confirm", False)

        if not params.get("custom_audience_name"):
            return _error("custom_audience_name is required in params")
        if not params.get("source_audience_ids"):
            return _error("source_audience_ids is required in params")

        if not confirm:
            return build_preview("create", "Lookalike audience", {
                "name": params.get("custom_audience_name"),
                "source_audience_ids": params.get("source_audience_ids"),
                "lookalike_spec": params.get("lookalike_spec", {}),
            })

        result = await self.client.create_lookalike_audience(params)
        audience_id = result.get("data", {}).get("custom_audience_id")
        self.cache.invalidate("audiences")
        return _success({
            "custom_audience_id": audience_id,
            "message": f"Lookalike audience created: {audience_id}",
        })

    async def _create_engagement(self, args: Dict[str, Any]) -> Dict[str, Any]:
        params = args.get("params", {})
        confirm = args.get("confirm", False)

        if not params.get("custom_audience_name"):
            return _error("custom_audience_name is required in params")

        if not confirm:
            return build_preview("create", "engagement audience", {
                "name": params.get("custom_audience_name"),
                "rules": params.get("rules", []),
                "retention_days": params.get("retention_days"),
            })

        result = await self.client.create_rule_audience(params)
        audience_id = result.get("data", {}).get("custom_audience_id")
        self.cache.invalidate("audiences")
        return _success({
            "custom_audience_id": audience_id,
            "message": f"Engagement audience created: {audience_id}",
        })

    async def _delete_audiences(self, args: Dict[str, Any]) -> Dict[str, Any]:
        audience_ids = args.get("custom_audience_ids", [])
        confirm = args.get("confirm", False)

        if not audience_ids:
            return _error("custom_audience_ids is required")

        if not confirm:
            return build_preview("delete", "audiences", {
                "custom_audience_ids": audience_ids,
                "count": len(audience_ids),
            }, warnings=[f"This will permanently delete {len(audience_ids)} audience(s)"])

        result = await self.client.delete_audience(audience_ids)
        self.cache.invalidate("audiences")
        return _success({
            "message": f"Deleted {len(audience_ids)} audience(s)",
            "custom_audience_ids": audience_ids,
        })

    # ── Handler dispatch table ──────────────────────────────────────────

    _handlers = {
        "list": _list_audiences,
        "estimate_reach": _estimate_reach,
        "estimate_size": _estimate_reach,  # alias for backward compat
        "create_crm": _create_crm,
        "create_lookalike": _create_lookalike,
        "create_engagement": _create_engagement,
        "delete": _delete_audiences,
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
