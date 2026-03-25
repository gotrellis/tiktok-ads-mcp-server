"""Tool 2: tiktok_entity_manage — Create, update, enable/disable/delete any entity.

All write operations require confirm=true to execute. Without it, a preview is returned.

Supported actions:
  create_campaign, update_campaign,
  create_adgroup, update_adgroup,
  create_ad, update_ad,
  enable_campaigns, disable_campaigns, delete_campaigns,
  enable_adgroups, disable_adgroups, delete_adgroups,
  enable_ads, disable_ads, delete_ads,
  create_pixel, update_pixel, track_event,
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from ..api.marketing_client import MarketingClient
from ..config import (
    BATCH_STATUS_UPDATE_LIMIT,
    BID_TYPE_ALIASES,
    GOAL_NAME_ALIASES,
    MANUAL_PLACEMENT_ONLY_OBJECTIVES,
    STATUS_ACTIONS,
)
from ..utils.confirmation import build_preview
from ..validators.ad_validator import validate_ad_create, validate_ad_update
from ..validators.adgroup_validator import validate_adgroup_create, validate_adgroup_update
from ..validators.campaign_validator import validate_campaign_create, validate_campaign_update

logger = logging.getLogger(__name__)


class EntityManageTool:
    """Consolidated write tool for all TikTok Ads entity operations."""

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
            logger.error(f"entity_manage({action}) failed: {e}")
            return _error(str(e))

    # ── Campaign handlers ───────────────────────────────────────────────

    async def _create_campaign(self, args: Dict[str, Any]) -> Dict[str, Any]:
        params = args.get("params", {})
        confirm = args.get("confirm", False)

        errors = validate_campaign_create(params)
        if errors:
            return _error("; ".join(errors))

        if not confirm:
            return build_preview("create", "campaign", {
                "campaign_name": params.get("campaign_name"),
                "objective_type": params.get("objective_type"),
                "budget": params.get("budget"),
                "budget_mode": params.get("budget_mode", "BUDGET_MODE_DAY"),
            })

        result = await self.client.create_campaign(params)
        campaign_id = result.get("data", {}).get("campaign_id")
        return _success({
            "campaign_id": campaign_id,
            "message": f"Campaign created: {campaign_id}",
        })

    async def _update_campaign(self, args: Dict[str, Any]) -> Dict[str, Any]:
        params = args.get("params", {})
        confirm = args.get("confirm", False)

        # Fix #6: Accept entity_ids alias for backwards compatibility
        if not params.get("campaign_id") and params.get("entity_ids"):
            ids = params.pop("entity_ids")
            params["campaign_id"] = ids[0] if isinstance(ids, list) else ids

        errors = validate_campaign_update(params)
        if errors:
            return _error("; ".join(errors))

        if not confirm:
            return build_preview("update", "campaign", params)

        result = await self.client.update_campaign(params)
        return _success({
            "campaign_id": params.get("campaign_id"),
            "message": "Campaign updated successfully",
        })

    # ── Ad Group handlers ───────────────────────────────────────────────

    async def _create_adgroup(self, args: Dict[str, Any]) -> Dict[str, Any]:
        params = args.get("params", {})
        confirm = args.get("confirm", False)
        campaign_objective = args.get("campaign_objective")

        # Auto-correct legacy goal names to TikTok v1.3 actual names
        goal = params.get("optimization_goal")
        if goal and goal in GOAL_NAME_ALIASES:
            corrected = GOAL_NAME_ALIASES[goal]
            logger.info(f"Auto-correcting optimization_goal '{goal}' → '{corrected}'")
            params["optimization_goal"] = corrected

        # Auto-correct legacy bid_type names
        bid_type = params.get("bid_type")
        if bid_type and bid_type in BID_TYPE_ALIASES:
            corrected = BID_TYPE_ALIASES[bid_type]
            logger.info(f"Auto-correcting bid_type '{bid_type}' → '{corrected}'")
            params["bid_type"] = corrected

        # Auto-fix placement for objectives that require PLACEMENT_TYPE_NORMAL
        if campaign_objective in MANUAL_PLACEMENT_ONLY_OBJECTIVES:
            if params.get("placement_type") != "PLACEMENT_TYPE_NORMAL":
                logger.info(f"Auto-setting PLACEMENT_TYPE_NORMAL for {campaign_objective}")
                params["placement_type"] = "PLACEMENT_TYPE_NORMAL"
            if not params.get("placements"):
                params["placements"] = ["PLACEMENT_TIKTOK"]

        # ── Auto-fill defaults BEFORE validation ─────────────────────
        # All TikTok-required fields that have safe defaults are set here,
        # so the validator and TikTok API both see complete params.

        # REACH campaigns require a frequency cap. Auto-set a sensible default
        # (3 impressions per 7 days) to prevent TikTok 40002 errors.
        if (campaign_objective in ("REACH", "RF_REACH") or
                params.get("optimization_goal") == "REACH"):
            if not params.get("frequency") and not params.get("frequency_cap"):
                params["frequency"] = 3
                params["frequency_schedule"] = 7

        # TikTok requires schedule_start_time even for SCHEDULE_FROM_NOW
        if params.get("schedule_type") == "SCHEDULE_FROM_NOW" and not params.get("schedule_start_time"):
            params["schedule_start_time"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        # Fix #1: Auto-set bid_type when omitted but bid_price is present.
        # TikTok defaults to BID_TYPE_NO_BID which conflicts with bid_price > 0.
        if not params.get("bid_type"):
            if params.get("bid_price") or params.get("conversion_bid_price"):
                params["bid_type"] = "BID_TYPE_CUSTOM"
            else:
                params["bid_type"] = "BID_TYPE_NO_BID"

        # TikTok defaults pacing to FAST, which is invalid with BID_TYPE_NO_BID
        # and BID_TYPE_MAX_CONVERSION. Auto-set to SMOOTH to prevent 40002 errors.
        no_fast_bid_types = {"BID_TYPE_NO_BID", "BID_TYPE_MAX_CONVERSION"}
        if params.get("bid_type") in no_fast_bid_types and not params.get("pacing"):
            params["pacing"] = "PACING_MODE_SMOOTH"

        # CPV billing with BID_TYPE_CUSTOM requires bid_display_mode
        if params.get("billing_event") == "CPV" and params.get("bid_type") == "BID_TYPE_CUSTOM":
            if not params.get("bid_display_mode"):
                params["bid_display_mode"] = "CPV"

        # Fix #7: gender is required by TikTok API but often omitted by callers.
        # Default to GENDER_UNLIMITED (target all) to prevent 40002 errors.
        if not params.get("gender"):
            params["gender"] = "GENDER_UNLIMITED"

        # Fix #2: promotion_type is required by TikTok but often omitted by callers.
        # Auto-set based on campaign objective if not provided.
        if not params.get("promotion_type") and campaign_objective:
            opt_goal = params.get("optimization_goal")
            objective_to_promotion = {
                "LEAD_GENERATION": "LEAD_GENERATION",
                "TRAFFIC": "WEBSITE",
                "WEB_CONVERSIONS": "WEBSITE",
                "REACH": "WEBSITE",
                "VIDEO_VIEWS": "WEBSITE",
                "PRODUCT_SALES": "TIKTOK_SHOP",
            }
            # ENGAGEMENT promotion_type depends on goal — only FOLLOWERS/PROFILE_VIEWS use WEBSITE
            if campaign_objective == "ENGAGEMENT" and opt_goal in ("FOLLOWERS", "PROFILE_VIEWS"):
                params["promotion_type"] = "WEBSITE"
            else:
                default_promo = objective_to_promotion.get(campaign_objective)
                if default_promo:
                    params["promotion_type"] = default_promo

        # ── Validation (runs after all auto-fills) ───────────────────

        errors = validate_adgroup_create(params, campaign_objective)
        if errors:
            return _error("; ".join(errors))

        if not confirm:
            return build_preview("create", "adgroup", {
                "adgroup_name": params.get("adgroup_name"),
                "campaign_id": params.get("campaign_id"),
                "placement_type": params.get("placement_type"),
                "budget": params.get("budget"),
                "budget_mode": params.get("budget_mode"),
                "schedule_type": params.get("schedule_type"),
                "optimization_goal": params.get("optimization_goal"),
                "billing_event": params.get("billing_event"),
                "location_ids": params.get("location_ids"),
            })

        result = await self.client.create_adgroup(params)
        adgroup_id = result.get("data", {}).get("adgroup_id")
        return _success({
            "adgroup_id": adgroup_id,
            "message": f"Ad group created: {adgroup_id}",
        })

    async def _update_adgroup(self, args: Dict[str, Any]) -> Dict[str, Any]:
        params = args.get("params", {})
        confirm = args.get("confirm", False)

        # Fix #6: Accept entity_ids alias for backwards compatibility
        if not params.get("adgroup_id") and params.get("entity_ids"):
            ids = params.pop("entity_ids")
            params["adgroup_id"] = ids[0] if isinstance(ids, list) else ids

        errors = validate_adgroup_update(params)
        if errors:
            return _error("; ".join(errors))

        if not confirm:
            return build_preview("update", "adgroup", params)

        result = await self.client.update_adgroup(params)
        return _success({
            "adgroup_id": params.get("adgroup_id"),
            "message": "Ad group updated successfully",
        })

    # ── Ad handlers ─────────────────────────────────────────────────────

    async def _create_ad(self, args: Dict[str, Any]) -> Dict[str, Any]:
        params = args.get("params", {})
        confirm = args.get("confirm", False)
        campaign_objective = args.get("campaign_objective")

        # Merge top-level ad fields into params (LLMs sometimes pass them outside params)
        _AD_FIELDS = {
            "adgroup_id", "ad_name", "ad_format", "ad_text", "video_id",
            "image_ids", "landing_page_url", "call_to_action",
            "tiktok_item_id", "item_group_id", "identity_id", "identity_type",
            "identity_authorized_bc_id", "page_id", "display_name",
        }
        for field in _AD_FIELDS:
            if field not in params and field in args:
                params[field] = args[field]

        # Handle lead_ad_form_id alias → TikTok API expects page_id
        if not params.get("page_id") and params.get("lead_ad_form_id"):
            params["page_id"] = params.pop("lead_ad_form_id")

        errors = validate_ad_create(params, campaign_objective)
        if errors:
            return _error("; ".join(errors))

        if not confirm:
            return build_preview("create", "ad", {
                "ad_name": params.get("ad_name"),
                "adgroup_id": params.get("adgroup_id"),
                "ad_format": params.get("ad_format"),
                "ad_text": params.get("ad_text"),
                "video_id": params.get("video_id"),
                "image_ids": params.get("image_ids"),
                "landing_page_url": params.get("landing_page_url"),
                "call_to_action": params.get("call_to_action"),
            })

        result = await self.client.create_ad(params)
        data = result.get("data", {})
        # TikTok returns a list of IDs under "ad_ids"
        ad_ids = data.get("ad_ids") or []
        ad_id = ad_ids[0] if ad_ids else data.get("ad_id")
        return _success({
            "ad_id": ad_id,
            "ad_ids": ad_ids,
            "message": f"Ad created: {ad_id}",
        })

    async def _update_ad(self, args: Dict[str, Any]) -> Dict[str, Any]:
        params = args.get("params", {})
        confirm = args.get("confirm", False)

        # Fix #6: Accept entity_ids alias for backwards compatibility
        if not params.get("ad_id") and params.get("entity_ids"):
            ids = params.pop("entity_ids")
            params["ad_id"] = ids[0] if isinstance(ids, list) else ids

        errors = validate_ad_update(params)
        if errors:
            return _error("; ".join(errors))

        if not confirm:
            return build_preview("update", "ad", params)

        result = await self.client.update_ad(params)
        return _success({
            "ad_id": params.get("ad_id"),
            "message": "Ad updated successfully",
        })

    # ── Status update handlers ──────────────────────────────────────────

    async def _status_update(
        self, entity_type: str, ids_key: str, ids: list, status: str, confirm: bool,
        update_fn,
    ) -> Dict[str, Any]:
        if not ids:
            return _error(f"{ids_key} is required")
        if len(ids) > BATCH_STATUS_UPDATE_LIMIT:
            return _error(f"Maximum {BATCH_STATUS_UPDATE_LIMIT} entities per batch")

        if not confirm:
            return build_preview(
                status.lower(), entity_type,
                {ids_key: ids, "count": len(ids)},
                warnings=[f"This will {status.lower()} {len(ids)} {entity_type}(s)"],
            )

        result = await update_fn(ids, status)
        return _success({
            "message": f"Successfully set {len(ids)} {entity_type}(s) to {status}",
            ids_key: ids,
        })

    async def _enable_campaigns(self, args: Dict[str, Any]) -> Dict[str, Any]:
        return await self._status_update(
            "campaign", "campaign_ids", args.get("campaign_ids", []),
            "ENABLE", args.get("confirm", False), self.client.update_campaign_status,
        )

    async def _disable_campaigns(self, args: Dict[str, Any]) -> Dict[str, Any]:
        return await self._status_update(
            "campaign", "campaign_ids", args.get("campaign_ids", []),
            "DISABLE", args.get("confirm", False), self.client.update_campaign_status,
        )

    async def _delete_campaigns(self, args: Dict[str, Any]) -> Dict[str, Any]:
        return await self._status_update(
            "campaign", "campaign_ids", args.get("campaign_ids", []),
            "DELETE", args.get("confirm", False), self.client.update_campaign_status,
        )

    async def _enable_adgroups(self, args: Dict[str, Any]) -> Dict[str, Any]:
        return await self._status_update(
            "adgroup", "adgroup_ids", args.get("adgroup_ids", []),
            "ENABLE", args.get("confirm", False), self.client.update_adgroup_status,
        )

    async def _disable_adgroups(self, args: Dict[str, Any]) -> Dict[str, Any]:
        return await self._status_update(
            "adgroup", "adgroup_ids", args.get("adgroup_ids", []),
            "DISABLE", args.get("confirm", False), self.client.update_adgroup_status,
        )

    async def _delete_adgroups(self, args: Dict[str, Any]) -> Dict[str, Any]:
        return await self._status_update(
            "adgroup", "adgroup_ids", args.get("adgroup_ids", []),
            "DELETE", args.get("confirm", False), self.client.update_adgroup_status,
        )

    async def _enable_ads(self, args: Dict[str, Any]) -> Dict[str, Any]:
        return await self._status_update(
            "ad", "ad_ids", args.get("ad_ids", []),
            "ENABLE", args.get("confirm", False), self.client.update_ad_status,
        )

    async def _disable_ads(self, args: Dict[str, Any]) -> Dict[str, Any]:
        return await self._status_update(
            "ad", "ad_ids", args.get("ad_ids", []),
            "DISABLE", args.get("confirm", False), self.client.update_ad_status,
        )

    async def _delete_ads(self, args: Dict[str, Any]) -> Dict[str, Any]:
        return await self._status_update(
            "ad", "ad_ids", args.get("ad_ids", []),
            "DELETE", args.get("confirm", False), self.client.update_ad_status,
        )

    # ── Pixel handlers ─────────────────────────────────────────────────

    async def _create_pixel(self, args: Dict[str, Any]) -> Dict[str, Any]:
        params = args.get("params", {})
        confirm = args.get("confirm", False)
        pixel_name = params.get("pixel_name")
        if not pixel_name:
            return _error("pixel_name is required in params")
        if not confirm:
            return build_preview("create", "pixel", {"pixel_name": pixel_name})
        result = await self.client.create_pixel(pixel_name)
        return _success(result.get("data", {}))

    async def _update_pixel(self, args: Dict[str, Any]) -> Dict[str, Any]:
        params = args.get("params", {})
        confirm = args.get("confirm", False)
        pixel_code = params.get("pixel_code")
        pixel_name = params.get("pixel_name")
        if not pixel_code or not pixel_name:
            return _error("pixel_code and pixel_name are required in params")
        if not confirm:
            return build_preview("update", "pixel", {"pixel_code": pixel_code, "pixel_name": pixel_name})
        result = await self.client.update_pixel(pixel_code, pixel_name)
        return _success(result.get("data", {}))

    # ── Event tracking handler ─────────────────────────────────────────

    async def _track_event(self, args: Dict[str, Any]) -> Dict[str, Any]:
        params = args.get("params", {})
        confirm = args.get("confirm", False)
        pixel_code = params.get("pixel_code")
        event = params.get("event")
        if not pixel_code or not event:
            return _error("pixel_code and event are required in params")
        if not confirm:
            return build_preview("track", "event", {
                "pixel_code": pixel_code, "event": event,
                "event_id": params.get("event_id"),
            })
        result = await self.client.track_event(
            pixel_code=pixel_code, event=event,
            event_id=params.get("event_id"),
            properties=params.get("properties"),
        )
        return _success(result.get("data", {}))

    # ── Handler dispatch table ──────────────────────────────────────────

    _handlers = {
        "create_campaign": _create_campaign,
        "update_campaign": _update_campaign,
        "create_adgroup": _create_adgroup,
        "update_adgroup": _update_adgroup,
        "create_ad": _create_ad,
        "update_ad": _update_ad,
        "enable_campaigns": _enable_campaigns,
        "disable_campaigns": _disable_campaigns,
        "delete_campaigns": _delete_campaigns,
        "enable_adgroups": _enable_adgroups,
        "disable_adgroups": _disable_adgroups,
        "delete_adgroups": _delete_adgroups,
        "enable_ads": _enable_ads,
        "disable_ads": _disable_ads,
        "delete_ads": _delete_ads,
        "create_pixel": _create_pixel,
        "update_pixel": _update_pixel,
        "track_event": _track_event,
    }


def _success(data: Any) -> Dict[str, Any]:
    return {"success": True, "executed": True, "data": data}


def _error(message: str) -> Dict[str, Any]:
    return {"success": False, "error_message": message}
