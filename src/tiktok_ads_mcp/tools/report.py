"""Tool 3: tiktok_report — Pull any report type.

Supported report_type values:
  performance     — Basic performance metrics (sync, paginated)
  audience        — Audience demographics/interest/geo/platform/language
  catalog         — Product/catalog performance
  async_report    — Create async report task (returns task_id)
  check_task      — Check async report task status
  download_task   — Download completed async report
  gmv_max         — GMV Max / Smart+ campaign report
"""

import logging
from typing import Any, Dict, List, Optional

from ..api.marketing_client import MarketingClient
from ..config import DATA_LEVELS
from ..utils.date_helpers import resolve_date_range, validate_date_string
from ..validators.report_validator import validate_report_params

logger = logging.getLogger(__name__)


class ReportTool:
    """Consolidated reporting tool for all TikTok Ads report types."""

    def __init__(self, client: MarketingClient):
        self.client = client

    async def execute(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Route to the appropriate handler based on report_type."""
        report_type = arguments.get("report_type", "")
        if not report_type:
            return _error("report_type is required")

        handler = self._handlers.get(report_type)
        if not handler:
            valid = ", ".join(sorted(self._handlers.keys()))
            return _error(f"Unknown report_type '{report_type}'. Valid: {valid}")

        try:
            return await handler(self, arguments)
        except Exception as e:
            logger.error(f"report({report_type}) failed: {e}")
            return _error(str(e))

    # ── Handlers ────────────────────────────────────────────────────────

    async def _performance_report(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Synchronous basic performance report."""
        return await self._run_sync_report("BASIC", args)

    async def _audience_report(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Synchronous audience report with breakdown support.

        TikTok AUDIENCE reports require at least one audience dimension
        (gender, age, country_code, platform, placement, etc.).
        stat_time_day is NOT supported — use report_type='performance' for time-based.
        """
        # If user specified a breakdown shortcut, auto-set dimensions
        breakdown = args.get("breakdown")
        if breakdown and not args.get("dimensions"):
            args["dimensions"] = self._audience_breakdown_dimensions(breakdown, args)

        # Default metrics for audience reports (subset that TikTok supports)
        if not args.get("metrics"):
            args["metrics"] = [
                "spend", "impressions", "clicks", "ctr", "cpc", "cpm",
                "conversion", "cost_per_conversion", "reach",
            ]

        return await self._run_sync_report("AUDIENCE", args)

    async def _catalog_report(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Synchronous catalog report."""
        return await self._run_sync_report("CATALOG", args)

    async def _run_sync_report(self, api_report_type: str, args: Dict[str, Any]) -> Dict[str, Any]:
        """Common logic for synchronous reports."""
        # Resolve dates
        start_date, end_date = self._resolve_dates(args)
        if not start_date:
            return _error(end_date)  # end_date contains error message

        # Resolve data level
        data_level = self._resolve_data_level(args)

        # Get dimensions and metrics
        dimensions = args.get("dimensions", [])
        metrics = args.get("metrics", [])

        if not dimensions:
            # Default dimensions based on data level
            dimensions = self._default_dimensions(data_level, api_report_type)

        if not metrics:
            metrics = ["spend", "impressions", "clicks", "ctr", "cpc", "cpm",
                       "conversion", "cost_per_conversion", "reach"]

        # Validate
        errors = validate_report_params(api_report_type, data_level, dimensions, metrics)
        if errors:
            return _error("; ".join(errors))

        # Build filtering
        filtering = self._build_filtering(args, data_level)

        # Fetch report (auto-paginated)
        result = await self.client.get_report_all_pages(
            report_type=api_report_type,
            data_level=data_level,
            dimensions=dimensions,
            metrics=metrics,
            start_date=start_date,
            end_date=end_date,
            filtering=filtering,
        )

        rows = result.get("data", {}).get("list", [])
        return {
            "success": True,
            "data": {
                "rows": rows,
                "total_rows": len(rows),
                "dimensions": dimensions,
                "metrics": metrics,
                "date_range": {"start_date": start_date, "end_date": end_date},
                "data_level": data_level,
            },
        }

    async def _async_report(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Create an async report task."""
        start_date, end_date = self._resolve_dates(args)
        if not start_date:
            return _error(end_date)

        data_level = self._resolve_data_level(args)
        dimensions = args.get("dimensions", [])
        metrics = args.get("metrics", [])
        api_report_type = args.get("api_report_type", "BASIC")

        if not dimensions:
            dimensions = self._default_dimensions(data_level, api_report_type)

        if not metrics:
            return _error("metrics is required for async_report")

        errors = validate_report_params(api_report_type, data_level, dimensions, metrics)
        if errors:
            return _error("; ".join(errors))

        filtering = args.get("filtering")

        result = await self.client.create_report_task(
            report_type=api_report_type,
            data_level=data_level,
            dimensions=dimensions,
            metrics=metrics,
            start_date=start_date,
            end_date=end_date,
            filtering=filtering,
        )

        task_id = result.get("data", {}).get("task_id")
        return {
            "success": True,
            "data": {
                "task_id": task_id,
                "message": f"Async report task created. Use report_type='check_task' with task_id='{task_id}' to check status.",
            },
        }

    async def _check_task(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Check async report task status."""
        task_id = args.get("task_id")
        if not task_id:
            return _error("task_id is required for check_task")

        result = await self.client.check_report_task(task_id)
        return {"success": True, "data": result.get("data", {})}

    async def _download_task(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Download completed async report."""
        task_id = args.get("task_id")
        if not task_id:
            return _error("task_id is required for download_task")

        result = await self.client.download_report_task(task_id)
        return {"success": True, "data": result.get("data", {})}

    async def _gmv_max_report(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """GMV Max / Smart+ report."""
        campaign_ids = args.get("campaign_ids")
        if not campaign_ids:
            return _error("campaign_ids is required for gmv_max report")

        start_date, end_date = self._resolve_dates(args)
        if not start_date:
            return _error(end_date)

        metrics = args.get("metrics", [
            "spend", "impressions", "clicks", "conversion",
            "complete_payment", "total_purchase_value", "complete_payment_roas",
        ])

        result = await self.client.get_gmv_max_report(
            campaign_ids=campaign_ids,
            metrics=metrics,
            start_date=start_date,
            end_date=end_date,
        )
        return {"success": True, "data": result.get("data", {})}

    # ── Helpers ─────────────────────────────────────────────────────────

    def _resolve_dates(self, args: Dict[str, Any]) -> tuple:
        """Resolve start_date and end_date from args. Returns (start, end) or (None, error_msg)."""
        date_range = args.get("date_range")
        start_date = args.get("start_date")
        end_date = args.get("end_date")

        if date_range:
            try:
                return resolve_date_range(date_range)
            except ValueError as e:
                return None, str(e)
        elif start_date and end_date:
            try:
                return validate_date_string(start_date), validate_date_string(end_date)
            except ValueError as e:
                return None, str(e)
        else:
            # Default to last 7 days
            return resolve_date_range("last_7_days")

    def _resolve_data_level(self, args: Dict[str, Any]) -> str:
        """Resolve data level from entity_level or data_level arg."""
        data_level = args.get("data_level")
        if data_level:
            return data_level
        entity_level = args.get("entity_level", "campaign")
        return DATA_LEVELS.get(entity_level, "AUCTION_CAMPAIGN")

    def _default_dimensions(self, data_level: str, report_type: str) -> list:
        """Return default dimensions for a report."""
        level_dim = {
            "AUCTION_CAMPAIGN": "campaign_id",
            "AUCTION_ADGROUP": "adgroup_id",
            "AUCTION_AD": "ad_id",
        }
        entity_dim = level_dim.get(data_level, "campaign_id")

        if report_type == "AUDIENCE":
            # AUDIENCE reports need an audience dimension; stat_time_day is NOT valid.
            # Default to gender + age breakdown at the given entity level.
            return [entity_dim, "gender", "age"]

        return [entity_dim, "stat_time_day"]

    def _audience_breakdown_dimensions(self, breakdown: str, args: dict) -> list:
        """Map a breakdown shortcut to the correct AUDIENCE dimensions.

        Breakdown values:
            demographic  — gender + age
            gender       — gender only
            age          — age only
            country      — country_code (add province_id via secondary)
            geo          — alias for country
            province     — country_code + province_id
            device       — platform
            device_brand — platform + device_brand_id
            placement    — placement
            language     — language
            interest     — interest_category
            network      — ac (network / connection type)
        """
        data_level = self._resolve_data_level(args)
        level_dim = {
            "AUCTION_CAMPAIGN": "campaign_id",
            "AUCTION_ADGROUP": "adgroup_id",
            "AUCTION_AD": "ad_id",
        }
        entity_dim = level_dim.get(data_level, "campaign_id")

        breakdown_map = {
            "demographic": [entity_dim, "gender", "age"],
            "gender": [entity_dim, "gender"],
            "age": [entity_dim, "age"],
            "country": [entity_dim, "country_code"],
            "geo": [entity_dim, "country_code"],
            "province": [entity_dim, "country_code", "province_id"],
            "device": [entity_dim, "platform"],
            "device_brand": [entity_dim, "platform", "device_brand_id"],
            "placement": [entity_dim, "placement"],
            "language": [entity_dim, "language"],
            "interest": [entity_dim, "interest_category"],
            "network": [entity_dim, "ac"],
        }

        dims = breakdown_map.get(breakdown)
        if not dims:
            valid = ", ".join(sorted(breakdown_map.keys()))
            raise ValueError(f"Unknown breakdown '{breakdown}'. Valid: {valid}")
        return dims

    def _build_filtering(self, args: Dict[str, Any], data_level: str) -> list | None:
        """Build filtering array from entity IDs and other supported filters.

        Supported report-level filters (verified against live API):
          - campaign_ids, adgroup_ids, ad_ids  (entity ID filters)
          - campaign_status  (STATUS_DELIVERY_OK, STATUS_DISABLE, STATUS_DELETE, etc.)
          - objective_type   (TRAFFIC, PRODUCT_SALES, REACH, etc.)
        """
        # Check for explicit filtering
        if args.get("filtering"):
            return args["filtering"]

        import json

        filters = []

        # Entity ID filters
        id_mappings = {
            "campaign_ids": "campaign_ids",
            "adgroup_ids": "adgroup_ids",
            "ad_ids": "ad_ids",
        }
        for arg_key, field_name in id_mappings.items():
            ids = args.get(arg_key)
            if ids:
                filters.append({
                    "field_name": field_name,
                    "filter_type": "IN",
                    "filter_value": json.dumps(ids),
                })

        # Status filter (use campaign_status, NOT primary_status — verified via API)
        status = args.get("campaign_status") or args.get("status")
        if status:
            values = status if isinstance(status, list) else [status]
            filters.append({
                "field_name": "campaign_status",
                "filter_type": "IN",
                "filter_value": json.dumps(values),
            })

        # Objective type filter
        objective_type = args.get("objective_type")
        if objective_type:
            values = objective_type if isinstance(objective_type, list) else [objective_type]
            filters.append({
                "field_name": "objective_type",
                "filter_type": "IN",
                "filter_value": json.dumps(values),
            })

        return filters if filters else None

    # ── Handler dispatch table ──────────────────────────────────────────

    _handlers = {
        "performance": _performance_report,
        "audience": _audience_report,
        "catalog": _catalog_report,
        "async_report": _async_report,
        "check_task": _check_task,
        "download_task": _download_task,
        "gmv_max": _gmv_max_report,
    }


def _error(message: str) -> Dict[str, Any]:
    return {"success": False, "error_message": message}
