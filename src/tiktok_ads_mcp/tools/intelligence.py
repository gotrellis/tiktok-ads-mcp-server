"""Tool 7: tiktok_intelligence — Cross-system analysis and optimization insights.

Supported analysis_type values:
  funnel_overview       — Campaigns grouped by objective with key metrics
  anomaly_check         — Detect performance anomalies vs recent averages
  optimization_actions  — Actionable recommendations (kill, scale, refresh, renew)
  scaling_readiness     — Check if campaigns are ready to scale
"""

import logging
from typing import Any, Dict, List

from ..api.marketing_client import MarketingClient
from ..cache.cache_manager import CacheManager
from ..utils.date_helpers import resolve_date_range

logger = logging.getLogger(__name__)


class IntelligenceTool:
    """Cross-system analysis tool that combines data from multiple API calls."""

    def __init__(self, client: MarketingClient, cache: CacheManager):
        self.client = client
        self.cache = cache

    async def execute(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Route to the appropriate analysis type."""
        analysis_type = arguments.get("analysis_type", "")
        if not analysis_type:
            return _error("analysis_type is required")

        handler = self._handlers.get(analysis_type)
        if not handler:
            valid = ", ".join(sorted(self._handlers.keys()))
            return _error(f"Unknown analysis_type '{analysis_type}'. Valid: {valid}")

        try:
            return await handler(self, arguments)
        except Exception as e:
            logger.error(f"intelligence({analysis_type}) failed: {e}")
            return _error(str(e))

    # ── Handlers ────────────────────────────────────────────────────────

    async def _funnel_overview(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Get all campaigns grouped by objective with key performance metrics."""
        date_range = args.get("date_range", "last_7_days")
        start_date, end_date = resolve_date_range(date_range)

        # Fetch all active campaigns
        campaigns_result = await self.client.get_campaigns(status="STATUS_NOT_DELETE", page_size=200)
        campaigns = campaigns_result.get("data", {}).get("list", [])

        if not campaigns:
            return _success({"funnel": [], "message": "No campaigns found"})

        campaign_ids = [c["campaign_id"] for c in campaigns if c.get("campaign_id")]

        # Fetch performance data for all campaigns
        metrics = [
            "spend", "impressions", "reach", "clicks", "ctr", "cpm", "cpc",
            "conversion", "cost_per_conversion", "result", "cost_per_result",
            "video_play_actions", "complete_payment", "total_purchase_value",
            "complete_payment_roas",
        ]

        report_result = await self.client.get_report_all_pages(
            report_type="BASIC",
            data_level="AUCTION_CAMPAIGN",
            dimensions=["campaign_id"],
            metrics=metrics,
            start_date=start_date,
            end_date=end_date,
        )
        perf_rows = report_result.get("data", {}).get("list", [])

        # Build campaign_id -> performance lookup
        perf_by_id = {}
        for row in perf_rows:
            dims = row.get("dimensions", {})
            perf_by_id[dims.get("campaign_id")] = row.get("metrics", {})

        # Group by objective
        funnel = {}
        for c in campaigns:
            objective = c.get("objective_type", "UNKNOWN")
            if objective not in funnel:
                funnel[objective] = {
                    "objective": objective,
                    "campaigns": [],
                    "total_spend": 0,
                    "total_impressions": 0,
                    "total_conversions": 0,
                }

            perf = perf_by_id.get(c.get("campaign_id"), {})
            spend = _to_float(perf.get("spend", 0))
            impressions = _to_float(perf.get("impressions", 0))
            conversions = _to_float(perf.get("conversion", 0))

            funnel[objective]["campaigns"].append({
                "campaign_id": c.get("campaign_id"),
                "campaign_name": c.get("campaign_name"),
                "status": c.get("primary_status"),
                "budget": c.get("budget"),
                "spend": spend,
                "key_metrics": {k: v for k, v in perf.items() if v and v != "0" and v != "0.00"},
            })
            funnel[objective]["total_spend"] += spend
            funnel[objective]["total_impressions"] += impressions
            funnel[objective]["total_conversions"] += conversions

        return _success({
            "funnel": list(funnel.values()),
            "date_range": {"start_date": start_date, "end_date": end_date},
            "total_campaigns": len(campaigns),
        })

    async def _anomaly_check(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Compare today's performance vs 7-day averages to detect anomalies."""
        threshold = args.get("threshold", 0.3)  # 30% deviation

        # Get last 7 days data at adgroup level
        start_7d, end_7d = resolve_date_range("last_7_days")
        start_today, end_today = resolve_date_range("yesterday")  # Use yesterday for complete data

        check_metrics = [
            "spend", "impressions", "clicks", "ctr", "cpc",
            "conversion", "cost_per_conversion",
        ]

        # Fetch 7-day daily data
        daily_result = await self.client.get_report_all_pages(
            report_type="BASIC",
            data_level="AUCTION_ADGROUP",
            dimensions=["adgroup_id", "stat_time_day"],
            metrics=check_metrics,
            start_date=start_7d,
            end_date=end_7d,
        )
        daily_rows = daily_result.get("data", {}).get("list", [])

        # Build 7-day averages per adgroup
        adgroup_sums: Dict[str, Dict[str, float]] = {}
        adgroup_days: Dict[str, int] = {}
        latest_day_data: Dict[str, Dict[str, float]] = {}

        for row in daily_rows:
            dims = row.get("dimensions", {})
            ag_id = dims.get("adgroup_id", "")
            day = dims.get("stat_time_day", "")
            row_metrics = row.get("metrics", {})

            if ag_id not in adgroup_sums:
                adgroup_sums[ag_id] = {m: 0.0 for m in check_metrics}
                adgroup_days[ag_id] = 0

            for m in check_metrics:
                adgroup_sums[ag_id][m] += _to_float(row_metrics.get(m, 0))
            adgroup_days[ag_id] += 1

            # Track latest day
            if day == end_today:
                latest_day_data[ag_id] = {m: _to_float(row_metrics.get(m, 0)) for m in check_metrics}

        # Detect anomalies
        anomalies = []
        for ag_id, sums in adgroup_sums.items():
            days = adgroup_days.get(ag_id, 1)
            if days < 3:
                continue  # Not enough data

            latest = latest_day_data.get(ag_id)
            if not latest:
                continue

            avg = {m: sums[m] / days for m in check_metrics}
            ag_anomalies = []

            for m in check_metrics:
                avg_val = avg[m]
                latest_val = latest[m]
                if avg_val == 0:
                    continue
                deviation = (latest_val - avg_val) / avg_val
                if abs(deviation) >= threshold:
                    direction = "up" if deviation > 0 else "down"
                    ag_anomalies.append({
                        "metric": m,
                        "latest_value": latest_val,
                        "avg_value": round(avg_val, 2),
                        "deviation_pct": round(deviation * 100, 1),
                        "direction": direction,
                    })

            if ag_anomalies:
                anomalies.append({
                    "adgroup_id": ag_id,
                    "anomalies": ag_anomalies,
                })

        return _success({
            "anomalies": anomalies,
            "total_adgroups_checked": len(adgroup_sums),
            "adgroups_with_anomalies": len(anomalies),
            "threshold_pct": threshold * 100,
            "comparison_period": f"{start_7d} to {end_7d}",
            "latest_day": end_today,
        })

    async def _optimization_actions(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Generate actionable optimization recommendations."""
        start_date, end_date = resolve_date_range("last_7_days")

        # Get adgroup-level performance
        metrics = [
            "spend", "impressions", "clicks", "ctr", "cpc",
            "conversion", "cost_per_conversion", "complete_payment_roas",
            "result", "cost_per_result",
        ]

        report_result = await self.client.get_report_all_pages(
            report_type="BASIC",
            data_level="AUCTION_ADGROUP",
            dimensions=["adgroup_id"],
            metrics=metrics,
            start_date=start_date,
            end_date=end_date,
        )
        perf_rows = report_result.get("data", {}).get("list", [])

        kill_list = []       # ROAS < 1.0 for 7+ days, significant spend
        scale_list = []      # ROAS > 3.0 and stable
        refresh_list = []    # CTR declined significantly
        watch_list = []      # Moderate performance, needs attention

        for row in perf_rows:
            dims = row.get("dimensions", {})
            m = row.get("metrics", {})
            ag_id = dims.get("adgroup_id", "")

            spend = _to_float(m.get("spend", 0))
            roas = _to_float(m.get("complete_payment_roas", 0))
            ctr = _to_float(m.get("ctr", 0))
            cpc = _to_float(m.get("cpc", 0))
            conversions = _to_float(m.get("conversion", 0))
            cpa = _to_float(m.get("cost_per_conversion", 0))

            if spend < 1:
                continue  # Skip adgroups with negligible spend

            entry = {
                "adgroup_id": ag_id,
                "spend": spend,
                "roas": roas,
                "ctr": ctr,
                "cpc": cpc,
                "conversions": conversions,
                "cpa": cpa,
            }

            # Categorize
            if roas > 0 and roas < 1.0 and spend > 50:
                entry["reason"] = f"ROAS {roas:.2f} < 1.0 with ${spend:.2f} spend"
                kill_list.append(entry)
            elif roas >= 3.0 and conversions >= 5:
                entry["reason"] = f"ROAS {roas:.2f} with {int(conversions)} conversions — ready to scale"
                scale_list.append(entry)
            elif ctr < 0.5 and spend > 20:
                entry["reason"] = f"Low CTR {ctr:.2f}% — consider creative refresh"
                refresh_list.append(entry)
            elif spend > 20 and conversions < 1:
                entry["reason"] = f"${spend:.2f} spent with 0 conversions"
                watch_list.append(entry)

        return _success({
            "actions": {
                "kill": {
                    "description": "Ad groups with poor ROAS — consider disabling",
                    "count": len(kill_list),
                    "items": kill_list,
                },
                "scale": {
                    "description": "High-performing ad groups — consider increasing budget",
                    "count": len(scale_list),
                    "items": scale_list,
                },
                "refresh": {
                    "description": "Low CTR — consider new creatives",
                    "count": len(refresh_list),
                    "items": refresh_list,
                },
                "watch": {
                    "description": "Spending with no conversions — monitor closely",
                    "count": len(watch_list),
                    "items": watch_list,
                },
            },
            "date_range": {"start_date": start_date, "end_date": end_date},
            "total_adgroups_analyzed": len(perf_rows),
        })

    async def _scaling_readiness(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Check if campaigns/adgroups are ready to scale."""
        campaign_ids = args.get("campaign_ids")
        start_date, end_date = resolve_date_range("last_7_days")

        metrics = [
            "spend", "impressions", "clicks", "ctr", "cpc",
            "conversion", "cost_per_conversion", "complete_payment_roas",
            "frequency",
        ]

        filtering = None
        if campaign_ids:
            import json
            filtering = [{
                "field_name": "campaign_ids",
                "filter_type": "IN",
                "filter_value": json.dumps(campaign_ids),
            }]

        report_result = await self.client.get_report_all_pages(
            report_type="BASIC",
            data_level="AUCTION_ADGROUP",
            dimensions=["adgroup_id", "stat_time_day"],
            metrics=metrics,
            start_date=start_date,
            end_date=end_date,
            filtering=filtering,
        )
        rows = report_result.get("data", {}).get("list", [])

        # Aggregate by adgroup
        adgroup_data: Dict[str, Dict] = {}
        for row in rows:
            dims = row.get("dimensions", {})
            m = row.get("metrics", {})
            ag_id = dims.get("adgroup_id", "")

            if ag_id not in adgroup_data:
                adgroup_data[ag_id] = {
                    "days": 0, "total_spend": 0, "total_conversions": 0,
                    "roas_values": [], "cpa_values": [], "frequency_values": [],
                }

            d = adgroup_data[ag_id]
            d["days"] += 1
            d["total_spend"] += _to_float(m.get("spend", 0))
            d["total_conversions"] += _to_float(m.get("conversion", 0))
            roas = _to_float(m.get("complete_payment_roas", 0))
            if roas > 0:
                d["roas_values"].append(roas)
            cpa = _to_float(m.get("cost_per_conversion", 0))
            if cpa > 0:
                d["cpa_values"].append(cpa)
            freq = _to_float(m.get("frequency", 0))
            if freq > 0:
                d["frequency_values"].append(freq)

        # Assess readiness
        results = []
        for ag_id, d in adgroup_data.items():
            readiness = {
                "adgroup_id": ag_id,
                "days_with_data": d["days"],
                "total_spend": round(d["total_spend"], 2),
                "total_conversions": int(d["total_conversions"]),
                "ready_to_scale": False,
                "reasons": [],
            }

            # Criteria for scaling readiness
            if d["days"] < 5:
                readiness["reasons"].append(f"Only {d['days']} days of data — need at least 5")
            if d["total_conversions"] < 10:
                readiness["reasons"].append(f"Only {int(d['total_conversions'])} conversions — need at least 10")
            if d["roas_values"]:
                avg_roas = sum(d["roas_values"]) / len(d["roas_values"])
                readiness["avg_roas"] = round(avg_roas, 2)
                if avg_roas < 1.5:
                    readiness["reasons"].append(f"Avg ROAS {avg_roas:.2f} < 1.5 — too risky to scale")
            if d["cpa_values"]:
                # Check CPA stability (coefficient of variation)
                avg_cpa = sum(d["cpa_values"]) / len(d["cpa_values"])
                if len(d["cpa_values"]) > 1:
                    variance = sum((x - avg_cpa) ** 2 for x in d["cpa_values"]) / len(d["cpa_values"])
                    std = variance ** 0.5
                    cv = std / avg_cpa if avg_cpa > 0 else 0
                    readiness["cpa_stability"] = round(1 - cv, 2)
                    if cv > 0.5:
                        readiness["reasons"].append(f"CPA unstable (CV={cv:.2f}) — wait for stabilization")
            if d["frequency_values"]:
                max_freq = max(d["frequency_values"])
                if max_freq > 4:
                    readiness["reasons"].append(f"High frequency ({max_freq:.1f}) — audience may be saturated")

            if not readiness["reasons"]:
                readiness["ready_to_scale"] = True
                readiness["reasons"].append("All criteria met — safe to increase budget by 20-30%")

            results.append(readiness)

        # Sort: ready first, then by conversions
        results.sort(key=lambda r: (-r["ready_to_scale"], -r["total_conversions"]))

        return _success({
            "adgroups": results,
            "total_analyzed": len(results),
            "ready_count": sum(1 for r in results if r["ready_to_scale"]),
            "date_range": {"start_date": start_date, "end_date": end_date},
        })

    # ── Targeting intelligence handlers ─────────────────────────────────

    async def _interests(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Get available interest categories for targeting."""
        language = args.get("language", "en")
        version = args.get("version", 2)
        placements = args.get("placements")

        result = await self.client.get_interest_categories(
            version=version,
            placements=placements,
            language=language,
        )

        categories = result.get("data", {}).get("interest_categories", [])

        # If keyword filter provided, search by name
        keyword = args.get("keyword", "").lower()
        if keyword:
            matched = []
            for cat in categories:
                if keyword in cat.get("interest_category_name", "").lower():
                    matched.append(cat)
                # Also check sub-categories by name match
            categories = matched

        return _success({
            "interest_categories": categories,
            "total": len(categories),
            "language": language,
        })

    async def _regions(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Get available targeting regions/locations."""
        objective_type = args.get("objective_type", "TRAFFIC")
        placements = args.get("placements", ["PLACEMENT_TIKTOK"])
        level = args.get("level")  # COUNTRY, PROVINCE, CITY
        country_code = args.get("country_code")
        keyword = args.get("keyword", "").lower()

        result = await self.client.get_regions(
            placements=placements,
            objective_type=objective_type,
        )

        region_list = result.get("data", {}).get("region_info", [])
        total = len(region_list)

        # Filter by level
        if level:
            region_list = [r for r in region_list if r.get("level") == level.upper()]
        elif not keyword and not country_code:
            # Default to country-level only
            region_list = [r for r in region_list if r.get("level") == "COUNTRY"]

        # Filter by country code
        if country_code:
            region_list = [r for r in region_list if r.get("region_code") == country_code.upper()]

        # Filter by keyword
        if keyword:
            region_list = [r for r in region_list if keyword in r.get("name", "").lower()]

        return _success({
            "regions": region_list[:100],
            "total_matching": len(region_list),
            "total_available": total,
        })

    async def _action_categories(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Get available user behavior/action categories for behavioral targeting."""
        result = await self.client.get_action_categories()
        categories = result.get("data", {}).get("action_categories", result.get("data", {}))
        return _success({"action_categories": categories})

    # ── Handler dispatch table ──────────────────────────────────────────

    _handlers = {
        "funnel_overview": _funnel_overview,
        "anomaly_check": _anomaly_check,
        "optimization_actions": _optimization_actions,
        "scaling_readiness": _scaling_readiness,
        "interests": _interests,
        "regions": _regions,
        "action_categories": _action_categories,
    }


def _to_float(val: Any) -> float:
    """Safely convert API metric value to float."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _success(data: Any) -> Dict[str, Any]:
    return {"success": True, "data": data}


def _error(message: str) -> Dict[str, Any]:
    return {"success": False, "error_message": message}
