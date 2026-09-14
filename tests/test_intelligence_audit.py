"""The wasted_spend_audit analysis: classification rules and end-to-end assembly."""

import pytest

from tiktok_ads_mcp.tools.intelligence import (
    IntelligenceTool,
    _audit_sort_score,
    _classify_wasted_spend,
)

from .conftest import report_response, report_row

THRESHOLDS = {"min_spend": 300.0, "min_clicks": 100.0, "high_ctr": 2.0, "high_cpc": 5.0}


@pytest.fixture
def tool(client, cache):
    return IntelligenceTool(client, cache)


def candidate(**metrics):
    base = {"spend": 0.0, "clicks": 0.0, "ctr": 0.0, "cpc": 0.0, "conversions": 0.0}
    base.update(metrics)
    return base


class TestClassification:
    def test_real_money_and_real_traffic_is_high_confidence(self):
        result = _classify_wasted_spend(candidate(spend=500, clicks=200), **THRESHOLDS)

        assert result["confidence"] == "High"
        assert result["recommendation"] == "pause_or_reduce"

    def test_traffic_without_the_spend_bar_is_only_medium(self):
        result = _classify_wasted_spend(candidate(spend=50, clicks=200), **THRESHOLDS)

        assert result["confidence"] == "Medium"
        assert result["recommendation"] == "keep_small_retest_budget"

    def test_spend_without_the_click_bar_is_only_medium(self):
        result = _classify_wasted_spend(candidate(spend=500, clicks=5), **THRESHOLDS)

        assert result["confidence"] == "Medium"

    def test_high_ctr_needs_half_the_click_bar_to_count(self):
        flagged = _classify_wasted_spend(candidate(ctr=6, clicks=60), **THRESHOLDS)
        assert flagged["likely_issue"] == "High CTR, no conversion signal"

        too_thin = _classify_wasted_spend(candidate(ctr=6, clicks=10), **THRESHOLDS)
        assert too_thin["confidence"] == "Low"

    def test_expensive_traffic_is_the_weakest_signal(self):
        result = _classify_wasted_spend(candidate(cpc=9, clicks=10), **THRESHOLDS)

        assert result["likely_issue"] == "Expensive traffic, no conversion signal"
        assert result["recommendation"] == "needs_more_data"

    def test_nothing_conclusive_says_so_rather_than_guessing(self):
        result = _classify_wasted_spend(candidate(spend=5, clicks=3), **THRESHOLDS)

        assert result["confidence"] == "Low"
        assert result["recommendation"] == "needs_more_data"

    def test_the_strongest_matching_rule_wins(self):
        # Clears every bar at once; must be reported as the High-confidence case.
        result = _classify_wasted_spend(
            candidate(spend=900, clicks=400, ctr=9, cpc=8), **THRESHOLDS
        )
        assert result["confidence"] == "High"

    def test_every_verdict_carries_its_evidence(self):
        result = _classify_wasted_spend(candidate(spend=500, clicks=200), **THRESHOLDS)

        assert "conversions=0" in result["evidence"]
        assert any("triggered by" in line for line in result["evidence"])

    def test_thresholds_are_configurable(self):
        strict = _classify_wasted_spend(
            candidate(spend=500, clicks=200),
            min_spend=10_000, min_clicks=5_000, high_ctr=2, high_cpc=5,
        )
        assert strict["confidence"] == "Low"


class TestRanking:
    def test_confidence_outranks_raw_spend(self):
        high_small = _classify_wasted_spend(candidate(spend=300, clicks=100), **THRESHOLDS)
        low_large = _classify_wasted_spend(candidate(spend=299, clicks=1), **THRESHOLDS)

        assert _audit_sort_score(high_small) > _audit_sort_score(low_large)

    def test_within_a_confidence_band_more_spend_ranks_first(self):
        bigger = _classify_wasted_spend(candidate(spend=900, clicks=200), **THRESHOLDS)
        smaller = _classify_wasted_spend(candidate(spend=400, clicks=200), **THRESHOLDS)

        assert _audit_sort_score(bigger) > _audit_sort_score(smaller)


class TestAuditEndToEnd:
    async def test_ranks_candidates_and_breaks_down_the_worst_campaign(self, tool, client):
        client.responses = [
            # campaigns listing
            {"code": 0, "data": {"list": [
                {"campaign_id": "c1", "campaign_name": "Prospecting"},
                {"campaign_id": "c2", "campaign_name": "Retargeting"},
            ]}},
            # campaign-level report
            report_response([
                report_row({"campaign_id": "c1"}, spend=800, clicks=300, conversion=0, ctr=1.2, cpc=2.6),
                report_row({"campaign_id": "c2"}, spend=40, clicks=9, conversion=0, ctr=0.9, cpc=4.4),
            ]),
            # adgroups for the worst campaign
            {"code": 0, "data": {"list": [{"adgroup_id": "a1", "adgroup_name": "Broad"}]}},
            # adgroup-level report
            report_response([
                report_row({"adgroup_id": "a1"}, spend=700, clicks=280, conversion=0, ctr=1.1, cpc=2.5),
            ]),
        ]

        result = await tool.execute({
            "analysis_type": "wasted_spend_audit", "date_range": "last_7_days",
        })

        assert result["success"] is True
        data = result["data"]
        assert data["campaigns_scanned"] == 2
        assert [c["id"] for c in data["candidates"]] == ["c1", "c2"]
        assert data["candidates"][0]["name"] == "Prospecting"
        assert data["candidates"][0]["confidence"] == "High"
        assert data["high_confidence_count"] == 1
        assert data["adgroup_breakdown"][0]["id"] == "a1"
        assert data["adgroup_breakdown"][0]["parent_campaign_id"] == "c1"

    async def test_converting_entities_are_not_reported_as_waste(self, tool, client):
        client.responses = [
            {"code": 0, "data": {"list": [{"campaign_id": "c1", "campaign_name": "Works"}]}},
            report_response([
                report_row({"campaign_id": "c1"}, spend=900, clicks=400, conversion=12),
            ]),
        ]

        result = await tool.execute({"analysis_type": "wasted_spend_audit"})

        assert result["data"]["candidates"] == []

    async def test_idle_entities_are_skipped(self, tool, client):
        client.responses = [
            {"code": 0, "data": {"list": [{"campaign_id": "c1", "campaign_name": "Paused"}]}},
            report_response([report_row({"campaign_id": "c1"}, spend=0, clicks=0, conversion=0)]),
        ]

        result = await tool.execute({"analysis_type": "wasted_spend_audit"})

        assert result["data"]["candidates"] == []

    async def test_at_risk_spend_counts_only_confident_findings(self, tool, client):
        client.responses = [
            {"code": 0, "data": {"list": [
                {"campaign_id": "c1", "campaign_name": "High"},
                {"campaign_id": "c2", "campaign_name": "Low"},
            ]}},
            report_response([
                report_row({"campaign_id": "c1"}, spend=800, clicks=300, conversion=0),
                report_row({"campaign_id": "c2"}, spend=7, clicks=2, conversion=0),
            ]),
            {"code": 0, "data": {"list": []}},
        ]

        result = await tool.execute({"analysis_type": "wasted_spend_audit"})

        assert result["data"]["at_risk_spend"] == 800.0

    async def test_no_campaigns_short_circuits_before_reporting(self, tool, client):
        client.responses = [{"code": 0, "data": {"list": []}}]

        result = await tool.execute({"analysis_type": "wasted_spend_audit"})

        assert result["success"] is True
        assert result["data"]["candidates"] == []
        assert client.endpoints() == ["campaign/get/"]

    async def test_the_breakdown_can_be_turned_off(self, tool, client):
        client.responses = [
            {"code": 0, "data": {"list": [{"campaign_id": "c1", "campaign_name": "Prospecting"}]}},
            report_response([
                report_row({"campaign_id": "c1"}, spend=800, clicks=300, conversion=0),
            ]),
        ]

        result = await tool.execute({
            "analysis_type": "wasted_spend_audit", "include_adgroup_breakdown": False,
        })

        assert result["data"]["adgroup_breakdown"] == []
        assert "adgroup/get/" not in client.endpoints()

    async def test_breakdown_depth_is_capped(self, tool, client):
        campaigns = [{"campaign_id": f"c{i}", "campaign_name": f"C{i}"} for i in range(5)]
        rows = [
            report_row({"campaign_id": f"c{i}"}, spend=800, clicks=300, conversion=0)
            for i in range(5)
        ]
        client.responses = [{"code": 0, "data": {"list": campaigns}}, report_response(rows)]
        # Every breakdown campaign finds no ad groups, so two calls each is enough.
        client.default_response = {"code": 0, "data": {"list": []}}

        await tool.execute({
            "analysis_type": "wasted_spend_audit", "max_adgroup_campaigns": 2,
        })

        assert client.endpoints().count("adgroup/get/") == 2

    async def test_thresholds_pass_through_and_are_reported_back(self, tool, client):
        client.responses = [
            {"code": 0, "data": {"list": [{"campaign_id": "c1", "campaign_name": "C"}]}},
            report_response([
                report_row({"campaign_id": "c1"}, spend=800, clicks=300, conversion=0),
            ]),
        ]

        result = await tool.execute({
            "analysis_type": "wasted_spend_audit",
            "min_spend": 5000,
            "min_clicks": 4000,
            "include_adgroup_breakdown": False,
        })

        assert result["data"]["thresholds"]["min_spend"] == 5000.0
        assert result["data"]["candidates"][0]["confidence"] == "Low"

    async def test_an_invalid_date_range_is_reported_not_raised(self, tool, client):
        result = await tool.execute({
            "analysis_type": "wasted_spend_audit", "date_range": "since_forever",
        })

        assert result["success"] is False
        assert "date_range" in result["error_message"]
        assert client.calls == []


class TestIntelligenceDispatch:
    async def test_the_audit_is_advertised_in_the_error_for_a_bad_type(self, tool):
        result = await tool.execute({"analysis_type": "nope"})

        assert result["success"] is False
        assert "wasted_spend_audit" in result["error_message"]
