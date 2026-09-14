"""Behaviour of the entity_get read tool, focused on the entity types this fork adds."""

import pytest

from tiktok_ads_mcp.tools.entity_get import EntityGetTool

from .conftest import report_response, report_row


@pytest.fixture
def tool(client, cache):
    return EntityGetTool(client, cache)


def advertiser_response(**overrides):
    advertiser = {
        "advertiser_id": "7000000000000000001",
        "advertiser_name": "Test Co",
        "currency": "USD",
        "timezone": "America/New_York",
        "display_timezone": "America/New_York",
        "create_time": "1700000000",
    }
    advertiser.update(overrides)
    return {"code": 0, "data": {"list": [advertiser]}}


class TestDispatch:
    async def test_unknown_entity_type_lists_the_valid_ones(self, tool):
        result = await tool.execute({"entity_type": "nope"})

        assert result["success"] is False
        assert "pixel_event_stats" in result["error_message"]
        assert "location_info" in result["error_message"]

    async def test_entity_type_is_required(self, tool):
        result = await tool.execute({})
        assert result["success"] is False
        assert "entity_type is required" in result["error_message"]

    async def test_client_errors_become_failed_results_not_exceptions(self, tool, client):
        client.responses = [RuntimeError("TikTok API Error 40001: bad request")]

        result = await tool.execute({"entity_type": "pixels"})

        assert result["success"] is False
        assert "40001" in result["error_message"]


class TestPixelEventStats:
    async def test_defaults_to_last_seven_days(self, tool, client):
        result = await tool.execute({"entity_type": "pixel_event_stats", "pixel_ids": ["p1"]})

        assert result["success"] is True
        params = client.calls[0]["params"]
        assert params["date_range"]["start_date"] == result["metadata"]["start_date"]
        assert params["date_range"]["end_date"] == result["metadata"]["end_date"]

    async def test_explicit_dates_override_the_named_range(self, tool, client):
        await tool.execute({
            "entity_type": "pixel_event_stats",
            "pixel_ids": ["p1"],
            "date_range": "last_30_days",
            "start_date": "2026-02-01",
            "end_date": "2026-02-14",
        })

        assert client.calls[0]["params"]["date_range"] == {
            "start_date": "2026-02-01",
            "end_date": "2026-02-14",
        }

    async def test_half_a_date_range_is_rejected_before_the_api_call(self, tool, client):
        result = await tool.execute({
            "entity_type": "pixel_event_stats",
            "pixel_ids": ["p1"],
            "start_date": "2026-02-01",
        })

        assert result["success"] is False
        assert "must be provided together" in result["error_message"]
        assert client.calls == []

    async def test_missing_pixel_ids_points_at_the_pixels_listing(self, tool, client):
        result = await tool.execute({"entity_type": "pixel_event_stats"})

        assert result["success"] is False
        assert "pixels" in result["suggestion"]
        assert client.calls == []

    async def test_malformed_date_is_rejected(self, tool, client):
        result = await tool.execute({
            "entity_type": "pixel_event_stats",
            "pixel_ids": ["p1"],
            "start_date": "01/02/2026",
            "end_date": "2026-02-14",
        })

        assert result["success"] is False
        assert client.calls == []


class TestLocationInfo:
    async def test_resolves_ids(self, tool, client):
        client.responses = [{"code": 0, "data": {"location_info": [{"id": "6252001"}]}}]

        result = await tool.execute({
            "entity_type": "location_info", "location_ids": ["6252001"],
        })

        assert result["success"] is True
        assert client.calls[0]["endpoint"] == "tool/targeting/info/"

    async def test_missing_ids_points_at_the_regions_browser(self, tool, client):
        result = await tool.execute({"entity_type": "location_info"})

        assert result["success"] is False
        assert "regions" in result["suggestion"]
        assert client.calls == []


class TestPixelsListing:
    async def test_unfiltered_listing_is_cached(self, tool, client):
        client.responses = [{"code": 0, "data": {"list": [{"pixel_id": "p1"}]}}]

        first = await tool.execute({"entity_type": "pixels"})
        second = await tool.execute({"entity_type": "pixels"})

        assert first["success"] is True
        assert second["metadata"]["cached"] is True
        assert len(client.calls) == 1

    async def test_filtered_listing_bypasses_the_cache(self, tool, client):
        client.responses = [
            {"code": 0, "data": {"list": [{"pixel_id": "p1"}]}},
            {"code": 0, "data": {"list": [{"pixel_id": "p2"}]}},
        ]

        await tool.execute({"entity_type": "pixels"})
        result = await tool.execute({"entity_type": "pixels", "name": "checkout"})

        assert len(client.calls) == 2
        assert client.calls[1]["params"]["name"] == "checkout"
        assert result["data"] == [{"pixel_id": "p2"}]


class TestAccountInfo:
    async def test_enriches_with_spend_history_and_local_clock(self, tool, client):
        client.responses = [
            advertiser_response(),
            report_response([report_row({"stat_time_day": "2026-01-15"}, spend=10)]),
            report_response([]),
            report_response([]),
        ]

        result = await tool.execute({
            "entity_type": "account_info", "include_spend_history": True,
        })

        advertiser = result["data"]["list"][0]
        assert advertiser["first_cost_day"] == "2026-01-15"
        assert advertiser["all_cost_days"] == ["2026-01-15"]
        assert "now_based_on_timezone" in advertiser
        assert advertiser["create_time_readable"].startswith("20")

    async def test_spend_history_is_opt_in(self, tool, client):
        """The default read must stay a single API call, as it was before."""
        client.responses = [advertiser_response()]

        result = await tool.execute({"entity_type": "account_info"})

        assert client.endpoints() == ["advertiser/info/"]
        assert "first_cost_day" not in result["data"]["list"][0]

    async def test_the_clock_is_added_even_without_spend_history(self, tool, client):
        client.responses = [advertiser_response()]

        result = await tool.execute({"entity_type": "account_info"})

        assert "now_based_on_timezone" in result["data"]["list"][0]

    async def test_the_two_variants_do_not_share_a_cache_entry(self, tool, client):
        client.responses = [
            advertiser_response(),
            advertiser_response(),
            report_response([]),
            report_response([]),
            report_response([]),
        ]

        await tool.execute({"entity_type": "account_info"})
        result = await tool.execute({
            "entity_type": "account_info", "include_spend_history": True,
        })

        assert result.get("metadata", {}).get("cached") is not True
        assert client.endpoints().count("advertiser/info/") == 2

    async def test_cached_reads_still_get_a_fresh_clock(self, tool, client):
        client.responses = [advertiser_response()]

        await tool.execute({"entity_type": "account_info"})
        cached = await tool.execute({"entity_type": "account_info"})

        assert cached["metadata"]["cached"] is True
        assert "now_based_on_timezone" in cached["data"]["list"][0]

    async def test_an_unknown_timezone_falls_back_to_utc(self, tool, client):
        client.responses = [
            advertiser_response(display_timezone="Mars/Olympus", timezone="Mars/Olympus"),
        ]

        result = await tool.execute({"entity_type": "account_info"})

        assert result["success"] is True
        assert "now_based_on_timezone" in result["data"]["list"][0]

    async def test_an_empty_advertiser_list_is_not_an_error(self, tool, client):
        client.responses = [{"code": 0, "data": {"list": []}}]

        result = await tool.execute({"entity_type": "account_info"})

        assert result["success"] is True

    async def test_a_junk_create_time_is_dropped_not_fatal(self, tool, client):
        client.responses = [advertiser_response(create_time="not-a-timestamp")]

        result = await tool.execute({"entity_type": "account_info"})

        assert result["success"] is True
        assert "create_time_readable" not in result["data"]["list"][0]


class TestExistingEntitiesStillWork:
    async def test_campaign_details_reports_a_missing_campaign(self, tool, client):
        client.responses = [{"code": 0, "data": {"list": []}}]

        result = await tool.execute({
            "entity_type": "campaign_details", "campaign_id": "123",
        })

        assert result["success"] is False
        assert "not found" in result["error_message"]

    async def test_campaigns_listing_surfaces_page_info(self, tool, client):
        client.responses = [{
            "code": 0,
            "data": {
                "list": [{"campaign_id": "1"}],
                "page_info": {"total_number": 42, "page": 1, "page_size": 200},
            },
        }]

        result = await tool.execute({"entity_type": "campaigns"})

        assert result["metadata"]["total_count"] == 42
