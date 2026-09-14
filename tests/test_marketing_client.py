"""Request shape and input validation for the endpoints this fork adds."""

import pytest

from tiktok_ads_mcp.config import (
    MAX_LOCATION_IDS_PER_REQUEST,
    MAX_PIXEL_IDS_PER_REQUEST,
)

from .conftest import report_response, report_row


class TestPixelEventStats:
    async def test_sends_nested_date_range(self, client):
        await client.get_pixel_event_stats(
            pixel_ids=["p1", "p2"], start_date="2026-01-01", end_date="2026-01-07"
        )

        call = client.calls[0]
        assert call["method"] == "GET"
        assert call["endpoint"] == "pixel/event/stats/"
        assert call["params"]["pixel_ids"] == ["p1", "p2"]
        assert call["params"]["date_range"] == {
            "start_date": "2026-01-01",
            "end_date": "2026-01-07",
        }

    async def test_rejects_empty_pixel_ids(self, client):
        with pytest.raises(ValueError, match="cannot be empty"):
            await client.get_pixel_event_stats([], "2026-01-01", "2026-01-07")
        assert client.calls == []

    async def test_rejects_more_ids_than_the_api_allows(self, client):
        too_many = [f"p{i}" for i in range(MAX_PIXEL_IDS_PER_REQUEST + 1)]
        with pytest.raises(ValueError, match="Maximum 10 pixel IDs"):
            await client.get_pixel_event_stats(too_many, "2026-01-01", "2026-01-07")
        assert client.calls == []

    async def test_accepts_exactly_the_limit(self, client):
        at_limit = [f"p{i}" for i in range(MAX_PIXEL_IDS_PER_REQUEST)]
        await client.get_pixel_event_stats(at_limit, "2026-01-01", "2026-01-07")
        assert len(client.calls) == 1


class TestPixelList:
    async def test_omits_absent_filters(self, client):
        await client.get_pixels()
        assert set(client.calls[0]["params"]) == {"page", "page_size"}

    async def test_passes_filters_through(self, client):
        await client.get_pixels(code="ABC", name="checkout", order_by="CREATE_TIME")
        params = client.calls[0]["params"]
        assert params["code"] == "ABC"
        assert params["name"] == "checkout"
        assert params["order_by"] == "CREATE_TIME"

    async def test_clamps_page_size_to_api_max(self, client):
        await client.get_pixels(page_size=500)
        assert client.calls[0]["params"]["page_size"] == 20


class TestLocationInfo:
    async def test_posts_geo_scene_with_defaults(self, client):
        await client.get_location_info(["6252001"])

        call = client.calls[0]
        assert call["method"] == "POST"
        assert call["endpoint"] == "tool/targeting/info/"
        assert call["data"]["scene"] == "GEO"
        assert call["data"]["targeting_ids"] == ["6252001"]
        assert call["data"]["objective_type"] == "TRAFFIC"
        assert call["data"]["placements"] == ["PLACEMENT_TIKTOK"]
        assert call["data"]["advertiser_id"] == client.advertiser_id

    async def test_overrides_are_honored(self, client):
        await client.get_location_info(
            ["6252001"], objective_type="REACH", placements=["PLACEMENT_PANGLE"]
        )
        assert client.calls[0]["data"]["objective_type"] == "REACH"
        assert client.calls[0]["data"]["placements"] == ["PLACEMENT_PANGLE"]

    async def test_rejects_empty_and_oversized_input(self, client):
        with pytest.raises(ValueError, match="cannot be empty"):
            await client.get_location_info([])

        too_many = [str(i) for i in range(MAX_LOCATION_IDS_PER_REQUEST + 1)]
        with pytest.raises(ValueError, match="Maximum 20 location IDs"):
            await client.get_location_info(too_many)

        assert client.calls == []


class TestSpendingDates:
    async def test_walks_the_window_in_thirty_day_chunks(self, client):
        client.responses = [report_response([]) for _ in range(3)]

        await client.get_spending_dates("2026-03-31")

        assert client.endpoints() == ["report/integrated/get/"] * 3
        windows = [(c["params"]["start_date"], c["params"]["end_date"]) for c in client.calls]
        assert windows == [
            ("2026-03-01", "2026-03-31"),
            ("2026-01-30", "2026-03-01"),
            ("2025-12-31", "2026-01-30"),
        ]
        for call in client.calls:
            assert call["params"]["data_level"] == "AUCTION_ADVERTISER"
            assert call["params"]["metrics"] == ["spend"]

    async def test_keeps_only_days_with_spend(self, client):
        client.responses = [
            report_response([
                report_row({"stat_time_day": "2026-03-10 00:00:00"}, spend=12.5),
                report_row({"stat_time_day": "2026-03-11 00:00:00"}, spend=0),
                report_row({"stat_time_day": "2026-03-12 00:00:00"}, spend="3.00"),
            ]),
            report_response([]),
            report_response([]),
        ]

        result = await client.get_spending_dates("2026-03-31")

        assert result["all_dates"] == ["2026-03-10", "2026-03-12"]
        assert result["first_date"] == "2026-03-10"

    async def test_deduplicates_and_sorts_across_chunks(self, client):
        client.responses = [
            report_response([report_row({"stat_time_day": "2026-03-10"}, spend=5)]),
            report_response([report_row({"stat_time_day": "2026-01-05"}, spend=5)]),
            report_response([report_row({"stat_time_day": "2026-03-10"}, spend=5)]),
        ]

        result = await client.get_spending_dates("2026-03-31")

        assert result["all_dates"] == ["2026-01-05", "2026-03-10"]

    async def test_a_failing_chunk_does_not_lose_the_others(self, client):
        client.responses = [
            report_response([report_row({"stat_time_day": "2026-03-10"}, spend=9)]),
            RuntimeError("TikTok API Error 40100"),
            report_response([report_row({"stat_time_day": "2026-01-05"}, spend=9)]),
        ]

        result = await client.get_spending_dates("2026-03-31")

        assert result["all_dates"] == ["2026-01-05", "2026-03-10"]

    async def test_no_spend_anywhere_returns_no_first_date(self, client):
        client.responses = [report_response([]) for _ in range(3)]

        result = await client.get_spending_dates("2026-03-31")

        assert result == {"first_date": None, "all_dates": []}


class TestAdvertiserInfo:
    async def test_requests_the_documented_field_set(self, client):
        await client.get_advertiser_info()

        params = client.calls[0]["params"]
        assert params["advertiser_ids"] == [client.advertiser_id]
        assert "display_timezone" in params["fields"]
        assert "currency" in params["fields"]

    async def test_explicit_advertiser_id_wins(self, client):
        await client.get_advertiser_info(advertiser_id="7000000000000000002")
        assert client.calls[0]["params"]["advertiser_ids"] == ["7000000000000000002"]
