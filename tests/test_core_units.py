"""Coverage for the pieces this fork added on top of upstream but never tested:
date resolution, the TTL cache, the write-confirmation gate, and the validators
that encode TikTok's real (undocumented) field rules.
"""

from datetime import datetime, timedelta

import pytest

from tiktok_ads_mcp.cache.cache_manager import CacheManager
from tiktok_ads_mcp.utils.confirmation import build_preview
from tiktok_ads_mcp.utils.date_helpers import resolve_date_range, validate_date_string
from tiktok_ads_mcp.validators.ad_validator import validate_ad_create
from tiktok_ads_mcp.validators.campaign_validator import validate_campaign_create


class TestDateHelpers:
    def test_named_ranges_end_yesterday_not_today(self):
        # Today's numbers are still moving; a report that includes them misleads.
        start, end = resolve_date_range("last_7_days")
        yesterday = (datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")

        assert end == yesterday
        assert start < end

    def test_today_is_available_when_explicitly_asked_for(self):
        start, end = resolve_date_range("today")
        assert start == end == datetime.now().date().strftime("%Y-%m-%d")

    @pytest.mark.parametrize("name,days", [
        ("last_3_days", 3), ("last_7_days", 7), ("last_30_days", 30), ("last_90_days", 90),
    ])
    def test_window_length_matches_the_name(self, name, days):
        start, end = resolve_date_range(name)
        span = datetime.strptime(end, "%Y-%m-%d") - datetime.strptime(start, "%Y-%m-%d")
        assert span.days == days - 1

    def test_last_month_is_a_whole_calendar_month(self):
        start, end = resolve_date_range("last_month")
        assert start.endswith("-01")
        assert datetime.strptime(start, "%Y-%m-%d") < datetime.strptime(end, "%Y-%m-%d")

    def test_an_unknown_range_names_the_valid_ones(self):
        with pytest.raises(ValueError, match="last_7_days"):
            resolve_date_range("last_fortnight")

    def test_date_validation_rejects_other_formats(self):
        assert validate_date_string("2026-02-14") == "2026-02-14"
        with pytest.raises(ValueError, match="YYYY-MM-DD"):
            validate_date_string("14/02/2026")

    def test_an_impossible_date_is_rejected(self):
        with pytest.raises(ValueError):
            validate_date_string("2026-02-30")


class TestCacheManager:
    def test_a_miss_is_none_not_an_error(self):
        assert CacheManager().get("nothing-here") is None

    def test_round_trip(self):
        cache = CacheManager()
        cache.set("pixels", [{"pixel_id": "p1"}])
        assert cache.get("pixels") == [{"pixel_id": "p1"}]

    def test_entries_expire(self):
        cache = CacheManager()
        cache.set("pixels", ["stale"], ttl_seconds=-1)
        assert cache.get("pixels") is None

    def test_known_keys_get_their_configured_ttl(self):
        cache = CacheManager()
        cache.set("interest_categories", ["x"])
        # Interests change far more slowly than pixels; the TTLs must differ.
        assert cache.TTL_MAP["interest_categories"] > cache.TTL_MAP["pixels"]

    def test_the_two_account_info_variants_both_have_a_ttl(self):
        assert CacheManager.TTL_MAP["account_info"] == CacheManager.TTL_MAP["account_info_basic"]

    def test_invalidate_and_clear(self):
        cache = CacheManager()
        cache.set("pixels", ["a"])
        cache.set("audiences", ["b"])

        cache.invalidate("pixels")
        assert cache.get("pixels") is None
        assert cache.get("audiences") == ["b"]

        cache.clear()
        assert cache.get("audiences") is None


class TestConfirmationGate:
    def test_a_preview_is_explicitly_not_executed(self):
        preview = build_preview("create", "campaign", {"campaign_name": "Q1"})

        assert preview["success"] is True
        assert preview["executed"] is False
        assert preview["data"]["preview"] is True
        assert "confirm=true" in preview["data"]["message"]

    def test_the_parameters_under_review_are_echoed_back(self):
        preview = build_preview("update", "adgroup", {"budget": 500})
        assert preview["data"]["parameters"] == {"budget": 500}

    def test_warnings_ride_along_when_given(self):
        preview = build_preview("delete", "campaign", {}, warnings=["This cannot be undone"])
        assert preview["data"]["warnings"] == ["This cannot be undone"]

    def test_no_warnings_key_when_there_are_none(self):
        assert "warnings" not in build_preview("create", "ad", {})["data"]


class TestCampaignValidator:
    def test_a_complete_campaign_passes(self):
        assert validate_campaign_create({
            "campaign_name": "Q1 Prospecting",
            "objective_type": "TRAFFIC",
            "budget": 100,
            "budget_mode": "BUDGET_MODE_DAY",
        }) == []

    def test_a_deprecated_objective_names_its_replacement(self):
        errors = validate_campaign_create({
            "campaign_name": "X", "objective_type": "VIDEO_VIEWS",
            "budget": 100, "budget_mode": "BUDGET_MODE_DAY",
        })
        assert any("ENGAGEMENT" in e for e in errors)

    def test_an_unknown_objective_is_rejected(self):
        errors = validate_campaign_create({
            "campaign_name": "X", "objective_type": "WORLD_DOMINATION",
            "budget": 100, "budget_mode": "BUDGET_MODE_DAY",
        })
        assert any("Invalid objective_type" in e for e in errors)

    def test_missing_fields_tell_the_agent_to_ask_rather_than_guess(self):
        errors = validate_campaign_create({})
        assert any("campaign_name" in e for e in errors)
        assert any("ask the user" in e for e in errors)

    def test_app_promotion_needs_its_promotion_type(self):
        errors = validate_campaign_create({
            "campaign_name": "X", "objective_type": "APP_PROMOTION",
            "budget": 100, "budget_mode": "BUDGET_MODE_DAY",
        })
        assert any("app_promotion_type" in e for e in errors)

    def test_app_promotion_type_is_rejected_on_other_objectives(self):
        errors = validate_campaign_create({
            "campaign_name": "X", "objective_type": "TRAFFIC",
            "budget": 100, "budget_mode": "BUDGET_MODE_DAY",
            "app_promotion_type": "APP_INSTALL",
        })
        assert any("app_promotion_type" in e for e in errors)


class TestAdValidator:
    def test_a_single_video_ad_needs_a_video(self):
        errors = validate_ad_create({
            "adgroup_id": "1", "ad_name": "Ad", "ad_format": "SINGLE_VIDEO",
            "ad_text": "Buy", "landing_page_url": "https://example.com",
        })
        assert any("video_id" in e for e in errors)

    def test_a_spark_ad_needs_no_uploaded_video(self):
        errors = validate_ad_create({
            "adgroup_id": "1", "ad_name": "Ad", "ad_format": "SINGLE_VIDEO",
            "tiktok_item_id": "7300000000000000000",
            "identity_id": "id1", "identity_type": "BC_AUTH_TT",
            "identity_authorized_bc_id": "bc1",
            "landing_page_url": "https://example.com",
        })
        assert not any("video_id" in e for e in errors)

    def test_carousel_image_count_is_bounded(self):
        too_few = validate_ad_create({
            "adgroup_id": "1", "ad_name": "Ad", "ad_format": "CAROUSEL",
            "ad_text": "Buy", "image_ids": ["i1"],
            "landing_page_url": "https://example.com",
        })
        assert any("at least 2 images" in e for e in too_few)

        too_many = validate_ad_create({
            "adgroup_id": "1", "ad_name": "Ad", "ad_format": "CAROUSEL",
            "ad_text": "Buy", "image_ids": [f"i{i}" for i in range(11)],
            "landing_page_url": "https://example.com",
        })
        assert any("maximum 10 images" in e for e in too_many)

    def test_an_unknown_ad_format_is_rejected(self):
        errors = validate_ad_create({
            "adgroup_id": "1", "ad_name": "Ad", "ad_format": "HOLOGRAM",
        })
        assert any("Invalid ad_format" in e for e in errors)
