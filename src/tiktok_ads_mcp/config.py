"""Configuration constants and enums for TikTok Ads MCP server."""

import os
from typing import Dict, List, Set

# API URLs
MARKETING_API_BASE_URL = "https://business-api.tiktok.com/open_api/v1.3"
SANDBOX_API_BASE_URL = "https://sandbox-ads.tiktok.com/open_api/v1.3"

# Rate limiting defaults
DEFAULT_RATE_LIMIT_PER_HOUR = 1000
DEFAULT_CONCURRENT_REQUESTS = 10

# Pagination
DEFAULT_PAGE_SIZE = 200
MAX_PAGE_SIZE = 1000

# Cache TTLs (seconds)
CACHE_TTL_INTEREST_CATEGORIES = 86400  # 24 hours
CACHE_TTL_LOCATIONS = 86400            # 24 hours
CACHE_TTL_ACCOUNT_INFO = 3600          # 1 hour
CACHE_TTL_AUDIENCES = 3600             # 1 hour
CACHE_TTL_PIXELS = 21600               # 6 hours

# Status filter values
ENTITY_STATUSES = [
    "STATUS_ALL",
    "STATUS_NOT_DELETE",
    "STATUS_NOT_DELIVERY",
    "STATUS_DELIVERY_OK",
    "STATUS_DISABLE",
    "STATUS_DELETE",
]

# Campaign objective types
OBJECTIVE_TYPES = [
    "REACH",
    "RF_REACH",
    "TRAFFIC",
    "ENGAGEMENT",
    "APP_PROMOTION",
    "LEAD_GENERATION",
    "PRODUCT_SALES",
]

# Deprecated objectives — rejected with a clear message pointing to the replacement
DEPRECATED_OBJECTIVES = {
    "VIDEO_VIEWS": "VIDEO_VIEWS has been deprecated by TikTok. Use ENGAGEMENT instead.",
}

# Optimization goals per objective
# Updated from live TikTok API v1.3 responses (2026-03-18)
# TikTok's actual valid goals differ significantly from their documentation.
OBJECTIVE_TO_GOALS: Dict[str, List[str]] = {
    "REACH": ["REACH"],
    "RF_REACH": ["REACH"],
    "TRAFFIC": ["CLICK", "TRAFFIC_LANDING_PAGE_VIEW"],
    "ENGAGEMENT": ["FOLLOWERS", "PROFILE_VIEWS"],
    "APP_PROMOTION": ["INSTALL", "IN_APP_EVENT", "VALUE"],
    "LEAD_GENERATION": ["LEAD_GENERATION", "LEADS", "CONVERSION_LEADS"],
    "PRODUCT_SALES": [
        "CONVERT", "VALUE", "CLICK", "TRAFFIC_LANDING_PAGE_VIEW",
        "INSTALL", "IN_APP_EVENT",
        "MT_LIVE_SHOPPING", "PRODUCT_CLICK_IN_LIVE",
    ],
}

# Legacy goal name → correct TikTok v1.3 goal name
# Auto-corrected in entity_manage to prevent 40002 errors
GOAL_NAME_ALIASES: Dict[str, str] = {
    "FOLLOW": "FOLLOWERS",
    "PROFILE_VISIT": "PROFILE_VIEWS",
    "LANDING_PAGE_VIEW": "TRAFFIC_LANDING_PAGE_VIEW",
    "LIKE": "POST_ENGAGEMNT",       # TikTok's actual spelling (not a typo)
    "COMMENT": "POST_ENGAGEMNT",
    "SHARE": "POST_ENGAGEMNT",
    "LIVE_VISIT": "MT_LIVE_SHOPPING",
    "PRODUCT_CLICK": "PRODUCT_CLICK_IN_LIVE",
    "STORE_VISIT": "MT_LIVE_SHOPPING",
}

# Promotion types (from TikTok API v1.3)
PROMOTION_TYPES = [
    "WEBSITE",
    "WEBSITE_OR_DISPLAY",
    "VIDEO_SHOPPING",
    "LIVE_SHOPPING",
    "TIKTOK_SHOP",
    "PSA_PRODUCT",
    "LEAD_GENERATION",
    "LEAD_GEN_CLICK_TO_TT_DIRECT_MESSAGE",
    "LEAD_GEN_CLICK_TO_SOCIAL_MEDIA_APP_MESSAGE",
    "LEAD_GEN_CLICK_TO_CALL",
    "APP_ANDROID",
    "APP_IOS",
    "AEO_APK",
    "GAME",
    "MINI_APP",
]

# Shopping ads types
SHOPPING_ADS_TYPES = ["VIDEO", "PRODUCT", "CATALOG"]

# Bid types
BID_TYPES = [
    "BID_TYPE_NO_BID",           # Lowest Cost
    "BID_TYPE_CUSTOM",           # Bid Cap
    "BID_TYPE_MAX_CONVERSION",   # Cost Cap (TikTok v1.3 actual name)
]

# Legacy bid type name → correct TikTok v1.3 name
BID_TYPE_ALIASES: Dict[str, str] = {
    "BID_TYPE_MAX_COST": "BID_TYPE_MAX_CONVERSION",
}

# Budget modes
BUDGET_MODES = ["BUDGET_MODE_DAY", "BUDGET_MODE_TOTAL", "BUDGET_MODE_INFINITE"]

# Age groups
AGE_GROUPS = [
    "AGE_13_17", "AGE_18_24", "AGE_25_34",
    "AGE_35_44", "AGE_45_54", "AGE_55_100",
]

# Gender values
GENDERS = ["GENDER_MALE", "GENDER_FEMALE", "GENDER_UNLIMITED"]

# Report data levels
DATA_LEVELS = {
    "campaign": "AUCTION_CAMPAIGN",
    "adgroup": "AUCTION_ADGROUP",
    "ad": "AUCTION_AD",
}

# Report types
REPORT_TYPES = ["BASIC", "AUDIENCE", "CATALOG", "PLAYABLE"]

# BASIC report allowed dimensions
BASIC_DIMENSIONS: Set[str] = {
    "campaign_id", "adgroup_id", "ad_id",
    "stat_time_day", "stat_time_hour",
    "country_code", "placement",
}

# AUDIENCE report primary dimensions
AUDIENCE_PRIMARY_DIMENSIONS: Set[str] = {
    "gender", "age", "country_code", "ac", "language",
    "platform", "interest_category", "placement",
}

# AUDIENCE report secondary dimensions requiring a parent
AUDIENCE_SECONDARY_DIMENSIONS: Dict[str, str] = {
    "province_id": "country_code",
    "dma_id": "country_code",
    "interest_category_v2": "interest_category",
    "interest_category_tier2": "interest_category",
    "interest_category_tier3": "interest_category_tier2",
    "interest_category_tier4": "interest_category_tier3",
    "device_brand_id": "platform",
}

# CATALOG report dimensions
CATALOG_DIMENSIONS: Set[str] = {
    "catalog_id", "product_set_id", "product_id", "sku_id",
    "stat_time_day",
}

# Core metrics available in all reports
CORE_METRICS = [
    "spend", "impressions", "reach", "clicks", "ctr", "cpc", "cpm",
    "frequency", "conversion", "cost_per_conversion", "conversion_rate_v2",
    "result", "cost_per_result", "result_rate",
    "real_time_conversion", "real_time_cost_per_conversion",
]

# Engagement metrics
ENGAGEMENT_METRICS = [
    "likes", "comments", "shares", "follows", "profile_visits",
]

# Video metrics
VIDEO_METRICS = [
    "video_play_actions", "video_watched_2s", "video_watched_6s",
    "average_video_play", "average_video_play_per_user",
    "video_views_p25", "video_views_p50", "video_views_p75", "video_views_p100",
]

# Shopping/conversion metrics
SHOPPING_METRICS = [
    "complete_payment", "total_purchase_value", "complete_payment_roas",
    "onsite_add_to_cart", "onsite_complete_payment", "onsite_complete_payment_roas",
    "app_install", "form_submission",
]

# Attribute metrics
ATTRIBUTE_METRICS = [
    "advertiser_name", "advertiser_id", "campaign_name", "campaign_id",
    "objective_type", "campaign_budget", "adgroup_name", "adgroup_id",
    "placement_type", "promotion_type", "budget", "smart_target",
    "billing_event", "bid_strategy", "bid",
    "ad_name", "ad_id", "ad_text", "call_to_action", "image_mode", "currency",
]

# ── Cross-field constraint maps ──────────────────────────────────────────

# Valid billing_event per optimization_goal
# Updated with live API testing (2026-03-23). Key findings:
#   CLICK→CPC, REACH→CPM, FOLLOWERS→OCPM, PROFILE_VIEWS→CPC (not OCPM!),
#   LEAD_GENERATION/LEADS/CONVERSION_LEADS→OCPM, TRAFFIC_LANDING_PAGE_VIEW→OCPM
GOAL_TO_BILLING: Dict[str, List[str]] = {
    "REACH": ["CPM"],
    "CLICK": ["CPC"],
    "TRAFFIC_LANDING_PAGE_VIEW": ["OCPM"],
    "VIDEO_VIEW": ["CPV"],
    "ENGAGED_VIEW": ["CPV"],
    "ENGAGED_VIEW_FIFTEEN": ["CPV"],
    "INSTALL": ["OCPM"],
    "IN_APP_EVENT": ["OCPM"],
    "VALUE": ["OCPM"],
    "CONVERT": ["OCPM"],
    "LEAD_GENERATION": ["OCPM"],
    "LEADS": ["OCPM"],
    "CONVERSION_LEADS": ["OCPM"],
    "FOLLOWERS": ["OCPM"],
    "PROFILE_VIEWS": ["CPC"],  # OCPM fails with "Only CPC is supported" (verified live API 2026-03-23)
    "POST_ENGAGEMNT": ["OCPM"],
    "MT_LIVE_SHOPPING": ["OCPM"],
    "PRODUCT_CLICK_IN_LIVE": ["CPC"],
}

# Valid bid_type per billing_event
BILLING_TO_BID_TYPES: Dict[str, List[str]] = {
    "CPM": ["BID_TYPE_NO_BID", "BID_TYPE_CUSTOM"],
    "OCPM": ["BID_TYPE_NO_BID", "BID_TYPE_CUSTOM", "BID_TYPE_MAX_CONVERSION"],
    "CPC": ["BID_TYPE_NO_BID", "BID_TYPE_CUSTOM"],
    "CPV": ["BID_TYPE_NO_BID", "BID_TYPE_CUSTOM"],
}

# Pacing restrictions: bid types that do NOT support accelerated delivery
NO_FAST_PACING_BID_TYPES = {"BID_TYPE_NO_BID", "BID_TYPE_MAX_CONVERSION"}

# Goals that do NOT support BID_TYPE_MAX_CONVERSION (Cost Cap)
# LEAD_GENERATION rejects it with "Invalid CPA smart bid type" (verified 2026-03-18)
NO_MAX_CONVERSION_GOALS = {"LEAD_GENERATION", "LEADS", "FOLLOWERS", "PROFILE_VIEWS"}

# Budget mode restrictions: BID_TYPE_NO_BID only supports daily budgets
NO_BID_BUDGET_MODES = {"BUDGET_MODE_DAY"}

# Objectives that do NOT support PLACEMENT_TYPE_AUTOMATIC
# These require PLACEMENT_TYPE_NORMAL with explicit placements (e.g. ["PLACEMENT_TIKTOK"])
# Verified via live API testing 2026-03-18: REACH, ENGAGEMENT, VIDEO_VIEWS all reject AUTOMATIC
MANUAL_PLACEMENT_ONLY_OBJECTIVES = {"LEAD_GENERATION", "REACH", "ENGAGEMENT"}

# TikTok API error codes
ERROR_CODES = {
    40001: "Authentication failed - access token invalid or expired",
    40002: "Invalid parameter",
    40100: "Permission denied - insufficient scope",
    50000: "Server error - retry later",
    50002: "Rate limit exceeded - slow down",
}

# Entity status update actions
STATUS_ACTIONS = {
    "enable": "ENABLE",
    "disable": "DISABLE",
    "delete": "DELETE",
}

# Batch limits for status updates
BATCH_STATUS_UPDATE_LIMIT = 100


def get_api_base_url() -> str:
    """Get the API base URL based on environment."""
    env = os.getenv("TIKTOK_API_ENV", "production")
    if env == "sandbox":
        return SANDBOX_API_BASE_URL
    return MARKETING_API_BASE_URL
