"""Ad creation/update validation."""

from typing import Any, Dict, List, Optional


# Ad formats supported by TikTok Ads API v1.3
AD_FORMATS = [
    "SINGLE_VIDEO",
    "SINGLE_IMAGE",
    "CAROUSEL",
    "CAROUSEL_ADS",
    "LIVE_CONTENT",
    "CATALOG_CAROUSEL",
]

# Note: Spark Ads are NOT a separate ad_format. They are SINGLE_VIDEO (or SINGLE_IMAGE)
# ads that promote an organic TikTok post. To create a Spark Ad, use ad_format=SINGLE_VIDEO
# and provide tiktok_item_id + identity_id + identity_type.

# Call-to-action values accepted by TikTok Ads API v1.3
# Source: live API testing across TRAFFIC and REACH objectives (2026-03-23).
# Notes:
#   - ENGAGEMENT campaigns reject ALL explicit CTAs — omit call_to_action for ENGAGEMENT.
#   - VIEW_PROFILE is accepted by TikTok's API enum but rejected for Spark Ads
#     (tiktok_item_id set); it may work for non-Spark ads with specific promotion types.
CALL_TO_ACTIONS = [
    "APPLY_NOW", "BOOK_NOW", "CALL_NOW", "CHECK_AVAILABILITY",
    "CONTACT_US", "DOWNLOAD_NOW", "EXPERIENCE_NOW", "GET_QUOTE",
    "GET_SHOWTIMES", "GET_TICKETS_NOW", "INSTALL_NOW", "INTERESTED",
    "JOIN_THIS_HASHTAG", "LEARN_MORE", "LISTEN_NOW", "ORDER_NOW",
    "PLAY_GAME", "PREORDER_NOW", "READ_MORE", "SEND_MESSAGE",
    "SHOOT_WITH_THIS_EFFECT", "SHOP_NOW", "SIGN_UP", "SUBSCRIBE",
    "VIEW_NOW", "VIEW_PROFILE", "VIEW_VIDEO_WITH_THIS_EFFECT",
    "VISIT_STORE", "WATCH_LIVE", "WATCH_NOW",
]

# Deprecated CTAs — TikTok rejects these for ALL objectives (not objective-specific).
# Verified via live API testing (2026-03-23): DOWNLOAD, BUY_NOW, WATCH_MORE, SEE_MORE, GET_OFFER
# all return error 40002 "one or more value of the param is not acceptable".
DEPRECATED_CTAS = {"DOWNLOAD", "BUY_NOW", "WATCH_MORE", "SEE_MORE", "GET_OFFER"}

# Valid identity types for Spark Ads (must NOT use CUSTOMIZED_USER)
SPARK_IDENTITY_TYPES = {"BC_AUTH_TT", "TT_USER", "AUTH_CODE"}


# Whether the ad is a Spark Ad (promoting an organic TikTok post) is determined by
# the presence of tiktok_item_id, NOT by ad_format.
def _is_spark_ad(data: Dict[str, Any]) -> bool:
    return bool(data.get("tiktok_item_id") or data.get("item_group_id"))


def validate_ad_create(data: Dict[str, Any], campaign_objective: str | None = None) -> List[str]:
    """Validate ad creation parameters. Returns list of error messages (empty = valid).

    Required fields that the calling agent MUST collect from the user:
      - adgroup_id
      - ad_name
      - ad_format (SINGLE_VIDEO, SINGLE_IMAGE, CAROUSEL, etc.)
      - For SINGLE_VIDEO (non-Spark): video_id
      - For SINGLE_IMAGE: image_ids
      - ad_text (the primary text / caption) — not needed for Spark Ads or LIVE_CONTENT
      - For Spark Ads (when tiktok_item_id is provided): identity_id, identity_type
      - For LEAD_GENERATION: page_id (Instant Form)
      - For TRAFFIC/WEB_CONVERSIONS (non-Spark): landing_page_url
    """
    errors = []
    missing = []

    # ── Required fields ────────────────────────────────────────────
    if not data.get("adgroup_id"):
        missing.append("adgroup_id (the ad group this ad belongs to)")

    if not data.get("ad_name"):
        missing.append("ad_name (ask the user what to name this ad)")

    ad_format = data.get("ad_format")
    if not ad_format:
        missing.append(
            "ad_format (ask the user — valid values: "
            + ", ".join(AD_FORMATS) + "). "
            "For Spark Ads (promoting an organic TikTok post), use SINGLE_VIDEO."
        )
    elif ad_format not in AD_FORMATS:
        errors.append(f"Invalid ad_format '{ad_format}'. Valid: {', '.join(AD_FORMATS)}")

    spark = _is_spark_ad(data)

    # ── Creative asset requirements per format ─────────────────────
    if ad_format == "SINGLE_VIDEO" and not spark:
        if not data.get("video_id"):
            missing.append("video_id (required for SINGLE_VIDEO ads — upload a video first via tiktok_creative)")

    elif ad_format == "SINGLE_IMAGE":
        if not data.get("image_ids"):
            missing.append("image_ids (required for SINGLE_IMAGE ads — upload an image first via tiktok_creative)")

    elif ad_format in ("CAROUSEL", "CAROUSEL_ADS"):
        if not data.get("image_ids"):
            missing.append("image_ids (required for CAROUSEL ads — provide 2-10 image IDs)")
        elif isinstance(data.get("image_ids"), list):
            if len(data["image_ids"]) < 2:
                errors.append("CAROUSEL ads require at least 2 images in image_ids")
            elif len(data["image_ids"]) > 10:
                errors.append(f"CAROUSEL ads allow maximum 10 images, got {len(data['image_ids'])}")

    elif ad_format == "CATALOG_CAROUSEL":
        if not data.get("product_set_id"):
            missing.append("product_set_id (required for CATALOG_CAROUSEL ads — the product set to display)")

    # Ad text is optional for Spark Ads (TikTok uses the original post caption) and LIVE_CONTENT
    if not data.get("ad_text") and not spark and ad_format not in ("LIVE_CONTENT",):
        missing.append("ad_text (the primary text/caption for the ad)")

    # ── Spark Ad requirements & constraints ────────────────────────
    if spark:
        if not data.get("identity_id"):
            missing.append("identity_id (required for Spark Ads — use tiktok_entity_get with entity_type='identities')")

        identity_type = data.get("identity_type")
        if not identity_type:
            missing.append("identity_type (required for Spark Ads — e.g. BC_AUTH_TT, TT_USER, AUTH_CODE)")
        elif identity_type == "CUSTOMIZED_USER":
            errors.append(
                "identity_type 'CUSTOMIZED_USER' cannot be used with Spark Ads. "
                "Use BC_AUTH_TT, TT_USER, or AUTH_CODE instead."
            )
        elif identity_type not in SPARK_IDENTITY_TYPES:
            errors.append(
                f"Invalid identity_type '{identity_type}' for Spark Ads. "
                f"Valid: {', '.join(sorted(SPARK_IDENTITY_TYPES))}"
            )

        # BC_AUTH_TT identity type requires identity_authorized_bc_id
        if identity_type == "BC_AUTH_TT" and not data.get("identity_authorized_bc_id"):
            missing.append(
                "identity_authorized_bc_id (required when identity_type is BC_AUTH_TT — "
                "the Business Center ID that authorized the TikTok account)"
            )

        # Spark Ads use the organic post's video — video_id should not be set
        if data.get("video_id"):
            errors.append(
                "video_id should not be set for Spark Ads. The organic post's video "
                "(tiktok_item_id) is used as the creative. Remove video_id."
            )

    # ── Objective-specific ad requirements ─────────────────────────

    # ENGAGEMENT: TikTok rejects ALL explicit CTAs for ENGAGEMENT campaigns (verified via live API,
    # 2026-03-23 — all 30 CTAs return "This Call to Action is not supported").
    # The correct behavior is to omit call_to_action and let TikTok auto-assign (e.g. "Follow").
    call_to_action = data.get("call_to_action")
    if campaign_objective == "ENGAGEMENT" and call_to_action:
        errors.append(
            f"call_to_action is not supported for ENGAGEMENT campaigns — TikTok rejects all explicit "
            f"CTAs for this objective. Remove call_to_action to let TikTok auto-assign (e.g. 'Follow')."
        )

    # LEAD_GENERATION requires page_id (Instant Form)
    if campaign_objective == "LEAD_GENERATION":
        if not data.get("page_id"):
            missing.append(
                "page_id (required for Lead Generation ads — the Instant Form page ID. "
                "Use tiktok_entity_get with entity_type='pages' or check existing ads.)"
            )

    # TRAFFIC / WEB_CONVERSIONS non-Spark ads need landing_page_url
    if campaign_objective in ("TRAFFIC", "WEB_CONVERSIONS") and not spark:
        if not data.get("landing_page_url"):
            missing.append(
                "landing_page_url (required for website-type ads — "
                "must start with https://)"
            )

    # ── Optional field validation ──────────────────────────────────
    call_to_action = data.get("call_to_action")
    if call_to_action:
        if call_to_action in DEPRECATED_CTAS:
            errors.append(
                f"call_to_action '{call_to_action}' has been deprecated by TikTok. "
                f"Use one of: LEARN_MORE, SHOP_NOW, SIGN_UP, CONTACT_US, SUBSCRIBE, "
                f"DOWNLOAD_NOW, GET_QUOTE, APPLY_NOW, BOOK_NOW, ORDER_NOW"
            )
        elif call_to_action not in CALL_TO_ACTIONS:
            errors.append(
                f"Invalid call_to_action '{call_to_action}'. "
                f"Common values: LEARN_MORE, SHOP_NOW, SIGN_UP, GET_QUOTE, CONTACT_US"
            )

    # Landing page URL format validation
    landing_page_url = data.get("landing_page_url")
    if landing_page_url and not landing_page_url.startswith(("http://", "https://")):
        errors.append("landing_page_url must start with http:// or https://")

    # display_name required for non-Spark ads (TikTok shows it on the ad)
    if not spark and not data.get("display_name") and ad_format not in ("LIVE_CONTENT", "CATALOG_CAROUSEL"):
        missing.append("display_name (the brand/account name shown on the ad, 1-40 chars)")

    # ── Report missing fields ─────────────────────────────────────
    if missing:
        errors.append(
            "Missing required fields — ask the user for these before creating: "
            + "; ".join(missing)
        )

    return errors


def validate_ad_update(data: Dict[str, Any]) -> List[str]:
    """Validate ad update parameters."""
    errors = []

    if not data.get("ad_id"):
        errors.append("ad_id is required for updates")

    call_to_action = data.get("call_to_action")
    if call_to_action:
        if call_to_action in DEPRECATED_CTAS:
            errors.append(
                f"call_to_action '{call_to_action}' has been deprecated by TikTok. "
                f"Use one of: LEARN_MORE, SHOP_NOW, SIGN_UP, CONTACT_US, DOWNLOAD_NOW"
            )
        elif call_to_action not in CALL_TO_ACTIONS:
            errors.append(
                f"Invalid call_to_action '{call_to_action}'. "
                f"Common values: LEARN_MORE, SHOP_NOW, SIGN_UP, GET_QUOTE"
            )

    landing_page_url = data.get("landing_page_url")
    if landing_page_url and not landing_page_url.startswith(("http://", "https://")):
        errors.append("landing_page_url must start with http:// or https://")

    return errors
