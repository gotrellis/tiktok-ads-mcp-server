"""Ad group creation/update validation."""

from typing import Any, Dict, List

from ..config import (
    AGE_GROUPS,
    BID_TYPES,
    BILLING_TO_BID_TYPES,
    BUDGET_MODES,
    GENDERS,
    GOAL_TO_BILLING,
    MANUAL_PLACEMENT_ONLY_OBJECTIVES,
    NO_BID_BUDGET_MODES,
    NO_FAST_PACING_BID_TYPES,
    OBJECTIVE_TO_GOALS,
    PROMOTION_TYPES,
)


def validate_adgroup_create(data: Dict[str, Any], campaign_objective: str | None = None) -> List[str]:
    """Validate ad group creation parameters. Returns list of error messages (empty = valid).

    Required fields that the calling agent MUST collect from the user
    (do not guess or use defaults):
      - campaign_id
      - adgroup_name
      - placement_type
      - budget + budget_mode
      - schedule_type
      - optimization_goal
      - location_ids (at least one country)
      - billing_event
    """
    errors = []
    missing = []

    # ── Required fields — agent must ask user, never guess ──────────
    if not data.get("campaign_id"):
        missing.append("campaign_id (the campaign this ad group belongs to)")

    if not data.get("adgroup_name"):
        missing.append("adgroup_name (ask the user what to name this ad group)")

    placement_type = data.get("placement_type")
    if not placement_type:
        if campaign_objective in MANUAL_PLACEMENT_ONLY_OBJECTIVES:
            missing.append(
                f"placement_type (must be PLACEMENT_TYPE_NORMAL for {campaign_objective} — "
                "PLACEMENT_TYPE_AUTOMATIC is not supported for this objective)"
            )
        else:
            missing.append(
                "placement_type (ask the user — PLACEMENT_TYPE_AUTOMATIC or PLACEMENT_TYPE_NORMAL)"
            )
    elif placement_type == "PLACEMENT_TYPE_AUTOMATIC" and campaign_objective in MANUAL_PLACEMENT_ONLY_OBJECTIVES:
        errors.append(
            f"PLACEMENT_TYPE_AUTOMATIC is not supported for {campaign_objective} campaigns. "
            "Use PLACEMENT_TYPE_NORMAL with placements: ['PLACEMENT_TIKTOK']."
        )
    elif placement_type == "PLACEMENT_TYPE_NORMAL" and not data.get("placements"):
        missing.append(
            "placements (required when placement_type is PLACEMENT_TYPE_NORMAL — "
            "e.g. ['PLACEMENT_TIKTOK'])"
        )

    # Budget is required
    budget = data.get("budget")
    if budget is None:
        missing.append("budget (ask the user for daily or total budget amount)")
    else:
        try:
            budget_val = float(budget)
            if budget_val <= 0:
                errors.append("budget must be greater than 0")
        except (TypeError, ValueError):
            errors.append("budget must be a number")

    budget_mode = data.get("budget_mode")
    if not budget_mode:
        missing.append("budget_mode (ask the user — BUDGET_MODE_DAY or BUDGET_MODE_TOTAL)")
    elif budget_mode not in BUDGET_MODES:
        errors.append(f"Invalid budget_mode '{budget_mode}'. Valid: {', '.join(BUDGET_MODES)}")

    if not data.get("schedule_type"):
        missing.append(
            "schedule_type (ask the user — SCHEDULE_FROM_NOW or SCHEDULE_START_END)"
        )

    # Optimization goal is required and must be compatible with campaign objective
    optimization_goal = data.get("optimization_goal")
    if not optimization_goal:
        if campaign_objective:
            allowed_goals = OBJECTIVE_TO_GOALS.get(campaign_objective, [])
            missing.append(
                f"optimization_goal (ask the user — for {campaign_objective} objective, "
                f"valid values: {', '.join(allowed_goals)})"
            )
        else:
            missing.append("optimization_goal (ask the user what to optimize for)")
    elif campaign_objective:
        allowed_goals = OBJECTIVE_TO_GOALS.get(campaign_objective, [])
        if allowed_goals and optimization_goal not in allowed_goals:
            errors.append(
                f"optimization_goal '{optimization_goal}' is not compatible with "
                f"campaign objective '{campaign_objective}'. "
                f"Allowed: {', '.join(allowed_goals)}"
            )

    if not data.get("location_ids"):
        missing.append(
            "location_ids (ask the user which countries/regions to target, "
            "e.g. ['6252001'] for US)"
        )

    if not data.get("billing_event"):
        missing.append(
            "billing_event (ask the user — CPC, CPM, CPV, or OCPM)"
        )

    # Schedule end time required for SCHEDULE_START_END
    schedule_type = data.get("schedule_type")
    if schedule_type == "SCHEDULE_START_END" and not data.get("schedule_end_time"):
        missing.append(
            "schedule_end_time (required when schedule_type is SCHEDULE_START_END, "
            "format: YYYY-MM-DD HH:MM:SS in UTC)"
        )

    # ── Objective-specific required fields ────────────────────────────

    # APP_PROMOTION requires app_id at adgroup level
    if campaign_objective == "APP_PROMOTION":
        if not data.get("app_id"):
            missing.append("app_id (required for APP_PROMOTION — get from tiktok_entity_get with entity_type='apps')")
        os_val = data.get("operating_systems")
        if not os_val:
            missing.append("operating_systems (required for APP_PROMOTION — ['ANDROID'] or ['IOS'])")
        elif isinstance(os_val, list):
            invalid_os = [v for v in os_val if v not in ("ANDROID", "IOS")]
            if invalid_os:
                errors.append(f"Invalid operating_systems values: {invalid_os}. Valid: ANDROID, IOS")
        elif isinstance(os_val, str):
            if os_val not in ("ANDROID", "IOS"):
                errors.append(f"Invalid operating_systems '{os_val}'. Valid: ['ANDROID'] or ['IOS']")
            # Auto-convert string to array for TikTok API compatibility
            data["operating_systems"] = [os_val]

    # Conversion-based goals require pixel_id + optimization_event
    conversion_goals = {"CONVERT", "TRAFFIC_LANDING_PAGE_VIEW", "LANDING_PAGE_VIEW", "VALUE"}
    optimization_goal = data.get("optimization_goal")
    if campaign_objective in ("WEB_CONVERSIONS", "TRAFFIC", "PRODUCT_SALES"):
        if optimization_goal in conversion_goals:
            if not data.get("pixel_id"):
                missing.append(
                    f"pixel_id (required for optimization_goal '{optimization_goal}' — "
                    "use tiktok_entity_get with entity_type='pixels')"
                )
            if optimization_goal in ("CONVERT", "VALUE", "TRAFFIC_LANDING_PAGE_VIEW", "LANDING_PAGE_VIEW") and not data.get("optimization_event"):
                missing.append(
                    "optimization_event (required for conversion/landing page optimization — "
                    "e.g. COMPLETE_PAYMENT, FORM, ADD_TO_CART, INITIATE_CHECKOUT, ON_WEB_ORDER)"
                )

    # PRODUCT_SALES shopping-specific fields
    if campaign_objective == "PRODUCT_SALES":
        if not data.get("identity_id"):
            missing.append("identity_id (required for PRODUCT_SALES — the TikTok account identity)")
        if not data.get("identity_type"):
            missing.append("identity_type (required for PRODUCT_SALES — e.g. BC_AUTH_TT, TT_USER)")
        if not data.get("catalog_id") and not data.get("store_id"):
            missing.append(
                "catalog_id or store_id (required for PRODUCT_SALES — "
                "catalog_id for product catalog ads, store_id for TikTok Shop ads)"
            )

    # REACH / RF_REACH campaigns require a frequency cap
    if campaign_objective in ("REACH", "RF_REACH") or optimization_goal == "REACH":
        freq = data.get("frequency") or data.get("frequency_cap")
        freq_sched = data.get("frequency_schedule")
        if not freq:
            missing.append(
                "frequency (required for REACH campaigns — max impressions per user, "
                "e.g. 3) and frequency_schedule (e.g. 7 for '3 times per 7 days')"
            )
        elif not freq_sched:
            missing.append(
                "frequency_schedule (required when frequency is set — "
                "number of days for the frequency cap, e.g. 7)"
            )

    # Placements must be non-empty when provided
    placements = data.get("placements")
    if placements is not None and isinstance(placements, list) and len(placements) == 0:
        errors.append("placements array must not be empty when placement_type is PLACEMENT_TYPE_NORMAL")

    # ── Cross-field constraint validation ─────────────────────────────

    billing_event = data.get("billing_event")
    bid_type = data.get("bid_type")
    optimization_goal = data.get("optimization_goal")
    pacing = data.get("pacing")

    # Fix #5: bid_price/conversion_bid_price without bid_type → auto-set BID_TYPE_CUSTOM
    # Without this, TikTok defaults to BID_TYPE_NO_BID which conflicts with bid_price.
    if not bid_type and (data.get("bid_price") or data.get("conversion_bid_price")):
        bid_type = "BID_TYPE_CUSTOM"
        data["bid_type"] = bid_type

    # 1. optimization_goal → billing_event must be compatible
    if optimization_goal and billing_event:
        valid_billing = GOAL_TO_BILLING.get(optimization_goal)
        if valid_billing and billing_event not in valid_billing:
            errors.append(
                f"billing_event '{billing_event}' is not compatible with "
                f"optimization_goal '{optimization_goal}'. "
                f"Valid billing_event: {', '.join(valid_billing)}"
            )

    # 2. billing_event → bid_type must be compatible
    if billing_event and bid_type:
        valid_bids = BILLING_TO_BID_TYPES.get(billing_event)
        if valid_bids and bid_type not in valid_bids:
            errors.append(
                f"bid_type '{bid_type}' is not compatible with "
                f"billing_event '{billing_event}'. "
                f"Valid bid_type: {', '.join(valid_bids)}"
            )

    # 3. BID_TYPE_NO_BID + PACING_MODE_FAST → invalid
    if bid_type in NO_FAST_PACING_BID_TYPES and pacing == "PACING_MODE_FAST":
        errors.append(
            f"Accelerated delivery (PACING_MODE_FAST) is not supported with "
            f"bid_type '{bid_type}'. Use PACING_MODE_SMOOTH or remove pacing."
        )

    # 4. BID_TYPE_NO_BID only supports daily budgets
    if bid_type == "BID_TYPE_NO_BID" and budget_mode and budget_mode not in NO_BID_BUDGET_MODES:
        errors.append(
            f"bid_type 'BID_TYPE_NO_BID' (lowest cost) only supports BUDGET_MODE_DAY. "
            f"Got '{budget_mode}'. Use daily budget or change bid_type."
        )

    # 5. Cost-cap/bid-cap with OCPM requires bid_price
    if billing_event == "OCPM" and bid_type in ("BID_TYPE_CUSTOM", "BID_TYPE_MAX_CONVERSION"):
        if not data.get("conversion_bid_price") and not data.get("bid_price"):
            errors.append(
                f"conversion_bid_price is required when billing_event is OCPM with "
                f"bid_type '{bid_type}'. Ask the user for their target cost per conversion. "
                "Alternatively, use bid_type 'BID_TYPE_NO_BID' for lowest cost (no cap)."
            )

    # 6. CPC/CPV with BID_TYPE_CUSTOM requires bid_price
    if billing_event in ("CPC", "CPV") and bid_type == "BID_TYPE_CUSTOM":
        if not data.get("bid_price"):
            errors.append(
                f"bid_price is required when billing_event is {billing_event} with "
                f"bid_type 'BID_TYPE_CUSTOM'. Ask the user for their max {billing_event} bid."
            )

    # 7. BID_TYPE_NO_BID must NOT have bid_price
    if bid_type == "BID_TYPE_NO_BID":
        if data.get("bid_price") or data.get("conversion_bid_price"):
            errors.append(
                "bid_price/conversion_bid_price must not be set with bid_type 'BID_TYPE_NO_BID' "
                "(lowest cost). Remove bid_price or change bid_type to 'BID_TYPE_CUSTOM'."
            )

    # ── Optional field validation ────────────────────────────────────

    # Bid type enum check
    if bid_type and bid_type not in BID_TYPES:
        errors.append(f"Invalid bid_type '{bid_type}'. Valid: {', '.join(BID_TYPES)}")

    # Promotion type requirements
    promotion_type = data.get("promotion_type")
    if promotion_type:
        if promotion_type not in PROMOTION_TYPES:
            errors.append(f"Invalid promotion_type '{promotion_type}'. Valid: {', '.join(PROMOTION_TYPES)}")

        shopping_types = {"VIDEO_SHOPPING", "LIVE_SHOPPING", "TIKTOK_SHOP", "PSA_PRODUCT"}
        if promotion_type in shopping_types and not data.get("store_id"):
            errors.append(f"store_id is required when promotion_type is {promotion_type}")

    # Dayparting validation
    dayparting = data.get("dayparting")
    if dayparting:
        if len(dayparting) != 336:
            errors.append(f"dayparting must be exactly 336 characters, got {len(dayparting)}")
        elif not all(c in ("0", "1") for c in dayparting):
            errors.append("dayparting must contain only '0' and '1' characters")

    # Targeting validation
    gender = data.get("gender")
    if gender:
        if isinstance(gender, list):
            errors.append(
                "gender must be a single string, not an array. "
                "Use GENDER_UNLIMITED to target all genders. "
                f"Valid: {', '.join(GENDERS)}"
            )
        elif gender not in GENDERS:
            errors.append(f"Invalid gender '{gender}'. Valid: {', '.join(GENDERS)}")

    age_groups = data.get("age_groups")
    if age_groups:
        invalid = [ag for ag in age_groups if ag not in AGE_GROUPS]
        if invalid:
            errors.append(f"Invalid age_groups: {', '.join(invalid)}. Valid: {', '.join(AGE_GROUPS)}")

    # Keywords only for SEARCH campaigns
    if data.get("keywords") and campaign_objective != "SEARCH":
        errors.append("keywords targeting is only allowed for SEARCH campaign types")

    # ── Report missing fields clearly so the agent asks the user ────
    if missing:
        errors.append(
            "Missing required fields — ask the user for these before creating: "
            + "; ".join(missing)
        )

    return errors


def validate_adgroup_update(data: Dict[str, Any]) -> List[str]:
    """Validate ad group update parameters."""
    errors = []

    if not data.get("adgroup_id"):
        errors.append("adgroup_id is required for updates")

    budget = data.get("budget")
    if budget is not None:
        try:
            if float(budget) <= 0:
                errors.append("budget must be greater than 0")
        except (TypeError, ValueError):
            errors.append("budget must be a number")

    dayparting = data.get("dayparting")
    if dayparting:
        if len(dayparting) != 336:
            errors.append(f"dayparting must be exactly 336 characters, got {len(dayparting)}")
        elif not all(c in ("0", "1") for c in dayparting):
            errors.append("dayparting must contain only '0' and '1' characters")

    return errors
