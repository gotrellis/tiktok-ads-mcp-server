"""Campaign creation/update validation."""

from typing import Any, Dict, List, Optional

from ..config import BUDGET_MODES, DEPRECATED_OBJECTIVES, OBJECTIVE_TYPES


def validate_campaign_create(data: Dict[str, Any]) -> List[str]:
    """Validate campaign creation parameters. Returns list of error messages (empty = valid).

    Required fields that the calling agent MUST collect from the user
    (do not guess or use defaults):
      - campaign_name
      - objective_type
      - budget + budget_mode (unless objective is RF_REACH)
    """
    errors = []
    missing = []

    # ── Required fields — agent must ask user, never guess ──────────
    if not data.get("campaign_name"):
        missing.append("campaign_name (ask the user what to name this campaign)")

    objective = data.get("objective_type")
    if not objective:
        missing.append(
            "objective_type (ask the user — valid values: "
            + ", ".join(OBJECTIVE_TYPES) + ")"
        )
    elif objective in DEPRECATED_OBJECTIVES:
        errors.append(DEPRECATED_OBJECTIVES[objective])
    elif objective not in OBJECTIVE_TYPES:
        errors.append(f"Invalid objective_type '{objective}'. Valid: {', '.join(OBJECTIVE_TYPES)}")

    # APP_PROMOTION requires app_promotion_type
    app_promo_type = data.get("app_promotion_type")
    if objective == "APP_PROMOTION":
        if not app_promo_type:
            missing.append("app_promotion_type (required for APP_PROMOTION — APP_INSTALL or APP_RETARGETING)")
        elif app_promo_type not in ("APP_INSTALL", "APP_RETARGETING"):
            errors.append(
                f"Invalid app_promotion_type '{app_promo_type}'. "
                "Valid: APP_INSTALL, APP_RETARGETING"
            )
    elif app_promo_type:
        errors.append("app_promotion_type should only be set when objective_type is APP_PROMOTION")

    # PRODUCT_SALES special fields
    if objective == "PRODUCT_SALES":
        # Warn if neither catalog nor shop destination is indicated
        if not data.get("catalog_enabled") and not data.get("sales_destination"):
            errors.append(
                "PRODUCT_SALES campaigns typically need catalog_enabled=true or "
                "a sales_destination (e.g. 'WEBSITE', 'SHOP'). "
                "Ask the user which product sales type they want."
            )

    # Budget is required for all objectives except RF_REACH
    if objective != "RF_REACH":
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

        if not data.get("budget_mode"):
            missing.append("budget_mode (ask the user — BUDGET_MODE_DAY or BUDGET_MODE_TOTAL)")

    if objective == "RF_REACH":
        budget_mode = data.get("budget_mode")
        if budget_mode and budget_mode != "BUDGET_MODE_INFINITE":
            errors.append("RF_REACH campaigns must use BUDGET_MODE_INFINITE")

    budget_mode = data.get("budget_mode")
    if budget_mode and budget_mode not in BUDGET_MODES:
        errors.append(f"Invalid budget_mode '{budget_mode}'. Valid: {', '.join(BUDGET_MODES)}")

    # ── Report missing fields clearly so the agent asks the user ────
    if missing:
        errors.append(
            "Missing required fields — ask the user for these before creating: "
            + "; ".join(missing)
        )

    return errors


def validate_campaign_update(data: Dict[str, Any]) -> List[str]:
    """Validate campaign update parameters."""
    errors = []

    if not data.get("campaign_id"):
        errors.append("campaign_id is required for updates")

    budget = data.get("budget")
    if budget is not None:
        try:
            if float(budget) <= 0:
                errors.append("budget must be greater than 0")
        except (TypeError, ValueError):
            errors.append("budget must be a number")

    return errors
