"""Date range helpers for TikTok Ads MCP server."""

from datetime import datetime, timedelta
from typing import Tuple


def resolve_date_range(date_range: str) -> Tuple[str, str]:
    """Convert a named date range to (start_date, end_date) in YYYY-MM-DD format.

    Args:
        date_range: One of: today, yesterday, last_3_days, last_7_days,
                    last_14_days, last_30_days, last_60_days, last_90_days,
                    this_month, last_month

    Returns:
        Tuple of (start_date, end_date) strings
    """
    today = datetime.now().date()

    presets = {
        "today": (today, today),
        "yesterday": (today - timedelta(days=1), today - timedelta(days=1)),
        "last_3_days": (today - timedelta(days=3), today - timedelta(days=1)),
        "last_7_days": (today - timedelta(days=7), today - timedelta(days=1)),
        "last_14_days": (today - timedelta(days=14), today - timedelta(days=1)),
        "last_30_days": (today - timedelta(days=30), today - timedelta(days=1)),
        "last_60_days": (today - timedelta(days=60), today - timedelta(days=1)),
        "last_90_days": (today - timedelta(days=90), today - timedelta(days=1)),
        "this_month": (today.replace(day=1), today),
        "last_month": (
            (today.replace(day=1) - timedelta(days=1)).replace(day=1),
            today.replace(day=1) - timedelta(days=1),
        ),
    }

    if date_range not in presets:
        raise ValueError(
            f"Invalid date_range '{date_range}'. "
            f"Valid options: {', '.join(presets.keys())}"
        )

    start, end = presets[date_range]
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def validate_date_string(date_str: str) -> str:
    """Validate and normalize a YYYY-MM-DD date string."""
    try:
        parsed = datetime.strptime(date_str, "%Y-%m-%d")
        return parsed.strftime("%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Invalid date format '{date_str}'. Expected YYYY-MM-DD.")
