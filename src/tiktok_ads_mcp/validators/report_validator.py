"""Report parameter validation."""

from typing import Any, Dict, List, Set

from ..config import (
    AUDIENCE_PRIMARY_DIMENSIONS,
    AUDIENCE_SECONDARY_DIMENSIONS,
    BASIC_DIMENSIONS,
    CATALOG_DIMENSIONS,
    DATA_LEVELS,
    REPORT_TYPES,
)


def validate_report_params(
    report_type: str,
    data_level: str,
    dimensions: List[str],
    metrics: List[str],
) -> List[str]:
    """Validate report parameters. Returns list of error messages (empty = valid)."""
    errors = []

    if report_type not in REPORT_TYPES:
        errors.append(f"Invalid report_type '{report_type}'. Valid: {', '.join(REPORT_TYPES)}")

    valid_levels = list(DATA_LEVELS.values())
    if data_level not in valid_levels:
        errors.append(f"Invalid data_level '{data_level}'. Valid: {', '.join(valid_levels)}")

    if not dimensions:
        errors.append("At least one dimension is required")

    if not metrics:
        errors.append("At least one metric is required")

    # Dimension validation per report type
    if report_type == "BASIC":
        invalid_dims = set(dimensions) - BASIC_DIMENSIONS
        if invalid_dims:
            errors.append(
                f"Dimensions {', '.join(invalid_dims)} are not valid for BASIC reports. "
                f"Valid: {', '.join(sorted(BASIC_DIMENSIONS))}"
            )

    elif report_type == "AUDIENCE":
        # Check that secondary dimensions have their parent
        for dim in dimensions:
            required_parent = AUDIENCE_SECONDARY_DIMENSIONS.get(dim)
            if required_parent and required_parent not in dimensions:
                errors.append(
                    f"Dimension '{dim}' requires parent dimension '{required_parent}'"
                )

    elif report_type == "CATALOG":
        invalid_dims = set(dimensions) - CATALOG_DIMENSIONS
        # Allow ID-type dims too
        id_dims = {"campaign_id", "adgroup_id", "ad_id"}
        truly_invalid = invalid_dims - id_dims
        if truly_invalid:
            errors.append(
                f"Dimensions {', '.join(truly_invalid)} are not valid for CATALOG reports. "
                f"Valid: {', '.join(sorted(CATALOG_DIMENSIONS))}"
            )

    return errors
