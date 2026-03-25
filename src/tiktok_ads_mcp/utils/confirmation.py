"""Write operation confirmation helper for TikTok Ads MCP server."""

from typing import Any, Dict


def build_preview(
    action: str,
    entity_type: str,
    params: Dict[str, Any],
    warnings: list[str] | None = None,
) -> Dict[str, Any]:
    """Build a preview response for a write operation that requires confirmation.

    Args:
        action: The action being performed (create, update, enable, disable, delete)
        entity_type: The entity type (campaign, adgroup, ad, audience, etc.)
        params: Key parameters for the operation
        warnings: Optional list of warning messages

    Returns:
        Preview response dict with confirm=False instruction
    """
    preview = {
        "success": True,
        "executed": False,
        "data": {
            "preview": True,
            "action": action,
            "entity_type": entity_type,
            "parameters": params,
            "message": (
                f"This will {action} a {entity_type}. "
                f"Pass confirm=true to execute this operation."
            ),
        },
    }

    if warnings:
        preview["data"]["warnings"] = warnings

    return preview
