"""Tool 6: tiktok_comment — Comment management for TikTok Ads.

Supported actions:
  list_comments   — List comments on an ad
  reply_comment   — Reply to a comment (requires confirm=true)
  hide_comment    — Hide/unhide comments (requires confirm=true)
"""

import logging
from typing import Any, Dict, List

from ..api.marketing_client import MarketingClient
from ..utils.confirmation import build_preview

logger = logging.getLogger(__name__)


class CommentTool:
    """Comment management tool for TikTok Ads."""

    def __init__(self, client: MarketingClient):
        self.client = client

    async def execute(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Route to the appropriate handler based on action."""
        action = arguments.get("action", "")
        if not action:
            return _error("action is required")

        handler = self._handlers.get(action)
        if not handler:
            valid = ", ".join(sorted(self._handlers.keys()))
            return _error(f"Unknown action '{action}'. Valid: {valid}")

        try:
            return await handler(self, arguments)
        except Exception as e:
            logger.error(f"comment({action}) failed: {e}")
            return _error(str(e))

    # ── Handlers ────────────────────────────────────────────────────────

    async def _list_comments(self, args: Dict[str, Any]) -> Dict[str, Any]:
        search_field = args.get("search_field")
        search_value = args.get("search_value")
        start_time = args.get("start_time")
        end_time = args.get("end_time")

        if not search_field or not search_value:
            return _error("search_field (ADGROUP_ID or AD_ID) and search_value are required")
        if not start_time or not end_time:
            return _error("start_time and end_time (YYYY-MM-DD) are required")

        page = args.get("page", 1)
        page_size = args.get("page_size", 50)
        status = args.get("status")
        result = await self.client.list_comments(
            search_field=search_field, search_value=search_value,
            start_time=start_time, end_time=end_time,
            page=page, page_size=page_size, status=status,
        )
        items = result.get("data", {}).get("comments", [])
        page_info = result.get("data", {}).get("page_info", {})
        return _success(items, page_info)

    async def _reply_comment(self, args: Dict[str, Any]) -> Dict[str, Any]:
        ad_id = args.get("ad_id")
        comment_id = args.get("comment_id")
        text = args.get("text")
        confirm = args.get("confirm", False)

        if not ad_id or not comment_id or not text:
            return _error("ad_id, comment_id, and text are required")

        if not confirm:
            return build_preview("reply", "comment", {
                "ad_id": ad_id,
                "comment_id": comment_id,
                "text": text,
            })

        result = await self.client.reply_comment(ad_id, comment_id, text)
        return _success(result.get("data", {}))

    async def _hide_comment(self, args: Dict[str, Any]) -> Dict[str, Any]:
        ad_id = args.get("ad_id")
        comment_ids = args.get("comment_ids", [])
        hidden = args.get("hidden", True)
        confirm = args.get("confirm", False)

        if not ad_id or not comment_ids:
            return _error("ad_id and comment_ids are required")

        if not confirm:
            action_word = "hide" if hidden else "unhide"
            return build_preview(action_word, "comments", {
                "ad_id": ad_id,
                "comment_ids": comment_ids,
                "count": len(comment_ids),
            }, warnings=[f"This will {action_word} {len(comment_ids)} comment(s)"])

        result = await self.client.hide_comments(ad_id, comment_ids, hidden)
        return _success(result.get("data", {}))

    # ── Handler dispatch table ──────────────────────────────────────────

    _handlers = {
        "list_comments": _list_comments,
        "reply_comment": _reply_comment,
        "hide_comment": _hide_comment,
    }


def _success(data: Any, page_info: Any = None) -> Dict[str, Any]:
    result: Dict[str, Any] = {"success": True, "data": data}
    if page_info:
        result["metadata"] = {
            "total_count": page_info.get("total_number", 0),
            "page": page_info.get("page", 1),
            "page_size": page_info.get("page_size", 0),
        }
    return result


def _error(message: str) -> Dict[str, Any]:
    return {"success": False, "error_message": message}
