"""Shared fixtures.

Every test runs against a recording stub in place of ``BaseAPIClient.request``:
no HTTP, no credentials, and each test can assert on the exact calls its code
under test made.
"""

from typing import Any, Dict, List, Optional

import pytest

from tiktok_ads_mcp.api.marketing_client import MarketingClient
from tiktok_ads_mcp.cache.cache_manager import CacheManager


class RecordingClient(MarketingClient):
    """MarketingClient with the HTTP layer replaced by a scripted responder."""

    def __init__(self, responses: Optional[List[Any]] = None):
        super().__init__(access_token="test-token", advertiser_id="7000000000000000001")
        self.calls: List[Dict[str, Any]] = []
        # Each entry is either a response dict or an Exception to raise.
        self.responses = list(responses or [])
        self.default_response: Dict[str, Any] = {"code": 0, "data": {}}

    async def request(  # type: ignore[override]
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        files: Optional[Dict[str, Any]] = None,
        include_advertiser_id: bool = True,
        max_retries: int = 3,
    ) -> Dict[str, Any]:
        self.calls.append(
            {"method": method, "endpoint": endpoint, "params": params, "data": data}
        )
        if not self.responses:
            return self.default_response
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def endpoints(self) -> List[str]:
        return [call["endpoint"] for call in self.calls]


@pytest.fixture
def client():
    return RecordingClient()


@pytest.fixture
def cache():
    return CacheManager()


def report_row(dimensions: Dict[str, Any], **metrics: Any) -> Dict[str, Any]:
    """Build a row shaped like a report/integrated/get/ result."""
    return {"dimensions": dimensions, "metrics": {k: str(v) for k, v in metrics.items()}}


def report_response(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {"code": 0, "data": {"list": rows, "page_info": {"total_page": 1, "page": 1}}}
