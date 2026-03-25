"""Base HTTP client with rate limiting, retries, and pagination for TikTok APIs."""

import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Optional

import httpx
from six import string_types

from ..config import (
    DEFAULT_CONCURRENT_REQUESTS,
    DEFAULT_PAGE_SIZE,
    DEFAULT_RATE_LIMIT_PER_HOUR,
    ERROR_CODES,
    MARKETING_API_BASE_URL,
    get_api_base_url,
)

logger = logging.getLogger(__name__)


class RateLimiter:
    """Token bucket rate limiter."""

    def __init__(self, max_per_hour: int = DEFAULT_RATE_LIMIT_PER_HOUR, max_concurrent: int = DEFAULT_CONCURRENT_REQUESTS):
        self.max_per_hour = max_per_hour
        self.max_concurrent = max_concurrent
        self.tokens = max_per_hour
        self.last_refill = time.monotonic()
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._lock = asyncio.Lock()

    async def acquire(self):
        """Acquire a rate limit token. Blocks if limit reached."""
        await self._semaphore.acquire()
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_refill
            # Refill tokens based on elapsed time
            refill = elapsed * (self.max_per_hour / 3600.0)
            self.tokens = min(self.max_per_hour, self.tokens + refill)
            self.last_refill = now

            if self.tokens < 1:
                wait_time = (1 - self.tokens) / (self.max_per_hour / 3600.0)
                logger.warning(f"Rate limit reached, waiting {wait_time:.1f}s")
                await asyncio.sleep(wait_time)
                self.tokens = 0
            else:
                self.tokens -= 1

    def release(self):
        """Release the concurrency semaphore."""
        self._semaphore.release()


class TikTokAPIError(Exception):
    """Structured error from TikTok API."""

    def __init__(self, code: int, message: str, request_id: str = ""):
        self.code = code
        self.message = message
        self.request_id = request_id
        suggestion = ERROR_CODES.get(code, "")
        self.suggestion = suggestion
        super().__init__(f"TikTok API Error {code}: {message}" + (f" ({suggestion})" if suggestion else ""))


class BaseAPIClient:
    """Base HTTP client for TikTok APIs with rate limiting, retries, and error handling."""

    def __init__(
        self,
        access_token: str,
        advertiser_id: str,
        base_url: str | None = None,
        rate_limit_per_hour: int = DEFAULT_RATE_LIMIT_PER_HOUR,
        max_concurrent: int = DEFAULT_CONCURRENT_REQUESTS,
    ):
        self.access_token = access_token
        self.advertiser_id = advertiser_id
        self.base_url = base_url or get_api_base_url()
        self.client = httpx.AsyncClient(timeout=30.0)
        self.rate_limiter = RateLimiter(rate_limit_per_hour, max_concurrent)

    async def request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        files: Optional[Dict[str, Any]] = None,
        include_advertiser_id: bool = True,
        max_retries: int = 3,
    ) -> Dict[str, Any]:
        """Make an authenticated API request with rate limiting and retries.

        Args:
            method: HTTP method (GET or POST)
            endpoint: API endpoint path (e.g., "campaign/get/")
            params: Query parameters
            data: POST body data
            files: Files for multipart upload
            include_advertiser_id: Whether to include advertiser_id in params
            max_retries: Max retry attempts for retryable errors

        Returns:
            Parsed API response dict
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        headers = {"Access-Token": self.access_token}

        # Build common params
        request_params = {}
        if include_advertiser_id:
            request_params["advertiser_id"] = self.advertiser_id
        if params:
            request_params.update(params)

        # Serialize non-string values for query string
        serialized_params = {
            k: v if isinstance(v, string_types) else json.dumps(v)
            for k, v in request_params.items()
        }

        last_error = None
        for attempt in range(max_retries):
            await self.rate_limiter.acquire()
            start_time = time.monotonic()
            try:
                if method.upper() == "GET":
                    response = await self.client.get(url, params=serialized_params, headers=headers)
                elif method.upper() == "POST":
                    if files:
                        response = await self.client.post(
                            url, params=serialized_params, files=files,
                            data=data or {}, headers=headers,
                        )
                    else:
                        response = await self.client.post(
                            url, params=serialized_params, json=data or {},
                            headers=headers,
                        )
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")

                duration_ms = (time.monotonic() - start_time) * 1000
                logger.debug(f"{method} {endpoint} -> {response.status_code} ({duration_ms:.0f}ms)")

                response.raise_for_status()
                result = response.json()

                # Check TikTok API-level errors
                code = result.get("code", 0)
                if code != 0:
                    request_id = result.get("request_id", "")
                    message = result.get("message", "Unknown error")
                    error = TikTokAPIError(code, message, request_id)

                    # Retryable errors: rate limit (50002) and server error (50000)
                    if code in (50000, 50002) and attempt < max_retries - 1:
                        wait = 2 ** (attempt + 1)
                        logger.warning(f"Retryable error {code}, waiting {wait}s (attempt {attempt + 1}/{max_retries})")
                        last_error = error
                        continue
                    raise error

                return result

            except httpx.HTTPStatusError as e:
                duration_ms = (time.monotonic() - start_time) * 1000
                logger.error(f"{method} {endpoint} -> HTTP {e.response.status_code} ({duration_ms:.0f}ms)")

                # Retry on 429 or 5xx
                if e.response.status_code in (429, 500, 502, 503) and attempt < max_retries - 1:
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"HTTP {e.response.status_code}, retrying in {wait}s")
                    last_error = e
                    await asyncio.sleep(wait)
                    continue
                raise TikTokAPIError(
                    e.response.status_code,
                    f"HTTP error: {e.response.text[:500]}",
                )

            except httpx.RequestError as e:
                logger.error(f"{method} {endpoint} -> Network error: {e}")
                if attempt < max_retries - 1:
                    wait = 2 ** (attempt + 1)
                    last_error = e
                    await asyncio.sleep(wait)
                    continue
                raise TikTokAPIError(0, f"Network error: {e}")

            finally:
                self.rate_limiter.release()

        # Should not reach here, but just in case
        raise last_error or TikTokAPIError(0, "Request failed after all retries")

    async def request_all_pages(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        page_size: int = DEFAULT_PAGE_SIZE,
        max_pages: int = 50,
        **kwargs,
    ) -> Dict[str, Any]:
        """Auto-paginate through all results.

        Returns a combined result with all items in data.list and accurate page_info.
        """
        all_items = []
        page = 1
        total_count = None

        while page <= max_pages:
            page_params = dict(params or {})
            page_params["page"] = page
            page_params["page_size"] = page_size

            result = await self.request(method, endpoint, params=page_params, **kwargs)

            items = result.get("data", {}).get("list", [])
            all_items.extend(items)

            page_info = result.get("data", {}).get("page_info", {})
            total_count = page_info.get("total_number", len(all_items))

            # Stop if we've fetched everything
            if len(all_items) >= total_count or len(items) < page_size:
                break

            page += 1

        # Build combined result
        return {
            "code": 0,
            "message": "OK",
            "data": {
                "list": all_items,
                "page_info": {
                    "total_number": total_count or len(all_items),
                    "page": 1,
                    "page_size": len(all_items),
                    "total_page": 1,
                },
            },
        }

    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()
