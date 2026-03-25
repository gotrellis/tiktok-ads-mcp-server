"""In-memory cache for slow-changing TikTok API data."""

import logging
import time
from typing import Any, Optional

from ..config import (
    CACHE_TTL_ACCOUNT_INFO,
    CACHE_TTL_AUDIENCES,
    CACHE_TTL_INTEREST_CATEGORIES,
    CACHE_TTL_LOCATIONS,
    CACHE_TTL_PIXELS,
)

logger = logging.getLogger(__name__)


class CacheEntry:
    """Single cached value with TTL."""

    __slots__ = ("value", "expires_at")

    def __init__(self, value: Any, ttl_seconds: int):
        self.value = value
        self.expires_at = time.monotonic() + ttl_seconds

    @property
    def is_expired(self) -> bool:
        return time.monotonic() >= self.expires_at


class CacheManager:
    """Simple in-memory cache with per-key TTL.

    Caches slow-changing data: interest categories, locations, account info,
    audiences, pixels. Does NOT cache performance/report data.
    """

    # Predefined cache keys and their TTLs
    TTL_MAP = {
        "interest_categories": CACHE_TTL_INTEREST_CATEGORIES,
        "locations": CACHE_TTL_LOCATIONS,
        "account_info": CACHE_TTL_ACCOUNT_INFO,
        "audiences": CACHE_TTL_AUDIENCES,
        "pixels": CACHE_TTL_PIXELS,
    }

    def __init__(self):
        self._store: dict[str, CacheEntry] = {}

    def get(self, key: str) -> Optional[Any]:
        """Get a cached value. Returns None if not found or expired."""
        entry = self._store.get(key)
        if entry is None:
            return None
        if entry.is_expired:
            del self._store[key]
            logger.debug(f"Cache expired: {key}")
            return None
        logger.debug(f"Cache hit: {key}")
        return entry.value

    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None):
        """Set a cached value. Uses predefined TTL if key is known."""
        if ttl_seconds is None:
            ttl_seconds = self.TTL_MAP.get(key, 3600)
        self._store[key] = CacheEntry(value, ttl_seconds)
        logger.debug(f"Cache set: {key} (TTL={ttl_seconds}s)")

    def invalidate(self, key: str):
        """Remove a specific cache entry."""
        self._store.pop(key, None)

    def clear(self):
        """Clear all cached data."""
        self._store.clear()
        logger.debug("Cache cleared")
