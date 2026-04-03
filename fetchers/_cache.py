"""
Simple disk-backed cache using diskcache.
Falls back to an in-memory dict if diskcache is unavailable.
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

_cache = None


def _get_cache():
    global _cache
    if _cache is not None:
        return _cache

    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from config import CACHE_DIR

    try:
        import diskcache
        os.makedirs(CACHE_DIR, exist_ok=True)
        _cache = diskcache.Cache(CACHE_DIR)
        logger.debug("Disk cache initialized at %s", CACHE_DIR)
    except ImportError:
        logger.warning("diskcache not available, using in-memory cache")
        _cache = {}

    return _cache


def cache_get(key: str) -> Any | None:
    c = _get_cache()
    try:
        if hasattr(c, "get"):
            val = c.get(key)
            return val  # diskcache returns None on miss
        return c.get(key)
    except Exception as exc:
        logger.debug("Cache get error for %s: %s", key, exc)
        return None


def cache_set(key: str, value: Any, ttl: int = 3600) -> None:
    c = _get_cache()
    try:
        if hasattr(c, "set"):
            c.set(key, value, expire=ttl)
        else:
            c[key] = value  # in-memory, no TTL
    except Exception as exc:
        logger.debug("Cache set error for %s: %s", key, exc)


def cache_delete(key: str) -> None:
    c = _get_cache()
    try:
        if hasattr(c, "delete"):
            c.delete(key)
        elif key in c:
            del c[key]
    except Exception as exc:
        logger.debug("Cache delete error for %s: %s", key, exc)
