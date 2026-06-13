"""
memory_layer.py — in-process LRU cache (L1).

Supports:
  • TTL expiry
  • LRU eviction (max_entries)
  • Tag-based & glob-pattern invalidation
  • Stale-while-revalidate helpers
"""
from __future__ import annotations

import fnmatch
import time
from collections import OrderedDict
from typing import Any, List, Optional, Set, Tuple

from .types import MemoryEntry


class MemoryLayer:
    def __init__(
        self,
        max_entries: int = 50_000,
        default_ttl: float = 60.0,
    ) -> None:
        self._store: OrderedDict[str, MemoryEntry] = OrderedDict()
        self._max_entries = max_entries
        self._default_ttl = default_ttl

    # ------------------------------------------------------------------
    # Core get / set / delete
    # ------------------------------------------------------------------

    def get(self, key: str) -> Tuple[Any, bool]:
        """Return (value, found). Promotes key to MRU on hit."""
        entry = self._store.get(key)
        if entry is None:
            return None, False
        if self._is_expired(entry):
            self._evict(key)
            return None, False
        # LRU promotion
        self._store.move_to_end(key)
        entry.last_accessed = time.monotonic()
        entry.access_count += 1
        return entry.value, True

    def get_stale(self, key: str, grace_seconds: float) -> Tuple[Any, bool]:
        """Return stale value if within grace period after expiry."""
        entry = self._store.get(key)
        if entry is None:
            return None, False
        stale_deadline = entry.expires_at + grace_seconds
        if time.monotonic() <= stale_deadline:
            return entry.value, True
        return None, False

    def needs_refresh_ahead(self, key: str, fraction: float) -> bool:
        """True if less than `fraction` of original TTL remains."""
        entry = self._store.get(key)
        if entry is None or entry.original_ttl == 0:
            return False
        remaining = entry.expires_at - time.monotonic()
        threshold = entry.original_ttl * fraction
        return remaining < threshold

    def has(self, key: str) -> bool:
        entry = self._store.get(key)
        if entry is None:
            return False
        if self._is_expired(entry):
            self._evict(key)
            return False
        return True

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None,
        tags: Optional[List[str]] = None,
    ) -> None:
        ttl = ttl if ttl is not None else self._default_ttl
        expires_at = (
            float("inf") if ttl == 0 or ttl is None
            else time.monotonic() + ttl
        )
        if key in self._store:
            del self._store[key]

        entry = MemoryEntry(
            value=value,
            expires_at=expires_at,
            original_ttl=ttl,
            tags=set(tags or []),
        )
        self._ensure_capacity()
        self._store[key] = entry

    def delete(self, key: str) -> bool:
        if key in self._store:
            del self._store[key]
            return True
        return False

    def clear(self) -> None:
        self._store.clear()

    # ------------------------------------------------------------------
    # Bulk invalidation
    # ------------------------------------------------------------------

    def invalidate_tag(self, tag: str) -> int:
        """Delete all entries carrying `tag`. Returns count removed."""
        to_remove = [
            k for k, e in self._store.items() if tag in e.tags
        ]
        for k in to_remove:
            del self._store[k]
        return len(to_remove)

    def invalidate_pattern(self, pattern: str) -> int:
        """Delete all keys matching a glob pattern. Returns count."""
        to_remove = [k for k in self._store if fnmatch.fnmatch(k, pattern)]
        for k in to_remove:
            del self._store[k]
        return len(to_remove)

    def keys_with_tag(self, tag: str) -> List[str]:
        return [k for k, e in self._store.items() if tag in e.tags]

    # ------------------------------------------------------------------
    # Stats helpers
    # ------------------------------------------------------------------

    @property
    def size(self) -> int:
        return len(self._store)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_expired(entry: MemoryEntry) -> bool:
        return entry.expires_at != float("inf") and time.monotonic() > entry.expires_at

    def _evict(self, key: str) -> None:
        self._store.pop(key, None)

    def _ensure_capacity(self) -> None:
        while len(self._store) >= self._max_entries:
            # Evict LRU (first item)
            self._store.popitem(last=False)
