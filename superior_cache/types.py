"""
types.py — shared dataclasses & TypedDicts for superior_cache.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Dict, List, Optional, Set


# ---------------------------------------------------------------------------
# Per-call options
# ---------------------------------------------------------------------------

@dataclass
class SetOptions:
    """Options for cache.set() and cache.fetch()."""
    ttl: Optional[float] = None          # seconds; overrides global default
    tags: List[str] = field(default_factory=list)
    local_only: bool = False             # skip Redis write


@dataclass
class FetchOptions(SetOptions):
    """Options for cache.fetch() (superset of SetOptions)."""
    refresh_ahead: bool = False          # background refresh before expiry
    force_refresh: bool = False          # bypass cache and re-execute loader


# ---------------------------------------------------------------------------
# Lock handle
# ---------------------------------------------------------------------------

@dataclass
class LockHandle:
    key: str
    value: str              # unique owner token
    acquired_at: float = field(default_factory=time.monotonic)


# ---------------------------------------------------------------------------
# Internal memory entry
# ---------------------------------------------------------------------------

@dataclass
class MemoryEntry:
    value: Any
    expires_at: float       # monotonic timestamp; float('inf') = never
    original_ttl: float
    tags: Set[str] = field(default_factory=set)
    last_accessed: float = field(default_factory=time.monotonic)
    access_count: int = 0


# ---------------------------------------------------------------------------
# Stats snapshot
# ---------------------------------------------------------------------------

@dataclass
class CacheStats:
    l1_entries: int = 0
    l1_hits: int = 0
    l2_hits: int = 0
    misses: int = 0
    loader_executions: int = 0
    redis_connected: bool = False
    active_locks: int = 0

    @property
    def hit_rate(self) -> float:
        total = self.l1_hits + self.l2_hits + self.misses
        return (self.l1_hits + self.l2_hits) / total if total else 0.0


# ---------------------------------------------------------------------------
# Plugin protocol
# ---------------------------------------------------------------------------

class CachePlugin:
    """Base class for cache plugins. Override the hooks you need."""
    name: str = "unnamed-plugin"

    def install(self, cache: Any) -> None:   # cache: SuperiorCache
        pass

    async def destroy(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Event callbacks type alias
# ---------------------------------------------------------------------------

EventCallback = Callable[..., Any]
