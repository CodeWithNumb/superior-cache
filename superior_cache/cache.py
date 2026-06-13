"""
cache.py — SuperiorCache main orchestrator.

Coordinates:
  L1 (MemoryLayer) → L2 (RedisLayer, optional) → loader function
Plus: deduplication, stampede protection, tag/pattern/cascade invalidation,
distributed locks, namespaces, event hooks, and plugins.
"""
from __future__ import annotations

import asyncio
import fnmatch
import json
import uuid
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from .memory_layer import MemoryLayer
from .types import (
    CachePlugin,
    CacheStats,
    FetchOptions,
    LockHandle,
    SetOptions,
)


# ---------------------------------------------------------------------------
# Default configuration constants
# ---------------------------------------------------------------------------

_DEFAULT_TTL = 60.0          # seconds
_DEFAULT_GRACE = 30.0        # stampede grace period (seconds)
_DEFAULT_REFRESH_FRAC = 0.2  # refresh-ahead fraction


# ---------------------------------------------------------------------------
# SuperiorCache
# ---------------------------------------------------------------------------

class SuperiorCache:
    """
    Production-grade multi-layer async cache for discord.py bots.

    Usage::

        cache = SuperiorCache()              # memory-only (no Redis)
        # or
        cache = SuperiorCache(redis_url="redis://localhost:6379")
        await cache.connect()

        value = await cache.fetch("user:123", loader=fetch_from_db)
    """

    def __init__(
        self,
        *,
        # Global
        default_ttl: float = _DEFAULT_TTL,
        debug: bool = False,
        # Memory (L1)
        max_entries: int = 50_000,
        # Redis (L2) — pass None to disable
        redis_url: Optional[str] = None,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_password: Optional[str] = None,
        redis_db: int = 0,
        redis_key_prefix: str = "sc:",
        redis_default_ttl: float = 300.0,
        redis_pubsub: bool = True,
        # Stampede protection
        stampede_enabled: bool = True,
        stampede_grace_seconds: float = _DEFAULT_GRACE,
        refresh_ahead_fraction: float = _DEFAULT_REFRESH_FRAC,
    ) -> None:
        self._default_ttl = default_ttl
        self._debug = debug

        # L1
        self._memory = MemoryLayer(
            max_entries=max_entries,
            default_ttl=default_ttl,
        )

        # L2 (lazy; connect() must be called)
        self._redis_layer = None
        self._redis_config = dict(
            url=redis_url,
            host=redis_host,
            port=redis_port,
            password=redis_password,
            db=redis_db,
            key_prefix=redis_key_prefix,
            default_ttl=redis_default_ttl,
            enable_pubsub=redis_pubsub,
        ) if redis_url or redis_host else None

        # Stampede
        self._stampede_enabled = stampede_enabled
        self._grace = stampede_grace_seconds
        self._refresh_frac = refresh_ahead_fraction

        # Deduplication: key → asyncio.Future
        self._inflight: Dict[str, asyncio.Future] = {}

        # Cascade dependencies: parent_key → {child_keys}
        self._deps: Dict[str, Set[str]] = defaultdict(set)

        # Background refresh in-progress guard
        self._refresh_in_progress: Set[str] = set()

        # Stored loaders for preloading (key → coroutine factory)
        self._loaders: Dict[str, Callable] = {}

        # Locks held by this instance
        self._locks: Dict[str, LockHandle] = {}

        # Stats
        self._l1_hits = 0
        self._l2_hits = 0
        self._misses = 0
        self._loader_execs = 0

        # Event listeners: event_name → [callbacks]
        self._listeners: Dict[str, List[Callable]] = defaultdict(list)

        # Plugins
        self._plugins: List[CachePlugin] = []

        self._log(f"SuperiorCache initialised (default_ttl={default_ttl}s)")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Connect to Redis (if configured). Call before using the cache."""
        if self._redis_config:
            from .redis_layer import RedisLayer
            cfg = self._redis_config
            self._redis_layer = RedisLayer(
                url=cfg["url"],
                host=cfg["host"],
                port=cfg["port"],
                password=cfg["password"],
                db=cfg["db"],
                key_prefix=cfg["key_prefix"],
                default_ttl=cfg["default_ttl"],
                enable_pubsub=cfg["enable_pubsub"],
            )
            await self._redis_layer.connect()
            self._redis_layer.on_invalidation(self._handle_remote_invalidation)
            self._log("Redis layer connected")

    async def destroy(self) -> None:
        """Graceful shutdown: release locks, disconnect Redis, destroy plugins."""
        # Destroy plugins
        for p in self._plugins:
            await p.destroy()

        # Release all held locks
        for handle in list(self._locks.values()):
            await self.unlock(handle)

        if self._redis_layer:
            await self._redis_layer.disconnect()

        self._memory.clear()
        self._log("SuperiorCache destroyed")

    # ------------------------------------------------------------------
    # Core read / write
    # ------------------------------------------------------------------

    async def get(self, key: str) -> Optional[Any]:
        """
        Fetch from L1 then L2. Returns None on miss.
        Does NOT invoke any loader.
        """
        # L1
        value, found = self._memory.get(key)
        if found:
            self._l1_hits += 1
            self._emit("hit", {"key": key, "layer": "l1"})
            return value

        # L2
        if self._redis_layer and self._redis_layer.is_connected:
            value, found = await self._redis_layer.get(key)
            if found:
                self._memory.set(key, value, self._default_ttl)
                self._l2_hits += 1
                self._emit("hit", {"key": key, "layer": "l2"})
                return value

        self._misses += 1
        self._emit("miss", {"key": key})
        return None

    async def set(
        self,
        key: str,
        value: Any,
        options: Optional[SetOptions] = None,
        *,
        # convenience kwargs (alternative to passing SetOptions)
        ttl: Optional[float] = None,
        tags: Optional[List[str]] = None,
        local_only: bool = False,
    ) -> None:
        """Write value to L1 (and L2 unless local_only=True)."""
        if options:
            ttl = options.ttl if options.ttl is not None else ttl
            tags = options.tags if options.tags else tags
            local_only = options.local_only or local_only

        effective_ttl = ttl if ttl is not None else self._default_ttl
        effective_tags = tags or []

        self._memory.set(key, value, effective_ttl, effective_tags)

        if not local_only and self._redis_layer and self._redis_layer.is_connected:
            await self._redis_layer.set(key, value, effective_ttl)
            if effective_tags:
                await self._redis_layer.add_tags(key, effective_tags)

        self._emit("set", {"key": key, "ttl": effective_ttl, "tags": effective_tags})

    async def delete(self, key: str) -> bool:
        """Delete key from L1 + L2, cascade to dependents."""
        # Cascade
        dependents = list(self._deps.pop(key, set()))
        for dep in dependents:
            await self._delete_internal(dep, cascaded=True)

        return await self._delete_internal(key, cascaded=False)

    async def fetch(
        self,
        key: str,
        loader: Callable[[], Any],
        options: Optional[FetchOptions] = None,
        *,
        ttl: Optional[float] = None,
        tags: Optional[List[str]] = None,
        refresh_ahead: bool = False,
        force_refresh: bool = False,
    ) -> Any:
        """
        Get-or-load: check L1 → L2 → run loader.

        Includes:
          • request deduplication
          • stampede / stale-while-revalidate
          • optional refresh-ahead
        """
        # Merge options
        if options:
            ttl = options.ttl if options.ttl is not None else ttl
            tags = options.tags or tags
            refresh_ahead = options.refresh_ahead or refresh_ahead
            force_refresh = options.force_refresh or force_refresh

        self._loaders[key] = loader

        # Force refresh bypasses all caches
        if force_refresh:
            return await self._execute_and_cache(key, loader, ttl=ttl, tags=tags)

        # L1 check
        value, found = self._memory.get(key)
        if found:
            self._l1_hits += 1
            self._emit("hit", {"key": key, "layer": "l1"})
            if refresh_ahead and self._memory.needs_refresh_ahead(key, self._refresh_frac):
                self._background_refresh(key, loader, ttl=ttl, tags=tags)
            return value

        # Stampede: serve stale while refreshing
        if self._stampede_enabled:
            stale, found = self._memory.get_stale(key, self._grace)
            if found:
                self._log(f"Stampede: serving stale for '{key}'")
                self._background_refresh(key, loader, ttl=ttl, tags=tags)
                return stale

        # L2 check
        if self._redis_layer and self._redis_layer.is_connected:
            value, found = await self._redis_layer.get(key)
            if found:
                self._memory.set(key, value, ttl or self._default_ttl, tags)
                self._l2_hits += 1
                self._emit("hit", {"key": key, "layer": "l2"})
                return value

        # Miss → load (with deduplication)
        self._misses += 1
        self._emit("miss", {"key": key})
        return await self._execute_and_cache(key, loader, ttl=ttl, tags=tags)

    async def clear(self) -> None:
        """Wipe all L1 entries."""
        self._memory.clear()
        self._inflight.clear()
        self._deps.clear()
        self._loaders.clear()
        self._refresh_in_progress.clear()
        self._log("Cache cleared")

    # ------------------------------------------------------------------
    # Batch operations
    # ------------------------------------------------------------------

    async def mget(self, keys: List[str]) -> Dict[str, Any]:
        """Fetch multiple keys. Missing keys are omitted from result."""
        results: Dict[str, Any] = {}
        l2_keys: List[str] = []

        for key in keys:
            val, found = self._memory.get(key)
            if found:
                results[key] = val
                self._l1_hits += 1
            else:
                l2_keys.append(key)

        if l2_keys and self._redis_layer and self._redis_layer.is_connected:
            redis_results = await self._redis_layer.mget(l2_keys)
            for k, v in redis_results.items():
                results[k] = v
                self._memory.set(k, v, self._default_ttl)
                self._l2_hits += 1

        self._misses += len(keys) - len(results)
        return results

    async def mset(self, entries: List[Dict]) -> None:
        """
        Set multiple keys. Each entry: {"key": ..., "value": ..., "options": SetOptions (opt)}
        """
        for entry in entries:
            await self.set(
                entry["key"],
                entry["value"],
                entry.get("options"),
            )

    async def mdelete(self, keys: List[str]) -> int:
        """Delete multiple keys. Returns count deleted."""
        count = 0
        for key in keys:
            if await self.delete(key):
                count += 1
        return count

    # ------------------------------------------------------------------
    # Advanced invalidation
    # ------------------------------------------------------------------

    async def invalidate_tag(self, tag: str) -> int:
        """Invalidate all entries bearing `tag`. Returns count removed."""
        l1_count = self._memory.invalidate_tag(tag)

        if self._redis_layer and self._redis_layer.is_connected:
            keys = await self._redis_layer.get_tag_members(tag)
            if keys:
                await self._redis_layer.mdelete(keys)
            await self._redis_layer.delete_tag(tag)
            await self._redis_layer.publish_invalidation(
                json.dumps({"type": "tag", "tag": tag})
            )

        self._emit("tag_invalidation", {"tag": tag, "keys_affected": l1_count})
        return l1_count

    async def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate all keys matching glob pattern."""
        l1_count = self._memory.invalidate_pattern(pattern)

        if self._redis_layer and self._redis_layer.is_connected:
            keys = await self._redis_layer.scan_pattern(pattern)
            if keys:
                await self._redis_layer.mdelete(keys)
            await self._redis_layer.publish_invalidation(
                json.dumps({"type": "pattern", "pattern": pattern})
            )

        return l1_count

    # ------------------------------------------------------------------
    # Cascade dependencies
    # ------------------------------------------------------------------

    def depends(self, parent_key: str, child_keys: List[str]) -> None:
        """When parent_key is deleted, child_keys are auto-deleted too."""
        self._deps[parent_key].update(child_keys)

    # ------------------------------------------------------------------
    # Distributed locks
    # ------------------------------------------------------------------

    async def lock(
        self,
        key: str,
        ttl_seconds: float = 30.0,
        max_retries: int = 50,
        retry_delay: float = 0.1,
    ) -> Optional[LockHandle]:
        """
        Acquire a distributed Redis lock.
        Returns LockHandle on success, None on failure.
        """
        if not (self._redis_layer and self._redis_layer.is_connected):
            self._log("Lock requested but Redis unavailable — returning None")
            return None

        token = str(uuid.uuid4())
        ttl_ms = int(ttl_seconds * 1000)

        for attempt in range(max_retries + 1):
            acquired = await self._redis_layer.acquire_lock(key, token, ttl_ms)
            if acquired:
                handle = LockHandle(key=key, value=token)
                self._locks[key] = handle
                self._log(f"Lock acquired: '{key}' (attempt {attempt + 1})")
                return handle
            if attempt < max_retries:
                await asyncio.sleep(retry_delay)

        self._log(f"Failed to acquire lock '{key}' after {max_retries + 1} attempts")
        return None

    async def unlock(self, handle: LockHandle) -> bool:
        """Release a lock acquired with lock()."""
        if not (self._redis_layer and self._redis_layer.is_connected):
            return False
        released = await self._redis_layer.release_lock(handle.key, handle.value)
        if released:
            self._locks.pop(handle.key, None)
            self._log(f"Lock released: '{handle.key}'")
        return released

    # ------------------------------------------------------------------
    # Namespaces
    # ------------------------------------------------------------------

    def namespace(self, name: str) -> "CacheNamespace":
        """Return a namespaced view: all keys auto-prefixed with `name:`."""
        from .namespace import CacheNamespace
        return CacheNamespace(self, name)

    # ------------------------------------------------------------------
    # Event system
    # ------------------------------------------------------------------

    def on(self, event: str, callback: Callable) -> None:
        """Register a callback for a cache event."""
        self._listeners[event].append(callback)

    def off(self, event: str, callback: Callable) -> None:
        """Remove a registered callback."""
        try:
            self._listeners[event].remove(callback)
        except ValueError:
            pass

    # ------------------------------------------------------------------
    # Plugin system
    # ------------------------------------------------------------------

    async def use(self, plugin: CachePlugin) -> None:
        """Install a plugin."""
        self._log(f"Installing plugin: {plugin.name}")
        plugin.install(self)
        self._plugins.append(plugin)

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    async def stats(self) -> CacheStats:
        return CacheStats(
            l1_entries=self._memory.size,
            l1_hits=self._l1_hits,
            l2_hits=self._l2_hits,
            misses=self._misses,
            loader_executions=self._loader_execs,
            redis_connected=(
                self._redis_layer.is_connected
                if self._redis_layer else False
            ),
            active_locks=len(self._locks),
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _delete_internal(self, key: str, cascaded: bool) -> bool:
        mem_deleted = self._memory.delete(key)
        redis_deleted = False

        if self._redis_layer and self._redis_layer.is_connected:
            redis_deleted = await self._redis_layer.delete(key)
            await self._redis_layer.publish_invalidation(
                json.dumps({"type": "delete", "key": key})
            )

        existed = mem_deleted or redis_deleted
        if existed:
            self._emit("delete", {"key": key, "cascaded": cascaded})
        return existed

    async def _execute_and_cache(
        self,
        key: str,
        loader: Callable,
        *,
        ttl: Optional[float] = None,
        tags: Optional[List[str]] = None,
    ) -> Any:
        """Run loader with deduplication, then cache result."""
        # Check if there's already an inflight future for this key
        if key in self._inflight:
            self._log(f"Deduplicating loader for '{key}'")
            value = await self._inflight[key]
            # Still put in L1 for this waiter
            self._memory.set(key, value, ttl or self._default_ttl, tags)
            return value

        loop = asyncio.get_event_loop()
        future: asyncio.Future = loop.create_future()
        self._inflight[key] = future

        try:
            # Support both async and sync loaders
            if asyncio.iscoroutinefunction(loader):
                value = await loader()
            else:
                value = loader()

            self._loader_execs += 1
            self._emit("loader_execution", {"key": key})

            future.set_result(value)
            await self.set(key, value, ttl=ttl, tags=tags)
            return value

        except Exception as exc:
            future.set_exception(exc)
            self._emit("error", {"key": key, "error": exc})
            raise
        finally:
            self._inflight.pop(key, None)

    def _background_refresh(
        self,
        key: str,
        loader: Callable,
        *,
        ttl: Optional[float] = None,
        tags: Optional[List[str]] = None,
    ) -> None:
        """Fire-and-forget background refresh."""
        if key in self._refresh_in_progress:
            return
        self._refresh_in_progress.add(key)

        async def _refresh():
            try:
                if asyncio.iscoroutinefunction(loader):
                    value = await loader()
                else:
                    value = loader()
                await self.set(key, value, ttl=ttl, tags=tags)
                self._log(f"Background refresh done: '{key}'")
            except Exception as exc:
                self._emit("error", {"key": key, "error": exc, "source": "background_refresh"})
                self._log(f"Background refresh failed: '{key}': {exc}")
            finally:
                self._refresh_in_progress.discard(key)

        asyncio.create_task(_refresh())

    def _handle_remote_invalidation(self, message: str) -> None:
        """Handle Pub/Sub invalidation from another Redis subscriber."""
        try:
            data = json.loads(message)
            itype = data.get("type")
            if itype == "delete":
                self._memory.delete(data["key"])
            elif itype == "tag":
                self._memory.invalidate_tag(data["tag"])
            elif itype == "pattern":
                self._memory.invalidate_pattern(data["pattern"])
        except Exception as exc:
            self._log(f"Failed to parse remote invalidation: {exc}")

    def _emit(self, event: str, payload: Dict) -> None:
        for cb in self._listeners.get(event, []):
            try:
                result = cb(payload)
                if asyncio.iscoroutine(result):
                    asyncio.create_task(result)
            except Exception:
                pass

    def _log(self, msg: str) -> None:
        if self._debug:
            print(f"[SuperiorCache] {msg}")


# Avoid circular import
from .namespace import CacheNamespace  # noqa: E402
