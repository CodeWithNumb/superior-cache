"""
redis_layer.py — Redis-backed cache layer (L2).

Uses `redis.asyncio` (bundled with the redis-py package).
Pass redis=False to SuperiorCache to skip this layer entirely.

Supports:
  • get / set / delete / mget / mdelete
  • Tag storage (SADD / SMEMBERS / DEL)
  • SCAN-based pattern invalidation
  • Pub/Sub distributed invalidation broadcast
  • Atomic lock acquire (SET NX PX) + Lua release
"""
from __future__ import annotations

import json
import asyncio
import fnmatch
from typing import Any, Callable, Dict, List, Optional, Tuple

try:
    import redis.asyncio as aioredis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


_RELEASE_LOCK_LUA = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""


class RedisLayer:
    def __init__(
        self,
        url: Optional[str] = None,
        host: str = "localhost",
        port: int = 6379,
        password: Optional[str] = None,
        db: int = 0,
        key_prefix: str = "sc:",
        default_ttl: float = 300.0,
        enable_pubsub: bool = True,
        pubsub_channel: str = "superior-cache:invalidation",
    ) -> None:
        if not REDIS_AVAILABLE:
            raise ImportError(
                "redis package not installed. Run: pip install redis"
            )
        self._url = url
        self._host = host
        self._port = port
        self._password = password
        self._db = db
        self._prefix = key_prefix
        self._default_ttl = default_ttl
        self._enable_pubsub = enable_pubsub
        self._channel = pubsub_channel

        self._client: Optional[aioredis.Redis] = None
        self._pubsub_client: Optional[aioredis.Redis] = None
        self._pubsub: Optional[Any] = None
        self._listener_task: Optional[asyncio.Task] = None
        self._invalidation_callbacks: List[Callable[[str], None]] = []
        self._connected = False

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        if self._url:
            self._client = aioredis.from_url(
                self._url, decode_responses=False
            )
        else:
            self._client = aioredis.Redis(
                host=self._host,
                port=self._port,
                password=self._password,
                db=self._db,
                decode_responses=False,
            )
        await self._client.ping()
        self._connected = True

        if self._enable_pubsub:
            await self._start_pubsub()

    async def disconnect(self) -> None:
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        if self._pubsub:
            await self._pubsub.unsubscribe(self._channel)
            await self._pubsub.close()
        if self._client:
            await self._client.aclose()
        self._connected = False

    @property
    def is_connected(self) -> bool:
        return self._connected

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def _k(self, key: str) -> str:
        return f"{self._prefix}{key}"

    async def get(self, key: str) -> Tuple[Any, bool]:
        """Return (value, found)."""
        raw = await self._client.get(self._k(key))
        if raw is None:
            return None, False
        return json.loads(raw), True

    async def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        ttl_ms = int((ttl if ttl is not None else self._default_ttl) * 1000)
        raw = json.dumps(value, default=str)
        await self._client.set(self._k(key), raw, px=ttl_ms)

    async def delete(self, key: str) -> bool:
        result = await self._client.delete(self._k(key))
        return bool(result)

    async def mget(self, keys: List[str]) -> Dict[str, Any]:
        if not keys:
            return {}
        prefixed = [self._k(k) for k in keys]
        raws = await self._client.mget(*prefixed)
        results: Dict[str, Any] = {}
        for k, raw in zip(keys, raws):
            if raw is not None:
                results[k] = json.loads(raw)
        return results

    async def mdelete(self, keys: List[str]) -> int:
        if not keys:
            return 0
        prefixed = [self._k(k) for k in keys]
        return await self._client.delete(*prefixed)

    # ------------------------------------------------------------------
    # Tag operations
    # ------------------------------------------------------------------

    def _tag_key(self, tag: str) -> str:
        return f"{self._prefix}__tag__:{tag}"

    async def add_tags(self, key: str, tags: List[str]) -> None:
        pipe = self._client.pipeline()
        for tag in tags:
            pipe.sadd(self._tag_key(tag), key)
        await pipe.execute()

    async def get_tag_members(self, tag: str) -> List[str]:
        members = await self._client.smembers(self._tag_key(tag))
        return [m.decode() if isinstance(m, bytes) else m for m in members]

    async def delete_tag(self, tag: str) -> None:
        await self._client.delete(self._tag_key(tag))

    # ------------------------------------------------------------------
    # Pattern scan (non-blocking SCAN)
    # ------------------------------------------------------------------

    async def scan_pattern(self, pattern: str) -> List[str]:
        """Return unprefixed keys matching glob pattern via SCAN."""
        full_pattern = self._k(pattern)
        matched_keys: List[str] = []
        async for key in self._client.scan_iter(match=full_pattern, count=100):
            decoded = key.decode() if isinstance(key, bytes) else key
            # Strip prefix before returning
            unprefixed = decoded[len(self._prefix):]
            matched_keys.append(unprefixed)
        return matched_keys

    # ------------------------------------------------------------------
    # Distributed invalidation (Pub/Sub)
    # ------------------------------------------------------------------

    def on_invalidation(self, callback: Callable[[str], None]) -> None:
        self._invalidation_callbacks.append(callback)

    async def publish_invalidation(self, message: str) -> None:
        if self._client and self._connected:
            await self._client.publish(self._channel, message)

    async def _start_pubsub(self) -> None:
        # Use a separate connection for Pub/Sub
        if self._url:
            self._pubsub_client = aioredis.from_url(
                self._url, decode_responses=True
            )
        else:
            self._pubsub_client = aioredis.Redis(
                host=self._host,
                port=self._port,
                password=self._password,
                db=self._db,
                decode_responses=True,
            )
        self._pubsub = self._pubsub_client.pubsub()
        await self._pubsub.subscribe(self._channel)
        self._listener_task = asyncio.create_task(self._listen_loop())

    async def _listen_loop(self) -> None:
        async for message in self._pubsub.listen():
            if message["type"] == "message":
                data = message["data"]
                for cb in self._invalidation_callbacks:
                    cb(data)

    # ------------------------------------------------------------------
    # Distributed locks
    # ------------------------------------------------------------------

    def _lock_key(self, key: str) -> str:
        return f"{self._prefix}__lock__:{key}"

    async def acquire_lock(
        self, key: str, value: str, ttl_ms: int
    ) -> bool:
        result = await self._client.set(
            self._lock_key(key), value, px=ttl_ms, nx=True
        )
        return result is True

    async def release_lock(self, key: str, value: str) -> bool:
        result = await self._client.eval(
            _RELEASE_LOCK_LUA, 1, self._lock_key(key), value
        )
        return bool(result)

    # ------------------------------------------------------------------
    # Ping
    # ------------------------------------------------------------------

    async def ping(self) -> float:
        import time
        t = time.monotonic()
        await self._client.ping()
        return (time.monotonic() - t) * 1000
