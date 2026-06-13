"""
namespace.py — Logical key namespaces for SuperiorCache.

Usage::

    users = cache.namespace("users")
    await users.set("123", {...})       # stored as "users:123"
    val = await users.get("123")        # fetches "users:123"
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from .types import FetchOptions, SetOptions


class CacheNamespace:
    """
    A lightweight view over SuperiorCache that prepends a fixed prefix
    to every key, giving logical separation of cache domains.
    """

    def __init__(self, cache: Any, namespace: str) -> None:  # cache: SuperiorCache
        self._cache = cache
        self._prefix = f"{namespace}:"

    # ------------------------------------------------------------------
    # Prefixing helpers
    # ------------------------------------------------------------------

    def _pk(self, key: str) -> str:
        return f"{self._prefix}{key}"

    def _unprefix(self, key: str) -> str:
        return key[len(self._prefix):] if key.startswith(self._prefix) else key

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    async def get(self, key: str) -> Optional[Any]:
        return await self._cache.get(self._pk(key))

    async def set(
        self,
        key: str,
        value: Any,
        options: Optional[SetOptions] = None,
        **kwargs,
    ) -> None:
        await self._cache.set(self._pk(key), value, options, **kwargs)

    async def delete(self, key: str) -> bool:
        return await self._cache.delete(self._pk(key))

    async def fetch(
        self,
        key: str,
        loader: Callable,
        options: Optional[FetchOptions] = None,
        **kwargs,
    ) -> Any:
        return await self._cache.fetch(self._pk(key), loader, options, **kwargs)

    # ------------------------------------------------------------------
    # Batch
    # ------------------------------------------------------------------

    async def mget(self, keys: List[str]) -> Dict[str, Any]:
        prefixed = [self._pk(k) for k in keys]
        raw = await self._cache.mget(prefixed)
        return {self._unprefix(k): v for k, v in raw.items()}

    async def mset(self, entries: List[Dict]) -> None:
        prefixed = [
            {**e, "key": self._pk(e["key"])} for e in entries
        ]
        await self._cache.mset(prefixed)

    async def mdelete(self, keys: List[str]) -> int:
        return await self._cache.mdelete([self._pk(k) for k in keys])

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def depends(self, parent_key: str, child_keys: List[str]) -> None:
        self._cache.depends(
            self._pk(parent_key),
            [self._pk(k) for k in child_keys],
        )

    async def invalidate_tag(self, tag: str) -> int:
        return await self._cache.invalidate_tag(tag)

    async def invalidate_pattern(self, pattern: str) -> int:
        return await self._cache.invalidate_pattern(self._pk(pattern))
