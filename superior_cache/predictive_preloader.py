"""
predictive_preloader.py — Learns access patterns and preloads co-accessed keys.

When key A is often accessed just before key B, the preloader learns this
pattern and automatically loads B in the background when A is accessed,
so B is already cached when it's needed.

Ported exactly from the Node.js SuperiorCache PredictivePreloader.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


_WINDOW_SECONDS = 5.0     # max gap between two accesses to be "sequential"
_MIN_FREQUENCY  = 3       # how many times a pair must occur before preloading
_MAX_PATTERNS   = 1_000   # memory cap on tracked patterns


@dataclass
class _PatternEntry:
    frequency: int = 1
    last_seen: float = field(default_factory=time.monotonic)


class PredictivePreloader:
    """
    Tracks sequential key-access pairs and returns preload targets.

    Usage (internal — called by SuperiorCache automatically):
        preloader.record_access("product:abc")
        preloader.record_access("product:abc:reviews")
        # After 3+ observations:
        preloader.get_preload_targets("product:abc")
        # → ["product:abc:reviews"]
    """

    def __init__(self, debug: bool = False) -> None:
        # composite key  →  PatternEntry
        # composite key format: "sourceKey\x00targetKey"
        self._patterns: Dict[str, _PatternEntry] = {}
        self._last_access: Optional[Tuple[str, float]] = None  # (key, monotonic_time)
        self._debug = debug

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record_access(self, key: str) -> None:
        """
        Record that `key` was just accessed.
        If the previous access was within the time window and a different key,
        increment the pattern counter for (previous → current).
        """
        now = time.monotonic()

        if self._last_access is not None:
            prev_key, prev_time = self._last_access
            elapsed = now - prev_time

            if elapsed <= _WINDOW_SECONDS and prev_key != key:
                self._increment_pattern(prev_key, key)

        self._last_access = (key, now)

    def get_preload_targets(self, key: str) -> List[str]:
        """
        Return keys that should be preloaded when `key` is accessed.
        Only returns keys whose (key → target) pattern frequency >= MIN_FREQUENCY.
        """
        targets: List[str] = []
        prefix = f"{key}\x00"

        for composite, entry in self._patterns.items():
            if composite.startswith(prefix) and entry.frequency >= _MIN_FREQUENCY:
                target = composite[len(prefix):]
                targets.append(target)

        return targets

    def get_patterns(self) -> List[dict]:
        """Return all learned patterns sorted by frequency (for debugging)."""
        result = []
        for composite, entry in self._patterns.items():
            source, _, target = composite.partition("\x00")
            result.append({
                "source": source,
                "target": target,
                "frequency": entry.frequency,
            })
        return sorted(result, key=lambda x: x["frequency"], reverse=True)

    def clear(self) -> None:
        self._patterns.clear()
        self._last_access = None
        self._log("All patterns cleared")

    def destroy(self) -> None:
        self.clear()
        self._log("Predictive preloader destroyed")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _increment_pattern(self, source: str, target: str) -> None:
        composite = f"{source}\x00{target}"
        entry = self._patterns.get(composite)

        if entry is not None:
            entry.frequency += 1
            entry.last_seen = time.monotonic()
        else:
            if len(self._patterns) >= _MAX_PATTERNS:
                self._evict_oldest()
            self._patterns[composite] = _PatternEntry()

        self._log(
            f"Pattern '{source}' → '{target}': "
            f"frequency={self._patterns[composite].frequency}"
        )

    def _evict_oldest(self) -> None:
        """Remove the least-recently-seen pattern to stay under MAX_PATTERNS."""
        if not self._patterns:
            return
        oldest_key = min(self._patterns, key=lambda k: self._patterns[k].last_seen)
        del self._patterns[oldest_key]

    def _log(self, msg: str) -> None:
        if self._debug:
            print(f"[PredictivePreloader] {msg}")