# superior-cache-py

> Drop-in async cache for discord.py bots. Memory + Redis layers, TTL, tags, namespaces, and thundering-herd protection out of the box.

Made by **[CodeWithNumb](https://github.com/CodeWithNumb)**, discord: numb.fy

---

## What is this?

`superior-cache-py` is a Python port of the [SuperiorCache](https://github.com/Saiop68/superior-cache) Node.js library, rebuilt from scratch for **discord.py** bots.

It gives your bot a fast two-layer cache:

- **L1 — Memory** (in-process, sub-millisecond)
- **L2 — Redis** (optional, cross-process / multi-bot)

On a cache miss, it runs your loader function, caches the result, and returns it — automatically.

---

## Features

- **get / set / delete / fetch** — core cache operations
- **TTL** — per-key expiry in seconds
- **Tag-based invalidation** — invalidate many keys at once with a tag
- **Pattern invalidation** — glob-style bulk delete (`users:*`)
- **Cascade dependencies** — delete a parent key → child keys auto-delete
- **Request deduplication** — 100 concurrent misses = 1 DB call
- **Stampede protection** — serve stale data while refreshing in background
- **Distributed locks** — atomic Redis locks (SET NX PX)
- **Namespaces** — logical key prefixing (`users:`, `guilds:`)
- **Batch ops** — `mget`, `mset`, `mdelete`
- **Event hooks** — `on("hit", ...)`, `on("miss", ...)`, etc.
- **Plugin system** — extend with custom logic
- **Stats** — hit rate, L1/L2 counts, active locks

---

## Installation

```bash
# Without Redis (memory-only)
pip install git+https://github.com/CodeWithNumb/superior-cache

# With Redis support
pip install "superior-cache[redis] @ git+https://github.com/CodeWithNumb/superior-cache"
```

> Redis is optional. The cache works perfectly fine in memory-only mode.

---

## Quick Start

```python
from superior_cache import SuperiorCache

cache = SuperiorCache(default_ttl=60.0)
await cache.connect()  # connects Redis if configured

# Fetch from cache, or run loader on miss
user = await cache.fetch(
    "user:123",
    loader=lambda: db.get_user(123),
    ttl=60.0
)
```

---

## Usage Examples

### Basic fetch (get-or-load)

```python
data = await cache.fetch(
    "guild:123",
    loader=lambda: fetch_guild_from_db(123),
    ttl=60.0,
    tags=["guilds"]
)
```

### Namespaces

```python
users = cache.namespace("users")

await users.set("123", {"name": "Alice"}, ttl=120.0)
user = await users.get("123")   # reads "users:123"
```

### Tag-based invalidation

```python
# Tag entries when setting
await cache.set("product:1", data, tags=["products"])
await cache.set("product:2", data, tags=["products"])

# Nuke all at once
await cache.invalidate_tag("products")
```

### Pattern invalidation

```python
await cache.invalidate_pattern("session:*")
```

### Cascade dependencies

```python
cache.depends("guild:100", ["guild:100:members", "guild:100:roles"])

# Deleting the parent auto-deletes children
await cache.delete("guild:100")
```

### Batch operations

```python
# mget
results = await cache.mget(["user:1", "user:2", "user:3"])

# mset
await cache.mset([
    {"key": "user:1", "value": {...}, "options": SetOptions(ttl=60)},
    {"key": "user:2", "value": {...}},
])
```

### Distributed lock

```python
lock = await cache.lock("payment:user:99", ttl_seconds=30.0)
if lock:
    try:
        await process_payment(99)
    finally:
        await cache.unlock(lock)
```

### Event hooks

```python
cache.on("hit",  lambda e: print(f"HIT [{e['layer']}] {e['key']}"))
cache.on("miss", lambda e: print(f"MISS {e['key']}"))
```

### Stats

```python
s = await cache.stats()
print(f"Hit rate: {s.hit_rate:.1%}")
print(f"L1 entries: {s.l1_entries}")
```

---

## Configuration

```python
cache = SuperiorCache(
    # Global
    default_ttl=60.0,           # default TTL in seconds
    debug=True,                 # verbose logging

    # Memory (L1)
    max_entries=50_000,         # max keys in memory

    # Redis (L2) — omit to use memory-only mode
    redis_url="redis://localhost:6379",
    redis_key_prefix="mybot:",
    redis_default_ttl=300.0,
    redis_pubsub=True,          # distributed invalidation

    # Stampede protection
    stampede_enabled=True,
    stampede_grace_seconds=30.0,
    refresh_ahead_fraction=0.2, # refresh when 20% TTL remains
)
```

---

## discord.py Bot Commands (included in `bot.py`)

| Command | What it does |
|---|---|
| `!guild` | Fetch & cache guild info |
| `!profile [@user]` | Fetch & cache user profile |
| `!leaderboard` | Cached leaderboard with cascade dependency |
| `!bulk` | Batch `mget` demo |
| `!refresh` | Force-refresh bypassing cache |
| `!invalidate [tag]` | Invalidate by tag |
| `!invalidate_pattern [glob]` | Invalidate by pattern |
| `!lock_demo` | Distributed lock demo |
| `!stats` | Cache stats embed |
| `!clear_cache` | Wipe entire L1 cache |

---

## Requirements

- Python 3.9+
- discord.py 2.x
- redis (optional, for L2)

---

## License

MIT

---

<p align="center">Made with ❤️ by <strong>CodeWithNumb</strong></p>
