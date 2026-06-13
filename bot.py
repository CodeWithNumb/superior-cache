"""
bot.py — Discord bot using SuperiorCache

Features demonstrated:
  1. Basic fetch (get-or-load) with TTL
  2. Tag-based invalidation
  3. Namespaces (guilds / users)
  4. Cascade dependencies
  5. Batch mget
  6. Distributed lock (prevents duplicate heavy tasks)
  7. Event hooks (hit / miss logging)
  8. Stats command
  9. Pattern invalidation

Install dependencies:
    pip install discord.py redis

Run:
    python bot.py
"""

import asyncio
import os
import time

import discord
from discord.ext import commands

# --- import superior_cache from the local package -----------------------
# If you put the superior_cache/ folder next to bot.py, this just works.
from superior_cache import SuperiorCache, FetchOptions, SetOptions

# -----------------------------------------------------------------------
# Bot setup
# -----------------------------------------------------------------------

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

# -----------------------------------------------------------------------
# Cache setup — memory-only (no Redis needed to get started)
# -----------------------------------------------------------------------
# To enable Redis, uncomment the redis_url line:
#
#   cache = SuperiorCache(redis_url="redis://localhost:6379", debug=True)
#
cache = SuperiorCache(
    default_ttl=60.0,           # 1-minute default TTL
    stampede_enabled=True,      # serve stale while refreshing
    stampede_grace_seconds=30,  # grace period
    debug=True,                 # prints [SuperiorCache] logs
)

# Namespaces for clean key organisation
guild_cache = cache.namespace("guilds")
user_cache  = cache.namespace("users")

# -----------------------------------------------------------------------
# Event hook: log every hit/miss to console
# -----------------------------------------------------------------------
def on_hit(payload):
    print(f"✅ CACHE HIT  [{payload['layer'].upper()}] key={payload['key']}")

def on_miss(payload):
    print(f"❌ CACHE MISS key={payload['key']}")

cache.on("hit",  on_hit)
cache.on("miss", on_miss)

# -----------------------------------------------------------------------
# Simulated "database" functions
# -----------------------------------------------------------------------

async def _fetch_guild_info(guild_id: int) -> dict:
    """Simulate a slow DB call (would be a real DB query in production)."""
    await asyncio.sleep(0.3)          # simulate 300 ms latency
    return {
        "id": guild_id,
        "name": f"Guild #{guild_id}",
        "member_count": 150 + guild_id % 50,
        "fetched_at": time.time(),
    }

async def _fetch_user_profile(user_id: int) -> dict:
    await asyncio.sleep(0.2)
    return {
        "id": user_id,
        "name": f"User#{user_id}",
        "xp": user_id * 7 % 1000,
        "fetched_at": time.time(),
    }

async def _fetch_leaderboard(guild_id: int) -> list:
    await asyncio.sleep(0.5)
    return [
        {"rank": i+1, "user": f"User#{i}", "xp": 1000 - i * 50}
        for i in range(10)
    ]

# -----------------------------------------------------------------------
# Bot events
# -----------------------------------------------------------------------

@bot.event
async def on_ready():
    try:
        await cache.connect()
    except Exception as e:
        print(f"⚠️ Redis not available, running in memory-only mode: {e}")
    print(f"✅ Logged in as {bot.user} — SuperiorCache ready!")

# -----------------------------------------------------------------------
# Commands
# -----------------------------------------------------------------------

@bot.command(name="guild")
async def guild_info(ctx):
    """
    !guild
    Fetch guild info — cached for 60 s.
    Shows L1 hit on second call.
    """
    gid = ctx.guild.id
    data = await guild_cache.fetch(
        str(gid),
        loader=lambda: _fetch_guild_info(gid),
        ttl=60.0,
        tags=["guild_info", f"guild:{gid}"],
    )
    await ctx.send(
        f"**{data['name']}** | Members: {data['member_count']} "
        f"| Cached at: <t:{int(data['fetched_at'])}:T>"
    )


@bot.command(name="profile")
async def user_profile(ctx, member: discord.Member = None):
    """
    !profile [@member]
    Fetch user profile — cached for 120 s.
    """
    member = member or ctx.author
    uid = member.id

    data = await user_cache.fetch(
        str(uid),
        loader=lambda: _fetch_user_profile(uid),
        ttl=120.0,
        tags=["user_profiles", f"user:{uid}"],
    )
    await ctx.send(
        f"**{member.display_name}** | XP: {data['xp']} "
        f"| Cached at: <t:{int(data['fetched_at'])}:T>"
    )


@bot.command(name="leaderboard")
async def leaderboard(ctx):
    """
    !leaderboard
    Guild leaderboard — cached 5 min.
    Depends on guild info (cascade delete).
    """
    gid = ctx.guild.id

    # Set up cascade: deleting guild info also nukes the leaderboard
    lb_key = f"guilds:{gid}:leaderboard"
    guild_key = f"guilds:{gid}"
    cache.depends(guild_key, [lb_key])

    lb = await cache.fetch(
        lb_key,
        loader=lambda: _fetch_leaderboard(gid),
        ttl=300.0,
        tags=[f"guild:{gid}", "leaderboards"],
    )
    lines = [f"{e['rank']}. {e['user']} — {e['xp']} XP" for e in lb[:5]]
    await ctx.send("**Top 5**\n" + "\n".join(lines))


@bot.command(name="bulk")
async def bulk_profiles(ctx):
    """
    !bulk
    Fetch multiple user profiles in one batched mget call.
    """
    ids = [str(m.id) for m in list(ctx.guild.members)[:5]]
    prefixed = [f"users:{uid}" for uid in ids]

    results = await cache.mget(prefixed)
    hits = len(results)
    misses = len(ids) - hits

    await ctx.send(
        f"Bulk mget for {len(ids)} users — "
        f"**{hits} hits**, **{misses} misses** in L1/L2."
    )


@bot.command(name="refresh")
async def force_refresh(ctx):
    """
    !refresh
    Force-reload guild info bypassing cache.
    """
    gid = ctx.guild.id
    data = await guild_cache.fetch(
        str(gid),
        loader=lambda: _fetch_guild_info(gid),
        ttl=60.0,
        force_refresh=True,
    )
    await ctx.send(f"🔄 Refreshed! New timestamp: <t:{int(data['fetched_at'])}:T>")


@bot.command(name="invalidate")
@commands.has_permissions(administrator=True)
async def invalidate(ctx, tag: str = "guild_info"):
    """
    !invalidate [tag]
    Invalidate all entries with a given tag. Admin only.
    """
    count = await cache.invalidate_tag(tag)
    await ctx.send(f"🗑️ Invalidated tag `{tag}` — {count} L1 entries removed.")


@bot.command(name="invalidate_pattern")
@commands.has_permissions(administrator=True)
async def invalidate_pat(ctx, pattern: str = "guilds:*"):
    """
    !invalidate_pattern [glob]
    Invalidate all keys matching a glob pattern.
    e.g.  !invalidate_pattern users:*
    """
    count = await cache.invalidate_pattern(pattern)
    await ctx.send(f"🗑️ Pattern `{pattern}` — {count} L1 entries removed.")


@bot.command(name="lock_demo")
async def lock_demo(ctx):
    """
    !lock_demo
    Demonstrate distributed lock. Only one winner per 5s.
    """
    handle = await cache.lock("demo_task", ttl_seconds=5.0, max_retries=3)
    if handle is None:
        await ctx.send("⏳ Someone else holds the lock — try again shortly.")
        return
    try:
        await ctx.send("🔒 Lock acquired! Simulating exclusive work…")
        await asyncio.sleep(2)
        await ctx.send("✅ Work done — releasing lock.")
    finally:
        await cache.unlock(handle)


@bot.command(name="stats")
async def cache_stats(ctx):
    """
    !stats
    Show cache statistics.
    """
    s = await cache.stats()
    embed = discord.Embed(title="SuperiorCache Stats", color=0x5865F2)
    embed.add_field(name="L1 Entries",     value=str(s.l1_entries),       inline=True)
    embed.add_field(name="L1 Hits",        value=str(s.l1_hits),          inline=True)
    embed.add_field(name="L2 Hits",        value=str(s.l2_hits),          inline=True)
    embed.add_field(name="Misses",         value=str(s.misses),           inline=True)
    embed.add_field(name="Loader Execs",   value=str(s.loader_executions),inline=True)
    embed.add_field(name="Hit Rate",       value=f"{s.hit_rate:.1%}",     inline=True)
    embed.add_field(name="Redis",          value="✅" if s.redis_connected else "❌", inline=True)
    embed.add_field(name="Active Locks",   value=str(s.active_locks),     inline=True)
    await ctx.send(embed=embed)


@bot.command(name="clear_cache")
@commands.has_permissions(administrator=True)
async def clear_cache(ctx):
    """
    !clear_cache
    Wipe the entire L1 cache. Admin only.
    """
    await cache.clear()
    await ctx.send("💥 Cache cleared!")

# -----------------------------------------------------------------------
# Cleanup on shutdown
# -----------------------------------------------------------------------

@bot.event
async def on_close():
    await cache.destroy()
    print("SuperiorCache destroyed cleanly.")

# -----------------------------------------------------------------------
# Entry point
# -----------------------------------------------------------------------

if __name__ == "__main__":
    TOKEN = os.environ.get("DISCORD_BOT_TOKEN", "YOUR_TOKEN_HERE")
    bot.run(TOKEN)