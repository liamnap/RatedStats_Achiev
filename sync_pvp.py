#!/usr/bin/env python3
import os
import json
import sqlite3
import tempfile
import asyncio
import aiohttp
import requests
import time
import datetime
import gc
import re
import argparse
from pathlib import Path
from collections import deque, Counter
from urllib.parse import urlparse
from asyncio import TimeoutError, CancelledError, create_task, as_completed, shield

try:
    import psutil
except ImportError:
    psutil = None

# --------------------------------------------------------------------------
# CLI + MODE + REGION + BATCH SETTINGS
# --------------------------------------------------------------------------
parser = argparse.ArgumentParser(description="PvP sync runner")
parser.add_argument("--mode", choices=["batch", "finalize"], default=None,
                    help="Mode: 'batch' to emit partials, 'finalize' to write full Lua")
parser.add_argument("--region", default=os.getenv("REGION", "eu"),
                    help="Region code: us, eu, kr, tw")
parser.add_argument("--batch-id",      type=int, default=int(os.getenv("BATCH_ID", "0")),
                    help="0-based batch index for batch mode")
parser.add_argument("--total-batches", type=int, default=int(os.getenv("TOTAL_BATCHES", "1")),
                    help="Total number of batches for batch mode")
args = parser.parse_args()

REGION        = args.region
BATCH_ID      = args.batch_id
TOTAL_BATCHES = args.total_batches
MODE          = args.mode or ("finalize" if TOTAL_BATCHES == 1 else "batch")

# --------------------------------------------------------------------------
# Globals & Constants
# --------------------------------------------------------------------------
UTC            = datetime.timezone.utc
start_time     = time.time()
CALLS_DONE     = 0
TOTAL_CALLS    = None
HTTP_429_QUEUED = 0
CALL_TIMES     = deque()

GREEN, YELLOW, RED, RESET = "\033[92m", "\033[93m", "\033[91m", "\033[0m"
OUTFILE        = Path(f"region_{REGION}.lua")
REGION_VAR     = f"ACHIEVEMENTS_{REGION.upper()}"
LOCALES        = {"us": "en_US", "eu": "en_GB", "kr": "ko_KR", "tw": "zh_TW"}
LOCALE         = LOCALES.get(REGION, "en_US")
API_HOST       = f"{REGION}.api.blizzard.com"
API_BASE       = f"https://{API_HOST}"
NAMESPACE_PROFILE = f"profile-{REGION}"

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def _fmt_duration(sec: int) -> str:
    if sec <= 0:
        return "0s"
    parts = []
    for name, length in [("y", 31_557_600), ("w", 604_800), ("d", 86_400), ("h", 3_600), ("m", 60)]:
        qty, sec = divmod(sec, length)
        if qty:
            parts.append(f"{qty}{name}")
    if sec:
        parts.append(f"{sec}s")
    return " ".join(parts)

def _bump_calls():
    global CALLS_DONE
    CALLS_DONE += 1
    now = time.time()
    CALL_TIMES.append(now)
    while CALL_TIMES and now - CALL_TIMES[0] > 60:
        CALL_TIMES.popleft()

class RetryCharacter(Exception):
    def __init__(self, char):
        super().__init__(f"Retry {char['name']}-{char['realm']}")
        self.char = char

class RateLimitExceeded(Exception):
    pass

# --------------------------------------------------------------------------
# RateLimiter
# --------------------------------------------------------------------------
class RateLimiter:
    def __init__(self, max_calls: int, period: float):
        self.capacity   = max_calls
        self.tokens     = 0
        self.fill_rate  = max_calls / period
        self.timestamp  = time.monotonic()
        self._lock      = asyncio.Lock()
        self.max_calls  = max_calls
        self.period     = period
        self.calls      = []

    async def acquire(self):
        async with self._lock:
            now     = time.monotonic()
            elapsed = now - self.timestamp
            self.timestamp = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.fill_rate)
            if self.tokens < 1:
                wait = (1 - self.tokens) / self.fill_rate
                await asyncio.sleep(wait)
                now = time.monotonic()
                self.timestamp = now
                self.tokens = 1
            self.tokens -= 1
            self.calls = [t for t in self.calls if now - t < self.period]
            self.calls.append(now)

# --------------------------------------------------------------------------
# Authentication & Blizzard endpoints
# --------------------------------------------------------------------------
def get_access_token(region: str) -> str:
    if region == "eu" and os.getenv("BLIZZARD_CLIENT_ID_EU"):
        cid = os.getenv("BLIZZARD_CLIENT_ID_EU")
        cs  = os.getenv("BLIZZARD_CLIENT_SECRET_EU")
    elif region == "us" and os.getenv("BLIZZARD_CLIENT_ID_US"):
        cid = os.getenv("BLIZZARD_CLIENT_ID_US")
        cs  = os.getenv("BLIZZARD_CLIENT_SECRET_US")
    else:
        cid = os.getenv("BLIZZARD_CLIENT_ID")
        cs  = os.getenv("BLIZZARD_CLIENT_SECRET")

    resp = requests.post(
        "https://us.battle.net/oauth/token",
        data={"grant_type": "client_credentials"},
        auth=(cid, cs),
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

def get_current_pvp_season_id(region: str) -> int:
    url    = f"{API_BASE}/data/wow/pvp-season/index?namespace=dynamic-{region}&locale=en_US"
    token  = get_access_token(region)
    resp   = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    resp.raise_for_status()
    return resp.json()["seasons"][-1]["id"]

def get_available_brackets(region: str, season_id: int) -> list[str]:
    url    = f"{API_BASE}/data/wow/pvp-season/{season_id}/pvp-leaderboard/index?namespace=dynamic-{region}&locale={LOCALE}"
    token  = get_access_token(region)
    resp   = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    if not resp.ok:
        raise RuntimeError(f"[FAIL] Unable to fetch PvP leaderboard index for season {season_id}: {resp.status_code}")
    lbs    = resp.json().get("leaderboards", [])
    prefixes = ("2v2","3v3","rbg","shuffle-","blitz-")
    brackets = []
    for entry in lbs:
        href = entry.get("key", {}).get("href", "")
        b    = urlparse(href).path.rstrip("/").split("/")[-1]
        if b.startswith(prefixes):
            brackets.append(b)
    print(f"[INFO] Valid brackets for season {season_id}: {', '.join(brackets)}")
    return brackets

# --------------------------------------------------------------------------
# Season & Bracket initialization
# --------------------------------------------------------------------------
from pathlib import Path

# make sure partial_outputs exists
CACHE_DIR     = Path("partial_outputs")
CACHE_DIR.mkdir(exist_ok=True)
BRACKET_CACHE = CACHE_DIR / f"{REGION}_brackets.json"

if BRACKET_CACHE.exists():
    cached = json.loads(BRACKET_CACHE.read_text())
    PVP_SEASON_ID = cached["season_id"]
    BRACKETS      = cached["brackets"]
else:
    PVP_SEASON_ID = get_current_pvp_season_id(REGION)
    BRACKETS      = get_available_brackets(REGION, PVP_SEASON_ID)
    try:
        BRACKET_CACHE.write_text(json.dumps({
            "season_id": PVP_SEASON_ID,
            "brackets":  BRACKETS
        }))
    except Exception:
        pass

# --------------------------------------------------------------------------
# Fetch PvP leaderboard characters
# --------------------------------------------------------------------------
def get_characters_from_leaderboards(region: str, headers: dict, season_id: int, brackets: list[str]) -> dict[int,dict]:
    seen: dict[int, dict] = {}
    for bracket in brackets:
        url = (
            f"https://{region}.api.blizzard.com/"
            f"data/wow/pvp-season/{season_id}/pvp-leaderboard/{bracket}"
            f"?namespace=dynamic-{region}&locale={LOCALE}"
        )
        resp = requests.get(url, headers=headers)
        if resp.status_code != 200:
            print(f"[WARN] Failed leaderboard: {bracket} - {resp.status_code}")
            continue
        for entry in resp.json().get("entries", []):
            c = entry.get("character")
            if not c or c["id"] in seen:
                continue
            seen[c["id"]] = {
                "id": c["id"],
                "name": c["name"],
                "realm": c["realm"]["slug"],
            }
    return seen

# --------------------------------------------------------------------------
# Static namespace discovery
# --------------------------------------------------------------------------
def get_latest_static_namespace(region: str) -> str:
    fallback = f"static-{region}"
    try:
        token = get_access_token("us")
        resp  = requests.get(
            f"https://{region}.api.blizzard.com/data/wow/achievement-category/index"
            f"?namespace={fallback}&locale=en_US",
            headers={"Authorization": f"Bearer {token}"},
        )
        if not resp.ok:
            return fallback
        href = resp.json().get("_links", {}).get("self", {}).get("href", "")
        if "namespace=" in href:
            return href.split("namespace=")[-1].split("&")[0]
    except Exception:
        pass
    return fallback

NAMESPACE_STATIC = get_latest_static_namespace(REGION)
print(f"[INFO] Region: {REGION}, Locale: {LOCALE}, Static NS: {NAMESPACE_STATIC}")

# --------------------------------------------------------------------------
# Rate limiters and in-memory cache
# --------------------------------------------------------------------------
REGION_CAP  = 9 if REGION in ("us","eu") else 100
per_sec     = RateLimiter(REGION_CAP, 1)
per_hour    = RateLimiter(36000, 3600)
SEM_CAP     = REGION_CAP
url_cache   = {}

async def fetch_with_rate_limit(session, url, headers, max_retries=5):
    cacheable = ("profile/wow/character" not in url and "oauth" not in url)
    if cacheable and url in url_cache:
        return url_cache[url]

    await per_sec.acquire()
    await per_hour.acquire()

    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if cacheable:
                        url_cache[url] = data
                    _bump_calls()
                    return data
                if resp.status == 429:
                    global HTTP_429_QUEUED
                    HTTP_429_QUEUED += 1
                    raise RateLimitExceeded()
                if 500 <= resp.status < 600:
                    raise RateLimitExceeded()
                resp.raise_for_status()
        except asyncio.TimeoutError:
            await asyncio.sleep(2 ** attempt)
    raise RuntimeError(f"fetch failed for {url} after {max_retries} retries")

# --------------------------------------------------------------------------
# Achievement keywords list (unchanged)
# --------------------------------------------------------------------------
async def get_pvp_achievements(session, headers):
    idx = await fetch_with_rate_limit(
        session,
        f"{API_BASE}/data/wow/achievement/index?namespace={NAMESPACE_STATIC}&locale=en_US",
        headers
    )
    KEYWORDS = [
        # Main Achievements
        {"type":"exact","value":"Scout"},
        {"type":"exact","value":"Private"},
        {"type":"exact","value":"Grunt"},
        {"type":"exact","value":"Corporal"},
        {"type":"exact","value":"Sergeant"},
        {"type":"exact","value":"Senior Sergeant"},
        {"type":"exact","value":"Master Sergeant"},
        {"type":"exact","value":"First Sergeant"},
        {"type":"exact","value":"Sergeant Major"},
        {"type":"exact","value":"Stone Guard"},
        {"type":"exact","value":"Knight"},
        {"type":"exact","value":"Blood Guard"},
        {"type":"exact","value":"Knight-Lieutenant"},
        {"type":"exact","value":"Legionnaire"},
        {"type":"exact","value":"Knight-Captain"},
        {"type":"exact","value":"Centurion"},
        {"type":"exact","value":"Knight-Champion"},
        {"type":"exact","value":"Champion"},
        {"type":"exact","value":"Lieutenant Commander"},
        {"type":"exact","value":"Lieutenant General"},
        {"type":"exact","value":"Commander"},
        {"type":"exact","value":"General"},
        {"type":"exact","value":"Marshal"},
        {"type":"exact","value":"Warlord"},
        {"type":"exact","value":"Field Marshal"},
        {"type":"exact","value":"High Warlord"},
        {"type":"exact","value":"Grand Marshal"},

        # Rated PvP Season Tiers
        {"type":"prefix","value":"Combatant I"},
        {"type":"prefix","value":"Combatant II"},
        {"type":"prefix","value":"Challenger I"},
        {"type":"prefix","value":"Challenger II"},
        {"type":"prefix","value":"Rival I"},
        {"type":"prefix","value":"Rival II"},
        {"type":"prefix","value":"Duelist"},
        {"type":"prefix","value":"Elite:"},
        {"type":"prefix","value":"Gladiator:"},
        {"type":"prefix","value":"Legend:"},

        # Special Achievements
        {"type":"prefix","value":"Three's Company: 2700"},

        # R1 Titles
        {"type":"prefix","value":"Hero of the Horde"},
        {"type":"prefix","value":"Hero of the Alliance"},
        {"type":"prefix","value":"Primal Gladiator"},
        {"type":"prefix","value":"Wild Gladiator"},
        {"type":"prefix","value":"Warmongering Gladiator"},
        {"type":"prefix","value":"Vindictive Gladiator"},
        {"type":"prefix","value":"Fearless Gladiator"},
        {"type":"prefix","value":"Cruel Gladiator"},
        {"type":"prefix","value":"Ferocious Gladiator"},
        {"type":"prefix","value":"Fierce Gladiator"},
        {"type":"prefix","value":"Demonic Gladiator"},
        {"type":"prefix","value":"Dread Gladiator"},
        {"type":"prefix","value":"Sinister Gladiator"},
        {"type":"prefix","value":"Notorious Gladiator"},
        {"type":"prefix","value":"Corrupted Gladiator"},
        {"type":"prefix","value":"Sinful Gladiator"},
        {"type":"prefix","value":"Unchained Gladiator"},
        {"type":"prefix","value":"Cosmic Gladiator"},
        {"type":"prefix","value":"Eternal Gladiator"},
        {"type":"prefix","value":"Crimson Gladiator"},
        {"type":"prefix","value":"Obsidian Gladiator"},
        {"type":"prefix","value":"Draconic Gladiator"},
        {"type":"prefix","value":"Seasoned Gladiator"},
        {"type":"prefix","value":"Forged Warlord:"},
        {"type":"prefix","value":"Forged Marshal:"},
        {"type":"prefix","value":"Forged Legend:"},
        {"type":"prefix","value":"Forged Gladiator:"},
        {"type":"prefix","value":"Prized Warlord:"},
        {"type":"prefix","value":"Prized Marshal:"},
        {"type":"prefix","value":"Prized Legend:"},
        {"type":"prefix","value":"Prized Gladiator:"},
    ]

    matches = {}
    for ach in idx.get("achievements", []):
        name = ach.get("name", "")
        for kw in KEYWORDS:
            if kw["type"] == "exact" and name == kw["value"]:
                matches[ach["id"]] = name
                break
            if kw["type"] == "prefix" and name.startswith(kw["value"]):
                matches[ach["id"]] = name
                break

    print(f"[DEBUG] Total PvP keyword matches: {len(matches)}")
    return matches

async def get_character_achievements(session, headers, realm, name):
    url = f"{API_BASE}/profile/wow/character/{realm}/{name.lower()}/achievements?namespace={NAMESPACE_PROFILE}&locale={LOCALE}"
    return await fetch_with_rate_limit(session, url, headers) or None

# --------------------------------------------------------------------------
# SQLite cache + seed-from-Lua
# --------------------------------------------------------------------------
DB_PATH = Path(tempfile.gettempdir()) / f"achiev_{REGION}.db"
db      = sqlite3.connect(DB_PATH)
db.execute("""
    CREATE TABLE IF NOT EXISTS char_data (
        key TEXT PRIMARY KEY,
        guid INTEGER,
        ach_json TEXT
    )
""")
def db_upsert(key: str, guid: int, ach: dict):
    db.execute(
        "INSERT OR REPLACE INTO char_data (key,guid,ach_json) VALUES (?,?,?)",
        (key, guid, json.dumps(ach, separators=(',',':')))
    )
def db_iter_rows():
    cur = db.execute("SELECT key,guid,ach_json FROM char_data ORDER BY key")
    for k, g, j in cur:
        yield k, g, json.loads(j)

def seed_db_from_lua(lua_path: Path) -> dict:
    rows = {}
    if not lua_path.exists():
        return rows
    txt = lua_path.read_text(encoding="utf-8")
    row_rx  = re.compile(r'\{[^{]*?character\s*=\s*"([^"]+)"[^}]*?\}', re.S)
    ach_rx  = re.compile(r'id(\d+)\s*=\s*(\d+),\s*name\1\s*=\s*"([^"]+)"')
    guid_rx = re.compile(r'guid\s*=\s*(\d+)')
    for m in row_rx.finditer(txt):
        block = m.group(0)
        key   = m.group(1)
        gm    = guid_rx.search(block)
        if not gm:
            continue
        guid = int(gm.group(1))
        ach  = {int(aid): name for _, aid, name in ach_rx.findall(block)}
        db_upsert(key, guid, ach)
        n, r = key.split('-', 1)
        rows[key] = {"id": guid, "name": n, "realm": r}
    db.commit()
    return rows

# --------------------------------------------------------------------------
# Main processing
# --------------------------------------------------------------------------
async def process_characters(characters: dict, leaderboard_keys: set):
    global HTTP_429_QUEUED, TOTAL_CALLS
    token       = get_access_token(REGION)
    headers     = {"Authorization": f"Bearer {token}"}
    inserted    = 0
    TOTAL_CALLS = len(characters) + 1

    timeout = aiohttp.ClientTimeout(total=5)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        pvp_achs = await get_pvp_achievements(session, headers)
        print(f"[DEBUG] PvP keywords loaded: {len(pvp_achs)}")

        per_sec.tokens     = 0
        per_sec.timestamp  = asyncio.get_event_loop().time()
        per_sec.calls.clear()
        sem = asyncio.Semaphore(SEM_CAP)
        total = len(characters)
        completed = 0
        last_hb = time.time()

        async def proc_one(c):
            nonlocal inserted
            async with sem:
                name, realm, cid = c["name"].lower(), c["realm"].lower(), c["id"]
                key = f"{name}-{realm}"
                try:
                    data = await get_character_achievements(session, headers, realm, name)
                except RateLimitExceeded:
                    raise RetryCharacter(c)
                if not data:
                    return
                earned = data.get("achievements", [])
                ach_dict = {}
                for ach in earned:
                    aid = ach["id"]
                    if aid not in pvp_achs:
                        continue
                    ts = ach.get("completed_timestamp")
                    ach_dict[aid] = {"name": ach["achievement"]["name"], "ts": ts}
                if ach_dict:
                    db_upsert(key, cid, ach_dict)
                    inserted += 1

        remaining = list(characters.values())
        retry_interval = 10
        BATCH_SIZE = 2500

        while remaining:
            retry_bucket = {}
            batches = (len(remaining) + BATCH_SIZE - 1) // BATCH_SIZE
            for i in range(batches):
                batch = remaining[i*BATCH_SIZE : (i+1)*BATCH_SIZE]
                tasks = [create_task(proc_one(c)) for c in batch]
                for t in as_completed(tasks):
                    try:
                        await shield(t)
                    except RetryCharacter as rc:
                        k = f"{rc.char['name']}-{rc.char['realm']}"
                        retry_bucket[k] = rc.char
                    except Exception:
                        continue
                    else:
                        completed += 1
                        now = time.time()
                        if now - last_hb > 10:
                            url_cache.clear()
                            gc.collect()
                            ts = time.strftime("%H:%M:%S", time.localtime(now))
                            sec_rate = len(per_sec.calls)/per_sec.period
                            avg60    = len(CALL_TIMES)/60
                            rem_calls= (TOTAL_CALLS - CALLS_DONE) if TOTAL_CALLS else None
                            eta      = _fmt_duration(int((now-start_time)/CALLS_DONE*rem_calls)) if CALLS_DONE and rem_calls else "–"
                            print(f"[{ts}] [HEARTBEAT] {completed}/{total} done ({completed/total*100:.1f}%), sec_rate={sec_rate:.1f}/s, avg60={avg60:.1f}/s, ETA={eta}")
                            last_hb = now
            url_cache.clear()
            if retry_bucket:
                await asyncio.sleep(retry_interval)
                remaining = list(retry_bucket.values())
            else:
                break

        db.commit()
        print(f"[DEBUG] inserted={inserted}, SQLite rows={sum(1 for _ in db_iter_rows())}")

    # build fingerprints & alt_map
    fingerprints = {
        k: {(aid, info["ts"]) for aid, info in ach.items() if info.get("ts") is not None}
        for k, _, ach in db_iter_rows()
    }
    alt_map = {k: [] for k in fingerprints}
    keys = list(fingerprints)
    for i in range(len(keys)):
        for j in range(i+1, len(keys)):
            a, b = keys[i], keys[j]
            if len(fingerprints[a] & fingerprints[b]) >= 5:
                alt_map[a].append(b)
                alt_map[b].append(a)

    # connected components
    visited = set()
    groups  = []
    for k in sorted(alt_map):
        if k in visited:
            continue
        comp = []
        stack = [k]
        while stack:
            u = stack.pop()
            if u in visited:
                continue
            visited.add(u)
            comp.append(u)
            for v in alt_map[u]:
                if v not in visited:
                    stack.append(v)
        groups.append(sorted(comp))

    # write out
    rows_map = {k: (g, ach) for k, g, ach in db_iter_rows()}
    if MODE == "finalize":
        with open(OUTFILE, "w", encoding="utf-8") as f:
            f.write(f"-- File: RatedStats_Achiev/region_{REGION}.lua\nlocal achievements={{\n")
            for comp in groups:
                leaders = [m for m in comp if m in leaderboard_keys]
                if not leaders:
                    continue
                root = leaders[0]
                alts = [m for m in comp if m != root]
                guid, ach_map = rows_map[root]
                alts_str = "{" + ",".join(f'"{a}"' for a in alts) + "}"
                parts = [f'character="{root}"', f'alts={alts_str}', f'guid={guid}']
                for i, (aid, info) in enumerate(sorted(ach_map.items()), start=1):
                    esc = info["name"].replace('"', '\\"')
                    parts += [f"id{i}={aid}", f'name{i}="{esc}"']
                f.write("    { " + ", ".join(parts) + " },\n")
            f.write("}\n\n")
            f.write(f"{REGION_VAR} = achievements\n")
        print(f"[DEBUG] Wrote full {OUTFILE}")
    else:
        PARTIAL_DIR = Path("partial_outputs")
        PARTIAL_DIR.mkdir(exist_ok=True)
        out_file = PARTIAL_DIR / f"{REGION}_batch_{BATCH_ID}.lua"
        with open(out_file, "w", encoding="utf-8") as f:
            f.write(f"-- Partial batch {BATCH_ID}/{TOTAL_BATCHES} for {REGION}\nlocal entries={{\n")
            for k, guid, ach_map in db_iter_rows():
                if k not in characters:
                    continue
                parts = [f'character="{k}"', "alts={}", f"guid={guid}"]
                for i, (aid, info) in enumerate(sorted(ach_map.items()), start=1):
                    esc = info["name"].replace('"', '\\"')
                    parts += [f"id{i}={aid}", f'name{i}="{esc}"']
                f.write("    { " + ", ".join(parts) + " },\n")
            f.write("}\n")
        print(f"[DEBUG] Wrote partial {out_file}")

    db.close()

# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------
if __name__ == "__main__":
    # 1) seed from existing full Lua
    old_chars = seed_db_from_lua(OUTFILE)
    print(f"[DEBUG] seed_db_from_lua loaded {len(old_chars)} prior entries")

    # 2) fetch leaderboard characters
    token = get_access_token(REGION)
    headers = {"Authorization": f"Bearer {token}"}
    raw_api = get_available_brackets  # silence unused
    # get_characters_from_leaderboards already returns { id: character_dict, … }
    api_chars_intkey = get_characters_from_leaderboards(REGION, headers, PVP_SEASON_ID, BRACKETS)

    # 3) normalize to "name-realm" keys
    api_chars = {
        f"{c['name'].lower()}-{c['realm'].lower()}": c
        for c in api_chars_intkey.values()
    }
    leaderboard_keys = set(api_chars)

    # 4) merge old + new
    merged = {**api_chars, **old_chars}

    # 5) slice for this batch
    keys = sorted(merged)
    BATCH_SIZE = 2500
    slice_keys = keys[BATCH_ID * BATCH_SIZE : (BATCH_ID + 1) * BATCH_SIZE]
    characters = {k: merged[k] for k in slice_keys}
    print(f"[FINAL DEBUG] Total chars this run: {len(characters)}")

    # if we're in batch mode but there are no chars left, do a finalize instead
    if MODE == "batch" and not characters:
        print("[INFO] No more characters left; switching to finalize mode automatically.")
        MODE = "finalize"

    # 6) process slice
    try:
        asyncio.run(process_characters(characters, leaderboard_keys))
    except CancelledError:
        print(f"{YELLOW}[WARN] Top-level run was cancelled, exiting.{RESET}")
