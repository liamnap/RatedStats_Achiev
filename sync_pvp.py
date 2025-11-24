#!/usr/bin/env python3
import os
import sys
import shutil
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
import email.utils as eut  # for HTTP-date parsing from Retry-After
from pathlib import Path
from collections import deque, Counter
from urllib.parse import urlparse
from asyncio import TimeoutError, CancelledError, create_task, as_completed, shield

try:
    import psutil
except ImportError:
    psutil = None

def is_lfs_pointer(path: Path) -> bool:
    """
    Return True if the file looks like a Git LFS pointer (not the real content).
    Detects the standard pointer header + oid line and keeps a small-size guard.
    """
    try:
        if not path.exists():
            return False
        # pointer stubs are tiny (usually <200 bytes)
        if path.stat().st_size > 1024:
            return False
        head = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return False
    return ("version https://git-lfs.github.com/spec" in head) and ("oid sha256:" in head)


def find_region_lua_paths(region: str) -> list[Path]:
    """
    Repo-root only:
      - region_{r}.lua
      - region_{r}-*.lua
      - region_{r}_part*.lua
    Skips Git LFS pointer stubs.
    """
    r = region.lower()
    candidates = [
        Path(f"region_{r}.lua"),
        *sorted(Path(".").glob(f"region_{r}-*.lua")),
        *sorted(Path(".").glob(f"region_{r}_part*.lua")),
    ]
    out: list[Path] = []
    for p in candidates:
        if p.exists():
            if is_lfs_pointer(p):
                print(f"[WARN] Skipping LFS pointer {p.name}")
            elif p.stat().st_size > 0:
                out.append(p)
    # de-dup while preserving order
    seen = set()
    uniq = []
    for p in out:
        if p not in seen and p.exists():
            uniq.append(p)
            seen.add(p)
    return uniq

def region_seed_candidates(region: str) -> list[Path]:
    """
    Return all on-disk files we should use to seed characters for a region.
    This includes:
      - monolithic:       region_{r}.lua
      - split (current):  region_{r}-*.lua
      - split (legacy):   region_{r}_part*.lua
    """
    return find_region_lua_paths(region)

def seed_db_from_lua_paths(paths: list[Path]) -> dict:
    """Seed the DB from any/all region Lua files provided."""
    rows: dict[str, dict] = {}
    if not paths:
        return rows
    row_rx  = re.compile(
        r'\{(?:[^{}]|\{[^{}]*\})*?character\s*=\s*"([^"]+)"(?:[^{}]|\{[^{}]*\})*?\}',
        re.S
    )
    ach_rx  = re.compile(r'id(\d+)\s*=\s*(\d+),\s*name\1\s*=\s*"([^"]+)"')
    guid_rx = re.compile(r"guid\s*=\s*(\d+)")
    alts_rx = re.compile(r"alts\s*=\s*\{\s*([^}]*)\s*\}")
    for lua_path in paths:
        try:
            txt = lua_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        for m in row_rx.finditer(txt):
            block = m.group(0)
            key = m.group(1).lower()  # normalize: name-realm
            gm = guid_rx.search(block)
            if not gm:
                continue
            guid = int(gm.group(1))
            ach = {int(aid): {"name": name, "ts": None}
                   for _, aid, name in ach_rx.findall(block)}
            db_upsert(key, guid, ach)
            if "-" in key:
                n, r = key.split("-", 1)
                rows[key] = {"id": guid, "name": n, "realm": r}
            # seed alt keys (id 0 → will be filled on fetch)
            am = alts_rx.search(block)
            if am:
                for alt in am.group(1).split(","):
                    altk = alt.strip().strip('"').lower()
                    if altk and altk not in rows and "-" in altk:
                        an, ar = altk.split("-", 1)
                        rows[altk] = {"id": 0, "name": an, "realm": ar}
    db.commit()
    return rows

# --------------------------------------------------------------------------
# CLI + MODE + REGION + BATCH SETTINGS
# --------------------------------------------------------------------------
parser = argparse.ArgumentParser(description="PvP sync runner")
parser.add_argument(
    "--mode",
    choices=["batch", "finalize"],
    default=None,
    help="Mode: 'batch' to emit partials, 'finalize' to write full Lua",
)
parser.add_argument(
   "--region", required=True,
   help="Region code: us, eu, kr, tw (must be explicitly passed)"
)
parser.add_argument(
    "--batch-id",
    type=int,
    default=int(os.getenv("BATCH_ID", "0")),
    help="0-based batch index for batch mode",
)
parser.add_argument(
    "--total-batches",
    type=int,
    default=int(os.getenv("TOTAL_BATCHES", "1")),
    help="Total number of batches for batch mode",
)

# ── Flags for matrix‐driven batching ──
parser.add_argument(
    "--list-ids-only",
    action="store_true",
    help="Print the total number of characters and exit",
)
parser.add_argument(
    "--with-brackets",
    action="store_true",
    help="When used with --list-ids-only, also include current leaderboard characters via the Blizzard API",
)
parser.add_argument(
    "--offset", type=int, default=0, help="Skip this many characters at start"
)
parser.add_argument(
    "--limit", type=int, default=None, help="Process at most this many characters"
)
parser.add_argument(
    "--cred_suffix",
    default=None,
    help="(dispatcher) Force use of this Blizzard client suffix",
)

args = parser.parse_args()

REGION = args.region.lower()
if REGION not in ("us", "eu", "kr", "tw"):
    print(f"[ERROR] Invalid region: {args.region!r}. Must be one of: us, eu, kr, tw")
    sys.exit(1)

CRED_SUFFIX_FORCE = args.cred_suffix


#def _emit_list_ids_only(region: str) -> None:
#    """Print union of keys from local Lua + bracket leaderboards (if available)."""
#    keys: set[str] = set()
#    char_rx = re.compile(r'character\s*=\s*"([^"]+)"')
#    alts_rx = re.compile(r"alts\s*=\s*\{\s*([^}]*)\s*\}")
#    # Consider both the main and _party variant
#    keys: set[str] = set()
#    char_rx = re.compile(r'character\s*=\s*"([^"]+)"')
#    alts_rx = re.compile(r"alts\s*=\s*\{\s*([^}]*)\s*\}")
#    for p in find_region_lua_paths(region):
#        try:
#            text = p.read_text(encoding="utf-8")
#        except Exception:
#            continue
#        for m in char_rx.finditer(text):
#            keys.add(m.group(1).lower())
#        for m in alts_rx.finditer(text):
#            for alt in m.group(1).split(","):
#                keys.add(alt.strip().strip('"').lower())
#    # Try to add bracket keys too (useful on first run when Lua is empty)
#    try:
#        token = get_access_token(region)
#        season = get_current_pvp_season_id(region)
#        brs = get_available_brackets(region, season)
#        headers = {"Authorization": f"Bearer {token}"}
#        for c in get_characters_from_leaderboards(
#            region, headers, season, brs
#        ).values():
#            keys.add(f"{c['name'].lower()}-{c['realm'].lower()}")
#    except Exception as e:
#        print(f"[WARN] list-ids-only: failed to include bracket keys: {e}")
#    for k in sorted(keys):
#        print(k)
#    sys.exit(0)

REGION = args.region
BATCH_ID = args.batch_id
TOTAL_BATCHES = args.total_batches
MODE = args.mode or "batch"

# --------------------------------------------------------------------------
# Globals & Constants
# --------------------------------------------------------------------------
UTC = datetime.timezone.utc
start_time = time.time()
CALLS_DONE = 0
TOTAL_CALLS = None
HTTP_429_QUEUED = 0
CALL_TIMES = deque()
# Latest Retry-After hint (in seconds) observed from a 429 response
RETRY_AFTER_HINT = 0

GREEN, YELLOW, RED, RESET = "\033[92m", "\033[93m", "\033[91m", "\033[0m"
OUTFILE = Path(f"region_{REGION}.lua")
REGION_VAR = f"ACHIEVEMENTS_{REGION.upper()}"
LOCALES = {"us": "en_US", "eu": "en_GB", "kr": "ko_KR", "tw": "zh_TW"}
LOCALE = LOCALES.get(REGION, "en_US")
API_HOST = f"{REGION}.api.blizzard.com"
API_BASE = f"https://{API_HOST}"
NAMESPACE_PROFILE = f"profile-{REGION}"
CRED_SUFFIX_USED = "_1"
REGION_HAS_FALLBACK = REGION in ("us", "eu", "tw", "kr")
SWITCHED_TO_429 = False
REGION_IDS = {"us": 1, "kr": 2, "eu": 3, "tw": 4}
REGION_ID = REGION_IDS[REGION.lower()]

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def _fmt_duration(sec: int) -> str:
    if sec <= 0:
        return "0s"
    parts = []
    for name, length in [
        ("y", 31_557_600),
        ("w", 604_800),
        ("d", 86_400),
        ("h", 3_600),
        ("m", 60),
    ]:
        qty, sec = divmod(sec, length)
        if qty:
            parts.append(f"{qty}{name}")
    if sec:
        parts.append(f"{sec}s")
    return " ".join(parts)


def _bump_calls():
    global CALLS_DONE
    CALLS_DONE += 1
    now = time.monotonic()
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
        self.capacity = max_calls
        self.tokens = 0
        self.fill_rate = max_calls / period
        self.timestamp = time.monotonic()
        self._lock = asyncio.Lock()
        self.max_calls = max_calls
        self.period = period
        self.calls = []

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
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
    global CRED_SUFFIX_USED

    # If dispatcher passed a suffix, honor it and skip internal fallback
    suffix = CRED_SUFFIX_FORCE or CRED_SUFFIX_USED
    region_upper = region.upper()
    cid_var = f"BLIZZARD_CLIENT_ID_{region_upper}{suffix}"
    cs_var = f"BLIZZARD_CLIENT_SECRET_{region_upper}{suffix}"

    cid = os.getenv(cid_var)
    cs = os.getenv(cs_var)

    if not cid or not cs:
        raise RuntimeError(f"[FATAL] Missing credentials for {cid_var}/{cs_var}")

    print(f"[AUTH] Using {cid_var}")
    resp = requests.post(
        "https://us.battle.net/oauth/token",
        data={"grant_type": "client_credentials"},
        auth=(cid, cs),
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_current_pvp_season_id(region: str) -> int:
    url = (
        f"{API_BASE}/data/wow/pvp-season/index?namespace=dynamic-{region}&locale=en_US"
    )
    token = get_access_token(region)
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    resp.raise_for_status()
    return resp.json()["seasons"][-1]["id"]


def get_available_brackets(region: str, season_id: int) -> list[str]:
    url = f"{API_BASE}/data/wow/pvp-season/{season_id}/pvp-leaderboard/index?namespace=dynamic-{region}&locale={LOCALE}"
    token = get_access_token(region)
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    if not resp.ok:
        raise RuntimeError(
            f"[FAIL] Unable to fetch PvP leaderboard index for season {season_id}: {resp.status_code}"
        )
    lbs = resp.json().get("leaderboards", [])
    prefixes = ("2v2", "3v3", "rbg", "shuffle-", "blitz-")
    brackets = []
    for entry in lbs:
        href = entry.get("key", {}).get("href", "")
        b = urlparse(href).path.rstrip("/").split("/")[-1]
        if b.startswith(prefixes):
            brackets.append(b)
    print(f"[INFO] Valid brackets for season {season_id}: {', '.join(brackets)}")
    return brackets


# --------------------------------------------------------------------------
# Season & Bracket initialization (skip all network in finalize)
# --------------------------------------------------------------------------
CACHE_DIR = Path("partial_outputs")
CACHE_DIR.mkdir(exist_ok=True)
BRACKET_CACHE = CACHE_DIR / f"{REGION}_brackets.json"

if MODE == "finalize":
    PVP_SEASON_ID = None
    BRACKETS = []
else:
    if BRACKET_CACHE.exists():
        cached = json.loads(BRACKET_CACHE.read_text())
        PVP_SEASON_ID = cached["season_id"]
        BRACKETS = cached["brackets"]
    else:
        PVP_SEASON_ID = get_current_pvp_season_id(REGION)
        BRACKETS = get_available_brackets(REGION, PVP_SEASON_ID)
        try:
            BRACKET_CACHE.write_text(
                json.dumps({"season_id": PVP_SEASON_ID, "brackets": BRACKETS})
            )
        except Exception:
            pass


# --------------------------------------------------------------------------
# Fetch PvP leaderboard characters
# --------------------------------------------------------------------------
def get_characters_from_leaderboards(
    region: str, headers: dict, season_id: int, brackets: list[str]
) -> dict[int, dict]:
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
        resp = requests.get(
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

# ── list-only short-circuit (now placed after all needed defs) ──
if args.list_ids_only:
    region = args.region
    keys: set[str] = set()

    # Track leaders vs alts separately for stats
    lua_leaders: set[str] = set()
    lua_alts: set[str] = set()

    char_rx = re.compile(r'character\s*=\s*"([^"]+)"')
    alts_rx = re.compile(r"alts\s*=\s*\{\s*([^}]*)\s*\}")

    # 1) repo-root region files first
    for p in find_region_lua_paths(region):
        try:
            text = p.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue

        # leaders
        for m in char_rx.finditer(text):
            k = m.group(1).lower()
            lua_leaders.add(k)
            keys.add(k)

        # alts
        for m in alts_rx.finditer(text):
            for alt in m.group(1).split(","):
                altk = alt.strip().strip('"').lower()
                if not altk:
                    continue
                lua_alts.add(altk)
                keys.add(altk)

    # 2) union with bracket “seen” (best-effort), with per-bracket stats
    bracket_counts: dict[str, int] = {}
    api_chars: dict[int, dict] = {}
    try:
        season_id = get_current_pvp_season_id(region)
        brackets = get_available_brackets(region, season_id)
        token = get_access_token(region)
        headers = {"Authorization": f"Bearer {token}"}

        for bracket in brackets:
            url = (
                f"https://{region}.api.blizzard.com/"
                f"data/wow/pvp-season/{season_id}/pvp-leaderboard/{bracket}"
                f"?namespace=dynamic-{region}&locale={LOCALE}"
            )
            resp = requests.get(url, headers=headers)
            if resp.status_code != 200:
                print(
                    f"[WARN] list-ids-only: failed leaderboard {bracket}: {resp.status_code}",
                    file=sys.stderr,
                )
                continue

            data = resp.json()
            entries = data.get("entries", [])
            bracket_counts[bracket] = len(entries)

            for entry in entries:
                c = entry.get("character")
                if not c:
                    continue
                cid = c["id"]
                if cid in api_chars:
                    continue
                api_chars[cid] = {
                    "name": c["name"],
                    "realm": c["realm"]["slug"],
                }

        # Update union of keys with bracket chars (name-realm)
        keys.update(
            f"{c['name'].lower()}-{c['realm'].lower()}" for c in api_chars.values()
        )

    except Exception as e:
        print(f"[WARN] list-ids-only: bracket union skipped: {e}", file=sys.stderr)
        bracket_counts = {}

    # 3) stats to STDERR (so wc -l only sees the raw key list)
    lua_leader_count = len(lua_leaders)
    lua_alt_count = len(lua_alts)
    lua_total = len(lua_leaders | lua_alts)
    bracket_union_count = len(api_chars)
    bracket_raw_sum = sum(bracket_counts.values())
    total_to_process = len(keys)

    print(
        f"[STATS] Region={region} | Lua leaders={lua_leader_count}",
        file=sys.stderr,
    )
    print(
        f"[STATS] Region={region} | Lua alts={lua_alt_count}",
        file=sys.stderr,
    )
    print(
        f"[STATS] Region={region} | Lua total (leaders+alts)={lua_total}",
        file=sys.stderr,
    )

    for b in sorted(bracket_counts):
        print(
            f"[STATS] Region={region} | Bracket {b}: {bracket_counts[b]} characters (raw entries)",
            file=sys.stderr,
        )

    print(
        f"[STATS] Region={region} | Bracket raw sum (entries across all brackets)={bracket_raw_sum}",
        file=sys.stderr,
    )
    print(
        f"[STATS] Region={region} | Bracket union (unique chars across all brackets)={bracket_union_count}",
        file=sys.stderr,
    )
    print(
        f"[STATS] Region={region} | TOTAL chars to process (Lua ∪ brackets)={total_to_process}",
        file=sys.stderr,
    )

    # 4) print merged + deduped keys to STDOUT (this is what wc -l counts)
    for k in sorted(keys):
        print(k)
    sys.exit(0)

# Avoid external call when finalizing: we don't hit achievement APIs then.
NAMESPACE_STATIC = (
    get_latest_static_namespace(REGION) if MODE != "finalize" else f"static-{REGION}"
)
print(f"[INFO] Region: {REGION}, Locale: {LOCALE}, Static NS: {NAMESPACE_STATIC}")

# --------------------------------------------------------------------------
# Rate limiters and in-memory cache
# --------------------------------------------------------------------------
REGION_CAP = 20 if REGION in ("us", "eu") else 100
per_sec = RateLimiter(REGION_CAP, 1)
per_hour = RateLimiter(36000, 3600)
SEM_CAP = REGION_CAP
url_cache = {}
METRICS = {"total": 0, "200": 0, "429": 0, "4xx": 0, "5xx": 0, "exceptions": 0}


async def fetch_with_rate_limit(session, url, headers, max_retries=5):
    cacheable = "profile/wow/character" not in url and "oauth" not in url
    if cacheable and url in url_cache:
        return url_cache[url]

    METRICS["total"] += 1
    await per_sec.acquire()
    await per_hour.acquire()

    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    METRICS["200"] += 1
                    # --- DEBUG: dump headers & body for status 200 ---
                    # raw_body = await resp.text()
                    # print(f"[DEBUG-200] URL: {url}")
                    # print("Response headers:")
                    # for hk, hv in resp.headers.items():
                    #    print(f"  {hk}: {hv}")
                    # print("Response body (first 5000 chars):\n", raw_body[:5000])
                    # --- end debug ---
                    # then parse as JSON
                    data = await resp.json()
                    if cacheable:
                        url_cache[url] = data
                    _bump_calls()
                    return data
                if resp.status == 429:
                    METRICS["429"] += 1
                    global HTTP_429_QUEUED, RETRY_AFTER_HINT, CRED_SUFFIX_USED, SWITCHED_TO_429
                    HTTP_429_QUEUED += 1
                    # --- DEBUG: dump entire response ---
                    # body = await resp.text()
                    # print(f"[DEBUG-429] URL: {url}")
                    # print("Response headers:")
                    # for hk, hv in resp.headers.items():
                    #    print(f"  {hk}: {hv}")
                    # print("Response body:\n", body)
                    # --- end debug dump ---
                    # Read Retry-After (seconds or HTTP-date). Header name is case-insensitive.
                    ra_val = resp.headers.get("Retry-After", "")
                    ra_secs = 0
                    if ra_val:
                        # Try integer seconds first
                        try:
                            ra_secs = int(ra_val.strip())
                        except ValueError:
                            # Fallback: parse HTTP-date and convert to seconds from now
                            try:
                                dt = eut.parsedate_to_datetime(ra_val)
                                if dt:
                                    # dt may be naive UTC or tz-aware; normalize to UTC seconds
                                    ra_secs = max(
                                        0,
                                        int(
                                            (
                                                dt
                                                - datetime.datetime.now(
                                                    datetime.timezone.utc
                                                )
                                            ).total_seconds()
                                        ),
                                    )
                            except Exception:
                                ra_secs = 0
                    # Keep the largest hint seen (if bursts yield multiple 429s)
                    if ra_secs > RETRY_AFTER_HINT:
                        RETRY_AFTER_HINT = ra_secs
                    # Credential fallback logic
                    if not SWITCHED_TO_429 and REGION_HAS_FALLBACK:
                        print(
                            f"[WARN] Switching to fallback credentials due to 429 (was {CRED_SUFFIX_USED})"
                        )
                        CRED_SUFFIX_USED = "_429"
                        SWITCHED_TO_429 = True
                    # Always treat as retryable, even after switching keys
                    # print(f"[RATE-LIMIT] 429 for {url} Retry-After='{ra_val or 'n/a'}' → hint={RETRY_AFTER_HINT}s", flush=True)
                    raise RateLimitExceeded()
                if 500 <= resp.status < 600:
                    METRICS["5xx"] += 1
                    raise RateLimitExceeded()
                resp.raise_for_status()
        except asyncio.TimeoutError:
            METRICS["exceptions"] += 1
            await asyncio.sleep(2**attempt)
    raise RuntimeError(f"fetch failed for {url} after {max_retries} retries")


# --------------------------------------------------------------------------
# Achievement keywords list (unchanged)
# --------------------------------------------------------------------------
async def get_pvp_achievements(session, headers):
    idx = await fetch_with_rate_limit(
        session,
        f"{API_BASE}/data/wow/achievement/index?namespace={NAMESPACE_STATIC}&locale=en_US",
        headers,
    )
    KEYWORDS = [
        # Main Achievements
        {"type": "exact", "value": "Scout"},
        {"type": "exact", "value": "Private"},
        {"type": "exact", "value": "Grunt"},
        {"type": "exact", "value": "Corporal"},
        {"type": "exact", "value": "Sergeant"},
        {"type": "exact", "value": "Senior Sergeant"},
        {"type": "exact", "value": "Master Sergeant"},
        {"type": "exact", "value": "First Sergeant"},
        {"type": "exact", "value": "Sergeant Major"},
        {"type": "exact", "value": "Stone Guard"},
        {"type": "exact", "value": "Knight"},
        {"type": "exact", "value": "Blood Guard"},
        {"type": "exact", "value": "Knight-Lieutenant"},
        {"type": "exact", "value": "Legionnaire"},
        {"type": "exact", "value": "Knight-Captain"},
        {"type": "exact", "value": "Centurion"},
        {"type": "exact", "value": "Knight-Champion"},
        {"type": "exact", "value": "Champion"},
        {"type": "exact", "value": "Lieutenant Commander"},
        {"type": "exact", "value": "Lieutenant General"},
        {"type": "exact", "value": "Commander"},
        # Rated PvP Season Tiers
        {"type": "prefix", "value": "Combatant I"},
        {"type": "prefix", "value": "Combatant II"},
        {"type": "prefix", "value": "Challenger I"},
        {"type": "prefix", "value": "Challenger II"},
        {"type": "prefix", "value": "Rival I"},
        {"type": "prefix", "value": "Rival II"},
        {"type": "prefix", "value": "Duelist"},
        # 2200 - 2400 frommain achievements
        {"type": "exact", "value": "General"},
        {"type": "exact", "value": "Marshal"},
        {"type": "exact", "value": "Warlord"},
        {"type": "exact", "value": "Field Marshal"},
        {"type": "exact", "value": "High Warlord"},
        {"type": "exact", "value": "Grand Marshal"},
        # Return to Rated PvP Season Tiers
        {"type": "prefix", "value": "Elite:"},
        {"type": "prefix", "value": "Gladiator:"},
        {"type": "prefix", "value": "Legend:"},
        # Special Achievements
        {"type": "prefix", "value": "Three's Company: 2700"},
        # R1 Titles
        {"type": "prefix", "value": "Hero of the Horde"},
        {"type": "prefix", "value": "Hero of the Alliance"},
        {"type": "prefix", "value": "Primal Gladiator"},
        {"type": "prefix", "value": "Wild Gladiator"},
        {"type": "prefix", "value": "Warmongering Gladiator"},
        {"type": "prefix", "value": "Vindictive Gladiator"},
        {"type": "prefix", "value": "Fearless Gladiator"},
        {"type": "prefix", "value": "Cruel Gladiator"},
        {"type": "prefix", "value": "Ferocious Gladiator"},
        {"type": "prefix", "value": "Fierce Gladiator"},
        {"type": "prefix", "value": "Demonic Gladiator"},
        {"type": "prefix", "value": "Dread Gladiator"},
        {"type": "prefix", "value": "Sinister Gladiator"},
        {"type": "prefix", "value": "Notorious Gladiator"},
        {"type": "prefix", "value": "Corrupted Gladiator"},
        {"type": "prefix", "value": "Sinful Gladiator"},
        {"type": "prefix", "value": "Unchained Gladiator"},
        {"type": "prefix", "value": "Cosmic Gladiator"},
        {"type": "prefix", "value": "Eternal Gladiator"},
        {"type": "prefix", "value": "Crimson Gladiator"},
        {"type": "prefix", "value": "Obsidian Gladiator"},
        {"type": "prefix", "value": "Draconic Gladiator"},
        {"type": "prefix", "value": "Seasoned Gladiator"},
        {"type": "prefix", "value": "Forged Warlord:"},
        {"type": "prefix", "value": "Forged Marshal:"},
        {"type": "prefix", "value": "Forged Legend:"},
        {"type": "prefix", "value": "Forged Gladiator:"},
        {"type": "prefix", "value": "Prized Warlord:"},
        {"type": "prefix", "value": "Prized Marshal:"},
        {"type": "prefix", "value": "Prized Legend:"},
        {"type": "prefix", "value": "Prized Gladiator:"},
        {"type": "prefix", "value": "Astral Warlord:"},
        {"type": "prefix", "value": "Astral Marshal:"},
        {"type": "prefix", "value": "Astral Legend:"},
        {"type": "prefix", "value": "Astral Gladiator:"},
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
db = sqlite3.connect(DB_PATH)
db.execute(
    """
    CREATE TABLE IF NOT EXISTS char_data (
        key TEXT PRIMARY KEY,
        guid INTEGER,
        ach_json TEXT
    )
"""
)


def db_upsert(key: str, guid: int, ach: dict):
    db.execute(
        "INSERT OR REPLACE INTO char_data (key,guid,ach_json) VALUES (?,?,?)",
        (key, guid, json.dumps(ach, separators=(",", ":"))),
    )


def db_iter_rows():
    cur = db.execute("SELECT key,guid,ach_json FROM char_data ORDER BY key")
    for k, g, j in cur:
        yield k, g, json.loads(j)

def _merge_ach_json(existing_json: str | None, incoming_json: str) -> str:
    """
    Merge two ach_json blobs (both are JSON dicts: aid -> {name, ts}).
    - Any ID only in incoming is added.
    - For IDs in both:
        * Prefer the one with a non-null ts.
        * If both have ts, prefer the later timestamp.
    This prevents stale Lua/old shards (ts=None, missing IDs) from
    overwriting fresher API data.
    """
    try:
        incoming = json.loads(incoming_json) if incoming_json else {}
    except Exception:
        incoming = {}
    try:
        existing = json.loads(existing_json) if existing_json else {}
    except Exception:
        existing = {}

    # Start with existing; layer incoming on top with the rules above.
    merged = dict(existing)

    for aid_str, info_new in incoming.items():
        info_old = merged.get(aid_str)
        if info_old is None:
            # Entirely new achievement
            merged[aid_str] = info_new
            continue

        ts_old = info_old.get("ts")
        ts_new = info_new.get("ts")

        # If new has a real timestamp and old doesn't, take new.
        if ts_old is None and ts_new is not None:
            merged[aid_str] = info_new
            continue

        # If new has no timestamp but old does, keep old.
        if ts_new is None and ts_old is not None:
            continue

        # Both None -> arbitrary, but keep existing.
        if ts_old is None and ts_new is None:
            continue

        # Both have timestamps: prefer the later one (best effort).
        try:
            if int(ts_new) >= int(ts_old):
                merged[aid_str] = info_new
        except Exception:
            # If we can't compare, just favour incoming.
            merged[aid_str] = info_new

    return json.dumps(merged, separators=(",", ":"))

    merged = 0
    for s in shards:
        try:
            with sqlite3.connect(s) as src:
                cnt = src.execute("SELECT COUNT(*) FROM char_data").fetchone()[0]
                print(f"[DEBUG] shard {s.name}: {cnt} rows")
                for k, g, j in src.execute("SELECT key,guid,ach_json FROM char_data"):
                    # Look for an existing row so we can merge achievements instead
                    # of letting this shard blindly overwrite fresher data.
                    cur = db.execute(
                        "SELECT guid,ach_json FROM char_data WHERE key=?", (k,)
                    )
                    row = cur.fetchone()
                    if row is None:
                        db.execute(
                            "INSERT INTO char_data (key,guid,ach_json) VALUES (?,?,?)",
                            (k, g, j),
                        )
                    else:
                        existing_guid, existing_json = row
                        merged_json = _merge_ach_json(existing_json, j)
                        db.execute(
                            "UPDATE char_data SET guid=?, ach_json=? WHERE key=?",
                            (existing_guid or g, merged_json, k),
                        )
                    merged += 1
        except Exception as e:
            print(f"[WARN] failed to merge shard {s}: {e}")
    db.commit()
    print(f"[DEBUG] merged {merged} row upserts from {len(shards)} shard(s)")
    return (merged, len(shards))

def merge_one_shard(shard_path: Path) -> int:
    """Merge rows from exactly one sqlite shard into the working DB.
    Returns upsert count.
    Uses _merge_ach_json so that newer API data is never clobbered by
    stale Lua/old shard content.
    """
    if not shard_path.exists():
        print(f"[ERROR] Shard not found: {shard_path}")
        return 0
    merged = 0
    try:
        with sqlite3.connect(shard_path) as src:
            cnt = src.execute("SELECT COUNT(*) FROM char_data").fetchone()[0]
            print(f"[DEBUG] shard {shard_path.name}: {cnt} rows")
            for k, g, j in src.execute("SELECT key,guid,ach_json FROM char_data"):
                cur = db.execute(
                    "SELECT guid,ach_json FROM char_data WHERE key=?", (k,)
                )
                row = cur.fetchone()
                if row is None:
                    db.execute(
                        "INSERT INTO char_data (key,guid,ach_json) VALUES (?,?,?)",
                        (k, g, j),
                    )
                else:
                    existing_guid, existing_json = row
                    merged_json = _merge_ach_json(existing_json, j)
                    db.execute(
                        "UPDATE char_data SET guid=?, ach_json=? WHERE key=?",
                        (existing_guid or g, merged_json, k),
                    )
                merged += 1
        db.commit()
        print(f"[DEBUG] merged {merged} row upserts from 1 shard")
    except Exception as e:
        print(f"[WARN] failed to merge shard {shard_path}: {e}")
    return merged

# --------------------------------------------------------------------------
# Main processing
# --------------------------------------------------------------------------
async def process_characters(characters: dict, leaderboard_keys: set):
    global HTTP_429_QUEUED, TOTAL_CALLS
    # Only need auth/HTTP if we're actually fetching characters
    if characters:
        token = get_access_token(REGION)
        headers = {"Authorization": f"Bearer {token}"}
    headers_seen_suffix = CRED_SUFFIX_USED
    inserted = 0
    TOTAL_CALLS = len(characters) + 1

    if characters:
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            pvp_achs = await get_pvp_achievements(session, headers)
            print(f"[DEBUG] PvP keywords loaded: {len(pvp_achs)}")

            per_sec.tokens = 0
            per_sec.timestamp = time.monotonic()
            per_sec.calls.clear()
            sem = asyncio.Semaphore(SEM_CAP)
            total = len(characters)
            completed = 0
            last_hb = time.monotonic()
            hb_prev_completed = 0
            hb_prev_429 = 0

            async def proc_one(c):
                nonlocal inserted
                async with sem:
                    name, realm, cid = c["name"].lower(), c["realm"].lower(), c["id"]
                    key = f"{name}-{realm}"
                    try:
                        data = await get_character_achievements(
                            session, headers, realm, name
                        )
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
            # allow overriding via env var for flexible sizing (safe default)
            BATCH_SIZE = int(os.getenv("BATCH_SIZE"))

            while remaining:
                # If we've just switched credentials, re-acquire token
                if SWITCHED_TO_429 and headers_seen_suffix != CRED_SUFFIX_USED:
                    token = get_access_token(REGION)
                    headers = {"Authorization": f"Bearer {token}"}
                    headers_seen_suffix = CRED_SUFFIX_USED
                retry_bucket = {}
                batches = (len(remaining) + BATCH_SIZE - 1) // BATCH_SIZE
                for i in range(batches):
                    batch = remaining[i * BATCH_SIZE : (i + 1) * BATCH_SIZE]
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
                            now = time.monotonic()
                            if (now - last_hb > 10) or (completed == total):
                                url_cache.clear()
                                gc.collect()
                                ts = time.strftime("%H:%M:%S", time.localtime())
                                sec_rate = len(per_sec.calls) / per_sec.period
                                avg60 = len(CALL_TIMES) / 60
                                rem_calls = (
                                    (TOTAL_CALLS - CALLS_DONE) if TOTAL_CALLS else None
                                )
                                elapsed = int(time.time() - start_time)
                                eta = (
                                    _fmt_duration(
                                        int((elapsed / CALLS_DONE) * rem_calls)
                                    )
                                    if CALLS_DONE and rem_calls
                                    else "–"
                                )
                                delta_done = completed - hb_prev_completed
                                delta_429 = HTTP_429_QUEUED - hb_prev_429
                                # Color thresholds
                                rate_window = per_sec.period
                                rate_threshold = sec_rate * rate_window
                                done_color = (
                                    RED if delta_done < 0.8 * rate_threshold else GREEN
                                )
                                err_color = (
                                    RED if delta_429 > 0.1 * rate_threshold else RESET
                                )
                                delta_done_str = f"{done_color}(+{delta_done}){RESET}"
                                delta_429_str = f"{err_color}(+{delta_429}){RESET}"
                                # True backlog view at this instant
                                pending_total = total - completed
                                retry_q_now = len(retry_bucket)
                                inflight = sum(1 for tt in tasks if not tt.done())
                                print(
                                    f"[{ts}] [HEARTBEAT] {completed}/{total}{delta_done_str} done ({completed/total*100:.1f}%), "
                                    f"sec_rate={sec_rate:.1f}/s, avg60={avg60:.1f}/s, "
                                    f"429s={HTTP_429_QUEUED}{delta_429_str}, "
                                    f"pending={pending_total}, retry_q={retry_q_now}, inflight={inflight}, "
                                    f"ETA={eta}, elapsed={_fmt_duration(elapsed)}",
                                    flush=True,
                                )
                                last_hb = now
                                hb_prev_completed = completed
                                hb_prev_429 = HTTP_429_QUEUED
                url_cache.clear()
                if retry_bucket:
                    queued = len(retry_bucket)
                    # If we saw a Retry-After, prefer that (but never less than our base interval)
                    global RETRY_AFTER_HINT
                    wait_for = max(retry_interval, int(RETRY_AFTER_HINT or 0))
                    print(
                        f"[{time.strftime('%H:%M:%S')}] [RETRY] {queued} queued after 429s; "
                        f"waiting {wait_for}s (Retry-After={RETRY_AFTER_HINT or 'n/a'})",
                        flush=True,
                    )
                    # Reset hint and sleep
                    RETRY_AFTER_HINT = 0
                    await asyncio.sleep(wait_for)
                    # loop will regenerate tasks using the refreshed headers
                    remaining = list(retry_bucket.values())
                else:
                    break

            db.commit()
            # Final summary heartbeat (prints even if the last update was <10s ago)
            ts = time.strftime("%H:%M:%S", time.localtime())
            sec_rate = len(per_sec.calls) / per_sec.period
            avg60 = len(CALL_TIMES) / 60
            delta_done = completed - hb_prev_completed
            delta_429 = HTTP_429_QUEUED - hb_prev_429
            # Color thresholds
            rate_window = per_sec.period
            rate_threshold = sec_rate * rate_window
            done_color = RED if delta_done < 0.8 * total else GREEN
            err_color = RED if delta_429 > 0.1 * total else RESET
            delta_done_str = f"{done_color}(+{delta_done}){RESET}"
            delta_429_str = f"{err_color}(+{delta_429}){RESET}"
            pct = (completed / total * 100) if total else 100.0
            elapsed = int(time.time() - start_time)
            pending_total = total - completed
            retry_q_now = 0
            inflight = 0
            print(
                f"[DEBUG] inserted={inserted}, SQLite rows={sum(1 for _ in db_iter_rows())}"
            )

            # ─── SUMMARY: Total API calls & HTTP response code breakdown ───
            print(
                "API‑calls summary: "
                f"total={METRICS['total']} | "
                f"200={METRICS['200']} | "
                f"429={METRICS['429']} | "
                f"5xx={METRICS['5xx']} | "
                f"exceptions={METRICS['exceptions']}",
                flush=True,
            )
            hb_prev_completed = completed
            hb_prev_429 = HTTP_429_QUEUED
            print(
                f"[DEBUG] inserted={inserted}, SQLite rows={sum(1 for _ in db_iter_rows())}"
            )

    # build fingerprints & alt_map
    fingerprints = {
        k: {
            (aid, info.get("ts") or 0) for aid, info in ach.items()
        }
        for k, _, ach in db_iter_rows()
    }
    alt_map = {k: [] for k in fingerprints}
    # scalable pair generation via inverted index over tokens (aid, ts)
    from collections import defaultdict

    bucket = defaultdict(list)  # token -> [char keys]
    for k, toks in fingerprints.items():
        for t in toks:
            bucket[t].append(k)
    pair_counts = defaultdict(int)
    # guard against giant buckets (very common tokens); skip if too large
    MAX_BUCKET = 1000
    for members in bucket.values():
        n = len(members)
        if n < 2 or n > MAX_BUCKET:
            continue
        for i in range(n - 1):
            ai = members[i]
            for j in range(i + 1, n):
                bj = members[j]
                if ai < bj:
                    pair_counts[(ai, bj)] += 1
                else:
                    pair_counts[(bj, ai)] += 1
    THRESH = 5
    for (a, b), cnt in pair_counts.items():
        if cnt >= THRESH:
            alt_map[a].append(b)
            alt_map[b].append(a)

    # connected components
    visited = set()
    groups = []
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

    # Prepare rows to write
    rows_map = {k: (g, ach) for k, g, ach in db_iter_rows()}

    if MODE == "finalize" and os.getenv("EXPORT_ONLY", "") == "1":
        total_mains = len(groups)                          # one main per group
        total_chars = sum(len(comp) for comp in groups)    # mains + alts
        total_alts = total_chars - total_mains
        print(
            f"[STATS] Region={REGION} finalize: mains={total_mains}, "
            f"alts={total_alts}, total_characters={total_chars}",
            flush=True,
        )
        
        entry_lines = []
        for comp in groups:
            real_leaders = [m for m in comp if m in leaderboard_keys]
            root = real_leaders[0] if real_leaders else comp[0]
            alts = [m for m in comp if m != root]
            guid, ach_map = rows_map[root]
            alts_str = "{" + ",".join(f'"{a}"' for a in alts) + "}"
            parts = [f'character="{root}"', f"alts={alts_str}", f"guid={guid}"]
            for i, (aid, info) in enumerate(sorted(ach_map.items()), start=1):
                esc = info["name"].replace('"', '\\"')
                parts += [f"id{i}={aid}", f'name{i}="{esc}"']
            entry_lines.append("    { " + ", ".join(parts) + " },\n")

        MAX_BYTES = int(os.getenv("MAX_LUA_PART_SIZE", str(49 * 1024 * 1024)))
        part_index = 1
        current_lines = []
        out_files = []

        def write_chunk(part_idx, lines, is_single_file):
            region_check = f"if GetCurrentRegion() ~= {REGION_ID} then return end\n"

            if is_single_file:
                varname = f"ACHIEVEMENTS_{REGION.upper()}"
                header = (
                    f"-- File: RatedStats_Achiev/region_{REGION}.lua\n"
                    f"{region_check}"
                    f"local achievements={{\n"
                )
                footer = f"}}\n\n{varname} = achievements\n"
                fname = OUTFILE
            else:
                varname = f"ACHIEVEMENTS_{REGION.upper()}_PART{part_idx}"
                header = (
                    f"-- File: RatedStats_Achiev/region_{REGION}_part{part_idx}.lua\n"
                    f"{region_check}"
                    f"local achievements={{\n"
                )
                footer = f"}}\n\n{varname} = achievements\n"
                # OUTFILE is a Path, so use .stem and join in the same directory
                fname = OUTFILE.parent / f"{OUTFILE.stem}_part{part_idx}.lua"

            content = header + "".join(lines) + footer
            with open(str(fname), "w", encoding="utf-8") as outf:
                outf.write(content)
            out_files.append(fname)
            print(
                f"[DEBUG] Wrote chunk: {fname.name} with ~{len(content.encode('utf-8'))} bytes"
            )

        # Try to chunk the data
        region_check = f"if GetCurrentRegion() ~= {REGION_ID} then return end\n"
        region_check_len = len(region_check.encode("utf-8"))

        for line in entry_lines:
            candidate = "".join(current_lines + [line])
            header_len = len(
                f"-- File: RatedStats_Achiev/region_{REGION}.lua\nlocal achievements={{\n".encode(
                    "utf-8"
                )
            )
            footer_len = len(
                f"}}\n\nACHIEVEMENTS_{REGION.upper()} = achievements\n".encode("utf-8")
            )

            total_size = (
                len(candidate.encode("utf-8"))
                + header_len
                + footer_len
                + region_check_len
            )

            if total_size > MAX_BYTES:
                write_chunk(part_index, current_lines, is_single_file=False)
                part_index += 1
                current_lines = [line]
            else:
                current_lines.append(line)

        # Final write
        if part_index == 1:
            # One chunk = use monolithic file
            write_chunk(1, current_lines, is_single_file=True)
        else:
            write_chunk(part_index, current_lines, is_single_file=False)
            # Clean up stale monolithic file
            if OUTFILE.exists():
                os.remove(OUTFILE)
                print(f"[DEBUG] Removed stale {OUTFILE.name}")

        print(f"[DEBUG] {len(out_files)} region files produced: {', '.join(f.name for f in out_files)}")

        final_marker = Path("partial_outputs") / f"{REGION}_final.marker"
        final_marker.parent.mkdir(exist_ok=True)
        final_marker.write_text("")
        print(f"[DEBUG] Wrote finalize marker {final_marker}")

    else:
        PARTIAL_DIR = Path("partial_outputs")
        PARTIAL_DIR.mkdir(exist_ok=True)
        out_file = PARTIAL_DIR / f"{REGION}_batch_{BATCH_ID}.lua"
        with open(out_file, "w", encoding="utf-8") as f:
            f.write(
                f"-- Partial batch {BATCH_ID}/{TOTAL_BATCHES} for {REGION}\nlocal entries={{\n"
            )
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
        # Single-batch guarantee: when running in --mode batch, do *not* process
        # any additional batches and do not fall through to finalize.
        if MODE == "batch":
            # export the SQLite shard so CI can upload it
            shard = Path("partial_outputs") / f"achdb_{REGION}_b{BATCH_ID}.sqlite"
            try:
                shutil.copy2(DB_PATH, shard)
                print(f"[DEBUG] Wrote DB shard {shard}")
            except Exception as e:
                print(f"[WARN] failed to export DB shard: {e}")
            return


# ─────────────────────────────────────────────────────────────────────────────
# Main entrypoint: seed + fetch + merge + batching loop + finalize
# ──────────────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
# Main entrypoint: seed + fetch + merge + batching loop + finalize
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Decide early if this is a streaming shard-merge or an export-only finalize.
    one_shard = os.getenv("ONE_SHARD", "").strip()
    export_only = os.getenv("EXPORT_ONLY", "") == "1"

    # 1) Seed from any existing region Lua files (mono or split),
    #    but DO NOT do this in:
    #      - EXPORT_ONLY finalize (final pass in CI), or
    #      - streaming shard merges (ONE_SHARD set), which build up
    #        /tmp/achiev_{REGION}.db incrementally.
    #
    # In CI:
    #   - shard merges:  ONE_SHARD is set  → skip seeding, just merge shard.
    #   - EXPORT_ONLY:   export_only=True  → skip seeding/shard scan, just export DB.
    if (not export_only) and (not one_shard):
        old_chars = seed_db_from_lua_paths(find_region_lua_paths(REGION))
    else:
        old_chars = {}

    if MODE == "finalize":
        # Optional fast-path: consume exactly one shard, then exit (used by CI streaming merge).
        if one_shard:
            merged = merge_one_shard(Path(one_shard))
            db.close()
            sys.exit(0)

        # offline finalize: do not hit any APIs
        api_chars = {}
        leaderboard_keys = set()
        chars = {**old_chars}
    else:
        # 2) fetch bracket‐API chars
        token = get_access_token(REGION)
        headers = {"Authorization": f"Bearer {token}"}
        api_chars_intkey = get_characters_from_leaderboards(
            REGION, headers, PVP_SEASON_ID, BRACKETS
        )
        api_chars = {
            f"{c['name'].lower()}-{c['realm'].lower()}": c
            for c in api_chars_intkey.values()
        }
        leaderboard_keys = set(api_chars)

        # 3) merge bracket + seeded chars (prefer API metadata where present)
        chars = {**old_chars, **api_chars}

    # 4) behavior depends on MODE
    all_keys = sorted(chars)
    batch_size = int(os.getenv("BATCH_SIZE"))
    computed_total = (len(all_keys) + batch_size - 1) // batch_size

    if MODE == "batch":
        # process exactly one batch, then exit
        batch_id = int(BATCH_ID) if "BATCH_ID" in globals() else 0
        total_batches = (
            int(TOTAL_BATCHES) if "TOTAL_BATCHES" in globals() else computed_total
        )
        # Prefer explicit CLI window when provided; otherwise use batch math
        start = args.offset if args.offset else batch_id * batch_size
        cur_limit = args.limit if args.limit else batch_size
        # Overshoot guard: still emit empty partial + shard so uploads don't fail
        if start >= len(all_keys):
            print(f"[INFO] Region={REGION} batch {batch_id+1}/{total_batches}: 0 chars "
                  f"(offset {start} ≥ keycount {len(all_keys)}). "
                  f"Likely missing seeded region Lua in this runner.")
            PARTIAL_DIR = Path("partial_outputs")
            PARTIAL_DIR.mkdir(exist_ok=True)
            empty_partial = PARTIAL_DIR / f"{REGION}_batch_{BATCH_ID}.lua"
            with open(empty_partial, "w", encoding="utf-8") as f:
                # Double braces {{}} emit a literal {} in the file
                f.write(
                    f"-- Partial batch {BATCH_ID}/{TOTAL_BATCHES} for {REGION}\n"
                    f"local entries={{}}\n"
                )
            shard = PARTIAL_DIR / f"achdb_{REGION}_b{BATCH_ID}.sqlite"
            try:
                # Always export a shard (even if empty DB) so artifact upload succeeds
                db.commit()
                shutil.copy2(DB_PATH, shard)
            except Exception as e:
                print(f"[WARN] failed to export empty DB shard: {e}")
            db.close()
            sys.exit(0)
        slice_keys = all_keys[start : start + cur_limit]        
        characters = {k: chars[k] for k in slice_keys}
        print(
            f"[INFO] Region={REGION} batch {batch_id+1}/{total_batches}: {len(characters)} chars"
        )
        try:
            asyncio.run(process_characters(characters, leaderboard_keys))
        except CancelledError:
            print(f"{YELLOW}[WARN] batch {batch_id+1} cancelled, exiting.{RESET}")
            sys.exit(1)
        db.close()
        sys.exit(0)

    elif MODE == "finalize":
        # Only finalize (no re-fetch): merge shards when running "offline",
        # but in CI streaming mode (EXPORT_ONLY=1) we *already* merged all shards
        # into /tmp/achiev_{REGION}.db via ONE_SHARD passes.
        print(f"[INFO] Finalizing region {REGION}")

        if not export_only:
            # Offline / local finalize: look for shards in OUTDIR if provided,
            # otherwise fall back to ./partial_outputs.
            shard_root = Path(os.getenv("OUTDIR", "partial_outputs"))
            merged_rows, shard_count = merge_db_shards(shard_root)
            if shard_count == 0 or merged_rows == 0:
                # No shards? That's okay — we already seeded from any region_*.lua above.
                print("⚠️ No DB shards merged; finalizing from existing region_*.lua seed.")
        else:
            # In CI EXPORT_ONLY run:
            # - /tmp/achiev_{REGION}.db already contains the union of all shards
            #   (merged by earlier ONE_SHARD calls).
            # - We deliberately skip merge_db_shards and also skipped Lua seeding,
            #   so we write exactly what the DB contains.
            print("[INFO] EXPORT_ONLY finalize: using pre-merged SQLite DB; skipping shard scan & Lua seeding.")

        # In streaming CI, export is run in a separate pass with EXPORT_ONLY=1.
        asyncio.run(process_characters({}, leaderboard_keys))
        db.close()
        sys.exit(0)

    else:
        print(f"{RED}[ERROR] Unknown MODE={MODE}{RESET}")
        sys.exit(2)
