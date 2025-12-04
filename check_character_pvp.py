#!/usr/bin/env python3
import os
import sys
import json
import argparse
import re
import sqlite3
import requests
from pathlib import Path
from urllib.parse import urlparse

LOCALE_MAP = {
    "us": "en_US",
    "eu": "en_GB",
    "kr": "ko_KR",
    "tw": "zh_TW",
}

def is_lfs_pointer(path: Path) -> bool:
    try:
        if not path.exists():
            return False
        if path.stat().st_size > 1024:
            return False
        head = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return False
    return ("version https://git-lfs.github.com/spec" in head) and ("oid sha256:" in head)

def find_region_lua_paths(region: str):
    r = region.lower()
    candidates = [Path(f"region_{r}.lua"),
                  *sorted(Path(".").glob(f"region_{r}-*.lua")),
                  *sorted(Path(".").glob(f"region_{r}_part*.lua"))]
    out = []
    for p in candidates:
        if p.exists() and not is_lfs_pointer(p) and p.stat().st_size > 0:
            out.append(p)
    seen = set()
    uniq = []
    for p in out:
        if p not in seen:
            uniq.append(p)
            seen.add(p)
    return uniq

def seed_from_lua_for_character(region: str, character_key: str):
    paths = find_region_lua_paths(region)
    # Match full character blocks that contain a character="..." field.
    row_rx = re.compile(
        r'\{(?:[^{}]|\{[^{}]*\})*?character\s*=\s*"([^"]+)"(?:[^{}]|\{[^{}]*\})*?\}',
        re.S,
    )
    # Match idN / nameN pairs inside a block.
    ach_rx = re.compile(r'id(\d+)\s*=\s*(\d+),\s*name\1\s*=\s*"([^"]+)"')
    # Optionally match alts={ "alt1","alt2",... } inside a block.
    alt_rx = re.compile(r'alts\s*=\s*\{([^}]*)\}')

    target = character_key.lower()

    for lua_path in paths:
        try:
            txt = lua_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue

        for m in row_rx.finditer(txt):
            block = m.group(0)
            main_key = m.group(1).lower()

            # Collect any alt names defined on this row.
            alt_keys = []
            alt_m = alt_rx.search(block)
            if alt_m:
                alt_body = alt_m.group(1)
                alt_keys = [s.lower() for s in re.findall(r'"([^"]+)"', alt_body)]

            # We treat the row as the baseline if the character is either:
            # - the main "character=...", or
            # - listed in alts={...}.
            if target != main_key and target not in alt_keys:
                continue

            ach = {
                int(aid): {"name": name, "ts": None}
                for _, aid, name in ach_rx.findall(block)
            }
            return ach

    return {}

def get_access_token(region: str):
    region_upper = region.upper()
    cid_var = f"CHAR_PVP_ACHIEVEMENTS_ID"
    cs_var = f"CHAR_PVP_ACHIEVEMENTS_SECRET"
    cid = os.getenv(cid_var)
    cs = os.getenv(cs_var)
    if not cid or not cs:
        print(f"[ERROR] Missing credentials for {cid_var}/{cs_var}", file=sys.stderr)
        sys.exit(2)
    resp = requests.post(
        "https://us.battle.net/oauth/token",
        data={"grant_type": "client_credentials"},
        auth=(cid, cs),
    )
    resp.raise_for_status()
    return resp.json().get("access_token")

def fetch_character_pvp_achievements(region: str, name: str, realm: str):
    region_lower = region.lower()
    locale = LOCALE_MAP.get(region_lower, "en_US")
    api_host = f"{region_lower}.api.blizzard.com"
    namespace = f"profile-{region_lower}"
    token = get_access_token(region_lower)
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://{api_host}/profile/wow/character/{realm}/{name.lower()}/achievements?namespace={namespace}&locale={locale}"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    achievements = {}
    for ach in data.get("achievements", []):
        aid = ach["id"]
        ts = ach.get("completed_timestamp")
        achievements[aid] = {"name": ach["achievement"]["name"], "ts": ts}
    return achievements

def get_current_pvp_season_id(region: str) -> int:
    """
    Synchronous copy of the daily runner's logic:
    hit the PvP season index for this region and take the latest season id.
    """
    region_lower = region.lower()
    api_host = f"{region_lower}.api.blizzard.com"
    url = (
        f"https://{api_host}/data/wow/pvp-season/index"
        f"?namespace=dynamic-{region_lower}&locale=en_US"
    )
    token = get_access_token(region_lower)
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    seasons = resp.json().get("seasons") or []
    if not seasons:
        raise RuntimeError(f"No PvP seasons returned for region={region_lower}")
    return seasons[-1]["id"]

def get_available_brackets(region: str, season_id: int) -> list[str]:
    """
    Mirror of the daily sync behaviour: list all PvP leaderboards for the
    current season and keep only the brackets we actually care about.
    """
    region_lower = region.lower()
    api_host = f"{region_lower}.api.blizzard.com"
    locale = LOCALE_MAP.get(region_lower, "en_US")
    url = (
        f"https://{api_host}/data/wow/pvp-season/{season_id}/pvp-leaderboard/index"
        f"?namespace=dynamic-{region_lower}&locale={locale}"
    )
    token = get_access_token(region_lower)
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    lbs = resp.json().get("leaderboards", []) or []

    prefixes = ("2v2", "3v3", "rbg", "shuffle-", "blitz-")
    brackets: list[str] = []
    for entry in lbs:
        href = entry.get("key", {}).get("href") or ""
        if not href:
            continue
        try:
            path = urlparse(href).path
        except Exception:
            continue
        b = path.rstrip("/").split("/")[-1]
        if b.startswith(prefixes):
            brackets.append(b)
    return brackets

def inspect_character_brackets(region: str, name: str, realm: str):
    """
    For the current season and the same bracket set the daily runner uses,
    check whether this character is present and, if so, at what rating/rank.

    This uses *current* leaderboards, so if you run it long after the daily
    job the membership might have changed – but the logic is identical.
    """
    region_lower = region.lower()
    locale = LOCALE_MAP.get(region_lower, "en_US")

    try:
        season_id = get_current_pvp_season_id(region_lower)
        brackets = get_available_brackets(region_lower, season_id)
    except Exception as e:
        print(
            f"\n[WARN] Failed to initialise PvP leaderboards for region={region_lower}: {e}",
            file=sys.stderr,
        )
        return

    if not brackets:
        print(
            f"\n[INFO] No PvP brackets discovered for region={region_lower}; "
            "skipping leaderboard presence check."
        )
        return

    token = get_access_token(region_lower)
    headers = {"Authorization": f"Bearer {token}"}

    target_name = name.lower()
    target_realm = realm.lower()
    matches = []

    for bracket in brackets:
        url = (
            f"https://{region_lower}.api.blizzard.com/"
            f"data/wow/pvp-season/{season_id}/pvp-leaderboard/{bracket}"
            f"?namespace=dynamic-{region_lower}&locale={locale}"
        )
        try:
            resp = requests.get(url, headers=headers)
            if resp.status_code != 200:
                print(
                    f"[WARN] Leaderboard fetch failed for {bracket}: {resp.status_code}; skipping.",
                    file=sys.stderr,
                )
                continue
            data = resp.json()
        except Exception as e:
            print(f"[WARN] Error reading leaderboard {bracket}: {e}", file=sys.stderr)
            continue

        entries = data.get("entries", []) or []
        if not entries:
            continue

        found = None
        for idx, entry in enumerate(entries, start=1):
            c = entry.get("character") or {}
            cname = (c.get("name") or "").lower()
            crealm = (c.get("realm", {}).get("slug") or "").lower()

            if cname == target_name and crealm == target_realm:
                rating = entry.get("rating")
                rank = entry.get("rank")
                found = {
                    "bracket": bracket,
                    "rating": rating,
                    "rank": rank,
                    "index": idx,
                }
                break

        if found:
            matches.append(found)

    print("\n=== Current PvP Leaderboard Presence (daily bracket set) ===")
    if not matches:
        print(
            f"{name}-{realm} is not present in any of the tracked brackets "
            f"for region={region_lower}."
        )
        return

    headers_row = ["bracket", "rating", "rank", "entry_index"]
    rows = [
        [
            m["bracket"],
            str(m["rating"] if m["rating"] is not None else "-"),
            str(m["rank"] if m["rank"] is not None else "-"),
            str(m["index"]),
        ]
        for m in matches
    ]

    col_widths = [
        max(len(headers_row[i]), *(len(row[i]) for row in rows))
        for i in range(len(headers_row))
    ]

    def _fmt_row(row: list[str]) -> str:
        return "  ".join(cell.ljust(col_widths[i]) for i, cell in enumerate(row))

    print(_fmt_row(headers_row))
    for row in rows:
        print(_fmt_row(row))

def diff_baseline_vs_api(baseline: dict, api: dict):
    missing_in_lua = [aid for aid in api if aid not in baseline]
    missing_in_api = [aid for aid in baseline if aid not in api]
    timestamp_changed = [aid for aid in api
                         if aid in baseline and baseline[aid]["name"] == api[aid]["name"]
                         and baseline[aid].get("ts") is not None and api[aid].get("ts") is not None
                         and baseline[aid]["ts"] != api[aid]["ts"]]
    return {
        "missing_in_lua": missing_in_lua,
        "missing_in_api": missing_in_api,
        "timestamp_changed": timestamp_changed,
    }

def generate_lua_snippet(character_key: str, guid: int, ach_map: dict):
    parts = [f'character="{character_key}"', f"guid={guid}"]
    for i, (aid, info) in enumerate(sorted(ach_map.items()), start=1):
        esc = info["name"].replace('"', '\\"')
        parts.append(f"id{i}={aid}")
        parts.append(f'name{i}="{esc}"')
    return "{ " + ", ".join(parts) + " }"

# ---------------------------------------------------------------------------
# Alt-detection from merged SQLite DB (achiev_<region>.db)
# ---------------------------------------------------------------------------

ALT_SHARED_THRESHOLD = 10  # min shared (aid, timestamp) pairs to consider a candidate


def _load_merged_db(region: str) -> sqlite3.Connection | None:
    region = region.lower()
    db_path = os.environ.get("MERGED_DB_PATH") or f"./achiev_{region}.db"
    p = Path(db_path)
    if not p.exists():
        print(f"\n[INFO] No merged DB found at {p}; skipping alt detection.", file=sys.stderr)
        return None
    try:
        conn = sqlite3.connect(str(p))
    except Exception as e:
        print(f"\n[WARN] Failed to open merged DB {p}: {e}", file=sys.stderr)
        return None
    return conn


def _load_char_rows(conn: sqlite3.Connection) -> dict:
    """
    Return:
      rows = {
        "name-realm": {
           "guid": int,
           "ach": { aid: {"name": str, "ts": int|None}, ... }
        },
        ...
      }
    """
    rows = {}
    cur = conn.execute("SELECT key, guid, ach_json FROM char_data")
    for key, guid, ach_json in cur:
        k = (key or "").lower()
        if not k:
            continue
        try:
            raw = json.loads(ach_json)
        except Exception:
            continue
        ach_map = {}
        if isinstance(raw, dict):
            for aid_str, info in raw.items():
                try:
                    aid = int(aid_str)
                except (TypeError, ValueError):
                    continue
                if not isinstance(info, dict):
                    continue
                name = info.get("name", "")
                ts = info.get("ts")
                ach_map[aid] = {"name": name, "ts": ts}
        rows[k] = {"guid": guid, "ach": ach_map}
    return rows


def _build_tokens(ach: dict[int, dict]) -> set[tuple[int, int]]:
    """
    Build a set of (achievement_id, completed_timestamp) tokens.
    Only entries with a non-null, integer timestamp are included.
    """
    tokens: set[tuple[int, int]] = set()
    for aid, info in ach.items():
        ts = info.get("ts")
        if ts is None:
            continue
        try:
            ts_int = int(ts)
        except (TypeError, ValueError):
            continue
        tokens.add((aid, ts_int))
    return tokens

def load_sqlite_snapshot_for_character(region: str, char_realm: str):
    """
    Load a single character's achievement map from the merged DB:
      { aid: { "name": str, "ts": int|None }, ... }
    Returns None if DB missing or char not present.
    """
    conn = _load_merged_db(region)
    if conn is None:
        return None

    key = char_realm.lower()
    try:
        cur = conn.execute(
            "SELECT guid, ach_json FROM char_data WHERE key = ?", (key,)
        )
        row = cur.fetchone()
    except Exception as e:
        print(
            f"\n[WARN] Failed to query merged DB for {key}: {e}",
            file=sys.stderr,
        )
        conn.close()
        return None

    conn.close()

    if row is None:
        print(
            f"\n[INFO] Character {key} not found in merged DB; skipping SQLite comparison.",
            file=sys.stderr,
        )
        return None

    guid, ach_json = row
    try:
        raw = json.loads(ach_json)
    except Exception as e:
        print(
            f"\n[WARN] Failed to decode ach_json for {key} from merged DB: {e}",
            file=sys.stderr,
        )
        return None

    ach_map: dict[int, dict] = {}
    if isinstance(raw, dict):
        for aid_str, info in raw.items():
            try:
                aid = int(aid_str)
            except (TypeError, ValueError):
                continue
            if not isinstance(info, dict):
                continue
            name = info.get("name", "")
            ts = info.get("ts")
            ach_map[aid] = {"name": name, "ts": ts}

    return {"guid": guid, "ach": ach_map}

def run_alt_detection(region: str, char_realm: str):
    """
    Uses merged DB (achiev_<region>.db) to find alt candidates for char_realm
    by matching identical (achievement_id, completed_timestamp) pairs.
    """
    conn = _load_merged_db(region)
    if conn is None:
        return

    try:
        rows = _load_char_rows(conn)
    finally:
        conn.close()

    target_key = char_realm.lower()
    if target_key not in rows:
        print(f"\n[WARN] Character {target_key} not found in merged DB; no alt analysis.", file=sys.stderr)
        return

    target = rows[target_key]
    target_tokens = _build_tokens(target["ach"])
    if not target_tokens:
        print(f"\n[WARN] Character {target_key} has no timestamped PvP achievements in merged DB.", file=sys.stderr)
        return

    candidates = []
    for key, row in rows.items():
        if key == target_key:
            continue
        tokens = _build_tokens(row["ach"])
        if not tokens:
            continue
        shared = target_tokens & tokens
        shared_count = len(shared)
        if shared_count >= ALT_SHARED_THRESHOLD:
            candidates.append(
                {
                    "key": key,
                    "guid": row["guid"],
                    "shared": shared_count,
                    "total_main": len(target_tokens),
                    "total_other": len(tokens),
                    # keep the actual shared (aid, ts) pairs so we can print them later
                    "shared_tokens": shared,
                }
            )

    candidates.sort(key=lambda c: c["shared"], reverse=True)

    print(f"\n=== Alt candidates from merged DB for {target_key} (region={region}) ===")
    print(f"Main GUID: {target['guid']}")
    print(f"Timestamped PvP achievements on main: {len(target_tokens)}")
    print()

    if not candidates:
        print(f"No alt candidates met threshold (shared timestamped achievements ≥ {ALT_SHARED_THRESHOLD}).")
        return

    # Compact, aligned summary table
    headers = ["candidate_key", "guid", "shared_pairs", "main_pairs", "candidate_pairs"]
    rows_out = [
        [
            c["key"],
            str(c["guid"]),
            str(c["shared"]),
            str(c["total_main"]),
            str(c["total_other"]),
        ]
        for c in candidates
    ]
    col_widths = [
        max(len(h), *(len(row[i]) for row in rows_out))
        for i, h in enumerate(headers)
    ]

    def _fmt_row(row: list[str]) -> str:
        return "  ".join(
            cell.ljust(col_widths[i]) for i, cell in enumerate(row)
        )

    print("candidate summary:")
    print(_fmt_row(headers))
    for row in rows_out:
        print(_fmt_row(row))

    # Detailed breakdown of shared achievements per candidate
    print("\n=== Shared timestamped achievements per candidate ===")
    for c in candidates:
        key = c["key"]
        shared_tokens = sorted(c["shared_tokens"])
        print(
            f"\n-- {key} -- shared={len(shared_tokens)} "
            f"(main={c['total_main']}, candidate={c['total_other']})"
        )
        print("id\tname\tmain_ts\tcandidate_ts")
        for aid, ts in shared_tokens:
            main_info = rows[target_key]["ach"].get(aid, {})
            cand_info = rows[key]["ach"].get(aid, {})
            name = main_info.get("name") or cand_info.get("name") or ""
            print(
                f"{aid}\t{name}\t"
                f"{main_info.get('ts')}\t{cand_info.get('ts')}"
            )

def main():
    parser = argparse.ArgumentParser(description="Check single character PvP achievements")
    parser.add_argument("--region", required=True, help="Region code: us, eu, kr, tw")
    parser.add_argument("--character-realm", required=True,
                        help="Character name-realm (e.g., MyChar-MyRealm)")
    parser.add_argument("--guid", type=int, default=0,
                        help="Optionally supply guid if known")
    parser.add_argument(
        "--check-alts",
        dest="check_alts",
        action="store_true",
        help="Also scan merged DB for alt candidates"
    )
    args = parser.parse_args()
    
    region = args.region.lower()
    char_realm = args.character_realm.lower()
    if "-" not in char_realm:
        print(f"[ERROR] character-realm must be in format name-realm", file=sys.stderr)
        sys.exit(1)
    name, realm = char_realm.split("-", 1)

    baseline_full = seed_from_lua_for_character(region, char_realm)
    # filter baseline to the same KEYWORDS list
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

    baseline = {}
    for aid, info in baseline_full.items():
        for kw in KEYWORDS:
            if (kw["type"] == "exact" and info["name"] == kw["value"]) or \
               (kw["type"] == "prefix" and info["name"].startswith(kw["value"])):
                baseline[aid] = info
                break

    print("=== Lua Baseline ===")
    for aid, info in sorted(baseline.items()):
        print(f"{aid}\t{info['name']}")

    api_map_full = fetch_character_pvp_achievements(region, name, realm)
    # filter API results to KEYWORDS
    api_map = {}
    for aid, info in api_map_full.items():
        for kw in KEYWORDS:
            if (kw["type"] == "exact" and info["name"] == kw["value"]) or \
               (kw["type"] == "prefix" and info["name"].startswith(kw["value"])):
                api_map[aid] = info
                break

    print("\n=== API Scan ===")
    for aid, info in sorted(api_map.items()):
        print(f"{aid}\t{info['name']}\t{info.get('ts')}")

    diff = diff_baseline_vs_api(baseline, api_map)
    print("\n\n=== Differences ===")
    print("Missing in Lua (API only):")
    for aid in diff["missing_in_lua"]:
        print(f"{aid}\t{api_map[aid]['name']}")
    print("\nMissing in API (Lua only):")
    for aid in diff["missing_in_api"]:
        print(f"{aid}\t{baseline[aid]['name']}")
    print("\nTimestamp changed:")
    for aid in diff["timestamp_changed"]:
        print(f"{aid}\t{baseline[aid]['name']}\tLuaTS={baseline[aid]['ts']}\tAPITS={api_map[aid]['ts']}")

    print("\n=== Suggested Lua Code Snippet ===")
    snippet = generate_lua_snippet(char_realm, args.guid, api_map)
    print(snippet)

    # SQLite comparison using merged DB (if available)
    sqlite_snapshot = load_sqlite_snapshot_for_character(region, char_realm)
    if sqlite_snapshot is not None:
        sqlite_full = sqlite_snapshot["ach"]
        sqlite_map: dict[int, dict] = {}
        for aid, info in sqlite_full.items():
            for kw in KEYWORDS:
                if (kw["type"] == "exact" and info["name"] == kw["value"]) or \
                   (kw["type"] == "prefix" and info["name"].startswith(kw["value"])):
                    sqlite_map[aid] = info
                    break

        print("\n=== SQLite Snapshot (merged DB) ===")
        for aid, info in sorted(sqlite_map.items()):
            print(f"{aid}\t{info['name']}\t{info.get('ts')}")

        sqlite_diff = diff_baseline_vs_api(sqlite_map, api_map)
        print("\n=== Differences vs merged SQLite (DB → API) ===")
        print("Missing in SQLite DB (API only):")
        for aid in sqlite_diff["missing_in_lua"]:
            print(f"{aid}\t{api_map[aid]['name']}")
        print("\nMissing in API (SQLite DB only):")
        for aid in sqlite_diff["missing_in_api"]:
            print(f"{aid}\t{sqlite_map[aid]['name']}")
        print("\nTimestamp changed (SQLite DB vs API):")
        for aid in sqlite_diff["timestamp_changed"]:
            print(
                f"{aid}\t{sqlite_map[aid]['name']}"
                f"\tDBTS={sqlite_map[aid]['ts']}\tAPITS={api_map[aid]['ts']}"
            )
    
    # Alt detection using merged SQLite DB (if requested and available)
    if args.check_alts:
        run_alt_detection(region, char_realm)

    # Bracket presence check using the same bracket set as the daily runner
    inspect_character_brackets(region, name, realm)

    sys.exit(0)
    
if __name__ == "__main__":
    main()
