#!/usr/bin/env python3
import os
import sys
import json
import argparse
import re
import requests
from pathlib import Path

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
    row_rx = re.compile(r'\{(?:[^{}]|\{[^{}]*\})*?character\s*=\s*"([^"]+)"(?:[^{}]|\{[^{}]*\})*?\}', re.S)
    ach_rx = re.compile(r'id(\d+)\s*=\s*(\d+),\s*name\1\s*=\s*"([^"]+)"')
    for lua_path in paths:
        try:
            txt = lua_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        for m in row_rx.finditer(txt):
            block = m.group(0)
            key = m.group(1).lower()
            if key != character_key:
                continue
            ach = {int(aid): {"name": name, "ts": None}
                   for _, aid, name in ach_rx.findall(block)}
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
    locale_map = {"us": "en_US", "eu": "en_GB", "kr": "ko_KR", "tw": "zh_TW"}
    locale = locale_map.get(region_lower, "en_US")
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

def main():
    parser = argparse.ArgumentParser(description="Check single character PvP achievements")
    parser.add_argument("--region", required=True, help="Region code: us, eu, kr, tw")
    parser.add_argument("--character-realm", required=True,
                        help="Character name-realm (e.g., MyChar-MyRealm)")
    parser.add_argument("--guid", type=int, default=0,
                        help="Optionally supply guid if known")
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

    sys.exit(0)

if __name__ == "__main__":
    main()
