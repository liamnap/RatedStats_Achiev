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
parser.add_argument("--mode", choices=["batch","finalize"], default=None,
                    help="Mode: 'batch' to emit partials, 'finalize' to write full Lua")
parser.add_argument("--region", default=os.getenv("REGION","eu"),
                    help="Region code: us, eu, kr, tw")
parser.add_argument("--batch-id",      type=int, default=int(os.getenv("BATCH_ID","0")),
                    help="0-based batch index for batch mode")
parser.add_argument("--total-batches", type=int, default=int(os.getenv("TOTAL_BATCHES","1")),
                    help="Total number of batches for batch mode")
args = parser.parse_args()

REGION        = args.region
BATCH_ID      = args.batch_id
TOTAL_BATCHES = args.total_batches
MODE          = args.mode or ("finalize" if TOTAL_BATCHES == 1 else "batch")

# --------------------------------------------------------------------------
# Basic imports & globals
# --------------------------------------------------------------------------
UTC        = datetime.timezone.utc
start_time = time.time()
CALLS_DONE    = 0
TOTAL_CALLS   = None
HTTP_429_QUEUED = 0
CALL_TIMES    = deque()

GREEN, YELLOW, RED, RESET = "\033[92m","\033[93m","\033[91m","\033[0m"
OUTFILE       = Path(f"region_{REGION}.lua")
REGION_VAR    = f"ACHIEVEMENTS_{REGION.upper()}"
LOCALES       = {"us":"en_US","eu":"en_GB","kr":"ko_KR","tw":"zh_TW"}
LOCALE        = LOCALES.get(REGION, "en_US")
API_HOST      = f"{REGION}.api.blizzard.com"
API_BASE      = f"https://{API_HOST}"
NAMESPACE_PROFILE = f"profile-{REGION}"

# --------------------------------------------------------------------------
# Utils
# --------------------------------------------------------------------------
def _fmt_duration(sec:int)->str:
    if sec<=0: return "0s"
    parts=[]
    for name, length in [("y",31_557_600),("w",604_800),("d",86400),("h",3600),("m",60)]:
        qty, sec = divmod(sec, length)
        if qty: parts.append(f"{qty}{name}")
    if sec: parts.append(f"{sec}s")
    return " ".join(parts)

def _bump_calls():
    global CALLS_DONE
    CALLS_DONE+=1
    now=time.time()
    CALL_TIMES.append(now)
    while CALL_TIMES and now-CALL_TIMES[0]>60:
        CALL_TIMES.popleft()

class RetryCharacter(Exception):
    def __init__(self,char): super().__init__(f"Retry {char['name']}-{char['realm']}"); self.char=char

class RateLimitExceeded(Exception): pass

# --------------------------------------------------------------------------
# RateLimiter
# --------------------------------------------------------------------------
class RateLimiter:
    def __init__(self,max_calls:int,period:float):
        self.capacity=max_calls; self.tokens=0
        self.fill_rate=max_calls/period; self.timestamp=time.monotonic()
        self._lock=asyncio.Lock(); self.max_calls=max_calls; self.period=period; self.calls=[]
    async def acquire(self):
        async with self._lock:
            now=time.monotonic()
            self.tokens=min(self.capacity,self.tokens+(now-self.timestamp)*self.fill_rate)
            self.timestamp=now
            if self.tokens<1:
                await asyncio.sleep((1-self.tokens)/self.fill_rate)
                self.timestamp=time.monotonic(); self.tokens=1
            self.tokens-=1
            self.calls=[t for t in self.calls if now-t<self.period]
            self.calls.append(now)

# --------------------------------------------------------------------------
# Auth & Blizzard helpers
# --------------------------------------------------------------------------
def get_access_token(region):
    if region=="eu" and os.getenv("BLIZZARD_CLIENT_ID_EU"):
        cid,cs=os.getenv("BLIZZARD_CLIENT_ID_EU"),os.getenv("BLIZZARD_CLIENT_SECRET_EU")
    elif region=="us" and os.getenv("BLIZZARD_CLIENT_ID_US"):
        cid,cs=os.getenv("BLIZZARD_CLIENT_ID_US"),os.getenv("BLIZZARD_CLIENT_SECRET_US")
    else:
        cid,cs=os.getenv("BLIZZARD_CLIENT_ID"),os.getenv("BLIZZARD_CLIENT_SECRET")
    resp=requests.post("https://us.battle.net/oauth/token",
                       data={"grant_type":"client_credentials"},auth=(cid,cs))
    resp.raise_for_status()
    return resp.json()["access_token"]

def get_current_pvp_season_id(region):
    url=f"https://{region}.api.blizzard.com/data/wow/pvp-season/index?namespace=dynamic-{region}&locale=en_US"
    token=get_access_token(region)
    resp=requests.get(url,headers={"Authorization":f"Bearer {token}"})
    resp.raise_for_status()
    return resp.json()["seasons"][-1]["id"]

def get_available_brackets(region,season_id):
    url=f"https://{region}.api.blizzard.com/data/wow/pvp-season/{season_id}/pvp-leaderboard/index?namespace=dynamic-{region}&locale={LOCALE}"
    token=get_access_token(region)
    resp=requests.get(url,headers={"Authorization":f"Bearer {token}"})
    if not resp.ok:
        raise RuntimeError(f"[FAIL] Unable to fetch PvP leaderboard index for season {season_id}: {resp.status_code}")
    lbs=resp.json().get("leaderboards",[])
    prefixes=("2v2","3v3","rbg","shuffle-","blitz-")
    brackets=[urlparse(e["key"]["href"]).path.rstrip("/").split("/")[-1]
              for e in lbs if e.get("key","").get("href","").split("/")[-1].startswith(prefixes)]
    print(f"[INFO] Valid brackets for season {season_id}: {', '.join(brackets)}")
    return brackets

# --------------------------------------------------------------------------
# Load or fetch & cache season/brackets
# --------------------------------------------------------------------------
CACHE_DIR = Path("partial_outputs"); CACHE_DIR.mkdir(exist_ok=True)
BRACKET_CACHE = CACHE_DIR/f"{REGION}_brackets.json"
if MODE=="batch" and BRACKET_CACHE.exists():
    data=json.loads(BRACKET_CACHE.read_text())
    PVP_SEASON_ID, BRACKETS = data["season_id"], data["brackets"]
else:
    PVP_SEASON_ID = get_current_pvp_season_id(REGION)
    BRACKETS      = get_available_brackets(REGION,PVP_SEASON_ID)
    try:
        BRACKET_CACHE.write_text(json.dumps({"season_id":PVP_SEASON_ID,"brackets":BRACKETS}))
    except: pass

# --------------------------------------------------------------------------
# Static namespace
# --------------------------------------------------------------------------
def get_latest_static_namespace(region):
    fallback=f"static-{region}"
    try:
        token=get_access_token("us")
        resp=requests.get(
            f"https://{region}.api.blizzard.com/data/wow/achievement-category/index?namespace={fallback}&locale=en_US",
            headers={"Authorization":f"Bearer {token}"})
        if not resp.ok: return fallback
        href=resp.json().get("_links",{}).get("self",{}).get("href","")
        if "namespace=" in href:
            return href.split("namespace=")[-1].split("&")[0]
    except:
        pass
    return fallback

NAMESPACE_STATIC = get_latest_static_namespace(REGION)
print(f"[INFO] Region: {REGION}, Locale: {LOCALE}, Static NS: {NAMESPACE_STATIC}")

# --------------------------------------------------------------------------
# Rate limiters + URL cache
# --------------------------------------------------------------------------
REGION_CAP=9 if REGION in ("us","eu") else 100
per_sec=RateLimiter(REGION_CAP,1)
per_hour=RateLimiter(36000,3600)
SEM_CAPACITY=REGION_CAP
url_cache={}

async def fetch_with_rate_limit(session,url,headers,max_retries=5):
    cacheable=("profile/wow/character" not in url and "oauth" not in url)
    if cacheable and url in url_cache:
        return url_cache[url]
    await per_sec.acquire(); await per_hour.acquire()
    for attempt in range(1,max_retries+1):
        try:
            async with session.get(url,headers=headers) as resp:
                if resp.status==200:
                    data=await resp.json()
                    if cacheable: url_cache[url]=data
                    _bump_calls()
                    return data
                if resp.status==429:
                    global HTTP_429_QUEUED; HTTP_429_QUEUED+=1
                    raise RateLimitExceeded()
                if 500<=resp.status<600:
                    raise RateLimitExceeded()
                resp.raise_for_status()
        except asyncio.TimeoutError:
            await asyncio.sleep(2**attempt)
    raise RuntimeError(f"fetch failed {url}")

# --------------------------------------------------------------------------
# Achievement discovery
# --------------------------------------------------------------------------
async def get_pvp_achievements(session,headers):
    idx=await fetch_with_rate_limit(session,
        f"{API_BASE}/data/wow/achievement/index?namespace={NAMESPACE_STATIC}&locale=en_US",headers)
    KEYWORDS=[{"type":"exact","value":v} for v in [
        "Scout",
	"Private",
	"Grunt",
	"Corporal",
	"Sergeant",
	"Senior Sergeant",
	"Master Sergeant",
        "First Sergeant",
	"Sergeant Major",
	"Stone Guard",
	"Knight",
	"Blood Guard",
	"Knight-Lieutenant",
        "Legionnaire",
	"Knight-Captain",
	"Centurion",
	"Knight-Champion",
	"Champion",
	"Lieutenant Commander",
	"Lieutenant General",
	"Commander",
	"General",
	"Marshal",
	"Warlord",
        "Field Marshal",
	"High Warlord",
	"Grand Marshal"
    ]]+[{"type":"prefix", "value":p} for p in [
        "Combatant I",
	"Combatant II",
	"Challenger I",
	"Challenger II",
	"Rival I",
	"Rival II",
        "Duelist",
	"Elite:",
	"Gladiator:",
	"Legend:",
	"Three's Company: 2700",
        "Hero of the Horde",
	"Hero of the Alliance",
	"Primal Gladiator",
	"Wild Gladiator",
        "Warmongering Gladiator",
	"Vindictive Gladiator",
	"Fearless Gladiator",
	"Cruel Gladiator",
        "Ferocious Gladiator",
	"Fierce Gladiator",
	"Demonic Gladiator",
	"Dread Gladiator",
        "Sinister Gladiator",
	"Notorious Gladiator",
	"Corrupted Gladiator",
	"Sinful Gladiator",
        "Unchained Gladiator",
	"Cosmic Gladiator",
	"Eternal Gladiator",
	"Crimson Gladiator",
        "Obsidian Gladiator",
	"Draconic Gladiator",
	"Seasoned Gladiator",
	"Forged Warlord:",
        "Forged Marshal:",
	"Forged Legend:",
	"Forged Gladiator:",
	"Prized Warlord:",
        "Prized Marshal:",
	"Prized Legend:",
	"Prized Gladiator:"
    ]]
    matches={}
    for ach in idx.get("achievements",[]):
        name=ach.get("name","")
        for kw in KEYWORDS:
            if (kw["type"]=="exact" and name==kw["value"]) or \
               (kw["type"]=="prefix" and name.startswith(kw["value"])):
                matches[ach["id"]]=name; break
    print(f"[DEBUG] Total PvP keyword matches: {len(matches)}")
    return matches

async def get_character_achievements(session,headers,realm,name):
    url=f"{API_BASE}/profile/wow/character/{realm}/{name.lower()}/achievements?namespace={NAMESPACE_PROFILE}&locale={LOCALE}"
    return await fetch_with_rate_limit(session,url,headers) or None

# --------------------------------------------------------------------------
# SQLite-backed cache + seed-from-Lua
# --------------------------------------------------------------------------
DB_PATH=Path(tempfile.gettempdir())/f"achiev_{REGION}.db"
db=sqlite3.connect(DB_PATH)
db.execute("""CREATE TABLE IF NOT EXISTS char_data (key TEXT PRIMARY KEY,guid INTEGER,ach_json TEXT)""")

def db_upsert(key:str,guid:int,ach_dict:dict)->None:
    db.execute("INSERT OR REPLACE INTO char_data (key,guid,ach_json) VALUES (?,?,?)",
               (key,guid,json.dumps(ach_dict,separators=(',',':'))))

def db_iter_rows():
    cur=db.execute("SELECT key,guid,ach_json FROM char_data ORDER BY key")
    for key,guid,aj in cur:
        yield key,guid,json.loads(aj)

def seed_db_from_lua(lua_path:Path)->dict:
    rows={}
    if not lua_path.exists(): return rows
    txt=lua_path.read_text(encoding="utf-8")
    row_rx=re.compile(r'\{[^{]*?character\s*=\s*"([^"]+)"[^}]*?\}',re.S)
    ach_rx=re.compile(r'id(\d+)\s*=\s*(\d+),\s*name\1\s*=\s*"([^"]+)"')
    guid_rx=re.compile(r'guid\s*=\s*(\d+)')
    for m in row_rx.finditer(txt):
        block=m.group(0); key=m.group(1)
        gm=guid_rx.search(block)
        if not gm: continue
        guid=int(gm.group(1))
        achs={int(aid):name for _,aid,name in ach_rx.findall(block)}
        db_upsert(key,guid,achs)
        name,realm=key.split("-",1)
        rows[key]={"id":guid,"name":name,"realm":realm}
    db.commit()
    return rows

# --------------------------------------------------------------------------
# Main processing pipeline
# --------------------------------------------------------------------------
async def process_characters(characters, leaderboard_keys):
    global HTTP_429_QUEUED, TOTAL_CALLS
    token=get_access_token(REGION)
    headers={"Authorization":f"Bearer {token}"}
    inserted_count=0
    TOTAL_CALLS=len(characters)+1

    timeout=aiohttp.ClientTimeout(total=5)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        pvp_achievements=await get_pvp_achievements(session,headers)
        print(f"[DEBUG] PvP keywords loaded: {len(pvp_achievements)}")

        per_sec.tokens=0; per_sec.timestamp=asyncio.get_event_loop().time(); per_sec.calls.clear()
        sem=asyncio.Semaphore(SEM_CAPACITY)
        total=len(characters); completed=0; last_hb=time.time()

        async def proc_one(c):
            nonlocal inserted_count
            async with sem:
                name,realm,id=c["name"].lower(),c["realm"].lower(),c["id"]
                key=f"{name}-{realm}"
                try:
                    data=await get_character_achievements(session,headers,realm,name)
                except RateLimitExceeded:
                    raise RetryCharacter(c)
                if not data: return
                ach_list=data.get("achievements",[])
                ach_dict={}
                for ach in ach_list:
                    aid=ach["id"]
                    if aid not in pvp_achievements: continue
                    ts=ach.get("completed_timestamp")
                    ach_dict[aid]={"name":ach["achievement"]["name"],"ts":ts}
                if ach_dict:
                    db_upsert(key,id,ach_dict)
                    inserted_count+=1

        remaining=list(characters.values())
        retry_interval=10; BATCH_SIZE=2500; prev_429=0
        while remaining:
            retry_bucket={}
            batches=(len(remaining)+BATCH_SIZE-1)//BATCH_SIZE
            for i in range(batches):
                batch=remaining[i*BATCH_SIZE:(i+1)*BATCH_SIZE]
                tasks=[create_task(proc_one(c)) for c in batch]
                for t in as_completed(tasks):
                    try:
                        await shield(t)
                    except RetryCharacter as rc:
                        key=f"{rc.char['name']}-{rc.char['realm']}"
                        retry_bucket[key]=rc.char
                    except Exception:
                        continue
                    else:
                        completed+=1
                        now=time.time()
                        if now-last_hb>10:
                            url_cache.clear(); gc.collect()
                            ts=time.strftime("%H:%M:%S",time.localtime(now))
                            sec_rate=len(per_sec.calls)/per_sec.period
                            avg60=len(CALL_TIMES)/60
                            rem_calls=(TOTAL_CALLS-CALLS_DONE) if TOTAL_CALLS else None
                            eta=_fmt_duration(int((now-start_time)/CALLS_DONE*rem_calls)) if CALLS_DONE and rem_calls else "–"
                            print(f"[{ts}] [HEARTBEAT] {completed}/{total} done ({completed/total*100:.1f}%), sec_rate={sec_rate:.1f}/s, avg60={avg60:.1f}/s, ETA={eta}")
                            last_hb=now
            url_cache.clear()
            if retry_bucket:
                await asyncio.sleep(retry_interval)
                remaining=list(retry_bucket.values())
            else:
                break
        db.commit()
        print(f"[DEBUG] inserted_count={inserted_count}, SQLite rows={sum(1 for _ in db_iter_rows())}")

    # build alt_map
    fingerprints={k:set((aid,info["ts"]) for aid,info in ach.items() if info.get("ts") is not None)
                  for k,_,ach in db_iter_rows()}
    alt_map={k:[] for k in fingerprints}
    for a in fingerprints:
        for b in fingerprints:
            if a>=b: continue
            if len(fingerprints[a]&fingerprints[b])>=5:
                alt_map[a].append(b); alt_map[b].append(a)
    # connected components
    visited=[]; groups=[]
    for k in sorted(alt_map):
        if k in visited: continue
        stack, comp=[k],[]
        while stack:
            u=stack.pop(); comp.append(u); visited.append(u)
            for v in alt_map[u]:
                if v not in visited: stack.append(v)
        groups.append(sorted(comp))
    # write output
    rows_map={k:(g,ach) for k,g,ach in [(r_k,r_g,r_ach) for r_k,r_g,r_ach in db_iter_rows()]}
    if MODE=="finalize":
        with open(OUTFILE,"w",encoding="utf-8") as f:
            f.write(f"-- File: RatedStats_Achiev/region_{REGION}.lua\nlocal achievements={{\n")
            for comp in groups:
                leaders=[m for m in comp if m in leaderboard_keys]
                if not leaders: continue
                root=leaders[0]; alts=[m for m in comp if m!=root]
                guid,ach_map=rows_map[root]
                parts=[f'character="{root}"',f'alts={{{",".join(f'"{a}"\' for a in alts)}}}',f'guid={guid}']
                for i,(aid,info) in enumerate(sorted(ach_map.items()),start=1):
                    parts+= [f"id{i}={aid}",f'name{i}="{info["name"].replace("\"","\\\"")}"']
                f.write("    { "+",".join(parts)+" },\n")
            f.write("}\n\n"+f"{REGION_VAR}=achievements\n")
    else:  # batch
        PARTIAL=Path("partial_outputs"); PARTIAL.mkdir(exist_ok=True)
        out=PARTIAL/f"{REGION}_batch_{BATCH_ID}.lua"
        with open(out,"w",encoding="utf-8") as f:
            f.write(f"-- Partial batch {BATCH_ID}/{TOTAL_BATCHES} for {REGION}\nlocal entries={{\n")
            for k,g,ach in db_iter_rows():
                if k not in characters: continue
                parts=[f'character="{k}"', 'alts={}',f'guid={g}']
                for i,(aid,info) in enumerate(sorted(ach.items()),start=1):
                    parts+= [f"id{i}={aid}",f'name{i}="{info["name"].replace("\"","\\\"")}"']
                f.write("    { "+",".join(parts)+" },\n")
            f.write("}\n")
    db.close()

# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------
if __name__=="__main__":
    # 1) seed old
    old_chars=seed_db_from_lua(OUTFILE)
    print(f"[DEBUG] seed_db_from_lua loaded {len(old_chars)} prior entries")
    # 2) fetch raw API chars
    token=get_access_token(REGION); headers={"Authorization":f"Bearer {token}"}
    raw=get_available_brackets  # placeholder to silence unused
    raw_api_chars={c["id"]:c for c in get_characters_from_leaderboards(REGION,headers,PVP_SEASON_ID,BRACKETS)}
    # 3) normalize to string keys
    api_chars={f"{c['name'].lower()}-{c['realm'].lower()}":c for c in raw_api_chars.values()}
    leaderboard_keys=set(api_chars)
    # 4) merge
    merged={**api_chars,**old_chars}
    # 5) slice
    keys=sorted(merged)
    slice_size=(len(keys)+TOTAL_BATCHES-1)//TOTAL_BATCHES
    slice_keys=keys[BATCH_ID*slice_size:(BATCH_ID+1)*slice_size]
    characters={k:merged[k] for k in slice_keys}
    print(f"[FINAL DEBUG] Total chars this run: {len(characters)}")
    # 6) process
    try:
        asyncio.run(process_characters(characters,leaderboard_keys))
    except CancelledError:
        print(f"{YELLOW}[WARN] Top-level run was cancelled, exiting.{RESET}")
