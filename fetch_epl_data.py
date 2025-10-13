import os
import re
import hashlib
from io import StringIO
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

# ---------------- paths / env ----------------
DATA_DIR = Path("data")
ODDS_DIR = DATA_DIR / "odds"
API_DIR  = DATA_DIR / "api"
for p in (DATA_DIR, ODDS_DIR, API_DIR):
    p.mkdir(parents=True, exist_ok=True)

ODDS_API_KEY       = os.getenv("ODDS_API_KEY")          # your odds provider key (The Odds API v4 style)
FOOTBALL_API_KEY   = os.getenv("FOOTBALL_API_KEY")      # your fbr.api key

# ---------------- helpers ----------------
def find_latest_season_url():
    """Locate the latest EPL E0.csv on football-data.co.uk (codes YYZZ)."""
    base = "https://www.football-data.co.uk/mmz4281"
    yy = datetime.utcnow().year % 100
    candidates = [f"{(yy+1)%100:02d}{(yy+2)%100:02d}",
                  f"{yy:02d}{(yy+1)%100:02d}",
                  f"{yy-1:02d}{yy:02d}"]  # newest → oldest
    for code in candidates:
        url = f"{base}/{code}/E0.csv"
        try:
            r = requests.head(url, timeout=10)
            if r.status_code == 200:
                print(f"✅ football-data.co.uk latest season found: {code}")
                return url, code
        except requests.RequestException:
            pass
    raise RuntimeError("No valid EPL E0.csv on football-data.co.uk")

def get_file_hash(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()

def fetch_csv_text(url: str) -> str:
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    r.raise_for_status()
    return r.text

# -------------- consolidate history --------------
SEASON_PATTERNS = [
    r"^\d{2}[:_]\d{2}\.csv$",    # 14:15.csv or 14_15.csv
    r"^E0_\d{4}\.csv$",          # E0_2425.csv
    r"^E0_\d{4}-\d{2}\.csv$",    # E0_2014-15.csv
]

def is_season_file(name: str) -> bool:
    return any(re.search(p, name) for p in SEASON_PATTERNS)

def infer_season_from_name(stem: str) -> str:
    m = re.match(r"^(\d{2})[:_](\d{2})$", stem)
    if m:
        a, b = m.groups()
        return f"20{a}/20{b}"
    m = re.match(r"^E0_(\d{2})(\d{2})$", stem)
    if m:
        a, b = m.groups()
        return f"20{a}/20{b}"
    m = re.match(r"^E0_(\d{4})-(\d{2})$", stem)
    if m:
        yr, b = m.groups()
        return f"{yr}/20{b}"
    return stem

def safe_read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8", "latin1"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path, engine="python", on_bad_lines="skip")

def build_all_seasons_csv():
    season_files = sorted([p for p in DATA_DIR.glob("*.csv") if is_season_file(p.name)])
    if not season_files:
        print("No season files found to build All_Seasons.csv.")
        return
    parts = []
    for p in season_files:
        try:
            df = safe_read_csv(p)
            if "Season" not in df.columns:
                df["Season"] = infer_season_from_name(p.stem)
            df["SeasonFile"] = p.stem
            parts.append(df)
        except Exception as e:
            print(f"Skipping {p.name}: {e}")
    if not parts:
        print("No readable season files.")
        return
    hist = pd.concat(parts, ignore_index=True)
    for c in ["Date", "HomeTeam", "AwayTeam"]:
        if c in hist.columns:
            hist[c] = hist[c].astype(str)
    key_cols = [c for c in ["Season", "Date", "HomeTeam", "AwayTeam"] if c in hist.columns]
    if key_cols:
        hist = hist.drop_duplicates(subset=key_cols, keep="last")
    sort_cols = [c for c in ["Season", "Date"] if c in hist.columns]
    if sort_cols:
        hist = hist.sort_values(sort_cols, kind="stable").reset_index(drop=True)
    out_path = DATA_DIR / "All_Seasons.csv"
    hist.to_csv(out_path, index=False)
    print(f"🧾 Wrote {out_path} -> {hist.shape}")

# -------------- football API integrations --------------
def fetch_pl_matches_api_football(season_year: int) -> pd.DataFrame:
    """
    API-Football via RapidAPI (league=39 = EPL).
    FOOTBALL_API_KEY passed as 'X-RapidAPI-Key'.
    """
    if not FOOTBALL_API_KEY:
        print("FOOTBALL_API_KEY missing → skipping API-Football.")
        return pd.DataFrame()
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
    params = {"league": 39, "season": season_year}
    headers = {"X-RapidAPI-Key": FOOTBALL_API_KEY,
               "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"}
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    js = r.json().get("response", [])
    rows = []
    for it in js:
        fixture = it.get("fixture", {})
        league  = it.get("league", {})
        teams   = it.get("teams", {})
        goals   = it.get("goals", {})
        rows.append({
            "provider": "api_football",
            "season": league.get("season"),
            "round": league.get("round"),
            "utcDate": fixture.get("date"),
            "status": fixture.get("status", {}).get("short"),
            "match_id": fixture.get("id"),
            "homeTeam": teams.get("home", {}).get("name"),
            "awayTeam": teams.get("away", {}).get("name"),
            "fullTimeHome": goals.get("home"),
            "fullTimeAway": goals.get("away"),
        })
    return pd.DataFrame(rows)

def fetch_pl_matches_football_data_org(season_year: int) -> pd.DataFrame:
    """
    football-data.org v4 (competition=PL). Requires X-Auth-Token header.
    """
    if not FOOTBALL_API_KEY:
        print("FOOTBALL_API_KEY missing → skipping football-data.org.")
        return pd.DataFrame()
    url = f"https://api.football-data.org/v4/competitions/PL/matches"
    params = {"season": season_year}
    headers = {"X-Auth-Token": FOOTBALL_API_KEY}
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    js = r.json().get("matches", [])
    rows = [{
        "provider": "football_data",
        "season": season_year,
        "round": m.get("matchday"),
        "utcDate": m.get("utcDate"),
        "status": m.get("status"),
        "match_id": m.get("id"),
        "homeTeam": (m.get("homeTeam") or {}).get("name"),
        "awayTeam": (m.get("awayTeam") or {}).get("name"),
        "fullTimeHome": (m.get("score") or {}).get("fullTime", {}).get("home"),
        "fullTimeAway": (m.get("score") or {}).get("fullTime", {}).get("away"),
    } for m in js]
    return pd.DataFrame(rows)

def fetch_and_save_football_api():
    """Choose provider, fetch current season, save under data/api/."""
    season_year = datetime.utcnow().year  # e.g., 2025 → API season 2024 or 2025 depending on provider
    # Heuristic: if we’re before July, most APIs still index as previous season
    if datetime.utcnow().month < 7:
        season_year -= 1

    try:
        if FOOTBALL_PROVIDER == "football-data":
            df = fetch_pl_matches_football_data_org(season_year)
            name = f"football_data_PL_{season_year}"
        else:
            df = fetch_pl_matches_api_football(season_year)
            name = f"api_football_PL_{season_year}"
        if not df.empty:
            out = API_DIR / f"{name}.csv"
            df.to_csv(out, index=False)
            print(f"💾 Saved {out} ({df.shape})")
        else:
            print("Football API returned empty dataset.")
    except Exception as e:
        print("⚠️ Football API fetch failed:", e)

# -------------- odds API integration (The Odds API style) --------------
def fetch_odds_snapshot():
    """
    Pull current EPL odds. Assumes The Odds API v4 style.
    """
    if not ODDS_API_KEY:
        print("ODDS_API_KEY missing → skipping odds.")
        return pd.DataFrame()
    base = "https://api.the-odds-api.com/v4"
    sport_key = "soccer_epl"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": "uk",              # adjust if needed
        "markets": "h2h,spreads,totals",
        "oddsFormat": "decimal",
        "dateFormat": "iso"
    }
    url = f"{base}/sports/{sport_key}/odds"
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    rows = []
    pulled_at = datetime.now(timezone.utc).isoformat()
    for game in data:
        gid = game.get("id")
        commence = game.get("commence_time")
        home = game.get("home_team")
        away = game.get("away_team")
        for bm in game.get("bookmakers", []):
            bm_key = bm.get("key")
            bm_title = bm.get("title")
            last_update = bm.get("last_update")
            for market in bm.get("markets", []):
                mkt = market.get("key")
                for out in market.get("outcomes", []):
                    rows.append({
                        "pulled_at": pulled_at,
                        "game_id": gid,
                        "commence_time": commence,
                        "home_team": home,
                        "away_team": away,
                        "bookmaker": bm_key,
                        "bookmaker_name": bm_title,
                        "last_update": last_update,
                        "market": mkt,                     # h2h/spreads/totals
                        "name": out.get("name"),           # Home/Away/Draw or Over/Under
                        "price": out.get("price"),
                        "point": out.get("point"),
                    })
    return pd.DataFrame(rows)

def save_odds(df: pd.DataFrame):
    if df.empty:
        return
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    snap = ODDS_DIR / f"odds_{ts}.csv"
    df.to_csv(snap, index=False)

    hist_path = ODDS_DIR / "odds_history.csv"
    if hist_path.exists():
        old = pd.read_csv(hist_path)
    else:
        old = pd.DataFrame(columns=df.columns)

    key_cols = [c for c in ["game_id","commence_time","bookmaker","market","name","point"] if c in df.columns]
    combined = pd.concat([old, df], ignore_index=True)
    if key_cols:
        combined = combined.drop_duplicates(subset=key_cols, keep="last")
    else:
        combined = combined.drop_duplicates(keep="last")
    combined.to_csv(hist_path, index=False)
    print(f"💾 Saved {snap.name} and updated odds_history.csv -> {combined.shape}")

# -------------- main EPL updater --------------
def update_epl_from_football_data_site():
    url, code = find_latest_season_url()
    csv_text = fetch_csv_text(url)
    new_hash = get_file_hash(csv_text)
    hash_file = DATA_DIR / "E0_hash.txt"
    old_hash = hash_file.read_text().strip() if hash_file.exists() else None

    if new_hash == old_hash:
        print("No new E0.csv updates detected.")
    else:
        df = pd.read_csv(StringIO(csv_text))
        if "Season" not in df.columns:
            df["Season"] = f"20{code[:2]}/20{code[2:]}"
        current_file = DATA_DIR / f"E0_{code}.csv"
        latest_file  = DATA_DIR / "E0_latest.csv"
        df.to_csv(current_file, index=False)
        df.to_csv(latest_file, index=False)
        hash_file.write_text(new_hash)
        print(f"✅ Updated {current_file.name} & E0_latest.csv ({len(df)} rows)")

    build_all_seasons_csv()

def run():
    # 1) Core EPL table from football-data.co.uk
    try:
        update_epl_from_football_data_site()
    except Exception as e:
        print("⚠️ EPL site fetch failed:", e)

    # 2) Football API enrichment (fixtures/results)
    try:
        fetch_and_save_football_api()
    except Exception as e:
        print("⚠️ Football API block failed:", e)

    # 3) Odds API snapshot + history
    try:
        odds = fetch_odds_snapshot()
        if odds is not None and not odds.empty:
            save_odds(odds)
        else:
            print("No odds rows to save.")
    except Exception as e:
        print("⚠️ Odds fetch failed:", e)

if __name__ == "__main__":
    run()
