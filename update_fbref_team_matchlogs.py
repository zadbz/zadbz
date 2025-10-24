#!/usr/bin/env python3
import os, sys, io, hashlib, time
from datetime import datetime
import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry

LEAGUE_SCHEDULE_PAGES = {
    "ENG-Premier_League": "https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures",
    "ESP-La_Liga": "https://fbref.com/en/comps/12/schedule/La-Liga-Scores-and-Fixtures",
    "ITA-Serie_A": "https://fbref.com/en/comps/11/schedule/Serie-A-Scores-and-Fixtures",
    "GER-Bundesliga": "https://fbref.com/en/comps/20/schedule/Bundesliga-Scores-and-Fixtures",
    "FRA-Ligue_1": "https://fbref.com/en/comps/13/schedule/Ligue-1-Scores-and-Fixtures",
}

SEASON_DIR = os.getenv("SEASON_DIR", "data/raw/25-26")  # change yearly in one place or set env
OUT_NAME = "league_schedule.csv"
KEY_CANDIDATES = ["Wk","Day","Date","Time","Home","Score","Away","xG","xG.1","Attendance","Venue","Referee"]

def _session():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.8, status_forcelist=[429,500,502,503,504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": "Mozilla/5.0 (analytics; +github-action fbref sync)"})
    return s

def _hash_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()[:12]

def _read_schedule_html(html: str) -> pd.DataFrame:
    tables = pd.read_html(html)
    if not tables: raise ValueError("No tables found on page.")
    df = tables[0]
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]  # drop index cols
    return df

def _anti_join(new: pd.DataFrame, old: pd.DataFrame) -> pd.DataFrame:
    keys = [c for c in KEY_CANDIDATES if c in new.columns and c in old.columns]
    if not keys:  # fallback: entire row string
        new["_row"] = new.astype(str).agg("|".join, axis=1)
        old["_row"] = old.astype(str).agg("|".join, axis=1)
        diff = new[~new["_row"].isin(old["_row"])].drop(columns=["_row"])
    else:
        diff = new.merge(old[keys].drop_duplicates(), on=keys, how="left", indicator=True)
        diff = diff[diff["_merge"]=="left_only"].drop(columns=["_merge"])
    return diff

def save_league_schedule_csv(league: str, url: str) -> dict:
    os.makedirs(os.path.join(SEASON_DIR, league), exist_ok=True)
    out_csv = os.path.join(SEASON_DIR, league, OUT_NAME)
    sess = _session()
    r = sess.get(url, timeout=30)
    r.raise_for_status()
    new_df = _read_schedule_html(r.text)
    new_df.insert(0, "_fetched_at_utc", datetime.utcnow().isoformat(timespec="seconds"))
    csv_buf = io.StringIO(); new_df.to_csv(csv_buf, index=False)
    new_hash = _hash_bytes(csv_buf.getvalue().encode())
    changed, added_rows, removed_rows = False, 0, 0
    if os.path.exists(out_csv):
        old_df = pd.read_csv(out_csv)
        add = _anti_join(new_df.drop(columns=["_fetched_at_utc"], errors="ignore"),
                         old_df.drop(columns=["_fetched_at_utc"], errors="ignore"))
        added_rows = len(add)
        rem = _anti_join(old_df.drop(columns=["_fetched_at_utc"], errors="ignore"),
                         new_df.drop(columns=["_fetched_at_utc"], errors="ignore"))
        removed_rows = len(rem)
        old_hash = _hash_bytes(open(out_csv,"rb").read())
        changed = (added_rows>0 or removed_rows>0 or new_hash!=old_hash)
    else:
        changed = True
    if changed:
        new_df.to_csv(out_csv, index=False)
    return {
        "league": league, "url": url, "out_csv": out_csv,
        "rows": len(new_df), "added": added_rows, "removed": removed_rows,
        "hash": new_hash, "changed": changed,
    }

def main():
    print(f"== FBref sync start @ {datetime.utcnow().isoformat()}Z ==")
    any_change = False
    for lg, url in LEAGUE_SCHEDULE_PAGES.items():
        try:
            info = save_league_schedule_csv(lg, url)
            badge = "UPDATED" if info["changed"] else "NO-CHANGE"
            print(f"[{badge}] {lg} -> {info['out_csv']} | rows={info['rows']} "
                  f"+{info['added']} -{info['removed']} | sha={info['hash']}")
            any_change = any_change or info["changed"]
        except Exception as e:
            print(f"[ERROR] {lg}: {e}")
    if any_change:
        # stage/commit here so the action can push
        os.system('git add -A')
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        msg = f"chore(data): fbref schedules auto-update @ {ts}"
        rc = os.system(f'git commit -m "{msg}"')  # non-zero if nothing to commit
        print(f"git commit rc={rc}")
    else:
        print("No changes detected across leagues; nothing to commit.")
    print("== FBref sync end ==")

if __name__ == "__main__":
    main()
