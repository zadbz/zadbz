import os
import re
import hashlib
from io import StringIO
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# ---------- paths ----------
DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ---------- helpers ----------
def find_latest_season_url():
    """
    Locate the latest EPL E0.csv on football-data.co.uk.
    Season codes are YYZZ (e.g. 2425 for 2024-25).
    """
    base = "https://www.football-data.co.uk/mmz4281"
    yy = datetime.utcnow().year % 100
    # try prev, current, next (newest first)
    candidates = [f"{(yy+1)%100:02d}{(yy+2)%100:02d}",
                  f"{yy:02d}{(yy+1)%100:02d}",
                  f"{yy-1:02d}{yy:02d}"]

    for code in candidates:
        url = f"{base}/{code}/E0.csv"
        try:
            r = requests.head(url, timeout=10)
            if r.status_code == 200:
                print(f"✅ Latest season found: {code}")
                return url, code
        except requests.RequestException:
            pass
    raise RuntimeError("Could not find a valid EPL E0.csv on football-data.co.uk")

def get_file_hash(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()

def fetch_csv_text(url: str) -> str:
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    r.raise_for_status()
    return r.text

# ---------- consolidation ----------
# Accept: 14:15.csv / 14_15.csv / E0_2425.csv / E0_2014-15.csv
SEASON_PATTERNS = [
    r"^\d{2}[:_]\d{2}\.csv$",
    r"^E0_\d{4}\.csv$",
    r"^E0_\d{4}-\d{2}\.csv$",
]

def is_season_file(name: str) -> bool:
    return any(re.search(p, name) for p in SEASON_PATTERNS)

def infer_season_from_name(stem: str) -> str:
    """
    Return a human-readable season label like '2014/15' from filename stem.
    """
    # 14:15 or 14_15
    m = re.match(r"^(\d{2})[:_](\d{2})$", stem)
    if m:
        a, b = m.groups()
        return f"20{a}/20{b}"
    # E0_2425
    m = re.match(r"^E0_(\d{2})(\d{2})$", stem)
    if m:
        a, b = m.groups()
        return f"20{a}/20{b}"
    # E0_2014-15
    m = re.match(r"^E0_(\d{4})-(\d{2})$", stem)
    if m:
        yr, b = m.groups()
        return f"{yr}/{20:int(b)}"  # rare pattern; keeps '2014/20'
    return stem  # fallback

def safe_read_csv(path: Path) -> pd.DataFrame:
    """
    football-data sometimes ships latin1; also tolerate odd extra columns.
    """
    for enc in ("utf-8", "latin1"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    # last resort
    return pd.read_csv(path, engine="python", on_bad_lines="skip")

def build_all_seasons_csv():
    """
    Concatenate all season CSVs into data/All_Seasons.csv.
    Adds a 'Season' column inferred from filename if not present.
    De-duplicates on (Date, HomeTeam, AwayTeam) when available.
    """
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

    # normalize typical columns if present
    # ensure Date as string for key; you can also parse to datetime if you like
    for c in ["Date", "HomeTeam", "AwayTeam"]:
        if c in hist.columns:
            hist[c] = hist[c].astype(str)

    # de-dup
    key_cols = [c for c in ["Season", "Date", "HomeTeam", "AwayTeam"] if c in hist.columns]
    if key_cols:
        hist = hist.drop_duplicates(subset=key_cols, keep="last")

    # sort
    sort_cols = [c for c in ["Season", "Date"] if c in hist.columns]
    if sort_cols:
        hist = hist.sort_values(sort_cols, kind="stable").reset_index(drop=True)

    out_path = DATA_DIR / "All_Seasons.csv"
    hist.to_csv(out_path, index=False)
    print(f"🧾 Wrote {out_path} -> {hist.shape}")

# ---------- main updater ----------
def update_data():
    url, code = find_latest_season_url()
    csv_text = fetch_csv_text(url)

    new_hash = get_file_hash(csv_text)
    hash_file = DATA_DIR / "E0_hash.txt"
    old_hash = hash_file.read_text().strip() if hash_file.exists() else None

    if new_hash == old_hash:
        print("No new updates detected. Dataset already up to date.")
    else:
        # robust read from text (handles BOM)
        df = pd.read_csv(StringIO(csv_text))
        # add explicit Season column for the current file
        if "Season" not in df.columns:
            df["Season"] = f"20{code[:2]}/20{code[2:]}"

        current_file = DATA_DIR / f"E0_{code}.csv"
        latest_file  = DATA_DIR / "E0_latest.csv"

        df.to_csv(current_file, index=False)
        df.to_csv(latest_file, index=False)
        hash_file.write_text(new_hash)

        print(f"✅ Updated EPL data saved to {current_file.name} and {latest_file.name} ({len(df)} rows)")

    # rebuild consolidated file every run
    build_all_seasons_csv()

if __name__ == "__main__":
    update_data()
