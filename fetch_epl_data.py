import os
import re
import hashlib
from io import StringIO
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests


DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Helpers
# -----------------------------
def find_latest_season_url():
    """
    Find the latest available EPL E0.csv on football-data.co.uk.
    Season codes are YYZZ (e.g., 2425 for 2024-25).
    """
    base = "https://www.football-data.co.uk/mmz4281"
    now = datetime.utcnow()
    yy = now.year % 100

    # Try previous, current, next
    candidates = [f"{yy-1:02d}{yy:02d}", f"{yy:02d}{(yy+1)%100:02d}", f"{(yy+1)%100:02d}{(yy+2)%100:02d}"]
    for code in reversed(candidates):  # newest first
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

# -----------------------------
# Consolidation
# -----------------------------
SEASON_PATTERNS = [
    r"^\d{2}[:_]\d{2}\.csv$",   # 14:15.csv or 14_15.csv
    r"^E0_\d{4}\.csv$",         # E0_2425.csv
    r"^E0_\d{4}-\d{2}\.csv$",   # E0_2014-15.csv (if you ever use this)
]

def is_season_file(name: str) -> bool:
    return any(re.search(p, name) for p in SEASON_PATTERNS)

def build_all_seasons_csv():
    """
    Concatenate all season CSVs into data/All_Seasons.csv.
    De-duplicate on (Date, HomeTeam, AwayTeam) if present.
    """
    season_files = sorted([p for p in DATA_DIR.glob("*.csv") if is_season_file(p.name)])
    if not season_files:
        print("No season files found to build All_Seasons.csv.")
        return

    parts = []
    for p in season_files:
        try:
            df = pd.read_csv(p)
            df["SeasonFile"] = p.stem  # traceability
            parts.append(df)
        except Exception as e:
            print(f"Skipping {p.name}: {e}")

    if not parts:
        print("No readable season files.")
        return

    hist = pd.concat(parts, ignore_index=True)

    # Optional de-duplication
    key_cols = [c for c in ["Date", "HomeTeam", "AwayTeam"] if c in hist.columns]
    if key_cols:
        hist = hist.drop_duplicates(subset=key_cols, keep="last")

    # Optional sort
    sort_cols = [c for c in ["Season", "Date"] if c in hist.columns]
    if sort_cols:
        hist = hist.sort_values(sort_cols, kind="stable").reset_index(drop=True)

    out_path = DATA_DIR / "All_Seasons.csv"
    hist.to_csv(out_path, index=False)
    print(f"🧾 Wrote {out_path} -> {hist.shape}")

# -----------------------------
# Main updater
# -----------------------------
def update_data():
    url, code = find_latest_season_url()
    csv_text = fetch_csv_text(url)

    new_hash = get_file_hash(csv_text)
    hash_file = DATA_DIR / "E0_hash.txt"
    old_hash = hash_file.read_text().strip() if hash_file.exists() else None

    if new_hash == old_hash:
        print("No new updates detected. Dataset already up to date.")
    else:
        df = pd.read_csv(StringIO(csv_text))

        current_file = DATA_DIR / f"E0_{code}.csv"
        latest_file  = DATA_DIR / "E0_latest.csv"

        df.to_csv(current_file, index=False)
        df.to_csv(latest_file, index=False)
        hash_file.write_text(new_hash)

        print(f"✅ Updated EPL data saved to {current_file.name} and {latest_file.name} ({len(df)} rows)")

    # Rebuild consolidated file every run (cheap and safe)
    build_all_seasons_csv()


if __name__ == "__main__":
    update_data()
