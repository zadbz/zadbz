import os
import requests
import pandas as pd
from io import StringIO
from datetime import datetime
import hashlib

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

def find_latest_season_url():
    """Automatically find the latest available Premier League E0.csv season file."""
    base = "https://www.football-data.co.uk/mmz4281"
    now = datetime.now()
    year = now.year % 100  # last two digits of current year
    # Try possible season codes
    possible_codes = [
        f"{year-1:02d}{year:02d}",  # previous season
        f"{year:02d}{year+1:02d}",  # current/next season
        f"{year+1:02d}{year+2:02d}",  # next-next season (just in case)
    ]

    for code in reversed(possible_codes):  # check newer first
        url = f"{base}/{code}/E0.csv"
        try:
            r = requests.head(url, timeout=5)
            if r.status_code == 200:
                print(f"✅ Latest season found: {code}")
                return url, code
        except Exception:
            continue
    raise Exception("⚠️ Could not find a valid EPL season file on football-data.co.uk")

def get_file_hash(content: str) -> str:
    return hashlib.md5(content.encode("utf-8")).hexdigest()

def fetch_csv(url: str) -> str:
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()
    return response.text

def update_data():
    """Fetch, compare, and update the latest EPL data automatically."""
    url, code = find_latest_season_url()
    csv_text = fetch_csv(url)

    new_hash = get_file_hash(csv_text)
    hash_file = os.path.join(DATA_DIR, "E0_hash.txt")

    old_hash = None
    if os.path.exists(hash_file):
        with open(hash_file, "r") as f:
            old_hash = f.read().strip()

    if new_hash == old_hash:
        print("No new updates detected. Dataset already up to date.")
        return

    df = pd.read_csv(StringIO(csv_text))
    current_file = os.path.join(DATA_DIR, f"E0_{code}.csv")
    latest_file = os.path.join(DATA_DIR, "E0_latest.csv")
    df.to_csv(current_file, index=False)
    df.to_csv(latest_file, index=False)

    with open(hash_file, "w") as f:
        f.write(new_hash)

    print(f"✅ Updated EPL data saved to {current_file} and {latest_file} ({len(df)} matches)")

if __name__ == "__main__":
    update_data()