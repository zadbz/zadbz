import os
import requests
import pandas as pd
from io import StringIO
from datetime import datetime
import hashlib

# Data source: Premier League 2024–25
URL = "https://www.football-data.co.uk/mmz4281/2425/E0.csv"
DATA_DIR = "data"
CURRENT_FILE = os.path.join(DATA_DIR, "E0_latest.csv")
HASH_FILE = os.path.join(DATA_DIR, "E0_hash.txt")

os.makedirs(DATA_DIR, exist_ok=True)

def get_file_hash(content: str) -> str:
    """Generate MD5 hash to detect file changes."""
    return hashlib.md5(content.encode("utf-8")).hexdigest()

def fetch_csv() -> str:
    """Download the CSV from football-data.co.uk with headers."""
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(URL, headers=headers, timeout=10)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"⚠️ Failed to fetch from URL: {e}")
        return None

def update_data():
    """Fetch, compare hash, and update dataset if changed."""
    csv_text = fetch_csv()
    if not csv_text:
        return

    new_hash = get_file_hash(csv_text)
    old_hash = None

    if os.path.exists(HASH_FILE):
        with open(HASH_FILE, "r") as f:
            old_hash = f.read().strip()

    if new_hash == old_hash:
        print("No new updates detected. Dataset already up to date.")
        return

    # Load CSV and save
    df = pd.read_csv(StringIO(csv_text))
    df.to_csv(CURRENT_FILE, index=False)
    with open(HASH_FILE, "w") as f:
        f.write(new_hash)

    # Timestamped backup
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    backup_file = os.path.join(DATA_DIR, f"E0_backup_{timestamp}.csv")
    df.to_csv(backup_file, index=False)

    print(f"✅ Dataset updated — saved {len(df)} matches")
    print(f"🗂 Main file: {CURRENT_FILE}")
    print(f"🕒 Backup: {backup_file}")

if __name__ == "__main__":
    update_data()