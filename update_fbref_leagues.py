import os
import requests
import pandas as pd
from bs4 import BeautifulSoup

# FBref stats URLs for 2025-26 top 5 European leagues
LEAGUE_STAT_PAGES = {
    "ENG-Premier_League": "https://fbref.com/en/comps/9/2025-2026/stats/2025-2026-Premier-League-Stats",
    "ESP-La_Liga": "https://fbref.com/en/comps/12/2025-2026/stats/2025-2026-La-Liga-Stats",
    "ITA-Serie_A": "https://fbref.com/en/comps/11/2025-2026/stats/2025-2026-Serie-A-Stats",
    "GER-Bundesliga": "https://fbref.com/en/comps/20/2025-2026/stats/2025-2026-Bundesliga-Stats",
    "FRA-Ligue_1": "https://fbref.com/en/comps/13/2025-2026/stats/2025-2026-Ligue-1-Stats"
}
RAW_FOLDER = "data/raw/25-26"

os.makedirs(RAW_FOLDER, exist_ok=True)

def scrape_league_tables(league, url):
    league_dir = os.path.join(RAW_FOLDER, league)
    os.makedirs(league_dir, exist_ok=True)
    r = requests.get(url)
    soup = BeautifulSoup(r.content, "html.parser")
    tables = soup.find_all("table")
    print(f"\nScraping {league}: found {len(tables)} tables")
    for i, table in enumerate(tables):
        # Use table's caption if present, otherwise number
        caption = table.find("caption")
        table_name = caption.text.strip().replace(" ", "_") if caption and caption.text else f"table_{i+1}"
        # Use pandas to parse the HTML table directly
        try:
            df = pd.read_html(str(table))[0]
            filename = os.path.join(league_dir, f"{table_name}.csv")
            df.to_csv(filename, index=False)
            print(f"Saved table: {filename}")
        except Exception as e:
            print(f"Failed to save table {table_name}: {e}")

if __name__ == "__main__":
    for league, url in LEAGUE_STAT_PAGES.items():
        scrape_league_tables(league, url)
