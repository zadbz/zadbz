import os
import requests
import pandas as pd
from bs4 import BeautifulSoup, Comment

LEAGUE_STAT_PAGES = {
    "ENG-Premier_League": "https://fbref.com/en/comps/9/2025-2026/stats/2025-2026-Premier-League-Stats",
    "ESP-La_Liga": "https://fbref.com/en/comps/12/2025-2026/stats/2025-2026-La-Liga-Stats",
    "ITA-Serie_A": "https://fbref.com/en/comps/11/2025-2026/stats/2025-2026-Serie-A-Stats",
    "GER-Bundesliga": "https://fbref.com/en/comps/20/2025-2026/stats/2025-2026-Bundesliga-Stats",
    "FRA-Ligue_1": "https://fbref.com/en/comps/13/2025-2026/stats/2025-2026-Ligue-1-Stats"
}
RAW_FOLDER = "data/raw/25-26"

os.makedirs(RAW_FOLDER, exist_ok=True)

def find_tables(soup):
    # Get normal tables
    tables = soup.find_all("table")
    # Get tables in HTML comments (FBref embeds some that way)
    comments = soup.find_all(string=lambda t: isinstance(t, Comment))
    for c in comments:
        temp_soup = BeautifulSoup(c, "html.parser")
        tables += temp_soup.find_all("table")
    return tables

def scrape_league_tables(league, url):
    league_dir = os.path.join(RAW_FOLDER, league)
    os.makedirs(league_dir, exist_ok=True)
    r = requests.get(url)
    soup = BeautifulSoup(r.content, "html.parser")
    tables = find_tables(soup)
    print(f"Scraping {league}: {len(tables)} tables found.")
    for i, table in enumerate(tables):
        caption = table.find("caption")
        table_name = caption.text.strip().replace(" ", "_") if caption and caption.text else f"table_{i+1}"
        try:
            df = pd.read_html(str(table), flavor='lxml')[0]
            filename = os.path.join(league_dir, f"{table_name}.csv")
            df.to_csv(filename, index=False)
            print(f"Saved {filename}")
        except Exception as e:
            print(f"Failed to save {table_name}: {e}")

if __name__ == "__main__":
    for league, url in LEAGUE_STAT_PAGES.items():
        scrape_league_tables(league, url)
