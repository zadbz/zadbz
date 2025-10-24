import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE_URL = "https://fbref.com"
SEASON = "2025-2026"
LEAGUE_SCHEDULE_URL = "https://fbref.com/en/comps/9/2025-2026/schedule/2025-2026-Premier-League-Scores-and-Fixtures"
RAW_FOLDER = "data/raw/25-26/ENG-Premier_League"

os.makedirs(RAW_FOLDER, exist_ok=True)

def get_team_urls(schedule_url):
    r = requests.get(schedule_url)
    soup = BeautifulSoup(r.content, "html.parser")
    teams = {}
    for a in soup.select("td[data-stat='team'] a"):
        team_name = a.text.strip().replace(" ", "_")
        main_url = urljoin(BASE_URL, a["href"])
        if "/squads/" in main_url:  # Only teams
            teams[team_name] = main_url.replace("/squads/", "/squads/matchlogs/")
    return teams

def save_team_matchlogs(team, team_url):
    try:
        r = requests.get(team_url)
        dfs = pd.read_html(r.text)
        if dfs:
            df = dfs[0]
            filename = os.path.join(RAW_FOLDER, f"{team}_{SEASON.replace('-', '')}_matchlog.csv")
            df.to_csv(filename, index=False)
            print(f"✔️ Saved {filename} ({df.shape})")
    except Exception as e:
        print(f"❗ Failed for {team}: {e}")

if __name__ == "__main__":
    teams = get_team_urls(LEAGUE_SCHEDULE_URL)
    print(f"Teams found: {list(teams.keys())}")
    for team, url in teams.items():
        save_team_matchlogs(team, url)
