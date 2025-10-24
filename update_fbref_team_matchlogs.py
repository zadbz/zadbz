import os
import requests
import pandas as pd

LEAGUE_SCHEDULE_PAGES = {
    "ENG-Premier_League": "https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures",
    "ESP-La_Liga": "https://fbref.com/en/comps/12/schedule/La-Liga-Scores-and-Fixtures",
    "ITA-Serie_A": "https://fbref.com/en/comps/11/schedule/Serie-A-Scores-and-Fixtures",
    "GER-Bundesliga": "https://fbref.com/en/comps/20/schedule/Bundesliga-Scores-and-Fixtures",
    "FRA-Ligue_1": "https://fbref.com/en/comps/13/schedule/Ligue-1-Scores-and-Fixtures"
}
RAW_FOLDER = "data/raw/25-26"

os.makedirs(RAW_FOLDER, exist_ok=True)

def save_league_schedule_csv(league, url):
    print(f"Fetching: {league} {url}")
    try:
        r = requests.get(url)
        r.raise_for_status()
        table = pd.read_html(r.text, attrs={"id": "sched_ks_3232_1"})[0] if 'id="sched_ks_3232_1"' in r.text else pd.read_html(r.text)[0]
        league_dir = os.path.join(RAW_FOLDER, league)
        os.makedirs(league_dir, exist_ok=True)
        out_csv = os.path.join(league_dir, "league_schedule.csv")
        table.to_csv(out_csv, index=False)
        print(f"✔️ Saved schedule for {league}: {out_csv}")
    except Exception as e:
        print(f"❗ Error for {league}: {e}")

if __name__ == "__main__":
    for league, url in LEAGUE_SCHEDULE_PAGES.items():
        save_league_schedule_csv(league, url)
