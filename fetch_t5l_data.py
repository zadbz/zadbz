import os
import requests
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

# --- Config ---
DATA_DIR = Path("data")
ODDS_DIR = DATA_DIR / "odds"
ODDS_API_KEY = os.getenv("ODDS_API_KEY") # Set in Github secrets or env
for p in (DATA_DIR, ODDS_DIR):
    p.mkdir(parents=True, exist_ok=True)

# Add Odds API sport keys for top-5 leagues as needed for your provider
LEAGUE_SPORT_KEYS = {
    "ENG-Premier_League": "soccer_epl",
    "ESP-La_Liga": "soccer_spain_la_liga",
    "ITA-Serie_A": "soccer_italy_serie_a",
    "GER-Bundesliga": "soccer_germany_bundesliga",
    "FRA-Ligue_1": "soccer_france_ligue_one"
}

def fetch_league_odds(sport_key):
    base = "https://api.the-odds-api.com/v4"
    url = f"{base}/sports/{sport_key}/odds"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": "eu,uk",       # adjust as needed
        "markets": "h2h",         # only match odds
        "oddsFormat": "decimal",
        "dateFormat": "iso"
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def select_best_odds(game):
    # Find the bookmaker with the best overall odds (highest sum of prices)
    best_bm = None
    best_total = -1
    best_odds = []
    for bm in game.get("bookmakers", []):
        for market in bm.get("markets", []):
            if market.get("key") == "h2h":
                prices = [out.get("price") for out in market.get("outcomes", []) if out.get("price") is not None]
                if prices and sum(prices) > best_total:
                    best_total = sum(prices)
                    best_bm = bm.get("title")
                    best_odds = market.get("outcomes", [])
    return best_bm, best_odds

def save_league_odds(league_name, games):
    rows = []
    pulled_at = datetime.now(timezone.utc).isoformat()
    for game in games:
        best_bm, best_odds = select_best_odds(game)
        for out in best_odds:
            rows.append({
                "league": league_name,
                "pulled_at": pulled_at,
                "commence_time": game.get("commence_time"),
                "home_team": game.get("home_team"),
                "away_team": game.get("away_team"),
                "bookmaker": best_bm,
                "market": "h2h",
                "name": out.get("name"),
                "price": out.get("price"),
            })
    df = pd.DataFrame(rows)
    fname = ODDS_DIR / f"{league_name}_odds_best_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.csv"
    df.to_csv(fname, index=False)
    print(f"Saved {fname} ({df.shape})")

if __name__ == "__main__":
    for league, sport_key in LEAGUE_SPORT_KEYS.items():
        print(f"Fetching odds for {league}...")
        try:
            games = fetch_league_odds(sport_key)
            save_league_odds(league, games)
        except Exception as e:
            print(f"Error fetching odds for {league}: {e}")
