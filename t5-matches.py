#!/usr/bin/env python3
import os, io, re, hashlib
from datetime import datetime
import pandas as pd, requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry

LEAGUES = {
  "ENG-Premier_League": ("9",  "Premier-League-Stats"),
  "ESP-La_Liga":        ("12", "La-Liga-Stats"),
  "ITA-Serie_A":        ("11", "Serie-A-Stats"),
  "GER-Bundesliga":     ("20", "Bundesliga-Stats"),
  "FRA-Ligue_1":        ("13", "Ligue-1-Stats"),
}
SEASON_DIR = os.getenv("SEASON_DIR","data/raw/25-26")         # e.g., data/raw/25-26
SHORT_SEASON = os.path.basename(SEASON_DIR).replace("-","")   # -> 2526
FULL_SEASON = f"20{SHORT_SEASON[:2]}-20{SHORT_SEASON[2:]}"    # -> 2025-2026
UA = {"User-Agent":"Mozilla/5.0 (data-sync; +github-actions)"}

def sess():
    s=requests.Session()
    s.headers.update(UA)
    s.mount("https://", HTTPAdapter(max_retries=Retry(total=5,backoff_factor=0.7,
      status_forcelist=[429,500,502,503,504])))
    return s

def sha12(b:bytes)->str: return hashlib.sha256(b).hexdigest()[:12]

def find_team_links(html:str):
    """Return list of (team_name, team_id) from any league 'Stats' page."""
    soup=BeautifulSoup(html,"lxml")
    links=set()
    for a in soup.select("a[href*='/en/squads/']"):
        m=re.search(r"/en/squads/([a-z0-9]{8})/", a.get("href",""))
        name=a.get_text(strip=True)
        if m and name and name.lower() not in {"squad","squads"}:
            links.add((name, m.group(1)))
    return sorted(links)

def fetch_matchlog_df(s:requests.Session, team_id:str):
    """Try a few canonical FBref paths for team season match logs."""
    bases=[
      f"https://fbref.com/en/squads/{team_id}/{FULL_SEASON}/matchlogs/all_comps/matchlogs",
      f"https://fbref.com/en/squads/{team_id}/{FULL_SEASON}/matchlogs/all_comps/matches",
      f"https://fbref.com/en/squads/{team_id}/{FULL_SEASON}/matchlogs/cmp_fa/matchlogs",
    ]
    for url in bases:
        r=s.get(url,timeout=30)
        if r.status_code!=200: continue
        try:
            tables=pd.read_html(r.text)
            # Pick the widest table that contains 'Date' & 'Comp'
            candidates=[df for df in tables if {"Date","Comp"}.issubset(set(df.columns))]
            if not candidates: continue
            df=max(candidates, key=lambda d: d.shape[1])
