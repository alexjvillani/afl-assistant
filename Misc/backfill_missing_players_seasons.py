# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import sqlite3
import time

DB_PATH = "players.db"

TEAM_NAME_TO_KEY = {
    "Adelaide": "adelaide",
    "Brisbane Bears": "brisbaneb",
    "Brisbane Lions": "brisbanel",
    "Carlton": "carlton",
    "Collingwood": "collingwood",
    "Essendon": "essendon",
    "Fitzroy": "fitzroy",
    "Fremantle": "fremantle",
    "Geelong": "geelong",
    "Gold Coast": "goldcoast",
    "Greater Western Sydney": "gws",
    "Hawthorn": "hawthorn",
    "Melbourne": "melbourne",
    "North Melbourne": "kangaroos",
    "Kangaroos": "kangaroos",
    "Port Adelaide": "padelaide",
    "Richmond": "richmond",
    "St Kilda": "stkilda",
    "Sydney": "swans",
    "South Melbourne": "swans",
    "University": "university",
    "West Coast": "westcoast",
    "Western Bulldogs": "bullldogs",
    "Footscray": "bullldogs",
}

def parse_player_seasons(player_url):

    r = requests.get(player_url, timeout=15)
    if r.status_code != 200:
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    seasons = set()

    for row in soup.find_all("tr"):
        cols = row.find_all("td")
        if len(cols) < 2:
            continue

        year_text = cols[0].get_text(strip=True)
        if not year_text.isdigit():
            continue

        year = int(year_text)
        if year < 1870 or year > 2100:
            continue

        team_key = None

        # Team is always in the early columns
        for col in cols[1:4]:
            text = col.get_text(" ", strip=True)
            if not text:
                continue

            # Handle merged team names
            for part in text.split("/"):
                part = part.strip()
                if part in TEAM_NAME_TO_KEY:
                    team_key = TEAM_NAME_TO_KEY[part]
                    break

            if team_key:
                break

        if team_key:
            seasons.add((year, team_key))

    return list(seasons)


conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# Find players with NO seasons
c.execute("""
    SELECT p.player_id, p.name
    FROM players p
    LEFT JOIN player_seasons ps
      ON p.player_id = ps.player_id
    WHERE ps.player_id IS NULL
""")

players = c.fetchall()
total = len(players)

print "Players missing seasons:", total
print "-" * 60

inserted = 0

for i, (pid, name) in enumerate(players, 1):

    print "Processing %d / %d:" % (i, total), name

    try:
        seasons = parse_player_seasons(pid)
    except Exception as e:
        print "  FAILED:", e
        continue

    for (year, team) in seasons:
        c.execute("""
            INSERT OR IGNORE INTO player_seasons (player_id, year, team)
            VALUES (?, ?, ?)
        """, (pid, year, team))

        if c.rowcount > 0:
            inserted += 1
            print "    INSERTED:", year, team

    conn.commit()
    time.sleep(0.3)

conn.close()

print "-" * 60
print "Season backfill complete"
print "Rows inserted:", inserted