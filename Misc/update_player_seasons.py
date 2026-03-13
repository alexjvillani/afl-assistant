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
    print "  Fetching:", player_url

    r = requests.get(player_url, timeout=15)
    if r.status_code != 200:
        print "  !! HTTP ERROR:", r.status_code
        return []

    soup = BeautifulSoup(r.text, "html.parser")

    seasons = []
    rows_checked = 0

    for row in soup.find_all("tr"):
        cols = row.find_all("td")
        if len(cols) < 2:
            continue

        rows_checked += 1

        year_text = cols[0].get_text(strip=True)
        if not year_text.isdigit():
            continue

        year = int(year_text)
        if year < 1870 or year > 2100:
            print "    Skipping invalid year:", year_text
            continue

        team_name = cols[1].get_text(" ", strip=True)

        team_key = TEAM_NAME_TO_KEY.get(team_name)
        if not team_key:
            print "    Unknown team:", team_name, "(year", year, ")"
            continue

        print "    Found season:", year, team_key
        seasons.append((year, team_key))

    print "  Rows scanned:", rows_checked
    print "  Seasons found:", len(seasons)

    return list(set(seasons))


conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# Ensure table exists
c.execute("""
CREATE TABLE IF NOT EXISTS player_seasons (
    player_id TEXT,
    year INTEGER,
    team TEXT,
    PRIMARY KEY (player_id, year, team)
)
""")
c.execute("CREATE INDEX IF NOT EXISTS idx_ps_year_team ON player_seasons (year, team)")
c.execute("CREATE INDEX IF NOT EXISTS idx_ps_player ON player_seasons (player_id)")
conn.commit()

print "\nResetting player_seasons table...\n"
c.execute("DELETE FROM player_seasons")
conn.commit()

c.execute("SELECT player_id, name FROM players")
players = c.fetchall()

total_players = len(players)
players_with_data = 0
rows_inserted = 0

print "Total players to process:", total_players
print "-" * 60

for i, (pid, name) in enumerate(players, 1):

    print "\nPlayer %d / %d: %s" % (i, total_players, name)

    try:
        seasons = parse_player_seasons(pid)
    except Exception as e:
        print "  !! FAILED to parse:", e
        continue

    if seasons:
        players_with_data += 1

    for (year, team_key) in seasons:
        c.execute("""
            INSERT OR IGNORE INTO player_seasons (player_id, year, team)
            VALUES (?, ?, ?)
        """, (pid, year, team_key))

        if c.rowcount > 0:
            rows_inserted += 1
            print "    INSERTED:", year, team_key

    conn.commit()

    print "  Running totals — Players OK:", players_with_data, \
          "| Rows inserted:", rows_inserted

    time.sleep(0.25)

conn.close()

print "\n" + "=" * 60
print "DONE"
print "Players with seasons:", players_with_data
print "Total season rows inserted:", rows_inserted
print "=" * 60
