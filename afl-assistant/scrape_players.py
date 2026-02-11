# scrape_players.py
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import sqlite3
import time

BASE_URL = "https://afltables.com"

AFL_TEAM_KEYS = [
    "adelaide", "brisbaneb", "brisbanel", "carlton", "collingwood",
    "essendon", "fitzroy", "fremantle", "geelong", "goldcoast",
    "gws", "hawthorn", "melbourne", "kangaroos",
    "padelaide", "richmond", "stkilda", "swans", "university",
    "westcoast", "bullldogs"
]

conn = sqlite3.connect("players.db")
c = conn.cursor()

# DROP OLD TABLE (important if you already ran broken versions)
c.execute("DROP TABLE IF EXISTS players")

c.execute("""
CREATE TABLE players (
    player_id TEXT PRIMARY KEY,
    name TEXT,
    career_games INTEGER,
    career_goals INTEGER,
    teams TEXT
)
""")

def scrape_team(team_key):
    url = "%s/afl/stats/teams/%s.html" % (BASE_URL, team_key)
    print "Fetching:", url

    try:
        r = requests.get(url, timeout=10)
    except Exception as e:
        print "Request failed:", e
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    players = []
    rows = table.find_all("tr")[1:]

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 8:
            continue

        link = cols[0].find("a")
        if not link:
            continue

        name = link.get_text(strip=True)
        player_id = link["href"]  # UNIQUE AFLTables ID

        try:
            games = int(cols[1].get_text(strip=True))
            goals = int(cols[7].get_text(strip=True))
        except:
            continue

        if games > 0:
            players.append((player_id, name, games, goals, team_key))

    return players


print "Starting AFLTables scrape..."
player_map = {}

for team in AFL_TEAM_KEYS:
    team_players = scrape_team(team)

    for pid, name, games, goals, team_key in team_players:
        if pid not in player_map:
            player_map[pid] = {
                "name": name,
                "games": 0,
                "goals": 0,
                "teams": set()
            }

        player_map[pid]["games"] += games
        player_map[pid]["goals"] += goals
        player_map[pid]["teams"].add(team_key)

    time.sleep(1)

print "Writing to database..."

for pid in player_map:
    data = player_map[pid]
    c.execute(
        """
        INSERT INTO players
        (player_id, name, career_games, career_goals, teams)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            pid,
            data["name"],
            data["games"],
            data["goals"],
            ", ".join(sorted(data["teams"]))
        )
    )

conn.commit()
conn.close()

print "Done. %d players saved safely." % len(player_map)
