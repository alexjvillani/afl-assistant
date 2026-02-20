# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import sqlite3
import time
from urlparse import urljoin

BASE_URL = "https://afltables.com"
DB_PATH = "players.db"

AFL_TEAM_KEYS = [
    "adelaide", "brisbaneb", "brisbanel", "carlton", "collingwood",
    "essendon", "fitzroy", "fremantle", "geelong", "goldcoast",
    "gws", "hawthorn", "melbourne", "kangaroos",
    "padelaide", "richmond", "stkilda", "swans", "university",
    "westcoast", "bullldogs"
]

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# Load existing player IDs
c.execute("SELECT player_id FROM players")
existing_ids = set(row[0] for row in c.fetchall())

print "Existing players:", len(existing_ids)
print "-" * 60


def scrape_team(team_key):

    url = "%s/afl/stats/alltime/%s.html" % (BASE_URL, team_key)
    print "Fetching:", url

    try:
        r = requests.get(url, timeout=10)
    except:
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    rows = table.find_all("tr")[1:]
    found = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 8:
            continue

        link = cols[2].find("a")
        if not link:
            continue

        pid = urljoin(url, link["href"])
        name = link.get_text(strip=True)

        if pid in existing_ids:
            continue

        # Games (required)
        games_text = cols[6].get_text(strip=True)
        try:
            games = int(games_text.split("(")[0])
        except:
            continue

        # Goals (optional!)
        goals_text = cols[7].get_text(strip=True)
        try:
            goals = int(goals_text)
        except:
            goals = 0

        found.append((pid, name, games, goals, team_key))

    return found


inserted = 0

for team in AFL_TEAM_KEYS:

    players = scrape_team(team)

    for pid, name, games, goals, team_key in players:

        c.execute("""
            INSERT INTO players (
                player_id, name, career_games, career_goals,
                teams, max_goals_game, max_kicks_game,
                max_handballs_game, max_disposals_game,
                first_year, last_year
            )
            VALUES (?, ?, ?, ?, ?, 0, 0, 0, 0, NULL, NULL)
        """, (
            pid,
            name,
            games,
            goals,
            team_key
        ))

        existing_ids.add(pid)
        inserted += 1
        print "ADDED:", name

    conn.commit()
    time.sleep(1)

conn.close()

print "-" * 60
print "Backfill complete"
print "Players added:", inserted