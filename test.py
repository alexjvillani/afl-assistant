# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import sqlite3
import time
import os

BASE_URL = "https://afltables.com"

# -------------------------------
# TEST TEAM
# -------------------------------
TEST_TEAM = "gws"

# -------------------------------
# DATABASE SETUP
# -------------------------------
DB_PATH = "gridley_test.db"
conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

c.execute("DROP TABLE IF EXISTS players")
c.execute("""
CREATE TABLE players (
    player_id TEXT PRIMARY KEY,
    name TEXT,
    career_games INTEGER,
    career_goals INTEGER,
    max_goals_game INTEGER,
    max_kicks_game INTEGER,
    max_handballs_game INTEGER,
    max_disposals_game INTEGER,
    first_year INTEGER,
    last_year INTEGER
)
""")

# -------------------------------
# SCRAPE TEAM PAGE
# -------------------------------
def scrape_team(team_key):
    url = "%s/afl/stats/teams/%s.html" % (BASE_URL, team_key)
    print("Fetching team:", url)
    try:
        r = requests.get(url, timeout=10)
    except:
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    players = []

    # Team page tables usually list players in rows with <a href="players/...">
    for link in soup.find_all("a"):
        href = link.get("href")
        if href and "/afl/stats/players/" in href:
            player_name = link.get_text(strip=True)
            # Avoid duplicates
            if player_name not in [p['name'] for p in players]:
                # Attempt to get career games/goals from surrounding columns
                row = link.find_parent("tr")
                if not row:
                    continue
                cols = row.find_all("td")
                career_games = 0
                career_goals = 0
                if len(cols) >= 8:
                    try:
                        career_games = int(cols[1].get_text(strip=True))
                    except:
                        pass
                    try:
                        career_goals = int(cols[7].get_text(strip=True))
                    except:
                        pass

                players.append({
                    "name": player_name,
                    "player_id": href,
                    "career_games": career_games,
                    "career_goals": career_goals
                })

    return players

# -------------------------------
# PARSE TOP 10 TABLE
# -------------------------------
def parse_top10_table(table):
    max_goals = max_kicks = max_handballs = max_disposals = 0

    for row in table.find_all("tr"):
        cols = row.find_all("td")
        if len(cols) < 2:
            continue

        stat_name = cols[0].get_text(strip=True)
        values = []
        for c in cols[1:]:
            try:
                values.append(int(c.get_text(strip=True)))
            except:
                continue
        if not values:
            continue
        max_val = max(values)

        if stat_name == "Goals":
            max_goals = max_val
        elif stat_name == "Kicks":
            max_kicks = max_val
        elif stat_name == "Handballs":
            max_handballs = max_val
        elif stat_name == "Disposals":
            max_disposals = max_val

    return max_goals, max_kicks, max_handballs, max_disposals

# -------------------------------
# SCRAPE PLAYER PAGE
# -------------------------------
def scrape_player(player):
    href = player["player_id"]
    if href.startswith("../"):
        href = href[3:]
    url = "%s/%s" % (BASE_URL, href)
    print("Scraping Top 10 for:", url)

    try:
        r = requests.get(url, timeout=10)
    except:
        return None

    soup = BeautifulSoup(r.text, "html.parser")

    # Find Top 10 table
    top10_table = None
    for table in soup.find_all("table"):
        if table.find("th") and "Top 10" in table.get_text():
            top10_table = table
            break

    max_goals = max_kicks = max_handballs = max_disposals = 0
    if top10_table:
        max_goals, max_kicks, max_handballs, max_disposals = parse_top10_table(top10_table)

    # Career span
    first_year = last_year = None
    for row in soup.find_all("tr"):
        cols = row.find_all("td")
        if len(cols) < 2:
            continue
        try:
            year = int(cols[0].get_text(strip=True))
            if 1800 < year < 2100:
                if first_year is None or year < first_year:
                    first_year = year
                if last_year is None or year > last_year:
                    last_year = year
        except:
            continue

    data = {
        "player_id": player["player_id"],
        "name": player["name"],
        "career_games": player["career_games"],
        "career_goals": player["career_goals"],
        "max_goals_game": max_goals,
        "max_kicks_game": max_kicks,
        "max_handballs_game": max_handballs,
        "max_disposals_game": max_disposals,
        "first_year": first_year,
        "last_year": last_year
    }

    return data

# -------------------------------
# MAIN
# -------------------------------
players = scrape_team(TEST_TEAM)
print("Found %d players on team %s" % (len(players), TEST_TEAM))

for p in players:
    data = scrape_player(p)
    if not data:
        continue
    time.sleep(0.5)

    c.execute("""
        INSERT OR REPLACE INTO players
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data["player_id"],
        data["name"],
        data["career_games"],
        data["career_goals"],
        data["max_goals_game"],
        data["max_kicks_game"],
        data["max_handballs_game"],
        data["max_disposals_game"],
        data["first_year"],
        data["last_year"]
    ))

    print("---")
    print("Name:", data["name"])
    print("Career Games:", data["career_games"], "Goals:", data["career_goals"])
    print("Max Goals:", data["max_goals_game"], "Kicks:", data["max_kicks_game"],
          "Handballs:", data["max_handballs_game"], "Disposals:", data["max_disposals_game"])
    print("Years:", data["first_year"], "-", data["last_year"])

conn.commit()
conn.close()
print("Test complete! Data saved to", DB_PATH)
