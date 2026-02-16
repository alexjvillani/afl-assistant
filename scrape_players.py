# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import sqlite3
import time
from urlparse import urljoin

BASE_URL = "https://afltables.com"

AFL_TEAM_KEYS = [
    "adelaide", "brisbaneb", "brisbanel", "carlton", "collingwood",
    "essendon", "fitzroy", "fremantle", "geelong", "goldcoast",
    "gws", "hawthorn", "melbourne", "kangaroos",
    "padelaide", "richmond", "stkilda", "swans", "university",
    "westcoast", "bullldogs"
]

# -------------------------------------------------------
# DATABASE SETUP
# -------------------------------------------------------

conn = sqlite3.connect("players.db")
c = conn.cursor()

c.execute("DROP TABLE IF EXISTS players")

c.execute("""
CREATE TABLE players (
    player_id TEXT PRIMARY KEY,
    name TEXT,
    career_games INTEGER,
    career_goals INTEGER,
    teams TEXT,
    max_goals_game INTEGER,
    max_kicks_game INTEGER,
    max_handballs_game INTEGER,
    max_disposals_game INTEGER,
    first_year INTEGER,
    last_year INTEGER
)
""")

conn.commit()


# -------------------------------------------------------
# UPDATED SCRAPE TEAM PAGE (ALL-TIME VERSION)
# -------------------------------------------------------

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

    players = []
    rows = table.find_all("tr")[1:]

    for row in rows:

        cols = row.find_all("td")
        if len(cols) < 9:
            continue

        link = cols[2].find("a")
        if not link:
            continue

        name = link.get_text(strip=True)
        player_id = urljoin(url, link["href"])

        try:
            games_text = cols[6].get_text(strip=True)
            games = int(games_text.split("(")[0].strip())
            goals = int(cols[7].get_text(strip=True))
        except:
            continue

        if games > 0:
            players.append((player_id, name, games, goals, team_key))

    return players


# -------------------------------------------------------
# SCRAPE INDIVIDUAL PLAYER PAGE (UNCHANGED)
# -------------------------------------------------------

def scrape_player_details(player_id):

    url = player_id
    print "Scraping:", url

    try:
        r = requests.get(url, timeout=10)
    except:
        print "Failed:", url
        return None

    soup = BeautifulSoup(r.text, "html.parser")

    max_goals = 0
    max_kicks = 0
    max_handballs = 0
    max_disposals = 0
    first_year = None
    last_year = None

    # ---------------------------------------------------
    # FIND TOP 10 STATS
    # ---------------------------------------------------

    for row in soup.find_all("tr"):

        cols = row.find_all("td")
        if len(cols) < 2:
            continue

        label = cols[0].get_text(strip=True)

        try:
            value = int(cols[1].get_text(strip=True))
        except:
            continue

        if label == "Goals":
            max_goals = value
        elif label == "Kicks":
            max_kicks = value
        elif label == "Handballs":
            max_handballs = value
        elif label == "Disposals":
            max_disposals = value

    # ---------------------------------------------------
    # FIND CAREER SPAN
    # ---------------------------------------------------

    years = []

    for row in soup.find_all("tr"):
        cols = row.find_all("td")
        if not cols:
            continue

        try:
            year = int(cols[0].get_text(strip=True))
            if 1800 < year < 2100:
                years.append(year)
        except:
            continue

    if years:
        first_year = min(years)
        last_year = max(years)

    return (
        max_goals,
        max_kicks,
        max_handballs,
        max_disposals,
        first_year,
        last_year
    )


# -------------------------------------------------------
# MAIN SCRAPER
# -------------------------------------------------------

print "Starting AFLTables scrape..."

player_map = {}

# STEP 1 — Build unique player list
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


# STEP 2 — Scrape each player page
print "\nScraping individual player pages...\n"

total_players = len(player_map)
counter = 1

for pid in player_map:

    data = player_map[pid]

    print "Player %d / %d" % (counter, total_players)

    details = scrape_player_details(pid)
    time.sleep(0.5)

    if not details:
        counter += 1
        continue

    max_goals, max_kicks, max_handballs, \
    max_disposals, first_year, last_year = details

    print "----------------------------------------"
    print "Player:", data["name"]
    print "Teams:", ", ".join(sorted(data["teams"]))
    print "Career Games:", data["games"]
    print "Career Goals:", data["goals"]
    print "Max Goals in Game:", max_goals
    print "Max Kicks in Game:", max_kicks
    print "Max Handballs in Game:", max_handballs
    print "Max Disposals in Game:", max_disposals
    print "Career Span:", first_year, "-", last_year
    print "----------------------------------------\n"

    c.execute("""
        INSERT INTO players
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        pid,
        data["name"],
        data["games"],
        data["goals"],
        ", ".join(sorted(data["teams"])),
        max_goals,
        max_kicks,
        max_handballs,
        max_disposals,
        first_year,
        last_year
    ))

    counter += 1

conn.commit()
conn.close()

print "Done! %d players saved." % total_players
