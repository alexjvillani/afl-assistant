# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import sqlite3
import time

DB_PATH = "players.db"

# ----------------------------
# DEBUG CONTROLS
# ----------------------------
DEBUG = True            # Print per-player summaries
DEBUG_SEASONS = False   # Print each season row parsed
DEBUG_LIMIT = None      # e.g. 10 for testing

REQUEST_DELAY = 0.25

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

# -------------------------------------------------
# DATABASE SETUP
# -------------------------------------------------

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

c.execute("""
CREATE TABLE IF NOT EXISTS player_club_stats (
    player_id TEXT,
    team TEXT,
    games INTEGER,
    goals INTEGER,
    PRIMARY KEY (player_id, team)
)
""")
conn.commit()

c.execute("SELECT player_id, name FROM players")
players = c.fetchall()
total = len(players)

print "Total players to process:", total
print "-" * 80


def _header_texts(table):
    """Return a set of header texts (th) for a table."""
    headers = set()
    for th in table.find_all("th"):
        txt = th.get_text(" ", strip=True)
        if txt:
            headers.add(txt)
    return headers


def find_season_table(soup):
    """
    Find the one 'year-by-year totals' table using header detection.
    We look for a table containing headers: Year, Team, GM, GL.
    """
    tables = soup.find_all("table")
    for idx, table in enumerate(tables):
        headers = _header_texts(table)
        # Some tables may use slightly different spacing; these are stable labels on AFLTables.
        if ("Year" in headers) and ("Team" in headers) and ("GM" in headers) and ("GL" in headers):
            return idx, table, headers
    return None, None, None


def scrape_season_stats(player_url, name):

    if DEBUG:
        print "\n[FETCH]", name
        print " URL:", player_url

    r = requests.get(player_url, timeout=15)
    if r.status_code != 200:
        print "  !! HTTP ERROR:", r.status_code
        return None

    soup = BeautifulSoup(r.text, "html.parser")

    table_idx, season_table, headers = find_season_table(soup)
    if not season_table:
        if DEBUG:
            print "  !! Could not find season table by headers"
        return None

    if DEBUG:
        print "  Using season table index:", table_idx
        # print a small header hint
        if headers:
            show = ["Year", "Team", "GM", "GL"]
            print "  Header check:", ", ".join([h for h in show if h in headers])

    club_stats = {}
    max_goals_season = 0
    max_goals_year = None

    # De-dupe identical season rows in case AFLTables repeats inside the same table
    seen_rows = set()
    seasons_seen = 0

    # Parse rows in THIS table only
    for row in season_table.find_all("tr"):
        cols = row.find_all("td")
        if len(cols) < 10:
            continue

        year_text = cols[0].get_text(strip=True)

        # Skip Totals/Averages/etc.
        if not year_text.isdigit():
            continue

        year = int(year_text)
        if year < 1870 or year > 2100:
            continue

        # TEAM (col 1 is team in this table)
        team_key = None
        team_cell_text = cols[1].get_text(" ", strip=True)

        # Handle merged team names like "Footscray / Western Bulldogs"
        for part in team_cell_text.split("/"):
            part = part.strip()
            if part in TEAM_NAME_TO_KEY:
                team_key = TEAM_NAME_TO_KEY[part]
                break

        if not team_key:
            if DEBUG:
                print "  !! Unknown team cell:", team_cell_text, "(year", year, ")"
            continue

        # GM is col 3 (0 Year, 1 Team, 2 #, 3 GM)
        try:
            games = int(cols[3].get_text(strip=True))
        except:
            games = 0

        # GL is col 9 (0 Year, 1 Team, 2 #, 3 GM, 4 W-D-L, 5 KI, 6 MK, 7 HB, 8 DI, 9 GL)
        try:
            goals = int(cols[9].get_text(strip=True))
        except:
            goals = 0

        row_key = (year, team_key, games, goals)
        if row_key in seen_rows:
            continue
        seen_rows.add(row_key)

        seasons_seen += 1

        if DEBUG_SEASONS:
            print "   Season:", year, team_key, "| GM:", games, "| GL:", goals

        if team_key not in club_stats:
            club_stats[team_key] = {"games": 0, "goals": 0}

        club_stats[team_key]["games"] += games
        club_stats[team_key]["goals"] += goals

        if goals > max_goals_season:
            max_goals_season = goals
            max_goals_year = year

    if DEBUG:
        print "  Seasons parsed:", seasons_seen
        print "  Club totals:"
        for team in sorted(club_stats):
            print "   ", team.ljust(12), \
                  "Games:", club_stats[team]["games"], \
                  "Goals:", club_stats[team]["goals"]
        print "  Max goals in a season:", max_goals_season, "(Year:", max_goals_year, ")"
        if seasons_seen == 0:
            print "  !! WARNING: no season rows detected in selected table"

    return club_stats, max_goals_season, max_goals_year


players_done = 0

for i, (pid, name) in enumerate(players, 1):

    if DEBUG_LIMIT and i > DEBUG_LIMIT:
        print "\nDEBUG LIMIT REACHED — stopping early"
        break

    print "\n[%d / %d] %s" % (i, total, name)

    try:
        result = scrape_season_stats(pid, name)
    except Exception as e:
        print "  !! FAILED:", e
        continue

    if not result:
        continue

    club_stats, max_goals, max_year = result

    # Write per-club stats
    for team, stats in club_stats.items():
        c.execute("""
            INSERT OR REPLACE INTO player_club_stats
            (player_id, team, games, goals)
            VALUES (?, ?, ?, ?)
        """, (pid, team, stats["games"], stats["goals"]))

    # Update players peak season goals
    c.execute("""
        UPDATE players
        SET max_goals_season = ?,
            max_goals_season_year = ?
        WHERE player_id = ?
    """, (max_goals, max_year, pid))

    conn.commit()
    players_done += 1

    if DEBUG:
        print "  ✓ DB updated"

    if i % 50 == 0:
        print "  Progress:", i, "/", total

    time.sleep(REQUEST_DELAY)

conn.close()

print "\n" + "-" * 80
print "DONE"
print "Players processed:", players_done
print "-" * 80