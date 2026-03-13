# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import sqlite3
import re

# -------------------------------------------------
# CONFIG
# -------------------------------------------------

DB_PATH = "players.db"
WIKI_URL = "https://en.wikipedia.org/wiki/List_of_VFL/AFL_minor_premiers"

HEADERS = {
    "User-Agent": "Gridley AFL Assistant/1.0 (contact:gridley@local)"
}

DEBUG = True


# -------------------------------------------------
# TEAM NORMALISATION
# -------------------------------------------------

TEAM_MAP = {
    "st kilda": "stkilda",
    "south melbourne": "swans",
    "sydney": "swans",

    "north melbourne": "kangaroos",

    "footscray": "bullldogs",
    "western bulldogs": "bullldogs",

    "brisbane bears": "brisbaneb",
    "brisbane lions": "brisbanel",

    "greater western sydney": "gws",

    "west coast": "westcoast",

    "gold coast": "goldcoast",

    "port adelaide": "padelaide",
}

def norm_team(s):

    raw = s

    s = s.lower()
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"[^a-z ]+", "", s).strip()

    team = TEAM_MAP.get(s, s.replace(" ", ""))

    if DEBUG:
        print("TEAM NORMALISE:", raw, "->", team)

    return team


# -------------------------------------------------
# SCRAPER
# -------------------------------------------------

def scrape_minor_premiers(conn):

    print("\nFetching minor premiers from Wikipedia...\n")

    r = requests.get(WIKI_URL, headers=HEADERS, timeout=20)

    print("HTTP status:", r.status_code)

    soup = BeautifulSoup(r.text, "html.parser")

    tables = soup.find_all("table", {"class": "wikitable"})

    print("WIKITABLES FOUND:", len(tables))

    if not tables:
        raise Exception("No wikitable found on page")

    table = tables[0]

    print("Using first wikitable (Minor Premiers table)\n")

    rows = table.find_all("tr")[1:]

    print("Rows detected:", len(rows), "\n")

    c = conn.cursor()

    inserted = 0
    zero_match_seasons = 0

    for tr in rows:

        tds = tr.find_all("td")

        if len(tds) < 2:
            continue

        year_text = tds[0].get_text(" ", strip=True)
        club_cell = tds[1]

        years = re.findall(r"\d{4}", year_text)

        if not years:
            continue

        clubs = [a.get_text(strip=True) for a in club_cell.find_all("a")]

        if not clubs:
            clubs = [club_cell.get_text(strip=True)]

        for i, year in enumerate(years):

            club_raw = clubs[i] if i < len(clubs) else clubs[0]

            team_key = norm_team(club_raw)

            print("\nYEAR:", year)
            print("CLUB:", club_raw)
            print("TEAM KEY:", team_key)

            c.execute("""
                SELECT DISTINCT p.player_id
                FROM players p
                JOIN player_seasons s
                  ON s.player_id = p.player_id
                WHERE s.team = ?
                AND s.year = ?
            """, (team_key, int(year)))

            pids = c.fetchall()

            print("PLAYERS FOUND:", len(pids))

            if not pids:

                print("!! WARNING: NO PLAYERS FOUND FOR", team_key, year)

                zero_match_seasons += 1
                continue

            for (pid,) in pids:

                c.execute("""
                    INSERT INTO minor_premiers (year, team, player_id)
                    VALUES (?, ?, ?)
                """, (int(year), team_key, pid))

                inserted += 1

    conn.commit()

    print("\nZero-player seasons:", zero_match_seasons)

    return inserted


# -------------------------------------------------
# UPDATE PLAYER COUNTS
# -------------------------------------------------

def update_counts(conn):

    print("\nUpdating player minor premiership counts...\n")

    c = conn.cursor()

    # ensure column exists
    try:
        c.execute("ALTER TABLE players ADD COLUMN minor_prem_count INTEGER DEFAULT 0")
    except:
        pass

    c.execute("UPDATE players SET minor_prem_count = 0")

    c.execute("""
        UPDATE players
        SET minor_prem_count = (
            SELECT COUNT(*)
            FROM minor_premiers mp
            WHERE mp.player_id = players.player_id
        )
    """)

    conn.commit()


# -------------------------------------------------
# MAIN
# -------------------------------------------------

def main():

    print("\n===== MINOR PREMIERS SCRAPER =====\n")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    print("Ensuring minor_premiers table exists...\n")

    c.execute("""
    CREATE TABLE IF NOT EXISTS minor_premiers (
        year INTEGER,
        team TEXT,
        player_id TEXT
    )
    """)

    print("Clearing existing records...\n")

    c.execute("DELETE FROM minor_premiers")
    conn.commit()

    inserted = scrape_minor_premiers(conn)

    update_counts(conn)

    conn.close()

    print("\nInserted records:", inserted)
    print("Updated player minor_prem_count")

    print("\nDone.")


if __name__ == "__main__":
    main()