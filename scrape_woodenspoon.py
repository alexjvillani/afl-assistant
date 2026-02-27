# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import sqlite3
import re

# -------------------------------------------------
# CONFIG
# -------------------------------------------------

DB_PATH = "players.db"
WIKI_URL = "https://en.wikipedia.org/wiki/List_of_VFL/AFL_wooden_spoons"

HEADERS = {
    "User-Agent": "Gridley AFL Assistant/1.0 (contact: gridley@local)"
}

DEBUG = True   # <<< TOGGLE THIS

# -------------------------------------------------
# TEAM NORMALISATION
# -------------------------------------------------

TEAM_MAP = {
    "st kilda": "stkilda",
    "south melbourne": "swans",
    "north melbourne": "kangaroos",
    "footscray": "bullldogs",
    "western bulldogs": "bullldogs",
    "brisbane bears": "brisbaneb",
    "brisbane lions": "brisbanel",
    "greater western sydney": "gws",
    "west coast": "westcoast",
    "gold coast": "goldcoast",
}

def norm_team(s):
    raw = s
    s = s.lower()
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"[^a-z ]+", "", s).strip()
    team = TEAM_MAP.get(s, s.replace(" ", ""))

    if DEBUG and team not in TEAM_MAP.values():
        pass  # still valid modern teams

    return team

# -------------------------------------------------
# SCRAPER
# -------------------------------------------------

def scrape_wiki_spoons(conn):
    print("Fetching wooden spoon list from Wikipedia...")

    soup = BeautifulSoup(
        requests.get(WIKI_URL, headers=HEADERS, timeout=20).text,
        "html.parser"
    )

    table = None
    for h in soup.find_all(["h2", "h3"]):
        if "wooden spoons by season" in h.get_text(strip=True).lower():
            table = h.find_next("table")
            break

    if not table:
        raise Exception("Could not find wooden spoon table")

    rows = table.find_all("tr")[1:]
    print(("Found", len(rows), "rows"))

    c = conn.cursor()
    inserted = 0
    zero_match_seasons = 0

    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        year_text = tds[0].get_text(" ", strip=True)
        years = re.findall(r"\d{4}", year_text)
        if not years:
            continue

        club_cell = tds[1]
        clubs = [a.get_text(strip=True) for a in club_cell.find_all("a")]
        if not clubs:
            clubs = [club_cell.get_text(strip=True)]

        for i, year in enumerate(years):
            club_raw = clubs[i] if i < len(clubs) else clubs[0]
            team_key = norm_team(club_raw)

            c.execute("""
                SELECT DISTINCT p.player_id
                FROM players p
                JOIN player_seasons s
                  ON s.player_id = p.player_id
                WHERE s.team = ?
                  AND s.year = ?
            """, (team_key, int(year)))

            pids = c.fetchall()

            if DEBUG:
                print("Year:", year, "| Club:", club_raw, "->", team_key,
                      "| Players:", len(pids))

            if not pids:
                zero_match_seasons += 1
                if DEBUG:
                    print("  !! NO PLAYERS FOUND FOR", team_key, year)
                continue

            for (pid,) in pids:
                c.execute("""
                    INSERT INTO wooden_spoons (year, team, player_id)
                    VALUES (?, ?, ?)
                """, (int(year), team_key, pid))
                inserted += 1

    conn.commit()

    if DEBUG:
        print("Zero-player seasons:", zero_match_seasons)

    return inserted

# -------------------------------------------------
# UPDATE COUNTS
# -------------------------------------------------

def update_counts(conn):
    c = conn.cursor()
    c.execute("UPDATE players SET wooden_spoon_count = 0")
    c.execute("""
        UPDATE players
        SET wooden_spoon_count = (
            SELECT COUNT(*)
            FROM wooden_spoons w
            WHERE w.player_id = players.player_id
        )
    """)
    conn.commit()

# -------------------------------------------------
# MAIN
# -------------------------------------------------

def main():
    conn = sqlite3.connect(DB_PATH)

    c = conn.cursor()
    c.execute("DELETE FROM wooden_spoons")
    conn.commit()

    inserted = scrape_wiki_spoons(conn)
    update_counts(conn)

    conn.close()

    print(("Inserted", inserted, "wooden spoon player records"))
    print("Updated player wooden_spoon_count")
    print("Done.")

if __name__ == "__main__":
    main()