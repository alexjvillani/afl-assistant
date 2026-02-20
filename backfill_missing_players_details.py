# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import sqlite3
import time

DB_PATH = "players.db"

# ----------------------------
# DEBUG TOGGLE
# ----------------------------
DEBUG = True          # set False to silence most output
DEBUG_LIMIT = None    # e.g. 50 to stop after N players

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

c.execute("""
    SELECT player_id, name
    FROM players
    WHERE first_year IS NULL
       OR last_year IS NULL
""")

players = c.fetchall()
total = len(players)

print "Players needing detail backfill:", total
print "-" * 70


def scrape_player_details(url, name):

    if DEBUG:
        print "\n[FETCH]", name
        print " URL:", url

    r = requests.get(url, timeout=15)
    if r.status_code != 200:
        print "  !! HTTP ERROR:", r.status_code
        return None

    soup = BeautifulSoup(r.text, "html.parser")

    max_goals = max_kicks = max_handballs = max_disposals = 0
    years = []

    # ---- RAW DEBUG HOLDERS ----
    raw_stats = {}

    for row in soup.find_all("tr"):
        cols = row.find_all("td")
        if len(cols) < 2:
            continue

        label = cols[0].get_text(strip=True)
        value_text = cols[1].get_text(strip=True)

        # ---- STAT PARSING ----
        try:
            value = int(value_text)
        except:
            value = 0

        if label in ("Goals", "Kicks", "Handballs", "Disposals"):
            raw_stats[label] = value_text

        if label == "Goals":
            max_goals = value
        elif label == "Kicks":
            max_kicks = value
        elif label == "Handballs":
            max_handballs = value
        elif label == "Disposals":
            max_disposals = value

        # ---- YEAR SCAN ----
        try:
            year = int(cols[0].get_text(strip=True))
            if 1870 < year < 2100:
                years.append(year)
        except:
            pass

    if DEBUG:
        print "  Raw stats:"
        for k in ("Goals", "Kicks", "Handballs", "Disposals"):
            print "   ", k.ljust(11), "=>", raw_stats.get(k, "MISSING")

        print "  Parsed max stats:"
        print "    Goals:", max_goals
        print "    Kicks:", max_kicks
        print "    Handballs:", max_handballs
        print "    Disposals:", max_disposals

        if years:
            print "  Years found:", min(years), "-", max(years)
        else:
            print "  !! NO YEARS FOUND"

    if not years:
        return None

    return (
        max_goals,
        max_kicks,
        max_handballs,
        max_disposals,
        min(years),
        max(years)
    )


updated = 0

for i, (pid, name) in enumerate(players, 1):

    if DEBUG_LIMIT and i > DEBUG_LIMIT:
        print "\nDEBUG LIMIT REACHED — stopping early"
        break

    print "\n[%d / %d]" % (i, total), name

    try:
        details = scrape_player_details(pid, name)
    except Exception as e:
        print "  !! FAILED:", e
        continue

    if not details:
        print "  !! SKIPPED (no usable data)"
        continue

    c.execute("""
        UPDATE players
        SET
            max_goals_game = ?,
            max_kicks_game = ?,
            max_handballs_game = ?,
            max_disposals_game = ?,
            first_year = ?,
            last_year = ?
        WHERE player_id = ?
    """, details + (pid,))

    if c.rowcount > 0:
        updated += 1
        print "  ✓ UPDATED DB ROW"
    else:
        print "  – NO CHANGE"

    conn.commit()
    time.sleep(0.35)

conn.close()

print "\n" + "-" * 70
print "DETAIL BACKFILL COMPLETE"
print "Players updated:", updated
print "-" * 70