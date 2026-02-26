# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import sqlite3
from urlparse import urljoin

BASE = "https://afltables.com"
URL  = BASE + "/afl/stats/alltime/leadinggk.html"

conn = sqlite3.connect("players.db")
c = conn.cursor()

print "\n[DEBUG] Checking DB columns...\n"

def ensure_column(name):
    try:
        c.execute("SELECT %s FROM players LIMIT 1" % name)
        print "[DEBUG] Column exists:", name
    except sqlite3.OperationalError:
        print "[DEBUG] Adding column:", name
        c.execute("ALTER TABLE players ADD COLUMN %s INTEGER DEFAULT 0" % name)
        conn.commit()

ensure_column("leading_gk_wins")
ensure_column("leading_gk_goals")

print "\n[DEBUG] Resetting Leading Goalkicker (H&A) data...\n"

c.execute("UPDATE players SET leading_gk_wins = 0")
c.execute("UPDATE players SET leading_gk_goals = 0")
conn.commit()

print "[DEBUG] Fetching:", URL

r = requests.get(URL)
print "[DEBUG] HTTP status:", r.status_code

soup = BeautifulSoup(r.text, "html.parser")
rows = soup.find_all("tr")

processed = matched = missed = 0

print "\n[DEBUG] Beginning row parse...\n"

for row in rows:

    cols = row.find_all("td")
    if len(cols) < 4:
        continue

    year = cols[0].get_text(strip=True)
    player_col = cols[1]
    goals_col  = cols[2]

    link = player_col.find("a")
    if not link:
        continue

    name = player_col.get_text(strip=True)

    # ✅ EXACT SAME LOGIC AS scrape_players.py
    player_id = urljoin(URL, link["href"])

    try:
        goals_ha = int(goals_col.get_text(strip=True))
    except:
        continue

    processed += 1

    print "[ROW]", year, name, "| Goals:", goals_ha
    print "      player_id:", player_id

    c.execute("""
        UPDATE players
        SET leading_gk_wins = leading_gk_wins + 1,
            leading_gk_goals = leading_gk_goals + ?
        WHERE player_id = ?
    """, (goals_ha, player_id))

    if c.rowcount == 1:
        matched += 1
        print "      [DB] ✔ matched"
    else:
        missed += 1
        print "      [DB] ✖ NO MATCH"

    print ""

conn.commit()
conn.close()

print "\n[DEBUG] Scrape complete"
print "Processed:", processed
print "Matched:", matched
print "Missed:", missed
print ""