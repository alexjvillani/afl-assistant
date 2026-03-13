# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import sqlite3

BASE = "https://afltables.com"

conn = sqlite3.connect("players.db")
c = conn.cursor()

print "\nResetting Brownlow data...\n"

c.execute("UPDATE players SET brownlow_votes = 0")
c.execute("UPDATE players SET brownlow_wins = 0")
conn.commit()

print "Scraping Brownlow totals...\n"

for page in range(0, 9):

    url = BASE + "/afl/brownlow/totals%d.html" % page
    print "Page:", page

    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")

    rows = soup.find_all("tr")

    for row in rows:

        cols = row.find_all("td")
        if len(cols) < 10:
            continue

        link = cols[1].find("a")
        if not link:
            continue

        # ------------------------------------------------
        # Build FULL correct player URL
        # ------------------------------------------------

        href = link["href"]

        # Brownlow page uses ../stats/players/...
        href = href.replace("..", "")

        full_url = BASE + "/afl" + href

        try:
            votes = int(cols[3].get_text(strip=True))
        except:
            continue

        wins_text = cols[-1].get_text(strip=True)
        wins = int(wins_text) if wins_text.isdigit() else 0

        print "Updating:", full_url, "Votes:", votes, "Wins:", wins

        c.execute("""
            UPDATE players
            SET brownlow_votes = ?,
                brownlow_wins = ?
            WHERE player_id = ?
        """, (votes, wins, full_url))

conn.commit()
conn.close()

print "\nBrownlow totals updated successfully.\n"
