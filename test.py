# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import sqlite3
import time

DB_PATH = "players.db"

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

c.execute("SELECT player_id, name FROM players")
players = c.fetchall()

print "Players:", len(players)
print "-" * 60


def scrape_gf(url, name):

    r = requests.get(url, timeout=15)
    if r.status_code != 200:
        return None

    soup = BeautifulSoup(r.text, "html.parser")

    gf_wins = 0
    gf_losses = 0
    gf_draws = 0

    tables = soup.find_all("table")

    for table in tables:

        rows = table.find_all("tr")

        for row in rows:

            cols = row.find_all("td")

            if len(cols) < 10:
                continue

            rd = cols[2].get_text(strip=True)

            if rd != "GF":
                continue

            result = cols[3].get_text(strip=True)

            if result == "W":
                gf_wins += 1
            elif result == "L":
                gf_losses += 1
            elif result == "D":
                gf_draws += 1

    return (gf_wins, gf_losses, gf_draws)


for i, (pid, name) in enumerate(players, 1):

    print "\n[%d / %d] %s" % (i, len(players), name)

    try:
        data = scrape_gf(pid, name)
    except:
        print "  failed"
        continue

    if not data:
        continue

    wins, losses, draws = data

    print "  GF:", wins, "-", draws, "-", losses

    c.execute("""
        UPDATE players
        SET
            gf_wins = ?,
            gf_losses = ?,
            gf_draws = ?
        WHERE player_id = ?
    """, (wins, losses, draws, pid))

    conn.commit()

    time.sleep(0.25)

conn.close()

print "\nGF rebuild complete."