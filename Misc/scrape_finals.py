# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import sqlite3
import time

DB_PATH = "players.db"

FINALS_ROUNDS = ("EF", "QF", "SF", "PF", "GF")

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# Only scrape players not yet processed
c.execute("""
    SELECT player_id, name
    FROM players
    WHERE finals_played IS NULL
       OR finals_played = 0
""")

players = c.fetchall()

print "Players to process:", len(players)
print "-" * 60


def scrape_finals(url, name):

    print "\n[FETCH]", name

    r = requests.get(url, timeout=15)
    if r.status_code != 200:
        print "  HTTP ERROR:", r.status_code
        return None

    soup = BeautifulSoup(r.text, "html.parser")

    finals_played = finals_wins = finals_losses = finals_draws = 0

    total_finals_goals = 0
    total_finals_disposals = 0
    finals_disposal_games = 0

    max_finals_goals = 0
    max_finals_disposals = 0

    max_EF_goals = max_SF_goals = max_PF_goals = max_GF_goals = 0

    tables = soup.find_all("table")

    for table in tables:
        rows = table.find_all("tr")

        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 10:
                continue

            rd = cols[2].get_text(strip=True)

            if rd not in FINALS_ROUNDS:
                continue

            result = cols[3].get_text(strip=True)

            # ---- Goals (always safe) ----
            try:
                goals = int(cols[9].get_text(strip=True) or 0)
            except:
                goals = 0

            # ---- Disposals (may not exist historically) ----
            di_text = cols[8].get_text(strip=True)

            if di_text != "":
                try:
                    disposals = int(di_text)
                    total_finals_disposals += disposals
                    finals_disposal_games += 1
                    max_finals_disposals = max(max_finals_disposals, disposals)
                except:
                    disposals = None
            else:
                disposals = None

            # ---- Record ----
            finals_played += 1

            if result == "W":
                finals_wins += 1
            elif result == "L":
                finals_losses += 1
            elif result == "D":
                finals_draws += 1

            # ---- Goals Totals ----
            total_finals_goals += goals
            max_finals_goals = max(max_finals_goals, goals)

            # ---- Round-specific goals ----
            if rd == "EF":
                max_EF_goals = max(max_EF_goals, goals)
            elif rd == "SF":
                max_SF_goals = max(max_SF_goals, goals)
            elif rd == "PF":
                max_PF_goals = max(max_PF_goals, goals)
            elif rd == "GF":
                max_GF_goals = max(max_GF_goals, goals)

    print "  Finals:", finals_played, "(%d-%d-%d)" % (finals_wins, finals_losses, finals_draws)

    return (
        finals_played,
        finals_wins,
        finals_losses,
        finals_draws,
        total_finals_goals,
        total_finals_disposals,
        finals_disposal_games,
        max_finals_goals,
        max_finals_disposals,
        max_EF_goals,
        max_SF_goals,
        max_PF_goals,
        max_GF_goals
    )


updated = 0

for i, (pid, name) in enumerate(players, 1):

    print "\n[%d / %d]" % (i, len(players))

    try:
        data = scrape_finals(pid, name)
    except Exception as e:
        print "  FAILED:", e
        continue

    if not data:
        continue

    c.execute("""
        UPDATE players
        SET
            finals_played = ?,
            finals_wins = ?,
            finals_losses = ?,
            finals_draws = ?,
            total_finals_goals = ?,
            total_finals_disposals = ?,
            finals_disposal_games = ?,
            max_finals_goals = ?,
            max_finals_disposals = ?,
            max_EF_goals = ?,
            max_SF_goals = ?,
            max_PF_goals = ?,
            max_GF_goals = ?
        WHERE player_id = ?
    """, data + (pid,))

    conn.commit()
    updated += 1
    time.sleep(0.25)

conn.close()

print "\nFinals scrape complete."
print "Updated:", updated