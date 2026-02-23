# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import sqlite3
import time
import re

DB_PATH = "players.db"

DEBUG = True
DEBUG_LIMIT = None


def ensure_columns(conn):
    """
    Add Top-10 stat columns if they don't already exist.
    Safe to run multiple times.
    """
    c = conn.cursor()
    c.execute("PRAGMA table_info(players)")
    existing = set([row[1] for row in c.fetchall()])

    columns = [
        ("max_marks_game", "INTEGER"),
        ("max_hitouts_game", "INTEGER"),
        ("max_tackles_game", "INTEGER"),
    ]

    for col, coltype in columns:
        if col not in existing:
            if DEBUG:
                print "Adding column:", col
            try:
                c.execute(
                    "ALTER TABLE players ADD COLUMN %s %s DEFAULT 0"
                    % (col, coltype)
                )
            except Exception as e:
                print "  !! Failed to add column", col, ":", e

    conn.commit()


def norm(s):
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


def first_int(text):
    m = re.search(r"\d+", text or "")
    return int(m.group(0)) if m else 0


conn = sqlite3.connect(DB_PATH)
ensure_columns(conn)
c = conn.cursor()

# Only players missing at least one Top-10 stat
c.execute("""
    SELECT player_id, name
    FROM players
    WHERE max_marks_game = 0
       OR max_hitouts_game = 0
       OR max_tackles_game = 0
""")

players = c.fetchall()
total = len(players)

print "Players needing Top-10 backfill:", total
print "-" * 60


def scrape_top10_stats(url, name):

    if DEBUG:
        print "\n[FETCH]", name
        print " URL:", url

    r = requests.get(url, timeout=15)
    if r.status_code != 200:
        print "  !! HTTP ERROR:", r.status_code
        return None

    soup = BeautifulSoup(r.text, "html.parser")

    max_marks = 0
    max_hitouts = 0
    max_tackles = 0

    for row in soup.find_all("tr"):
        cols = row.find_all("td")
        if len(cols) < 2:
            continue

        label = norm(cols[0].get_text(strip=True))
        value = first_int(cols[1].get_text(" ", strip=True))

        if label == "marks":
            max_marks = value
        elif label == "hitouts":
            max_hitouts = value
        elif label == "tackles":
            max_tackles = value

    if DEBUG:
        print "  Parsed:"
        print "    Marks   :", max_marks
        print "    Hitouts :", max_hitouts
        print "    Tackles :", max_tackles

    return max_marks, max_hitouts, max_tackles


updated = 0

for i, (pid, name) in enumerate(players, 1):

    if DEBUG_LIMIT and i > DEBUG_LIMIT:
        print "\nDEBUG LIMIT REACHED — stopping early"
        break

    print "\n[%d / %d]" % (i, total), name

    try:
        stats = scrape_top10_stats(pid, name)
    except Exception as e:
        print "  !! FAILED:", e
        continue

    if not stats:
        continue

    c.execute("""
        UPDATE players
        SET
            max_marks_game = ?,
            max_hitouts_game = ?,
            max_tackles_game = ?
        WHERE player_id = ?
    """, stats + (pid,))

    if c.rowcount:
        updated += 1
        print "  ✓ UPDATED"

    conn.commit()
    time.sleep(0.25)

conn.close()

print "\n" + "-" * 60
print "TOP-10 BACKFILL COMPLETE"
print "Players updated:", updated
print "-" * 60