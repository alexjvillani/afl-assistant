# -*- coding: utf-8 -*-

import sqlite3

DB = "players.db"

SAM_PID = "https://afltables.com/afl/stats/players/S/Sam_Docherty.html"

conn = sqlite3.connect(DB)
c = conn.cursor()

def print_rows(title, rows):
    print("\n" + title)
    print("-" * len(title))
    for r in rows:
        print(r)
    if not rows:
        print("(none)")

# ---------------------------------
# Find Ben Hudson
# ---------------------------------
c.execute("""
SELECT player_id, name
FROM players
WHERE name LIKE '%Hudson%'
ORDER BY name
""")
hudsons = c.fetchall()
print_rows("HUDSON MATCHES", hudsons)

ben_pid = None
for pid, name in hudsons:
    if name == "Hudson, Ben":
        ben_pid = pid

print("\nBEN PID:", ben_pid)
print("SAM PID:", SAM_PID)

# ---------------------------------
# Sam Docherty seasons
# ---------------------------------
c.execute("""
SELECT year, team
FROM player_seasons
WHERE player_id = ?
ORDER BY year
""", (SAM_PID,))
sam_rows = c.fetchall()
print_rows("SAM DOCHERTY SEASONS", sam_rows)

# ---------------------------------
# Ben Hudson seasons
# ---------------------------------
c.execute("""
SELECT year, team
FROM player_seasons
WHERE player_id = ?
ORDER BY year
""", (ben_pid,))
ben_rows = c.fetchall()
print_rows("BEN HUDSON SEASONS", ben_rows)

# ---------------------------------
# Same team + same year overlap
# ---------------------------------
c.execute("""
SELECT ps1.year, ps1.team
FROM player_seasons ps1
JOIN player_seasons ps2
  ON ps1.year = ps2.year
 AND ps1.team = ps2.team
WHERE ps1.player_id = ?
AND ps2.player_id = ?
ORDER BY ps1.year
""", (SAM_PID, ben_pid))

overlap = c.fetchall()
print_rows("DIRECT TEAMMATE OVERLAP", overlap)

conn.close()