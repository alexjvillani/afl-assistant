import sqlite3

DB = "players.db"

PLAYER_NAME = "Docherty, Sam"

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
c = conn.cursor()

print("\nTESTING TEAMMATE LOGIC\n")

# ---------------------------------
# Find player_id
# ---------------------------------

row = c.execute("""
SELECT player_id
FROM players
WHERE name = ?
""", (PLAYER_NAME,)).fetchone()

if not row:
    print("Player not found:", PLAYER_NAME)
    exit()

pid = row["player_id"]

print("Testing teammates for:", PLAYER_NAME)
print("Player ID:", pid)


# ---------------------------------
# Teammate query (same logic as site)
# ---------------------------------

rows = c.execute("""
SELECT DISTINCT p.name
FROM player_seasons ps1
JOIN player_seasons ps2
  ON ps1.team = ps2.team
 AND ps1.year = ps2.year
JOIN players p
  ON ps2.player_id = p.player_id
WHERE ps1.player_id = ?
AND ps2.player_id != ?
ORDER BY p.name
""", (pid, pid)).fetchall()


print("\nTotal teammates found:", len(rows))


print("\nFirst 50 teammates:\n")

for r in rows[:50]:
    print(r["name"])


conn.close()

print("\nDONE\n")