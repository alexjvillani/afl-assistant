# -*- coding: utf-8 -*-

import sqlite3

conn = sqlite3.connect("players.db")
c = conn.cursor()

print("\nUpdating GF appearances...\n")

c.execute("""
UPDATE players
SET gf_appearances =
COALESCE(gf_wins,0) +
COALESCE(gf_losses,0) +
COALESCE(gf_draws,0)
""")

conn.commit()

print("Rows updated:", conn.total_changes)

print("\nTesting result...\n")

c.execute("""
SELECT name, gf_wins, gf_losses, gf_draws, gf_appearances
FROM players
WHERE gf_appearances > 0
ORDER BY gf_appearances DESC
LIMIT 10
""")

for r in c.fetchall():
    print(r)

conn.close()

print("\nDone.\n")