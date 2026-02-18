# -*- coding: utf-8 -*-

import sqlite3

conn = sqlite3.connect("players.db")
c = conn.cursor()

c.execute("""
CREATE TABLE IF NOT EXISTS player_seasons (
    player_id TEXT,
    year INTEGER,
    team TEXT,
    PRIMARY KEY (player_id, year, team)
)
""")

c.execute("CREATE INDEX IF NOT EXISTS idx_ps_year_team ON player_seasons (year, team)")
c.execute("CREATE INDEX IF NOT EXISTS idx_ps_player ON player_seasons (player_id)")

conn.commit()
conn.close()

print "player_seasons table ready."
