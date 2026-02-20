# -*- coding: utf-8 -*-

import sqlite3

DB_PATH = "players.db"

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

print "Ensuring schema exists..."
print "-" * 60

# ------------------------------------------------
# 1) player_club_stats table
# ------------------------------------------------

c.execute("""
CREATE TABLE IF NOT EXISTS player_club_stats (
    player_id TEXT,
    team TEXT,
    games INTEGER,
    goals INTEGER,
    PRIMARY KEY (player_id, team)
)
""")
print "✓ player_club_stats table ready"

# ------------------------------------------------
# 2) Add columns to players table (if missing)
# ------------------------------------------------

def column_exists(table, column):
    c.execute("PRAGMA table_info(%s)" % table)
    return column in [row[1] for row in c.fetchall()]

if not column_exists("players", "max_goals_season"):
    c.execute("ALTER TABLE players ADD COLUMN max_goals_season INTEGER")
    print "✓ Added players.max_goals_season"
else:
    print "– players.max_goals_season already exists"

if not column_exists("players", "max_goals_season_year"):
    c.execute("ALTER TABLE players ADD COLUMN max_goals_season_year INTEGER")
    print "✓ Added players.max_goals_season_year"
else:
    print "– players.max_goals_season_year already exists"

conn.commit()
conn.close()

print "-" * 60
print "Schema check complete"