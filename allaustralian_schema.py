# -*- coding: utf-8 -*-

import sqlite3

DB_PATH = "players.db"

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

print "Ensuring DraftGuru All-Australian schema..."

c.execute("""
CREATE TABLE IF NOT EXISTS all_australian_selections (
    year INTEGER,
    player_id TEXT,
    raw_name TEXT,
    position TEXT,
    role TEXT,
    club TEXT,
    draft_pick TEXT,
    times_aa INTEGER,
    source TEXT,
    match_quality TEXT,
    PRIMARY KEY (year, raw_name)
)
""")

c.execute("CREATE INDEX IF NOT EXISTS idx_aa_player ON all_australian_selections (player_id)")
c.execute("CREATE INDEX IF NOT EXISTS idx_aa_year ON all_australian_selections (year)")

# optional convenience column
def col_exists(col):
    c.execute("PRAGMA table_info(players)")
    return col in [r[1] for r in c.fetchall()]

if not col_exists("all_aus_count"):
    c.execute("ALTER TABLE players ADD COLUMN all_aus_count INTEGER")
    print "✓ Added players.all_aus_count"

conn.commit()
conn.close()

print "DONE"