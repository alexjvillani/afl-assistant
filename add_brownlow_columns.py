# -*- coding: utf-8 -*-

import sqlite3

conn = sqlite3.connect("players.db")
c = conn.cursor()

try:
    c.execute("ALTER TABLE players ADD COLUMN brownlow_winner INTEGER DEFAULT 0")
except:
    print "brownlow_winner column already exists"

try:
    c.execute("ALTER TABLE players ADD COLUMN brownlow_votes INTEGER DEFAULT 0")
except:
    print "brownlow_votes column already exists"

conn.commit()
conn.close()

print "Brownlow columns ready."
