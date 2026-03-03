# -*- coding: utf-8 -*-

import sqlite3

conn = sqlite3.connect("players.db")
c = conn.cursor()

try:
    c.execute("ALTER TABLE players ADD COLUMN brownlow_wins INTEGER DEFAULT 0")
    print "brownlow_wins column added."
except:
    print "brownlow_wins column already exists."

conn.commit()
conn.close()
