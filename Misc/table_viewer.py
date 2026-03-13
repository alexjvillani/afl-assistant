# -*- coding: utf-8 -*-

import sqlite3

DB = "players.db"

conn = sqlite3.connect(DB)
c = conn.cursor()

print "\nDATABASE:", DB
print "\nTABLES:\n"

# ----------------------------------
# List all tables
# ----------------------------------

c.execute("SELECT name FROM sqlite_master WHERE type='table'")

tables = c.fetchall()

for t in tables:

    table = t[0]

    print "------------------------------------"
    print "TABLE:", table
    print "------------------------------------"

    # ----------------------------------
    # Show columns
    # ----------------------------------

    c.execute("PRAGMA table_info(%s)" % table)

    cols = c.fetchall()

    print "\nCOLUMNS:\n"

    for col in cols:
        print col[1], "-", col[2]

    # ----------------------------------
    # Show sample rows
    # ----------------------------------

    print "\nSAMPLE ROWS:\n"

    try:

        c.execute("SELECT * FROM %s LIMIT 5" % table)

        rows = c.fetchall()

        for r in rows:
            print r

    except:
        print "(Could not fetch rows)"

    print "\n"

conn.close()