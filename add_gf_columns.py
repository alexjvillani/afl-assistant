import sqlite3

conn = sqlite3.connect("players.db")
c = conn.cursor()

try:
    c.execute("ALTER TABLE players ADD COLUMN height INTEGER")
    print("Height column added.")
except:
    print("Height column already exists.")

conn.commit()
conn.close()
