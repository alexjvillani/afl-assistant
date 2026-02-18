import sqlite3

conn = sqlite3.connect("players.db")
c = conn.cursor()

c.execute("UPDATE players SET teams = REPLACE(teams, ' ', '')")
conn.commit()
conn.close()

print("Teams cleaned.")
