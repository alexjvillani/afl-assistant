# quick_check_finals.py
import sqlite3
conn = sqlite3.connect("players.db")
c = conn.cursor()

c.execute("SELECT COUNT(*) FROM players WHERE COALESCE(max_finals_goals,0) >= 5")
print("Players with max_finals_goals >= 5:", c.fetchone()[0])

c.execute("SELECT name, max_finals_goals FROM players WHERE COALESCE(max_finals_goals,0) >= 5 ORDER BY max_finals_goals DESC LIMIT 20")
for r in c.fetchall():
    print(r)

conn.close()