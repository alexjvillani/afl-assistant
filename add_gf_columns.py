import sqlite3

conn = sqlite3.connect("players.db")
c = conn.cursor()

try:
    c.execute("ALTER TABLE players ADD COLUMN gf_wins INTEGER DEFAULT 0")
except:
    pass

try:
    c.execute("ALTER TABLE players ADD COLUMN gf_losses INTEGER DEFAULT 0")
except:
    pass

try:
    c.execute("ALTER TABLE players ADD COLUMN gf_draws INTEGER DEFAULT 0")
except:
    pass

conn.commit()
conn.close()

print("GF columns ensured.")
