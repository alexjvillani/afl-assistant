# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import sqlite3
from urlparse import urljoin   # <-- PYTHON 2 FIX
import time

BASE = "https://afltables.com"
DB_PATH = "players.db"

TEAM_PAGES = [
    "adelaide", "brisbaneb", "brisbanel", "carlton", "collingwood",
    "essendon", "fitzroy", "fremantle", "geelong", "goldcoast",
    "gws", "hawthorn", "melbourne", "kangaroos",
    "padelaide", "richmond", "stkilda", "swans",
    "university", "westcoast", "bullldogs"
]

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

print("\nResetting heights...\n")
c.execute("UPDATE players SET height = NULL")
conn.commit()

updated = 0

for team in TEAM_PAGES:

    url = "%s/afl/stats/alltime/%s.html" % (BASE, team)
    print("Scraping:", url)

    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")

    rows = soup.find_all("tr")

    for row in rows:
        cols = row.find_all("td")

        if len(cols) < 6:
            continue

        link = cols[2].find("a")
        if not link:
            continue

        player_url = urljoin(url, link["href"])

        height_text = cols[4].get_text(strip=True)

        if "cm" not in height_text:
            continue

        height = int(height_text.replace("cm", ""))

        c.execute("""
            UPDATE players
            SET height = ?
            WHERE player_id = ?
        """, (height, player_url))

        if c.rowcount > 0:
            updated += 1
            print("Updated:", height, player_url)

    time.sleep(0.5)

conn.commit()
conn.close()

print("\nDone.")
print("Total players updated:", updated)
