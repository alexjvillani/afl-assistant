# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import sqlite3
import re
from urlparse import urljoin  # Python 2 version

BASE = "https://afltables.com"
GF_URL = "https://afltables.com/afl/teams/allteams/gfplayers.html"

# Connect to database
conn = sqlite3.connect("players.db")
c = conn.cursor()

print "\nResetting Grand Final data...\n"

# Reset GF stats
c.execute("UPDATE players SET gf_wins = 0")
c.execute("UPDATE players SET gf_draws = 0")
c.execute("UPDATE players SET gf_losses = 0")
conn.commit()

print "Scraping Grand Final page...\n"

# Get the page
r = requests.get(GF_URL)
soup = BeautifulSoup(r.text, "html.parser")

rows = soup.find_all("tr")

updated = 0

for row in rows:
    cols = row.find_all("td")
    if len(cols) < 2:
        continue

    link = cols[0].find("a")
    if not link:
        continue

    href = link.get("href")
    if not href:
        continue

    # Build full URL
    full_url = urljoin(GF_URL, href)

    gm_text = cols[1].get_text().strip()

    match = re.search(r"\((\d+)-(\d+)-(\d+)\)", gm_text)
    if not match:
        continue

    wins = int(match.group(1))
    draws = int(match.group(2))
    losses = int(match.group(3))

    # Update the database
    c.execute("""
        UPDATE players
        SET gf_wins = ?,
            gf_draws = ?,
            gf_losses = ?
        WHERE player_id = ?
    """, (wins, draws, losses, full_url))

    if c.rowcount > 0:
        updated += 1
        print "Matched:", full_url

conn.commit()
conn.close()

print "\nDone."
print "Players updated:", updated
