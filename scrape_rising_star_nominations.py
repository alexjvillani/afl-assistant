# -*- coding: utf-8 -*-

import requests
import sqlite3
import re
from bs4 import BeautifulSoup

DB_PATH = "players.db"
URL = "https://www.zerohanger.com/afl/afl-rising-star-nominations/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# -------------------------------------------------
# NAME ALIASES (ZeroHanger → AFLTables)
# -------------------------------------------------

ALIASES = {
    # Already added
    "cameron rayner": "cam rayner",
    "edward allan": "ed allan",
    "jack macrae": "jack macrae",
    "mitch golby": "mitchell golby",
    "marcus bullen": "marc bullen",
    "paddy ryder": "patrick ryder",

    # Rising Star cleanups
    "lukas pedlar": "luke pedlar",
    "reubin ginbey": "reuben ginbey",
    "nikolas cox": "nik cox",
    "mitchell lewis": "mitch lewis",
    "matt taberner": "matthew taberner",
    "jackson macrae": "jack macrae",
    "matthew capuano": "mathew capuano",  # AFLTables spelling

    # 🔧 FINAL FIXES
    "harrison jones": "harry jones",
    "lachlan jones": "lachie jones",
}

# -------------------------------------------------
# NORMALISATION
# -------------------------------------------------

def normalise_name(name):
    name = name.lower().strip()

    if name in ALIASES:
        name = ALIASES[name]

    name = re.sub(r"[^\w\s]", "", name)
    name = re.sub(r"\s+", " ", name)
    return name.strip()


# -------------------------------------------------
# PLAYER INDEX (DUAL FORMAT)
# -------------------------------------------------

def build_player_index():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT player_id, name FROM players")

    idx = {}

    for pid, name in c.fetchall():
        name = name.strip()

        # Original format
        norm = normalise_name(name)
        idx[norm] = pid

        # AFLTables format: "Surname, Firstname"
        if "," in name:
            parts = [p.strip() for p in name.split(",", 1)]
            if len(parts) == 2:
                flipped = parts[1] + " " + parts[0]
                idx[normalise_name(flipped)] = pid

    conn.close()
    print("('Player index keys:', %d)" % len(idx))
    return idx


# -------------------------------------------------
# DATABASE SETUP
# -------------------------------------------------

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

print("🟢 Recreating rising_star_nominations table")

c.execute("DROP TABLE IF EXISTS rising_star_nominations")
c.execute("""
CREATE TABLE rising_star_nominations (
    player_id TEXT,
    year INTEGER,
    round INTEGER,
    team TEXT,
    is_winner INTEGER DEFAULT 0,
    PRIMARY KEY (player_id, year, round)
)
""")

conn.commit()
conn.close()


# -------------------------------------------------
# SCRAPE
# -------------------------------------------------

player_index = build_player_index()

print("('Fetching:', '%s')" % URL)
r = requests.get(URL, headers=HEADERS, timeout=20)

if r.status_code != 200:
    print("HTTP ERROR:", r.status_code)
    raise SystemExit

soup = BeautifulSoup(r.text, "html.parser")

rows = soup.find_all("tr")
print("('Total rows found:', %d)" % len(rows))

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

inserted = 0
matched = 0
no_match = 0

for row in rows:
    cols = row.find_all("td")
    if len(cols) < 4:
        continue

    # Year
    try:
        year = int(cols[0].get_text(strip=True))
    except:
        continue

    # Round (skip OR / notes rows)
    try:
        rnd = int(cols[1].get_text(strip=True))
    except:
        continue

    # Player
    player_cell = cols[2]
    raw_name = player_cell.get_text(strip=True)
    norm_name = normalise_name(raw_name)

    is_winner = 1 if player_cell.find("strong") else 0

    # Team
    team = cols[3].get_text(strip=True).lower()

    player_id = player_index.get(norm_name)

    if not player_id:
        print(
            "('NO MATCH:', 'Year:', %d, 'Rnd:', %d, '| Raw:', %r, '->', %r, '| Team:', %r)"
            % (year, rnd, raw_name, norm_name, team)
        )
        no_match += 1
        continue

    c.execute("""
        INSERT OR IGNORE INTO rising_star_nominations
        (player_id, year, round, team, is_winner)
        VALUES (?, ?, ?, ?, ?)
    """, (player_id, year, rnd, team, is_winner))

    inserted += 1
    matched += 1

conn.commit()
conn.close()

print("\nDONE")
print("('Inserted:', %d)" % inserted)
print("('Matched:', %d)" % matched)
print("('No match:', %d)" % no_match)