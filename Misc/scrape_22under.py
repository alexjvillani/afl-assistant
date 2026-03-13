# -*- coding: utf-8 -*-

import re
import sqlite3
import unicodedata
import urllib2
from bs4 import BeautifulSoup

WIKI_URL = "https://en.wikipedia.org/wiki/22_Under_22_team"
DB_PATH = "players.db"

HEADERS = {
    "User-Agent": "AFLStatsBot/1.0 (contact: alex@example.com)"
}

# -------------------------------------------------
# HELPERS (FINAL)
# -------------------------------------------------

NICKNAME_MAP = {
    u"dan": u"daniel",
    u"mike": u"michael",
    u"nick": u"nicholas",
    u"tom": u"thomas",
    u"josh": u"joshua",
}

def to_unicode(s):
    if isinstance(s, str):
        return s.decode("utf-8", "ignore")
    return s

def normalise(s):
    s = to_unicode(s)
    s = unicodedata.normalize("NFKD", s)
    s = u"".join(c for c in s if not unicodedata.combining(c))
    s = s.replace(u"'", u"")
    s = s.replace(u"’", u"")
    s = re.sub(ur"\s+", u" ", s.strip())
    return s.lower()

def name_variants(name):
    """
    Generates AFLTables-safe name variants:
    - patrick dangerfield
    - dangerfield, patrick
    - daniel curtin (from dan curtin)
    """
    base = normalise(name)
    parts = base.split(u" ")

    variants = {base}

    if len(parts) >= 2:
        first = parts[0]
        last = u" ".join(parts[1:])

        # surname-first
        variants.add(u"%s, %s" % (last, first))

        # nickname expansion
        if first in NICKNAME_MAP:
            full = NICKNAME_MAP[first]
            variants.add(u"%s %s" % (full, last))
            variants.add(u"%s, %s" % (last, full))

    return variants

# -------------------------------------------------
# DATABASE SETUP
# -------------------------------------------------

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

c.execute("""
CREATE TABLE IF NOT EXISTS player_22u22 (
    player_id TEXT,
    year INTEGER,
    UNIQUE(player_id, year)
)
""")
conn.commit()

print("[DEBUG] DB connected")

c.execute("SELECT player_id, name FROM players")
rows = c.fetchall()

lookup = {}
for pid, name in rows:
    lookup[normalise(name)] = pid

print("[DEBUG] players loaded:", len(lookup))

# -------------------------------------------------
# FETCH WIKIPEDIA
# -------------------------------------------------

print("[DEBUG] Requesting", WIKI_URL)
req = urllib2.Request(WIKI_URL, headers=HEADERS)
html = urllib2.urlopen(req).read()
print("[DEBUG] Download length:", len(html))

soup = BeautifulSoup(html, "html.parser")
content = soup.find("div", class_="mw-parser-output")

# AFLW cutoff
aflw_h2 = content.find("h2", id="AFLW_teams")

# -------------------------------------------------
# PARSE AFL YEARS ONLY
# -------------------------------------------------

seen = 0
inserted = 0
missed = set()

for heading in content.find_all("div", class_="mw-heading"):

    # hard stop once AFLW begins
    if aflw_h2 and heading.sourceline and aflw_h2.sourceline:
        if heading.sourceline > aflw_h2.sourceline:
            break

    h3 = heading.find("h3")
    if not h3:
        continue

    m = re.search(r"(19|20)\d{2}", h3.get_text())
    if not m:
        continue

    year = int(m.group(0))
    print("\n[DEBUG] Processing AFL year:", year)

    table = heading.find_next_sibling("table", class_="wikitable")
    if not table:
        print("  [WARN] No table for year", year)
        continue

    for td in table.find_all("td"):
        a = td.find("a")
        if not a:
            continue

        name = a.get_text(strip=True)

        # skip clubs
        if "Football Club" in name:
            continue

        seen += 1
        matched = False

        for key in name_variants(name):
            pid = lookup.get(key)
            if pid:
                print("  [MATCH]", name.encode("utf-8"), "->", pid)
                c.execute(
                    "INSERT OR IGNORE INTO player_22u22 (player_id, year) VALUES (?, ?)",
                    (pid, year)
                )
                inserted += 1
                matched = True
                break

        if not matched:
            print("  [MISS]", name.encode("utf-8"))
            missed.add(name)

conn.commit()

# -------------------------------------------------
# SUMMARY
# -------------------------------------------------

print("\n==============================")
print("22 UNDER 22 (AFL ONLY)")
print("==============================")
print("Players seen:", seen)
print("Inserted:", inserted)
print("Unmatched:", len(missed))

if missed:
    print("\nSample unmatched:")
    for n in sorted(missed):
        print(" -", n.encode("utf-8"))