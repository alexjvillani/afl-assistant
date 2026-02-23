# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import sqlite3
import re
import time

# -------------------------------------------------
# CONFIG
# -------------------------------------------------

DB_PATH = "players.db"
BASE_URL = "https://www.draftguru.com.au/awards/best-and-fairest"

HEADERS = {
    "User-Agent": "Gridley AFL Assistant/1.0 (contact: gridley@local)"
}

START_YEAR = 2025
END_YEAR = 1982
REQUEST_DELAY = 0.4
DEBUG = True

# -------------------------------------------------
# CLUB NAME NORMALISATION
# -------------------------------------------------

DRAFTGURU_CLUB_TO_TEAMKEY = {
    "Adelaide": "adelaide",
    "Brisbane": "brisbanel",
    "Carlton": "carlton",
    "Collingwood": "collingwood",
    "Essendon": "essendon",
    "Fremantle": "fremantle",
    "Geelong": "geelong",
    "Gold Coast": "goldcoast",
    "GWS": "gws",
    "Hawthorn": "hawthorn",
    "Melbourne": "melbourne",
    "North Melbourne": "kangaroos",
    "Port Adelaide": "padelaide",
    "Richmond": "richmond",
    "St Kilda": "stkilda",
    "Sydney": "swans",
    "West Coast": "westcoast",
    "Western Bulldogs": "bullldogs",
}

# -------------------------------------------------
# NAME NORMALISATION
# -------------------------------------------------

NAME_ALIASES = {
    "brad hill": "bradley hill",
    "steve malaxos": "stephen malaxos",
    "matt rendell": "matthew rendell",
    "nathan fyfe": "nat fyfe"
}

def norm(s):
    if not s:
        return ""

    try:
        if not isinstance(s, unicode):
            s = s.decode("utf-8", "ignore")
    except:
        try:
            s = unicode(s)
        except:
            return ""

    # unify non-ASCII apostrophes to ASCII
    s = s.replace(u"\u2019", u"'")
    s = s.replace(u"\u2018", u"'")
    s = s.replace(u"\uFF07", u"'")  # fullwidth apostrophe
    s = s.replace(u"\u00B4", u"'")  # acute accent
    s = s.replace(u"`", u"'")       # grave accent

    # drop apostrophes entirely for matching
    s = s.replace("'", "")

    # whitespace cleanup
    s = s.replace(u"\xa0", u" ")
    s = s.replace(u"\u00a0", u" ")
    s = re.sub(ur"[^a-z0-9 ]+", u" ", s.lower())
    s = re.sub(ur"\s+", u" ", s).strip()

    return s

def name_variants(db_name):
    variants = set()
    if "," in db_name:
        last, first = [x.strip() for x in db_name.split(",", 1)]
        variants.add(norm(first + " " + last))
        variants.add(norm(last + " " + first))
    else:
        variants.add(norm(db_name))
    return variants

# -------------------------------------------------
# DRAFT PICK PARSING
# -------------------------------------------------

def parse_draft_pick(raw):
    if not raw:
        return None, None

    r = raw.lower()
    m = re.search(r"#(\d+)", r)
    if m:
        return raw, int(m.group(1))

    return raw, None

# -------------------------------------------------
# BUILD INDICES
# -------------------------------------------------

def build_player_index(conn):
    idx = {}
    c = conn.cursor()
    c.execute("SELECT player_id, name, first_year, last_year FROM players")
    for pid, name, fy, ly in c.fetchall():
        for k in name_variants(name):
            idx.setdefault(k, []).append((pid, name, fy, ly))
    return idx

def build_seasons_index(conn):
    seasons = {}
    c = conn.cursor()
    c.execute("SELECT player_id, year, team FROM player_seasons")
    for pid, yr, team in c.fetchall():
        seasons.setdefault((pid, int(yr)), set()).add(team)
    return seasons

# -------------------------------------------------
# MATCHING LOGIC
# -------------------------------------------------

def pick_by_span(cands, year):
    for pid, _, fy, ly in cands:
        try:
            if fy and ly and int(fy) <= year <= int(ly):
                return pid, "span"
        except:
            pass
    return None, None

def match_player(player_index, seasons_index, raw_name, year, club):
    key = NAME_ALIASES.get(norm(raw_name), norm(raw_name))
    cands = player_index.get(key, [])

    if not cands:
        return None, "no_match"

    if len(cands) == 1:
        return cands[0][0], "exact"

    team_key = DRAFTGURU_CLUB_TO_TEAMKEY.get(club)
    if team_key:
        club_hits = [
            pid for pid, _, _, _ in cands
            if team_key in seasons_index.get((pid, year), set())
        ]
        if len(club_hits) == 1:
            return club_hits[0], "club_year"

    return pick_by_span(cands, year)

# -------------------------------------------------
# SCHEMA SETUP
# -------------------------------------------------

def ensure_schema(conn):
    c = conn.cursor()

    # Create table if it does not exist (base schema)
    c.execute("""
        CREATE TABLE IF NOT EXISTS best_and_fairest (
            year INTEGER,
            club TEXT,
            player_id TEXT,
            raw_name TEXT,
            raw_draft_pick TEXT,
            draft_pick_num INTEGER,
            source TEXT,
            match_quality TEXT,
            PRIMARY KEY (year, club)
        )
    """)

    # ---- column upgrades (safe) ----
    c.execute("PRAGMA table_info(best_and_fairest)")
    existing = set([row[1] for row in c.fetchall()])

    if "bnf_wins" not in existing:
        print "Adding best_and_fairest.bnf_wins column"
        c.execute(
            "ALTER TABLE best_and_fairest "
            "ADD COLUMN bnf_wins INTEGER"
        )

    # ---- players table upgrade ----
    c.execute("PRAGMA table_info(players)")
    cols = set([row[1] for row in c.fetchall()])

    if "bnf_count" not in cols:
        print "Adding players.bnf_count column"
        c.execute(
            "ALTER TABLE players "
            "ADD COLUMN bnf_count INTEGER DEFAULT 0"
        )

    conn.commit()

# -------------------------------------------------
# SCRAPE ONE YEAR
# -------------------------------------------------

def scrape_year(year, conn, player_index, seasons_index):
    url = BASE_URL + "/" + str(year)
    print "\nYEAR", year, "-", url

    r = requests.get(url, headers=HEADERS, timeout=20)
    if r.status_code != 200:
        print "  HTTP", r.status_code
        return 0, 0, 0

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    if not table:
        print "  No table found"
        return 0, 0, 0

    rows = table.find_all("tr")[1:]
    c = conn.cursor()

    total = matched = nomatch = 0

    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 6:
            continue

        club = tds[0].get_text(strip=True)
        raw_name = tds[1].get_text(" ", strip=True)
        raw_draft = tds[3].get_text(" ", strip=True)

        try:
            bnf_wins = int(tds[5].get_text(strip=True))
        except:
            bnf_wins = None

        raw_draft, draft_num = parse_draft_pick(raw_draft)
        pid, q = match_player(player_index, seasons_index, raw_name, year, club)

        if pid:
            matched += 1
        else:
            nomatch += 1
            print "  !! NO MATCH:", raw_name, "(", club, ")"

        c.execute("""
            INSERT OR REPLACE INTO best_and_fairest
            (year, club, player_id, raw_name,
             raw_draft_pick, draft_pick_num,
             bnf_wins, source, match_quality)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            year, club, pid, raw_name,
            raw_draft, draft_num,
            bnf_wins, "draftguru", q
        ))

        total += 1

    conn.commit()
    return total, matched, nomatch

# -------------------------------------------------
# UPDATE COUNTS
# -------------------------------------------------

def update_bnf_counts(conn):
    c = conn.cursor()
    c.execute("UPDATE players SET bnf_count = 0")
    c.execute("""
        UPDATE players
        SET bnf_count = (
            SELECT COUNT(*)
            FROM best_and_fairest b
            WHERE b.player_id = players.player_id
        )
    """)

# -------------------------------------------------
# MAIN
# -------------------------------------------------

def main():
    conn = sqlite3.connect(DB_PATH)

    ensure_schema(conn)

    player_index = build_player_index(conn)
    seasons_index = build_seasons_index(conn)

    c = conn.cursor()
    c.execute("DELETE FROM best_and_fairest")
    conn.commit()

    total = matched = nomatch = 0

    for year in range(START_YEAR, END_YEAR - 1, -1):
        t, m, n = scrape_year(year, conn, player_index, seasons_index)
        total += t
        matched += m
        nomatch += n
        time.sleep(REQUEST_DELAY)

    update_bnf_counts(conn)
    conn.commit()
    conn.close()

    print "\nDONE"
    print "Inserted:", total
    print "Matched:", matched
    print "No match:", nomatch

if __name__ == "__main__":
    main()