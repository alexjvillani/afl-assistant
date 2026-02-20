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
BASE_URL = "https://www.draftguru.com.au/awards/all-australian"

HEADERS = {
    "User-Agent": "Gridley AFL Assistant/1.0 (contact: gridley@local)"
}

START_YEAR = 2025
END_YEAR = 1991
REQUEST_DELAY = 0.4
DEBUG = True

# -------------------------------------------------
# DRAFTGURU CLUB → TEAM KEY
# -------------------------------------------------

DRAFTGURU_CLUB_TO_TEAMKEY = {
    "Adelaide": "adelaide",
    "Brisbane": "brisbanel",
    "Brisbane Lions": "brisbanel",
    "Carlton": "carlton",
    "Collingwood": "collingwood",
    "Essendon": "essendon",
    "Fremantle": "fremantle",
    "Geelong": "geelong",
    "Gold Coast": "goldcoast",
    "GWS": "gws",
    "Greater Western Sydney": "gws",
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
# AFLTABLES CANONICAL NAME ALIASES
# -------------------------------------------------

NAME_ALIASES = {
    "nathan fyfe": "nat fyfe",
    "ryan o'keefe": "ryan okeefe",
    "michael o'loughlin": "michael oloughlin",
}

# -------------------------------------------------
# NORMALISATION (PY2 SAFE)
# -------------------------------------------------

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

    s = s.replace(u"\xa0", u" ")
    s = s.replace(u"\u2019", u"'")
    s = s.replace(u"\u2018", u"'")
    s = s.replace(u"\u2013", u"-")
    s = s.replace(u"\u2014", u"-")

    s = s.lower()
    s = re.sub(ur"\(.*?\)", u"", s)
    s = re.sub(ur"[^a-z0-9' ]+", u" ", s)
    s = re.sub(ur"\s+", u" ", s).strip()
    return s

def name_variants(db_name):
    variants = set()

    if "," in db_name:
        last, first = [x.strip() for x in db_name.split(",", 1)]

        variants.add(norm(first + " " + last))
        variants.add(norm(last + " " + first))

        first_no_mid = re.sub(ur"\b[a-z]\.?\b", u"", first, flags=re.I)
        first_no_mid = re.sub(ur"\s+", u" ", first_no_mid).strip()

        if first_no_mid != first:
            variants.add(norm(first_no_mid + " " + last))
            variants.add(norm(last + " " + first_no_mid))
    else:
        variants.add(norm(db_name))

    return variants

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
    if DEBUG:
        print "Player index keys:", len(idx)
    return idx

def build_seasons_index(conn):
    seasons = {}
    c = conn.cursor()
    c.execute("SELECT player_id, year, team FROM player_seasons")
    for pid, yr, team in c.fetchall():
        seasons.setdefault((pid, int(yr)), set()).add(team)
    if DEBUG:
        print "Season index rows:", len(seasons)
    return seasons

# -------------------------------------------------
# PLAYER MATCHING
# -------------------------------------------------

def pick_by_span(cands, year):
    hits = []
    for pid, name, fy, ly in cands:
        try:
            if fy and ly and int(fy) <= year <= int(ly):
                hits.append(pid)
        except:
            pass
    if len(hits) == 1:
        return hits[0], "span"
    return None, None

def match_player(player_index, seasons_index, raw_name, year, club):
    key = norm(raw_name)

    # apply AFLTables canonical alias
    key = NAME_ALIASES.get(key, key)

    cands = player_index.get(key, [])
    if not cands:
        return None, "no_match"

    if len(cands) == 1:
        return cands[0][0], "exact"

    team_key = DRAFTGURU_CLUB_TO_TEAMKEY.get(club)
    if team_key:
        club_hits = []
        for pid, _, _, _ in cands:
            teams = seasons_index.get((pid, int(year)), set())
            if team_key in teams:
                club_hits.append(pid)
        if len(club_hits) == 1:
            return club_hits[0], "club_year"

    pid, q = pick_by_span(cands, year)
    if pid:
        return pid, q

    return None, "ambiguous"

# -------------------------------------------------
# SCRAPE ONE YEAR
# -------------------------------------------------

def scrape_year(year, conn, player_index, seasons_index):
    url = BASE_URL + "/" + str(year)
    print "\nYEAR %d - %s" % (year, url)

    r = requests.get(url, headers=HEADERS, timeout=20)
    if r.status_code != 200:
        print "  HTTP", r.status_code
        return 0, 0, 0, 0

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    if not table:
        print "  No table found"
        return 0, 0, 0, 0

    rows = table.find_all("tr")[1:]
    print "  Players found:", len(rows)

    c = conn.cursor()
    total = matched = ambig = nomatch = 0

    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 8:
            continue

        position = tds[0].get_text(strip=True)

        role = None
        if tds[1].get_text(strip=True) == "C":
            role = "captain"
        elif tds[1].get_text(strip=True) == "VC":
            role = "vice-captain"

        name = tds[2].get_text(" ", strip=True)
        club = tds[4].get_text(" ", strip=True)
        draft_pick = tds[5].get_text(" ", strip=True)

        try:
            times_aa = int(tds[7].get_text(strip=True))
        except:
            times_aa = None

        pid, q = match_player(player_index, seasons_index, name, year, club)

        if pid:
            matched += 1
        elif q == "ambiguous":
            ambig += 1
            print "  ?? AMBIG:", name
        else:
            nomatch += 1
            print "  !! NO MATCH:", name

        c.execute("""
            INSERT OR REPLACE INTO all_australian_selections
            (year, player_id, raw_name, position, role, club,
             draft_pick, times_aa, source, match_quality)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            year, pid, name, position, role, club,
            draft_pick, times_aa, "draftguru", q
        ))

        total += 1

    conn.commit()
    return total, matched, ambig, nomatch

# -------------------------------------------------
# UPDATE COUNTS
# -------------------------------------------------

def update_all_aus_counts(conn):
    c = conn.cursor()
    c.execute("UPDATE players SET all_aus_count = 0")
    c.execute("""
        UPDATE players
        SET all_aus_count = (
            SELECT COUNT(*)
            FROM all_australian_selections a
            WHERE a.player_id = players.player_id
        )
    """)

# -------------------------------------------------
# MAIN
# -------------------------------------------------

def main():
    conn = sqlite3.connect(DB_PATH)

    player_index = build_player_index(conn)
    seasons_index = build_seasons_index(conn)

    c = conn.cursor()
    c.execute("DELETE FROM all_australian_selections")
    conn.commit()

    total = matched = ambig = nomatch = 0

    for year in range(START_YEAR, END_YEAR - 1, -1):
        t, m, a, n = scrape_year(year, conn, player_index, seasons_index)
        total += t
        matched += m
        ambig += a
        nomatch += n
        time.sleep(REQUEST_DELAY)

    print "\nUpdating players.all_aus_count..."
    update_all_aus_counts(conn)
    conn.commit()
    conn.close()

    print "\nDONE"
    print "Inserted:", total
    print "Matched:", matched
    print "Ambiguous:", ambig
    print "No match:", nomatch

if __name__ == "__main__":
    main()