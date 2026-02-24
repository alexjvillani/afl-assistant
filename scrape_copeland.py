# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import sqlite3
import re

# -------------------------------------------------
# CONFIG
# -------------------------------------------------

DB_PATH = "players.db"

HEADERS = {
    "User-Agent": "Gridley AFL Assistant/1.0 (contact: gridley@local)"
}

MIN_YEAR = 1890
MAX_YEAR = 1981   # pre-1982 only

# -------------------------------------------------
# WIKIPEDIA AWARDS TO SCRAPE
# -------------------------------------------------

WIKI_AWARDS = [
    {"url": "https://en.wikipedia.org/wiki/Malcolm_Blight_Medal", "club": "adelaide", "source": "wikipedia_malcolm_blight"},
    {"url": "https://en.wikipedia.org/wiki/Merrett%E2%80%93Murray_Medal", "club": "brisbanel", "source": "wikipedia_merrett_murray"},
    {"url": "https://en.wikipedia.org/wiki/John_Nicholls_Medal", "club": "carlton", "source": "wikipedia_john_nicholls"},
    {"url": "https://en.wikipedia.org/wiki/Crichton_Medal", "club": "essendon", "source": "wikipedia_crichton"},
    {"url": "https://en.wikipedia.org/wiki/Carji_Greeves_Medal", "club": "geelong", "source": "wikipedia_carji_greeves"},
    {"url": "https://en.wikipedia.org/wiki/Peter_Crimmins_Medal", "club": "hawthorn", "source": "wikipedia_peter_crimmins"},
    {"url": "https://en.wikipedia.org/wiki/Keith_%27Bluey%27_Truscott_Trophy", "club": "melbourne", "source": "wikipedia_bluey_truscott"},
    {"url": "https://en.wikipedia.org/wiki/Syd_Barker_Medal", "club": "kangaroos", "source": "wikipedia_syd_barker"},
    {"url": "https://en.wikipedia.org/wiki/Trevor_Barker_Award", "club": "stkilda", "source": "wikipedia_trevor_barker"},
    {"url": "https://en.wikipedia.org/wiki/Bob_Skilton_Medal", "club": "swans", "source": "wikipedia_bob_skilton"},
    {"url": "https://en.wikipedia.org/wiki/Charles_Sutton_Medal", "club": "bullldogs", "source": "wikipedia_charles_sutton"},
    {"url": "https://en.wikipedia.org/wiki/Jack_Dyer_Medal", "club": "richmond", "source": "wikipedia_jack_dyer"},
]

# -------------------------------------------------
# NAME NORMALISATION + ALIASES
# -------------------------------------------------

NAME_ALIASES = {
    "garry crane": "gary crane",
    "tom clarke": "thomas clarke",
    "perc bushby": "percy bushby",
    "andy wilson": "andrew wilson",
    "ern utting": "ernest utting",
    "andy angwin": "andrew angwin",
    "col austen": "colin austen",
    "john kennedy sr": "john kennedy",
    "john kennedy snr": "john kennedy",
    "miles sellers": "bob sellers",
    "charles cameron": "charlie cameron",
    "charlie skinner": "charles skinner",
    "sid dyer": "syd dyer",
    "robert hancock": "bobby hancock",
    "charlie stanbridge": "charles stanbridge",
    "bill williams": "william williams",
    "rick quade": "ricky quade",
    "norman ware": "norm ware",
    "allan collins": "alan collins",
    "william mahoney": "bill mahoney",
    "sid reeves": "syd reeves",
    "hugh james": "hughie james",
    "thomas o'halloran": "thomas ohalloran",
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

    s = s.replace(u"\u2019", u"'").replace(u"\u2018", u"'")
    s = s.replace("'", "")
    s = re.sub(ur"[^a-z0-9 ]+", u" ", s.lower())
    s = re.sub(ur"\s+", u" ", s).strip()
    return s

def strip_refs(s):
    return re.sub(r"\[[^\]]+\]", "", s).strip()

def name_variants(db_name):
    if "," in db_name:
        last, first = [x.strip() for x in db_name.split(",", 1)]
        return {norm(first + " " + last), norm(last + " " + first)}
    return {norm(db_name)}

# -------------------------------------------------
# BUILD INDICES
# -------------------------------------------------

def build_player_index(conn):
    idx = {}
    c = conn.cursor()
    c.execute("SELECT player_id, name, first_year, last_year FROM players")
    for pid, name, fy, ly in c.fetchall():
        for k in name_variants(name):
            idx.setdefault(k, []).append((pid, fy, ly))
    return idx

def build_seasons_index(conn):
    seasons = {}
    c = conn.cursor()
    c.execute("SELECT player_id, year, team FROM player_seasons")
    for pid, yr, team in c.fetchall():
        seasons.setdefault((pid, int(yr)), set()).add(team)
    return seasons

# -------------------------------------------------
# MATCHING
# -------------------------------------------------

def match_player(player_index, seasons_index, raw_name, year, club):
    key = NAME_ALIASES.get(norm(raw_name), norm(raw_name))
    cands = player_index.get(key, [])

    if not cands:
        return None, "no_match"

    if len(cands) == 1:
        return cands[0][0], "exact"

    club_hits = [
        pid for pid, fy, ly in cands
        if club in seasons_index.get((pid, year), set())
    ]
    if len(club_hits) == 1:
        return club_hits[0], "club_year"

    for pid, fy, ly in cands:
        if fy and ly and fy <= year <= ly:
            return pid, "span"

    return None, "ambiguous"

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
    conn.commit()

# -------------------------------------------------
# SCRAPER
# -------------------------------------------------

def scrape_award(conn, player_index, seasons_index, award):
    print "\nScraping:", award["url"]

    soup = BeautifulSoup(
        requests.get(award["url"], headers=HEADERS, timeout=20).text,
        "html.parser"
    )

    tables = soup.find_all("table", class_="wikitable sortable")
    if not tables:
        return 0, 0, 0

    c = conn.cursor()
    total = matched = nomatch = 0

    for table in tables:
        for tr in table.find_all("tr")[1:]:
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue

            try:
                year = int(strip_refs(tds[0].get_text(strip=True)))
            except:
                continue

            if not (MIN_YEAR <= year <= MAX_YEAR):
                continue

            for a in tds[1].find_all("a"):
                raw_name = strip_refs(a.get_text(strip=True))
                pid, q = match_player(player_index, seasons_index, raw_name, year, award["club"])

                if pid:
                    matched += 1
                else:
                    nomatch += 1
                    print " !! NO MATCH:", raw_name, year, award["club"]

                c.execute("""
                    INSERT OR REPLACE INTO best_and_fairest
                    (year, club, player_id, raw_name,
                     raw_draft_pick, draft_pick_num,
                     source, match_quality, bnf_wins)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
                """, (year, award["club"], pid, raw_name, None, None, award["source"], q))

                total += 1

    conn.commit()
    return total, matched, nomatch

# -------------------------------------------------
# MAIN
# -------------------------------------------------

def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Clear legacy pre-1982 rows (except Collingwood)
    c.execute("""
        DELETE FROM best_and_fairest
        WHERE year < 1982
          AND club != 'collingwood'
    """)
    conn.commit()

    player_index = build_player_index(conn)
    seasons_index = build_seasons_index(conn)

    for award in WIKI_AWARDS:
        scrape_award(conn, player_index, seasons_index, award)

    update_bnf_counts(conn)
    conn.close()

    print "\nDONE"

if __name__ == "__main__":
    main()