# -*- coding: utf-8 -*-

import sqlite3
import re
from datetime import date

DB_PATH = "players.db"


# -------------------------------------------------
# TEAM ALIASES
# -------------------------------------------------

TEAM_ALIASES = {

    "adelaide": ["adelaide", "crows"],
    "brisbaneb": ["brisbane bears", "bears"],
    "brisbanel": ["brisbane lions", "lions"],
    "carlton": ["carlton", "blues"],
    "collingwood": ["collingwood", "magpies"],
    "essendon": ["essendon", "bombers"],
    "fitzroy": ["fitzroy"],
    "fremantle": ["fremantle", "dockers"],
    "geelong": ["geelong", "cats"],
    "goldcoast": ["gold coast", "suns"],
    "gws": ["gws", "giants", "greater western sydney"],
    "hawthorn": ["hawthorn", "hawks"],
    "melbourne": ["melbourne", "demons"],
    "kangaroos": ["north melbourne", "kangaroos", "north"],
    "padelaide": ["port adelaide", "power"],
    "richmond": ["richmond", "tigers"],
    "stkilda": ["st kilda", "saints"],
    "swans": ["sydney", "swans", "sydney swans", "south melbourne"],
    "westcoast": ["west coast", "eagles"],
    "bullldogs": ["western bulldogs", "bulldogs", "footscray"],
    "university": ["university"]

}


# -------------------------------------------------
# SHORT CLUE DISPLAY
# -------------------------------------------------

def short_clue(text):

    if not text:
        return text

    t = text.lower()

    if "teammate of" in t:
        return "Teammate"

    if "two different" in t:
        return "2 Clubs"

    if "disposals" in t:
        return "Disposals"

    if "marks" in t:
        return "Marks"

    if "tackles" in t:
        return "Tackles"

    if "games" in t:
        return "Games"

    if "draft" in t:
        return "Draft Pick"

    if "first place" in t:
        return "Minor Prem"

    return text[:35]


# -------------------------------------------------
# NUMBER EXTRACTION
# -------------------------------------------------

def extract_number(clue):

    nums = re.findall(r"\d+", clue)

    if nums:
        return int(nums[0])

    return None


# -------------------------------------------------
# TEAM DETECTION
# -------------------------------------------------

def detect_team(clue):

    c = clue.lower()

    # fix south melbourne issue first
    if "south melbourne" in c:
        return "swans"

    for team, aliases in TEAM_ALIASES.items():

        for alias in aliases:

            if alias in c:
                return team

    return None


# -------------------------------------------------
# TEAMMATE DETECTION
# -------------------------------------------------

def detect_teammate(clue):

    c = clue.lower()

    if "teammate of" not in c:
        return None

    name = c.split("teammate of")[-1]
    name = name.replace(".", "").strip()

    parts = name.split()

    if len(parts) >= 2:

        first = parts[0].capitalize()
        last = parts[1].capitalize()

        return "%s, %s" % (last, first)

    return None


# -------------------------------------------------
# DECADE DETECTION
# -------------------------------------------------

def detect_decade(clue):

    c = clue.lower()

    if "2010" in c:
        return (2010, 2019)

    if "2000" in c or "noughties" in c:
        return (2000, 2009)

    if "1990" in c:
        return (1990, 1999)

    return None


# -------------------------------------------------
# UNIVERSAL CLUE PARSER
# -------------------------------------------------

def parse_gridley_clue(clue):

    clue_lower = clue.lower()

    filters = {}

    # ---------------------------------------------
    # TEAM
    # ---------------------------------------------

    team = detect_team(clue)

    if team:
        filters["team"] = team

    # ---------------------------------------------
    # TEAMMATE
    # ---------------------------------------------

    teammate = detect_teammate(clue)

    if teammate:
        filters["teammate"] = teammate

    # ---------------------------------------------
    # TWO CLUB GAMES
    # ---------------------------------------------

    if "games for two different" in clue_lower:

        num = extract_number(clue)

        if num:
            filters["min_two_clubs_games"] = num

    # ---------------------------------------------
    # DISPOSALS
    # ---------------------------------------------

    if "disposals" in clue_lower:

        num = extract_number(clue)

        if num:
            filters["min_max_disposals_game"] = num
            
            
    if "best and fairest" in clue.lower():
        filters["min_bnf"] = 1

    if "bnf" in clue.lower():
        filters["min_bnf"] = 1

    # ---------------------------------------------
    # TACKLES
    # ---------------------------------------------

    if "tackles" in clue_lower:

        num = extract_number(clue)

        if num:
            filters["min_max_tackles_game"] = num

    # ---------------------------------------------
    # MARKS
    # ---------------------------------------------

    if "marks" in clue_lower:

        num = extract_number(clue)

        if num:
            filters["min_max_marks_game"] = num

    # ---------------------------------------------
    # CAREER GAMES
    # ---------------------------------------------

    if "games at vfl/afl level" in clue_lower or "200 or more games" in clue_lower:

        num = extract_number(clue)

        if num:
            filters["min_games"] = num

    # ---------------------------------------------
    # GOALS
    # ---------------------------------------------

    if "goals over their career" in clue_lower:

        num = extract_number(clue)

        if num:
            filters["max_goals"] = num

    # ---------------------------------------------
    # MINOR PREMIERS
    # ---------------------------------------------

    if "finished in first place" in clue_lower:
        filters["min_minor_prems"] = 1
        
    # ---------------------------------------------
    # PLAYED AT LEAST X GAME
    # ---------------------------------------------

    if "played at least" in clue_lower and "game" in clue_lower:

        num = extract_number(clue)

        if num:
            filters["min_games"] = num

    # ---------------------------------------------
    # DECADE
    # ---------------------------------------------

    decade = detect_decade(clue)

    if decade:

        filters["min_first_year"] = decade[0]
        filters["max_last_year"] = decade[1]

    return filters


# -------------------------------------------------
# GRIDLEY HISTORY DATABASE
# -------------------------------------------------

def ensure_gridley_table():

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS gridley_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        clue TEXT,
        UNIQUE(date, clue)
    )
    """)

    conn.commit()
    conn.close()


# -------------------------------------------------
# STORE GRIDLEY CLUES
# -------------------------------------------------

def store_gridley_clues(clues):

    ensure_gridley_table()

    today = str(date.today())

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    for clue in clues:

        try:

            c.execute("""
            INSERT OR IGNORE INTO gridley_history
            (date, clue)
            VALUES (?, ?)
            """, (today, clue))

        except:
            pass

    conn.commit()
    conn.close()


# -------------------------------------------------
# DEBUG VIEW
# -------------------------------------------------

def print_recent_gridleys(limit=30):

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
    SELECT date, clue
    FROM gridley_history
    ORDER BY date DESC
    LIMIT ?
    """, (limit,))

    rows = c.fetchall()

    print("\nRecent Gridley clues:\n")

    for r in rows:
        print(r)

    conn.close()