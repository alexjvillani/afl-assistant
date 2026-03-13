# -*- coding: utf-8 -*-

import sqlite3
import re
from datetime import date

DB_PATH = "players.db"


# -------------------------------------------------
# TEAM ALIASES
# -------------------------------------------------

TEAM_ALIASES = {
    "adelaide": ["adelaide", "adelaide crows"],
    "brisbane": ["brisbane", "brisbane lions", "brisbane bears", "fitzroy"],
    "carlton": ["carlton", "carlton blues"],
    "collingwood": ["collingwood", "collingwood magpies"],
    "essendon": ["essendon", "essendon bombers"],
    "fremantle": ["fremantle", "fremantle dockers"],
    "geelong": ["geelong", "geelong cats"],
    "goldcoast": ["gold coast", "gold coast suns"],
    "gws": ["gws", "gws giants", "greater western sydney"],
    "hawthorn": ["hawthorn", "hawthorn hawks"],
    "melbourne": ["melbourne", "melbourne demons"],
    "kangaroos": ["north melbourne", "kangaroos"],
    "richmond": ["richmond", "richmond tigers"],
    "stkilda": ["st kilda", "stkilda", "st kilda saints"],
    "swans": ["sydney", "sydney swans", "south melbourne"],
    "westcoast": ["west coast", "west coast eagles"],
    "bullldogs": ["western bulldogs", "footscray"]
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

    if "games for two different" in t:
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
# TEAM DETECTION
# -------------------------------------------------

def detect_team(clue):

    c = clue.lower()

    for team, aliases in TEAM_ALIASES.items():

        for alias in aliases:

            if alias in c:
                return team

    return None


# -------------------------------------------------
# NUMBER EXTRACTION
# -------------------------------------------------

def extract_number(clue):

    nums = re.findall(r'\d+', clue)

    if nums:
        return int(nums[0])

    return None


# -------------------------------------------------
# STAT TYPE DETECTION
# -------------------------------------------------

def detect_stat_type(clue):

    c = clue.lower()

    if "disposal" in c:
        return "disposals_game"

    if "tackle" in c:
        return "tackles_game"

    if "mark" in c:
        return "marks_game"

    if "two different" in c and "game" in c:
        return "two_club_games"

    if "game" in c:
        return "career_games"

    if "goal" in c:
        return "career_goals"

    if "mcg" in c:
        return "mcg_games"

    if "grand final" in c:
        return "gf_disposals"

    return None


# -------------------------------------------------
# TEAMMATE DETECTION
# -------------------------------------------------

def detect_teammate(clue):

    c = clue.lower()

    if "teammate of" not in c:
        return None

    name = c.split("teammate of")[1]

    name = name.replace(".", "").strip()

    return name.title()


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

    filters = {}

    team = detect_team(clue)
    if team:
        filters["team"] = team

    teammate = detect_teammate(clue)
    if teammate:
        filters["teammate"] = teammate

    decade = detect_decade(clue)
    if decade:
        filters["decade_start"] = decade[0]
        filters["decade_end"] = decade[1]

    num = extract_number(clue)
    stat = detect_stat_type(clue)

    if num and stat:

        if stat == "disposals_game":
            filters["min_max_disposals_game"] = num

        elif stat == "tackles_game":
            filters["min_max_tackles_game"] = num

        elif stat == "marks_game":
            filters["min_max_marks_game"] = num

        elif stat == "career_games":
            filters["min_games"] = num

        elif stat == "two_club_games":
            filters["min_two_clubs_games"] = num

        elif stat == "career_goals":
            filters["max_goals"] = num

        elif stat == "mcg_games":
            filters["min_mcg_games"] = num

        elif stat == "gf_disposals":
            filters["min_max_GF_disposals"] = num

    if "finished in first place" in clue.lower():
        filters["min_minor_prems"] = 1

    if "top" in clue.lower() and "draft" in clue.lower():
        filters["max_draft_pick"] = num

    if "led team in goals" in clue.lower():
        filters["min_gk_wins"] = 1

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