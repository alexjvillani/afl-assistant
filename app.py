# -*- coding: utf-8 -*-

from flask import Flask, render_template, request
from fetch_gridley import get_today_grid
from gridley_util import parse_gridley_clue, short_clue
import sqlite3
import re
import random

app = Flask(__name__)

DB_PATH = "players.db"

TEAM_OPTIONS = [
    "adelaide", "brisbaneb", "brisbanel", "carlton", "collingwood",
    "essendon", "fitzroy", "fremantle", "geelong", "goldcoast",
    "gws", "hawthorn", "melbourne", "kangaroos",
    "padelaide", "richmond", "stkilda", "swans", "university",
    "westcoast", "bullldogs"
]

TEAM_ALIASES = {

    "brisbane": ["brisbaneb", "brisbanel"],

    "bulldogs": ["footscray", "bullldogs"],
    "footscray": ["footscray", "bullldogs"],

    "kangaroos": ["kangaroos", "northmelbourne"],
    "northmelbourne": ["kangaroos", "northmelbourne"],

    "sydney": ["southmelbourne", "swans"],
    "swans": ["southmelbourne", "swans"],

}

# -------------------------------------------------
# SCHEMA SAFETY
# -------------------------------------------------

def ensure_top10_columns():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("PRAGMA table_info(players)")
    existing = set([row[1] for row in c.fetchall()])

    columns = [
        ("max_marks_game", "INTEGER"),
        ("max_hitouts_game", "INTEGER"),
        ("max_tackles_game", "INTEGER"),
    ]

    for col, coltype in columns:
        if col not in existing:
            try:
                c.execute(
                    "ALTER TABLE players ADD COLUMN %s %s DEFAULT 0"
                    % (col, coltype)
                )
            except:
                pass

    conn.commit()
    conn.close()

ensure_top10_columns()

# -------------------------------------------------
# DB HELPERS
# -------------------------------------------------

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def scalar(v):
    if isinstance(v, (list, tuple)):
        return v[0] if v else None
    return v


def get_player_options():
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT player_id, name FROM players ORDER BY name")
    rows = c.fetchall()
    conn.close()
    return rows


def get_player_club_stats(player_id):
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        SELECT team, games, goals
        FROM player_club_stats
        WHERE player_id = ?
        ORDER BY games DESC
    """, (player_id,))
    rows = c.fetchall()
    conn.close()
    return rows


# -------------------------------------------------
# ✅ NEW: DERIVE TEAMS FROM player_seasons (DISPLAY ONLY)
# -------------------------------------------------

def get_player_teams(player_id):
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        SELECT DISTINCT team
        FROM player_seasons
        WHERE player_id = ?
        ORDER BY team
    """, (player_id,))
    teams = [r[0] for r in c.fetchall()]
    conn.close()
    return ", ".join(teams)
    
def get_player_name_by_id(pid):

    conn = get_db()
    c = conn.cursor()

    c.execute("""
        SELECT name
        FROM players
        WHERE player_id = ?
    """, (pid,))

    row = c.fetchone()
    conn.close()

    if row:
        return row[0]

    return None
    
def get_player_id_by_name(name):

    conn = get_db()
    c = conn.cursor()

    name = name.strip()

    c.execute("""
        SELECT player_id
        FROM players
        WHERE TRIM(name) = TRIM(?)
    """, (name,))

    row = c.fetchone()
    conn.close()

    if row:
        return row[0]

    return None

# -------------------------------------------------
# ALL AUSTRALIAN
# -------------------------------------------------

def get_aa_years_map():
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        SELECT player_id, year
        FROM all_australian_selections
        ORDER BY year DESC
    """)
    aa = {}
    for pid, yr in c.fetchall():
        aa.setdefault(pid, []).append(yr)
    conn.close()
    return aa


def get_best_aa_draft_picks():
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        SELECT player_id, draft_pick
        FROM all_australian_selections
        WHERE draft_pick IS NOT NULL
    """)

    best = {}
    for pid, raw in c.fetchall():
        disp, num = normalise_draft_pick(raw)
        if pid not in best or (
            num is not None and
            (best[pid]["numeric"] is None or num < best[pid]["numeric"])
        ):
            best[pid] = {"display": disp, "numeric": num}

    conn.close()
    return best

# -------------------------------------------------
# RISING STAR
# -------------------------------------------------

def get_rs_counts():
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        SELECT player_id, COUNT(*)
        FROM rising_star_nominations
        GROUP BY player_id
    """)
    rs = {pid: cnt for pid, cnt in c.fetchall()}
    conn.close()
    return rs

# -------------------------------------------------
# BEST & FAIREST
# -------------------------------------------------

def get_bnf_years_map():
    conn = get_db()
    c = conn.cursor()

    c.execute("""
        SELECT player_id, year, club, source
        FROM best_and_fairest
        ORDER BY
            year DESC,
            CASE
                WHEN source LIKE 'wikipedia%' THEN 0
                ELSE 1
            END
    """)

    seen = set()
    bnf = {}

    for pid, yr, club, source in c.fetchall():
        key = (yr, club)
        if key in seen:
            continue
        seen.add(key)
        bnf.setdefault(pid, []).append((yr, club))

    conn.close()
    return bnf


def get_best_bnf_draft_picks():
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        SELECT player_id, raw_draft_pick, draft_pick_num
        FROM best_and_fairest
        WHERE draft_pick_num IS NOT NULL
    """)

    best = {}
    for pid, raw, num in c.fetchall():
        if pid not in best or num < best[pid]["numeric"]:
            best[pid] = {"display": raw, "numeric": num}

    conn.close()
    return best

# -------------------------------------------------
# DRAFT PICK NORMALISATION
# -------------------------------------------------

def normalise_draft_pick(raw):
    if not raw:
        return ("", None)

    r = raw.lower().strip()

    if "father" in r:
        return ("FS", None)
    if "academy" in r:
        return ("ACA", None)
    if "foundation" in r:
        return ("FDN", None)
    if "rookie" in r:
        return ("R", None)
    if "zone" in r or "pre" in r:
        return ("PL", None)

    m = re.search(r"(\d+)", r)
    if m:
        v = int(m.group(1))
        return ("Pick %d" % v, v)

    return (raw, None)

# -------------------------------------------------
# UNIFIED DRAFT PICK
# -------------------------------------------------

def get_unified_draft_picks():
    unified = {}

    aa = get_best_aa_draft_picks()
    bnf = get_best_bnf_draft_picks()

    for pid, dp in bnf.items():
        unified[pid] = dp

    for pid, dp in aa.items():
        if pid not in unified:
            unified[pid] = dp

    return unified
    
# -------------------------------------------------
# SCHEMA SAFETY — COLEMAN
# -------------------------------------------------

def ensure_coleman_columns():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("PRAGMA table_info(players)")
    existing = set([row[1] for row in c.fetchall()])

    columns = [
        ("leading_gk_wins", "INTEGER"),
        ("leading_gk_goals", "INTEGER"),
    ]

    for col, coltype in columns:
        if col not in existing:
            try:
                c.execute(
                    "ALTER TABLE players ADD COLUMN %s %s DEFAULT 0"
                    % (col, coltype)
                )
            except:
                pass

    conn.commit()
    conn.close()

ensure_coleman_columns()


# -------------------------------------------------
# NICHE: TOP 10 SEASON LEADERS
# -------------------------------------------------

def get_top10_season(year, stat):
    conn = get_db()
    c = conn.cursor()

    stat_map = {
        "goals": "COALESCE(ps.goals, 0)",
        "kicks": "COALESCE(ps.kicks, 0)",
        "handballs": "COALESCE(ps.handballs, 0)",
        "marks": "COALESCE(ps.marks, 0)",
        "disposals": "(COALESCE(ps.kicks,0) + COALESCE(ps.handballs,0))"
    }

    if stat not in stat_map:
        return []

    expr = stat_map[stat]

    query = """
        SELECT
            ps.player_id,
            p.name,
            ps.team,
            %s AS value
        FROM player_seasons ps
        JOIN players p ON p.player_id = ps.player_id
        WHERE ps.year = ?
          AND %s > 0
        ORDER BY value DESC
        LIMIT 10
    """ % (expr, expr)

    c.execute(query, (year,))
    rows = c.fetchall()
    conn.close()
    return rows
    
def get_wooden_spoon_counts():
    """
    player_id -> total wooden spoon count
    """
    years = get_wooden_spoon_years_map()
    return {pid: len(v) for pid, v in years.items()}

# -------------------------------------------------
# WOODEN SPOON HELPERSahgcbvc 
# -------------------------------------------------

def get_wooden_spoon_years_map():
    """
    player_id -> list of (year, team)
    Deduplicated at (player, year, team) level
    """
    conn = get_db()
    c = conn.cursor()

    c.execute("""
        SELECT DISTINCT
            player_id,
            year,
            team
        FROM wooden_spoons
        ORDER BY year DESC
    """)

    ws = {}
    for pid, year, team in c.fetchall():
        ws.setdefault(pid, []).append((year, team))

    conn.close()
    return ws
    
def normalise_player_id(pid):
    """
    Convert any 22u22 player_id into full afltables URL
    """
    if not pid:
        return None

    pid = pid.strip()

    if pid.startswith("http"):
        return pid

    # Nick_Daicos.html
    if pid.endswith(".html"):
        letter = pid[0].upper()
        return "https://afltables.com/afl/stats/players/%s/%s" % (letter, pid)

    # Nick_Daicos
    letter = pid[0].upper()
    return "https://afltables.com/afl/stats/players/%s/%s.html" % (letter, pid)
    
def get_22u22_years_map():
    conn = get_db()
    c = conn.cursor()

    c.execute("""
        SELECT player_id, year
        FROM player_22u22
        ORDER BY year DESC
    """)

    u22 = {}
    for pid, yr in c.fetchall():
        u22.setdefault(pid, []).append(yr)

    conn.close()
    return u22


def get_22u22_counts():
    conn = get_db()
    c = conn.cursor()

    c.execute("""
        SELECT player_id, COUNT(*)
        FROM player_22u22
        GROUP BY player_id
    """)

    counts = {pid: cnt for pid, cnt in c.fetchall()}
    conn.close()
    return counts
    
def get_minor_prem_years_map():
    """
    player_id -> list of (year, team)
    """
    conn = get_db()
    c = conn.cursor()

    c.execute("""
        SELECT DISTINCT
            player_id,
            year,
            team
        FROM minor_premiers
        ORDER BY year DESC
    """)

    mp = {}

    for pid, year, team in c.fetchall():
        mp.setdefault(pid, []).append((year, team))

    conn.close()

    return mp


def get_minor_prem_counts():
    """
    player_id -> count
    """
    years = get_minor_prem_years_map()

    return {pid: len(v) for pid, v in years.items()}
    
    
# -------------------------------------------------
# QUERY
# -------------------------------------------------

def query_players(filters, visible):

    conn = get_db()
    c = conn.cursor()

    query = "SELECT * FROM players WHERE 1=1"
    params = []

    if filters.get("team1"):

        team = filters["team1"]
        teams = TEAM_ALIASES.get(team, [team])

        query += """
        AND player_id IN (
            SELECT DISTINCT player_id
            FROM player_seasons
            WHERE team IN (%s)
        )
        """ % ",".join(["?"]*len(teams))

        params.extend(teams)


    if filters.get("team2"):

        team = filters["team2"]
        teams = TEAM_ALIASES.get(team, [team])

        query += """
        AND player_id IN (
            SELECT DISTINCT player_id
            FROM player_seasons
            WHERE team IN (%s)
        )
        """ % ",".join(["?"]*len(teams))

        params.extend(teams)

    if filters.get("teammate_of"):

        query += """
        AND player_id IN (

            SELECT DISTINCT ps2.player_id
            FROM player_seasons ps1
            JOIN player_seasons ps2
              ON ps1.year = ps2.year
              AND ps1.team = ps2.team
            WHERE ps1.player_id = ?
            AND ps2.player_id != ps1.player_id

        )   
        """

        params.append(filters["teammate_of"])

    if filters.get("min_games"):
        query += " AND career_games >= ?"
        params.append(filters["min_games"])

    if filters.get("max_games"):
        query += " AND career_games <= ?"
        params.append(filters["max_games"])

    if filters.get("min_goals"):
        query += " AND career_goals >= ?"
        params.append(filters["min_goals"])

    if filters.get("max_goals"):
        query += " AND career_goals <= ?"
        params.append(filters["max_goals"])
        
            # --- Max Disposals (Game) ---
    if filters.get("min_max_disposals_game"):
        query += " AND max_disposals_game >= ?"
        params.append(filters["min_max_disposals_game"])

    if filters.get("max_max_disposals_game"):
        query += " AND max_disposals_game <= ?"
        params.append(filters["max_max_disposals_game"])

    if filters.get("min_max_goals_game"):
        query += " AND max_goals_game >= ?"
        params.append(filters["min_max_goals_game"])

    if filters.get("min_max_goals_season"):
        query += " AND max_goals_season >= ?"
        params.append(filters["min_max_goals_season"])

    if filters.get("min_max_marks_game"):
        query += " AND max_marks_game >= ?"
        params.append(filters["min_max_marks_game"])

    if filters.get("max_max_marks_game"):
        query += " AND max_marks_game <= ?"
        params.append(filters["max_max_marks_game"])

    if filters.get("min_max_hitouts_game"):
        query += " AND max_hitouts_game >= ?"
        params.append(filters["min_max_hitouts_game"])

    if filters.get("max_max_hitouts_game"):
        query += " AND max_hitouts_game <= ?"
        params.append(filters["max_max_hitouts_game"])

    if filters.get("min_max_tackles_game"):
        query += " AND max_tackles_game >= ?"
        params.append(filters["min_max_tackles_game"])

    if filters.get("max_max_tackles_game"):
        query += " AND max_tackles_game <= ?"
        params.append(filters["max_max_tackles_game"])

    if filters.get("min_all_aus"):
        query += " AND all_aus_count >= ?"
        params.append(filters["min_all_aus"])
        
        # --- Brownlow (career) ---
    if filters.get("min_wins"):
        query += " AND brownlow_wins >= ?"
        params.append(filters["min_wins"])

    if filters.get("min_votes"):
        query += " AND brownlow_votes >= ?"
        params.append(filters["min_votes"])
    
    # --- Best Brownlow Season ---
    if filters.get("min_best_brownlow_votes"):
        query += " AND best_brownlow_votes >= ?"
        params.append(filters["min_best_brownlow_votes"])

    if filters.get("max_best_brownlow_votes"):
        query += " AND best_brownlow_votes <= ?"
        params.append(filters["max_best_brownlow_votes"])
        
    if filters.get("min_minor_prems"):
        query += " AND minor_prem_count >= ?"
        params.append(filters["min_minor_prems"])
        
    if filters.get("min_wooden_spoons"):
        query += " AND wooden_spoon_count >= ?"
        params.append(filters["min_wooden_spoons"])

    if filters.get("max_wooden_spoons"):
        query += " AND wooden_spoon_count <= ?"
        params.append(filters["max_wooden_spoons"])
        
    if filters.get("min_finals_played"):
        query += " AND finals_played >= ?"
        params.append(filters["min_finals_played"])
        
    if filters.get("min_gf_apps"):
        query += " AND gf_appearances >= ?"
        params.append(int(filters["min_gf_apps"]))
        
    if filters.get("min_max_finals_goals"):
        query += " AND max_finals_goals >= ?"
        params.append(filters["min_max_finals_goals"])
        
    if filters.get("min_max_finals_disposals"):
        query += " AND max_finals_disposals >= ?"
        params.append(filters["min_max_finals_disposals"])
        
    if filters.get("min_total_finals_goals"):
        query += " AND total_finals_goals >= ?"
        params.append(filters["min_total_finals_goals"])
        
    if filters.get("min_max_GF_goals"):
        query += " AND max_GF_goals >= ?"
        params.append(filters["min_max_GF_goals"])

    if filters.get("max_max_GF_goals"):
        query += " AND max_GF_goals <= ?"
        params.append(filters["max_max_GF_goals"])

    if filters.get("min_height"):
        query += " AND height >= ?"
        params.append(filters["min_height"])

    if filters.get("max_height"):
        query += " AND height <= ?"
        params.append(filters["max_height"])
        
    if filters.get("min_gk_wins"):
        query += " AND leading_gk_wins >= ?"
        params.append(filters["min_gk_wins"])

    if filters.get("min_gk_goals"):
        query += " AND leading_gk_goals >= ?"
        params.append(filters["min_gk_goals"])

    if filters.get("min_first_year"):
        query += " AND first_year >= ?"
        params.append(filters["min_first_year"])

    if filters.get("min_last_year"):
        query += " AND last_year >= ?"
        params.append(filters["min_last_year"])
        
    if filters.get("rising_star_winner"):
        query += """
        AND player_id IN (
            SELECT player_id
            FROM rising_star_nominations
            WHERE is_winner = 1
        )
        """
    if filters.get("rising_star_nominee"):
        query += """
        AND player_id IN (
            SELECT player_id
            FROM rising_star_nominations
        )
        """
    if filters.get("min_bnf_wins"):
        query += """
        AND player_id IN (
            SELECT player_id
            FROM best_and_fairest
            GROUP BY player_id
            HAVING SUM(bnf_wins) >= ?
        )
        """
        params.append(filters["min_bnf_wins"])
        
    if filters.get("under22_selection"):
        query += """
        AND player_id IN (
            SELECT player_id
            FROM player_22u22
        )
        """
    if filters.get("min_premierships"):
        query += " AND gf_wins >= ?"
        params.append(filters["min_premierships"])
        
    if filters.get("min_clubs"):
        query += """
        AND (
            LENGTH(teams) - LENGTH(REPLACE(teams, ',', '')) + 1
        ) >= ?
        """
        params.append(filters["min_clubs"])
        
    if filters.get("min_finals_wins"):
        query += " AND finals_wins >= ?"
        params.append(filters["min_finals_wins"])
        
    if filters.get("decade_start") and filters.get("decade_end"):
        query += " AND first_year <= ? AND last_year >= ?"
        params.append(int(filters["decade_end"]))
        params.append(int(filters["decade_start"]))
        
    if "max_height" in filters:
        visible["height"] = True

    if "min_finals" in filters:
        visible["finals_games"] = True
        
    if filters.get("min_bnf"):

        query += """
        AND player_id IN (
            SELECT player_id
            FROM best_and_fairest
            GROUP BY player_id
            HAVING COUNT(*) >= ?
        )
        """

        params.append(filters["min_bnf"])
        
    if filters.get("min_two_clubs_games"):
        query += """
        AND player_id IN (
            SELECT player_id
            FROM player_club_stats
            WHERE games >= ?
            GROUP BY player_id
            HAVING COUNT(*) >= 2
        )
        """
        params.append(int(filters["min_two_clubs_games"]))
    
    if filters.get("max_draft_pick"):
        query += """
        AND player_id IN (
            SELECT DISTINCT player_id
            FROM best_and_fairest
            WHERE draft_pick_num IS NOT NULL
                AND draft_pick_num <= ?
        )
        """
        params.append(int(filters["max_draft_pick"]))
    
    if filters.get("min_max_GF_disposals"):
        query += " AND max_GF_disposals >= ?"
        params.append(int(filters["min_max_GF_disposals"]))

    sort_column = filters.get("sort_by") or "career_games"
    sort_order = filters.get("sort_order") or "DESC"

    if sort_column == "finals_win_pct":

        query += """
            ORDER BY
            CASE
                WHEN finals_played < 5 THEN NULL
                ELSE (CAST(finals_wins AS FLOAT) / finals_played)
            END %s
        """ % sort_order

    elif sort_column == "finals_record":

        query += " ORDER BY finals_wins %s" % sort_order
        
    elif sort_column == "gf_record":

        query += " ORDER BY gf_wins %s" % sort_order

    else:

        query += " ORDER BY %s %s" % (sort_column, sort_order)

    c.execute(query, params)
    rows = c.fetchall()
    conn.close()
    return rows

# -------------------------------------------------
# ROUTE
# -------------------------------------------------

@app.route("/", methods=["GET"])
def index():

    try:
        grid_rows, grid_cols = get_today_grid()

        print("GRID ROWS:", grid_rows)
        print("GRID COLS:", grid_cols)

    except Exception as e:

        print("GRIDLEY FETCH FAILED:", e)

        grid_rows = []
        grid_cols = []

    raw = dict(request.args)

    filters = {}
    visible = {}

    row_clue = request.args.get("row_clue")
    col_clue = request.args.get("col_clue")

    # ---------------------------------
    # Parse Gridley clues
    # ---------------------------------

    for clue in [row_clue, col_clue]:

        if not clue:
            continue

        f = parse_gridley_clue(clue)

        # Team mapping
        if "team" in f:

            if "team1" not in filters:
                filters["team1"] = f.pop("team")
            else:
                filters["team2"] = f.pop("team")

        # Teammate mapping (Gridley already converts)
        if "teammate" in f:

            name = f.pop("teammate")
            pid = get_player_id_by_name(name)

            if pid:
                filters["teammate_of"] = pid
                filters["teammate_display"] = get_player_name_by_id(pid)

        filters.update(f)

    # ---------------------------------
    # Parse manual filters
    # ---------------------------------

    for k, v in raw.items():

        val = scalar(v)

        if k.startswith("show_"):
            visible[k.replace("show_", "")] = True

        elif k in ["row_clue", "col_clue"]:
            continue

        else:
            if k not in filters:
                filters[k] = val

    # ---------------------------------
    # 🔥 FIX: Safe teammate handling
    # ---------------------------------

    if filters.get("teammate_of"):

        val = filters["teammate_of"]
        pid = None

        # Case 1: already a player_id (Gridley handled it)
        if isinstance(val, int):
            pid = val
        elif isinstance(val, str) and val.isdigit():
            pid = int(val)

        # Case 2: manual input (name) → convert to ID
        if pid is None:
            pid = get_player_id_by_name(val)

        if pid:
            filters["teammate_of"] = pid
            if not filters.get("teammate_display"):
                filters["teammate_display"] = get_player_name_by_id(pid)
        else:
            filters.pop("teammate_of", None)

    # ---------------------------------
    # Visible columns logic
    # ---------------------------------

    visible["teams"] = True
    visible["years"] = True

    if "min_games" in filters:
        visible["career_games"] = True

    if "max_goals" in filters:
        visible["career_goals"] = True

    if "min_two_clubs_games" in filters:
        visible["club_stats"] = True

    if "min_max_disposals_game" in filters:
        visible["max_disposals_game"] = True

    if "min_max_tackles_game" in filters:
        visible["max_tackles_game"] = True

    if "min_max_marks_game" in filters:
        visible["max_marks_game"] = True

    if "min_minor_prems" in filters:
        visible["minor_prems"] = True

    if "max_draft_pick" in filters:
        visible["draft_pick"] = True

    if "min_bnf" in filters:
        visible["bnf_years"] = True

    # ---------------------------------
    # Main query
    # ---------------------------------

    players = query_players(filters, visible)
    total_results = len(players)

    # ---------------------------------
    # Lookup data
    # ---------------------------------

    player_options = get_player_options()

    aa_years = get_aa_years_map()
    rs_counts = get_rs_counts()
    bnf_years = get_bnf_years_map()

    u22_years = get_22u22_years_map()
    u22_counts = get_22u22_counts()

    wooden_spoon_years = get_wooden_spoon_years_map()
    wooden_spoon_counts = get_wooden_spoon_counts()

    minor_prem_years = get_minor_prem_years_map()
    minor_prem_counts = get_minor_prem_counts()

    unified_draft = get_unified_draft_picks()

    # ---------------------------------
    # Suggested niche player logic
    # ---------------------------------

    suggested_player = None

    if players:

        scored = []

        for p in players:

            games = p["games"] if "games" in p.keys() else 200

            draft = unified_draft.get(p["player_id"], 100)

            if isinstance(draft, dict):
                draft = draft.get("pick", 100)

            score = 0

            score += min(games, 200) * 0.6
            score += draft * 0.3
            score += random.random() * 20

            if "min_max_disposals_game" in filters and filters["min_max_disposals_game"]:

                target = int(filters["min_max_disposals_game"])

                if "max_disposals_game" in p.keys() and p["max_disposals_game"]:

                    diff = abs(int(p["max_disposals_game"]) - target)
                    score += diff * 2

            if "min_bnf" in filters:

                pid = p["player_id"]
                count = len(bnf_years.get(pid, []))
                score += abs(count - 1) * 20

            scored.append((score, p))

        scored.sort()
        niche_pool = [p for s, p in scored[:15]]
        suggested_player = random.choice(niche_pool)

    # ---------------------------------
    # Render
    # ---------------------------------

    return render_template(

        "index.html",

        players=players,
        total_results=total_results,

        teams=TEAM_OPTIONS,

        filters=filters,
        visible=visible,

        player_options=player_options,

        grid_rows=grid_rows,
        grid_cols=grid_cols,

        suggested_player=suggested_player,

        aa_years=aa_years,
        rs_counts=rs_counts,
        bnf_years=bnf_years,

        u22_years=u22_years,
        u22_counts=u22_counts,

        wooden_spoon_years=wooden_spoon_years,
        wooden_spoon_counts=wooden_spoon_counts,

        minor_prem_years=minor_prem_years,
        minor_prem_counts=minor_prem_counts,

        unified_draft=unified_draft,

        get_player_club_stats=get_player_club_stats,
        get_player_teams=get_player_teams
    )
# -------------------------------------------------
# NICHE ROUTE — SEASON TOP 10s
# -------------------------------------------------

@app.route("/niche")
def niche():
    year = int(request.args.get("year", 2024))
    stat = request.args.get("stat", "goals")

    top10 = get_top10_season(year, stat)

    return render_template(
        "niche.html",
        year=year,
        stat=stat,
        top10=top10,
        total_results=len(top10)
    )

if __name__ == "__main__":
    app.run(debug=True)
