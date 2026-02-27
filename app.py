# -*- coding: utf-8 -*-

from flask import Flask, render_template, request
import sqlite3
import re

app = Flask(__name__)

DB_PATH = "players.db"

TEAM_OPTIONS = [
    "adelaide", "brisbaneb", "brisbanel", "carlton", "collingwood",
    "essendon", "fitzroy", "fremantle", "geelong", "goldcoast",
    "gws", "hawthorn", "melbourne", "kangaroos",
    "padelaide", "richmond", "stkilda", "swans", "university",
    "westcoast", "bullldogs"
]

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
# WOODEN SPOON HELPERS
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

# -------------------------------------------------
# QUERY
# -------------------------------------------------

def query_players(filters):

    conn = get_db()
    c = conn.cursor()

    query = "SELECT * FROM players WHERE 1=1"
    params = []

    if filters.get("team1"):
        query += " AND (',' || REPLACE(teams, ' ', '') || ',') LIKE ?"
        params.append("%," + filters["team1"] + ",%")

    if filters.get("team2"):
        query += " AND (',' || REPLACE(teams, ' ', '') || ',') LIKE ?"
        params.append("%," + filters["team2"] + ",%")

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
        
    if filters.get("min_wooden_spoons"):
        query += """
            AND player_id IN (
                SELECT player_id
                FROM wooden_spoons
                GROUP BY player_id
                HAVING COUNT(*) >= ?
            )
        """
        params.append(filters["min_wooden_spoons"])

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

    sort_column = filters.get("sort_by") or "career_games"
    sort_order = filters.get("sort_order") or "DESC"

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
    raw = dict(request.args)

    filters = {}
    visible = {}

    # ---------------------------------
    # Parse filters + visible columns
    # ---------------------------------
    for k, v in raw.items():
        val = scalar(v)
        if k.startswith("show_"):
            visible[k.replace("show_", "")] = True
        else:
            filters[k] = val

    # ---------------------------------
    # Main query
    # ---------------------------------
    players = query_players(filters)
    total_results = len(players)

    # ---------------------------------
    # Lookup / helper maps
    # ---------------------------------
    player_options = get_player_options()
    aa_years = get_aa_years_map()
    rs_counts = get_rs_counts()
    bnf_years = get_bnf_years_map()
    u22_years = get_22u22_years_map()
    u22_counts = get_22u22_counts()

    # ✅ Wooden Spoon YEARS (list of years + club)
    wooden_spoon_years = get_wooden_spoon_years_map()
    wooden_spoon_counts = get_wooden_spoon_counts()

    # ✅ Unified draft pick (AA + B&F combined)
    unified_draft = get_unified_draft_picks()

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

        # Awards
        aa_years=aa_years,
        rs_counts=rs_counts,
        bnf_years=bnf_years,
        
            # 22 Under 22
        u22_years=u22_years,
        u22_counts=u22_counts,

        # Wooden Spoon
        wooden_spoon_years=wooden_spoon_years,
        wooden_spoon_counts=wooden_spoon_counts,

        # Draft
        unified_draft=unified_draft,

        # Helpers
        get_player_club_stats=get_player_club_stats,
        get_player_teams=get_player_teams,   # ✅ already correct
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