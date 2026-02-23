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
        SELECT player_id, year, club
        FROM best_and_fairest
        ORDER BY year DESC
    """)
    bnf = {}
    for pid, yr, club in c.fetchall():
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
# UNIFIED DRAFT PICK (AA + BNF)
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
# MAIN QUERY
# -------------------------------------------------

def query_players(filters):
    conn = get_db()
    c = conn.cursor()

    q = "SELECT * FROM players WHERE 1=1"
    p = []

    if filters.get("team1"):
        q += " AND (',' || REPLACE(teams,' ','') || ',') LIKE ?"
        p.append("%," + filters["team1"] + ",%")

    if filters.get("team2"):
        q += " AND (',' || REPLACE(teams,' ','') || ',') LIKE ?"
        p.append("%," + filters["team2"] + ",%")

    if filters.get("teammate_of"):
        q += """
        AND player_id IN (
            SELECT ps2.player_id
            FROM player_seasons ps1
            JOIN player_seasons ps2
              ON ps1.year = ps2.year
             AND ps1.team = ps2.team
            WHERE ps1.player_id = ?
              AND ps2.player_id != ps1.player_id
        )
        """
        p.append(filters["teammate_of"])

    numeric_filters = [
        ("career_games", "min_games", ">="),
        ("career_games", "max_games", "<="),
        ("career_goals", "min_goals", ">="),
        ("career_goals", "max_goals", "<="),
        ("max_goals_game", "min_max_goals_game", ">="),
        ("max_goals_season", "min_max_goals_season", ">="),
        ("max_marks_game", "min_max_marks_game", ">="),
        ("max_marks_game", "max_max_marks_game", "<="),
        ("max_hitouts_game", "min_max_hitouts_game", ">="),
        ("max_hitouts_game", "max_max_hitouts_game", "<="),
        ("max_tackles_game", "min_max_tackles_game", ">="),
        ("max_tackles_game", "max_max_tackles_game", "<="),
        ("height", "min_height", ">="),
        ("height", "max_height", "<="),
        ("first_year", "min_first_year", ">="),
        ("last_year", "min_last_year", ">="),
        ("all_aus_count", "min_all_aus", ">="),
    ]

    for col, key, op in numeric_filters:
        if filters.get(key):
            q += " AND %s %s ?" % (col, op)
            p.append(filters[key])

    sort_col = filters.get("sort_by") or "career_games"
    sort_dir = filters.get("sort_order") or "DESC"
    q += " ORDER BY %s %s" % (sort_col, sort_dir)

    c.execute(q, p)
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

    for k, v in raw.items():
        val = scalar(v)
        if k.startswith("show_"):
            visible[k.replace("show_", "")] = True
        else:
            filters[k] = val

    players = query_players(filters)

    return render_template(
        "index.html",
        players=players,
        teams=TEAM_OPTIONS,
        filters=filters,
        visible=visible,
        player_options=get_player_options(),
        aa_years=get_aa_years_map(),
        rs_counts=get_rs_counts(),
        bnf_years=get_bnf_years_map(),
        unified_draft=get_unified_draft_picks(),
        get_player_club_stats=get_player_club_stats
    )

if __name__ == "__main__":
    app.run(debug=True)