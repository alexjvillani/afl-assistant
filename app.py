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
# DATABASE HELPERS
# -------------------------------------------------

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


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
# ALL-AUSTRALIAN HELPERS
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


# -------------------------------------------------
# PYTHON 2.7 / FLASK SAFETY
# -------------------------------------------------

def scalar(v):
    if isinstance(v, (list, tuple)):
        return v[0] if v else None
    return v


# -------------------------------------------------
# DRAFT PICK NORMALISATION
# -------------------------------------------------

def normalise_draft_pick(raw):
    if not raw:
        return ("", None)

    r = raw.strip().lower()

    if "father" in r:
        return ("FS", None)
    if "foundation" in r:
        return ("FDN", None)
    if "academy" in r:
        return ("ACA", None)
    if "rookie" in r:
        return ("R", None)
    if "pre" in r or "zone" in r:
        return ("PL", None)

    m = re.search(r"(\d+)", r)
    if m:
        val = int(m.group(1))
        return ("Pick %d" % val, val)

    return (raw, None)


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

        if pid not in best:
            best[pid] = {"display": disp, "numeric": num}
            continue

        cur = best[pid]["numeric"]
        if num is not None and (cur is None or num < cur):
            best[pid] = {"display": disp, "numeric": num}

    conn.close()
    return best


# -------------------------------------------------
# MAIN QUERY LOGIC
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

    if filters.get("min_max_goals_game"):
        query += " AND max_goals_game >= ?"
        params.append(filters["min_max_goals_game"])

    if filters.get("min_max_goals_season"):
        query += " AND max_goals_season >= ?"
        params.append(filters["min_max_goals_season"])

    if filters.get("min_all_aus"):
        query += " AND all_aus_count >= ?"
        params.append(filters["min_all_aus"])

    if filters.get("min_height"):
        query += " AND height >= ?"
        params.append(filters["min_height"])

    if filters.get("max_height"):
        query += " AND height <= ?"
        params.append(filters["max_height"])

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

    # 🔒 Separate filters vs checkboxes (NEW)
    for k, v in raw.items():
        val = scalar(v)

        if k.startswith("show_"):
            visible[k.replace("show_", "")] = True
        else:
            filters[k] = val

    players = query_players(filters)
    player_options = get_player_options()
    aa_years = get_aa_years_map()
    best_aa_draft = get_best_aa_draft_picks()

    # Post-filter: numeric AA draft picks only
    if filters.get("max_aa_draft_pick"):
        try:
            limit = int(filters["max_aa_draft_pick"])
            players = [
                p for p in players
                if best_aa_draft.get(p["player_id"], {}).get("numeric", 999) <= limit
            ]
        except:
            pass

    return render_template(
        "index.html",
        players=players,
        teams=TEAM_OPTIONS,
        filters=filters,
        visible=visible,  # ✅ FIXED
        player_options=player_options,
        aa_years=aa_years,
        best_aa_draft=best_aa_draft,
        get_player_club_stats=get_player_club_stats
    )


if __name__ == "__main__":
    app.run(debug=True)