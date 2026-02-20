# -*- coding: utf-8 -*-

from flask import Flask, render_template, request
import sqlite3

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
    """
    Per-club games & goals for a player.
    """
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
# MAIN QUERY LOGIC
# -------------------------------------------------

def query_players(filters):

    conn = get_db()
    c = conn.cursor()

    query = "SELECT * FROM players WHERE 1=1"
    params = []

    # ----------------------------
    # TEAM FILTERS
    # ----------------------------

    if filters.get("team1"):
        query += " AND (',' || REPLACE(teams, ' ', '') || ',') LIKE ?"
        params.append("%," + filters["team1"] + ",%")

    if filters.get("team2"):
        query += " AND (',' || REPLACE(teams, ' ', '') || ',') LIKE ?"
        params.append("%," + filters["team2"] + ",%")

    # ----------------------------
    # TEAMMATE FILTER
    # ----------------------------

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

    # ----------------------------
    # CAREER TOTAL FILTERS
    # ----------------------------

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

    # ----------------------------
    # SINGLE-GAME PEAKS
    # ----------------------------

    if filters.get("min_max_goals_game"):
        query += " AND max_goals_game >= ?"
        params.append(filters["min_max_goals_game"])

    if filters.get("min_max_disposals_game"):
        query += " AND max_disposals_game >= ?"
        params.append(filters["min_max_disposals_game"])

    # ----------------------------
    # SEASON PEAKS
    # ----------------------------

    if filters.get("min_max_goals_season"):
        query += " AND max_goals_season >= ?"
        params.append(filters["min_max_goals_season"])

    if filters.get("max_max_goals_season"):
        query += " AND max_goals_season <= ?"
        params.append(filters["max_max_goals_season"])

    # ----------------------------
    # HEIGHT
    # ----------------------------

    if filters.get("min_height"):
        query += " AND height >= ?"
        params.append(filters["min_height"])

    if filters.get("max_height"):
        query += " AND height <= ?"
        params.append(filters["max_height"])

    # ----------------------------
    # CAREER SPAN
    # ----------------------------

    if filters.get("min_first_year"):
        query += " AND first_year >= ?"
        params.append(filters["min_first_year"])

    if filters.get("min_last_year"):
        query += " AND last_year >= ?"
        params.append(filters["min_last_year"])

    # ----------------------------
    # SORTING (MAX VISIBILITY)
    # ----------------------------

    sort_column = filters.get("sort_by") or "career_games"
    sort_order = filters.get("sort_order") or "DESC"

    allowed_columns = [
        # Career
        "career_games",
        "career_goals",

        # Single-game peaks
        "max_goals_game",
        "max_disposals_game",
        "max_kicks_game",
        "max_handballs_game",

        # Season peaks
        "max_goals_season",
        "max_goals_season_year",

        # Awards
        "brownlow_votes",
        "brownlow_wins",

        # Finals
        "gf_appearances",
        "gf_wins",

        # Bio
        "height",
        "first_year",
        "last_year"
    ]

    if sort_column not in allowed_columns:
        sort_column = "career_games"

    if sort_order not in ("ASC", "DESC"):
        sort_order = "DESC"

    query += " ORDER BY %s %s" % (sort_column, sort_order)

    c.execute(query, params)
    results = c.fetchall()
    conn.close()
    return results


# -------------------------------------------------
# ROUTES
# -------------------------------------------------

@app.route("/", methods=["GET"])
def index():

    filters = {
        # Team & teammate
        "team1": request.args.get("team1"),
        "team2": request.args.get("team2"),
        "teammate_of": request.args.get("teammate_of"),

        # Career
        "min_games": request.args.get("min_games"),
        "max_games": request.args.get("max_games"),
        "min_goals": request.args.get("min_goals"),
        "max_goals": request.args.get("max_goals"),

        # Single-game
        "min_max_goals_game": request.args.get("min_max_goals_game"),
        "min_max_disposals_game": request.args.get("min_max_disposals_game"),

        # Season
        "min_max_goals_season": request.args.get("min_max_goals_season"),
        "max_max_goals_season": request.args.get("max_max_goals_season"),

        # Bio
        "min_height": request.args.get("min_height"),
        "max_height": request.args.get("max_height"),
        "min_first_year": request.args.get("min_first_year"),
        "min_last_year": request.args.get("min_last_year"),

        # Sorting
        "sort_by": request.args.get("sort_by"),
        "sort_order": request.args.get("sort_order"),
    }

    # ----------------------------
    # COLUMN VISIBILITY FLAGS
    # ----------------------------

    visible_columns = {
        # Career
        "career_games": request.args.get("show_career_games"),
        "career_goals": request.args.get("show_career_goals"),

        # Single-game
        "max_goals_game": request.args.get("show_max_goals_game"),
        "max_disposals_game": request.args.get("show_max_disposals"),
        "max_kicks_game": request.args.get("show_max_kicks"),
        "max_handballs_game": request.args.get("show_max_handballs"),

        # Season
        "max_goals_season": request.args.get("show_max_goals_season"),
        "max_goals_season_year": request.args.get("show_max_goals_season_year"),

        # Awards
        "brownlow_votes": request.args.get("show_votes"),
        "brownlow_wins": request.args.get("show_wins"),

        # Finals
        "gf_appearances": request.args.get("show_gf_apps"),
        "gf_wins": request.args.get("show_gf_wins"),

        # Bio
        "height": request.args.get("show_height"),
        "years": request.args.get("show_years"),

        # Club breakdown
        "club_stats": request.args.get("show_club_stats"),
    }

    players = query_players(filters)
    player_options = get_player_options()

    return render_template(
        "index.html",
        players=players,
        teams=TEAM_OPTIONS,
        filters=filters,
        visible=visible_columns,
        player_options=player_options,
        get_player_club_stats=get_player_club_stats
    )


if __name__ == "__main__":
    app.run(debug=True)