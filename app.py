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


def query_players(filters):

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    query = "SELECT * FROM players WHERE 1=1"
    params = []

    # ----------------------------
    # TEAM FILTERS (Exact Matching Fix)
    # ----------------------------

    if filters["team1"]:
        query += " AND (',' || teams || ',') LIKE ?"
        params.append("%," + filters["team1"] + ",%")

    if filters["team2"]:
        query += " AND (',' || teams || ',') LIKE ?"
        params.append("%," + filters["team2"] + ",%")

    # ----------------------------
    # CAREER FILTERS
    # ----------------------------

    if filters["min_games"]:
        query += " AND career_games >= ?"
        params.append(filters["min_games"])

    if filters["max_games"]:
        query += " AND career_games <= ?"
        params.append(filters["max_games"])

    if filters["min_goals"]:
        query += " AND career_goals >= ?"
        params.append(filters["min_goals"])

    if filters["max_goals"]:
        query += " AND career_goals <= ?"
        params.append(filters["max_goals"])
        
    # ----------------------------
    # HEIGHT FILTERS
    # ----------------------------

    if filters["min_height"]:
        query += " AND height >= ?"
        params.append(filters["min_height"])

    if filters["max_height"]:
        query += " AND height <= ?"
        params.append(filters["max_height"])
        
    # ----------------------------
    # CAREER YEAR FILTERS
    # ----------------------------

    if filters["min_first_year"]:
        query += " AND first_year >= ?"
        params.append(filters["min_first_year"])

    if filters["min_last_year"]:
        query += " AND last_year >= ?"
        params.append(filters["min_last_year"])
    # ----------------------------
    # BROWNLOW FILTERS
    # ----------------------------

    if filters["min_votes"]:
        query += " AND brownlow_votes >= ?"
        params.append(filters["min_votes"])

    if filters["min_wins"]:
        query += " AND brownlow_wins >= ?"
        params.append(filters["min_wins"])

    # ----------------------------
    # GRAND FINAL FILTERS
    # ----------------------------

    if filters.get("min_gf_apps"):
        query += " AND gf_appearances >= ?"
        params.append(filters["min_gf_apps"])

    if filters.get("min_gf_wins"):
        query += " AND gf_wins >= ?"
        params.append(filters["min_gf_wins"])

    # ----------------------------
    # SORTING
    # ----------------------------

    sort_column = filters.get("sort_by") or "career_games"
    sort_order = filters.get("sort_order") or "DESC"

    allowed_columns = [
        "career_games",
        "career_goals",
        "max_goals_game",
        "max_disposals_game",
        "max_kicks_game",
        "brownlow_votes",
        "brownlow_wins",
        "gf_wins",
        "height",
        "first_year",
        "last_year"
    ]

    if sort_column not in allowed_columns:
        sort_column = "career_games"

    if sort_order not in ["ASC", "DESC"]:
        sort_order = "DESC"

    query += " ORDER BY %s %s" % (sort_column, sort_order)

    c.execute(query, params)
    results = c.fetchall()
    conn.close()

    return results


@app.route("/", methods=["GET"])
def index():

    filters = {
        "team1": request.args.get("team1"),
        "team2": request.args.get("team2"),
        "min_games": request.args.get("min_games"),
        "max_games": request.args.get("max_games"),
        "min_goals": request.args.get("min_goals"),
        "max_goals": request.args.get("max_goals"),
        "min_height": request.args.get("min_height"),
        "max_height": request.args.get("max_height"),
        "min_votes": request.args.get("min_votes"),
        "min_wins": request.args.get("min_wins"),
        "min_first_year": request.args.get("min_first_year"),
        "min_last_year": request.args.get("min_last_year"),
        "sort_by": request.args.get("sort_by"),
        "sort_order": request.args.get("sort_order")
    }

    # ----------------------------
    # Column Visibility
    # ----------------------------

    visible_columns = {
        "max_goals": request.args.get("show_max_goals"),
        "max_disposals": request.args.get("show_max_disposals"),
        "max_kicks": request.args.get("show_max_kicks"),
        "brownlow_votes": request.args.get("show_votes"),
        "brownlow_wins": request.args.get("show_wins"),
        "gf_record": request.args.get("show_gf_record"),
        "height": request.args.get("show_height"),
        "years": request.args.get("show_years"),
    }

    players = query_players(filters)

    return render_template(
        "index.html",
        players=players,
        teams=TEAM_OPTIONS,
        filters=filters,
        visible=visible_columns
    )


if __name__ == "__main__":
    app.run(debug=True)
