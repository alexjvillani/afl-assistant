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
    "westcoast", "bulldogs"
]


def query_players(filters):

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    query = "SELECT * FROM players WHERE 1=1"
    params = []

    # ----------------------------
    # TEAM FILTERS
    # ----------------------------

    if filters["team1"]:
        query += " AND teams LIKE ?"
        params.append("%" + filters["team1"] + "%")

    if filters["team2"]:
        query += " AND teams LIKE ?"
        params.append("%" + filters["team2"] + "%")

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
    # BROWNLOW FILTERS
    # ----------------------------

    if filters["min_votes"]:
        query += " AND brownlow_votes >= ?"
        params.append(filters["min_votes"])

    if filters["min_wins"]:
        query += " AND brownlow_wins >= ?"
        params.append(filters["min_wins"])

    # ----------------------------
    # SORTING
    # ----------------------------

    sort_column = filters["sort_by"] or "career_games"
    sort_order = filters["sort_order"] or "DESC"

    allowed_columns = [
        "career_games",
        "career_goals",
        "max_goals_game",
        "max_disposals_game",
        "max_kicks_game",
        "brownlow_votes",
        "brownlow_wins"
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
        "min_votes": request.args.get("min_votes"),
        "min_wins": request.args.get("min_wins"),
        "sort_by": request.args.get("sort_by"),
        "sort_order": request.args.get("sort_order")
    }

    players = query_players(filters)

    return render_template(
        "index.html",
        players=players,
        teams=TEAM_OPTIONS,
        filters=filters
    )


if __name__ == "__main__":
    app.run(debug=True)
