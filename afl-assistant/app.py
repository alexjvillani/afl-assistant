# app.py
# -*- coding: utf-8 -*-

from flask import Flask, render_template, request
import sqlite3

app = Flask(__name__)
DB_PATH = "players.db"

AFL_TEAMS = [
    "adelaide", "brisbaneb", "brisbanel", "carlton", "collingwood",
    "essendon", "fitzroy", "fremantle", "geelong", "goldcoast",
    "gws", "hawthorn", "melbourne", "kangaroos",
    "padelaide", "richmond", "stkilda", "swans", "university",
    "westcoast", "bulldogs"
]

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def query_players(team1, team2, min_games):
    conn = get_db()
    cur = conn.cursor()

    sql = """
    SELECT name, career_games, career_goals, teams
    FROM players
    WHERE teams LIKE ?
      AND career_games >= ?
    """
    params = ["%" + team1 + "%", min_games]

    if team2:
        sql += " AND teams LIKE ?"
        params.append("%" + team2 + "%")

    sql += " ORDER BY career_games ASC, career_goals ASC"

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return rows

@app.route("/", methods=["GET", "POST"])
def index():
    players = []
    team1 = ""
    team2 = ""
    min_games = 0

    if request.method == "POST":
        team1 = request.form.get("team1", "")
        team2 = request.form.get("team2", "")
        try:
            min_games = int(request.form.get("min_games", 0))
        except:
            min_games = 0

        if team1:
            players = query_players(team1, team2, min_games)

    return render_template(
        "index.html",
        teams=AFL_TEAMS,
        players=players,
        team1=team1,
        team2=team2,
        min_games=min_games
    )

if __name__ == "__main__":
    app.run(debug=True)
