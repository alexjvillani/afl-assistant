# -*- coding: utf-8 -*-

import sqlite3

DB_PATH = "players.db"


def get_player_id_by_name(name):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        SELECT player_id
        FROM players
        WHERE LOWER(TRIM(name)) = LOWER(TRIM(?))
    """, (name,))

    row = c.fetchone()
    conn.close()

    if row:
        return row[0]
    return None


def test_teammate_filter(input_name):

    print "=== TEST TEAMMATE FILTER ==="
    print "Input:", input_name

    # Step 1: convert name -> player_id
    if input_name.startswith("http"):
        player_id = input_name
        print "[INFO] Already a player_id"
    else:
        player_id = get_player_id_by_name(input_name)
        print "[INFO] Converted to player_id:", player_id

    if not player_id:
        print "[ERROR] Could not find player"
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # Step 2: run SAME query as your app
    query = """
        SELECT *
        FROM players
        WHERE player_id IN (

            SELECT DISTINCT ps2.player_id
            FROM player_seasons ps1
            JOIN player_seasons ps2
              ON ps1.year = ps2.year
              AND ps1.team = ps2.team
            WHERE ps1.player_id = ?
            AND ps2.player_id != ps1.player_id

        )
        LIMIT 20
    """

    c.execute(query, (player_id,))
    rows = c.fetchall()

    print "Results found:", len(rows)

    if not rows:
        print "[ERROR] ZERO RESULTS"
    else:
        print "[OK] Sample teammates:"
        for r in rows:
            print "-", r["name"]

    conn.close()


if __name__ == "__main__":

    # 🔥 TEST CASES

    # Case 1: name input (like your UI)
    test_teammate_filter("Grigg, Shaun")

    print "\n-------------------\n"

    # Case 2: direct player_id (like Gridley)
    test_teammate_filter("https://afltables.com/afl/stats/players/S/Shaun_Grigg.html")