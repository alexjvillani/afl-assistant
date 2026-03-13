# AFL Gridley Assistant

A Flask-based tool that helps solve **Gridley AFL puzzles** by searching a large AFL player database using the puzzle's row and column clues.

The app automatically scrapes the current Gridley puzzle, converts clues into database filters, and returns players that satisfy the conditions.

---

# Features

* Scrapes the **daily Gridley puzzle** automatically

* Converts clues into database filters

* Searches a database of **13,000+ AFL/VFL players**

* Supports many clue types including:

  * Club filters (Adelaide, Sydney, etc.)
  * Career games
  * Games per club
  * Teammate detection
  * Disposal / tackle / mark records
  * Draft pick filters
  * Minor premierships

* Displays relevant columns automatically based on the clue

---

# Project Structure

```
afl-assistant/

app.py                # Main Flask application
fetch_gridley.py      # Scrapes the daily Gridley puzzle
gridley_utils.py      # Converts clues into database filters
players.db            # SQLite database of AFL players
requirements.txt      # Python dependencies

/templates
    index.html

/static
```

---

# Requirements

The project requires:

* Python **2.7**
* Firefox browser
* Geckodriver (for Selenium)

---

# Installation

Clone the repository:

```
git clone <repo_url>
cd afl-assistant
```

Install Python dependencies:

```
pip install -r requirements.txt
```

Install Firefox if it is not already installed:

[https://www.mozilla.org/firefox/](https://www.mozilla.org/firefox/)

Download **geckodriver**:

[https://github.com/mozilla/geckodriver/releases](https://github.com/mozilla/geckodriver/releases)

Place the `geckodriver.exe` file in the project root directory.

---

# Running the App

Start the Flask server:

```
python app.py
```

Open your browser and go to:

```
http://127.0.0.1:5000
```

The page will load the current Gridley puzzle and allow you to search each square.

---

# Database

The included `players.db` contains:

* All VFL/AFL players
* Games played
* Clubs played for
* Awards
* Draft picks
* Statistical records

If the database is missing, the scraping scripts in the repository can rebuild it.

---

# Optional Data Scripts

The repository includes several scripts used to populate the database:

```
scrape_players.py
scrape_22under.py
update_all_australians.py
scrape_brownlow.py
```

These are only needed if rebuilding the database.

---

# Example Gridley Clues Supported

Examples of clues that the app can interpret automatically:

```
Played at least 1 game for the Adelaide Crows
Played 200 or more games
Has played 50 or more games for two different clubs
Player has been a teammate of Jordan Dawson
Collected 35 or more disposals in a single game
```

The system extracts:

* Club names
* Player names
* Numeric values
* Statistical categories

and converts them into SQL filters.

---


# License

This project is provided for personal and educational use.

AFL statistics sourced from public AFL data sites.
