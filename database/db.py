"""
Database connection pool and CRUD helpers for SQLite.
Thread-safe connections with WAL mode for concurrent access.
"""

import os
import sqlite3
import threading
from datetime import datetime

_local = threading.local()

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "projectpslcricket.db")
SCHEMA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema.sql")


def get_connection():
    """Get a thread-local database connection."""
    if not hasattr(_local, "connection") or _local.connection is None:
        _local.connection = sqlite3.connect(DB_PATH, timeout=30)
        _local.connection.row_factory = sqlite3.Row
        _local.connection.execute("PRAGMA journal_mode=WAL")
        _local.connection.execute("PRAGMA foreign_keys=ON")
        _local.connection.execute("PRAGMA busy_timeout=5000")
    return _local.connection


def init_db():
    """Initialize database tables using CREATE TABLE IF NOT EXISTS."""
    conn = get_connection()
    if os.path.exists(SCHEMA_PATH):
        with open(SCHEMA_PATH, "r") as f:
            schema = f.read()
        # Only run CREATE TABLE statements (skip PRAGMAs which can cause issues)
        for statement in schema.split(';'):
            statement = statement.strip()
            if statement and ('CREATE TABLE' in statement or 'CREATE INDEX' in statement):
                try:
                    conn.execute(statement)
                except sqlite3.OperationalError:
                    pass  # Table/index already exists
        conn.commit()


def fetch_one(sql, params=None):
    """Fetch a single row."""
    conn = get_connection()
    cursor = conn.execute(sql, params or [])
    row = cursor.fetchone()
    return dict(row) if row else None


def fetch_all(sql, params=None):
    """Fetch all rows."""
    conn = get_connection()
    cursor = conn.execute(sql, params or [])
    return [dict(row) for row in cursor.fetchall()]


def execute(sql, params=None):
    """Execute a single statement and commit."""
    conn = get_connection()
    cursor = conn.execute(sql, params or [])
    conn.commit()
    return cursor.lastrowid


def execute_many(sql, params_list):
    """Execute a statement with multiple parameter sets."""
    conn = get_connection()
    conn.executemany(sql, params_list)
    conn.commit()


def execute_script(sql):
    """Execute a SQL script (multiple statements)."""
    conn = get_connection()
    conn.executescript(sql)
    conn.commit()


def close():
    """Close the thread-local connection."""
    if hasattr(_local, "connection") and _local.connection:
        _local.connection.close()
        _local.connection = None


def table_exists(table_name):
    """Check if a table exists."""
    result = fetch_one(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        [table_name]
    )
    return result is not None


def row_count(table_name):
    """Get row count for a table."""
    result = fetch_one(f"SELECT COUNT(*) as cnt FROM {table_name}")
    return result["cnt"] if result else 0


def now_iso():
    """Return current UTC time in ISO format."""
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")


def migrate_add_league_column():
    """Add league column to existing tables and fix unique constraints for league separation."""
    conn = get_connection()

    # Step 1: Add league column to all tables
    tables = [
        "matches", "fixtures", "predictions", "odds", "value_bets",
        "team_ratings", "venue_stats", "player_stats", "head_to_head",
        "sentiment", "weather", "model_tracker", "model_performance",
        "live_matches",
    ]
    for table in tables:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN league TEXT DEFAULT 'psl'")
        except sqlite3.OperationalError:
            pass  # Column already exists

    # Step 2: Recreate tables that need UNIQUE(... league) constraints
    # Only do this if the constraint is wrong (check by trying a dummy insert)
    _fix_unique_constraint(conn, "player_stats",
        """CREATE TABLE player_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL, team TEXT NOT NULL, league TEXT DEFAULT 'psl',
            role TEXT, batting_avg REAL, batting_sr REAL, bowling_avg REAL,
            bowling_economy REAL, bowling_sr REAL, catches INTEGER DEFAULT 0,
            stumpings INTEGER DEFAULT 0, matches_played INTEGER DEFAULT 0,
            innings_batted INTEGER DEFAULT 0, innings_bowled INTEGER DEFAULT 0,
            runs_scored INTEGER DEFAULT 0, wickets_taken INTEGER DEFAULT 0,
            fifties INTEGER DEFAULT 0, hundreds INTEGER DEFAULT 0,
            three_wicket_hauls INTEGER DEFAULT 0, powerplay_sr REAL, death_sr REAL,
            powerplay_economy REAL, death_economy REAL, dot_ball_pct REAL,
            boundary_pct REAL, availability TEXT DEFAULT 'available',
            impact_score REAL, updated_at TEXT,
            UNIQUE(name, team, league)
        )""",
        ["name","team","league","role","batting_avg","batting_sr","bowling_avg",
         "bowling_economy","bowling_sr","catches","stumpings","matches_played",
         "innings_batted","innings_bowled","runs_scored","wickets_taken","fifties",
         "hundreds","three_wicket_hauls","powerplay_sr","death_sr","powerplay_economy",
         "death_economy","dot_ball_pct","boundary_pct","availability","impact_score","updated_at"])

    _fix_unique_constraint(conn, "venue_stats",
        """CREATE TABLE venue_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            venue TEXT NOT NULL, league TEXT DEFAULT 'psl', city TEXT,
            matches_played INTEGER DEFAULT 0, avg_first_innings REAL,
            avg_second_innings REAL, chase_win_pct REAL, toss_bat_first_pct REAL,
            avg_wides REAL, avg_noballs REAL, avg_sixes REAL, avg_fours REAL,
            avg_extras REAL, pace_wicket_pct REAL, spin_wicket_pct REAL,
            highest_total INTEGER, lowest_total INTEGER, avg_powerplay_score REAL,
            avg_death_score REAL, day_avg_score REAL, night_avg_score REAL,
            dew_impact_score REAL, updated_at TEXT,
            UNIQUE(venue, league)
        )""",
        ["venue","league","city","matches_played","avg_first_innings","avg_second_innings",
         "chase_win_pct","toss_bat_first_pct","avg_wides","avg_noballs","avg_sixes","avg_fours",
         "avg_extras","pace_wicket_pct","spin_wicket_pct","highest_total","lowest_total",
         "avg_powerplay_score","avg_death_score","day_avg_score","night_avg_score",
         "dew_impact_score","updated_at"])

    _fix_unique_constraint(conn, "head_to_head",
        """CREATE TABLE head_to_head (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_a TEXT NOT NULL, team_b TEXT NOT NULL, league TEXT DEFAULT 'psl',
            matches_played INTEGER DEFAULT 0, team_a_wins INTEGER DEFAULT 0,
            team_b_wins INTEGER DEFAULT 0, no_results INTEGER DEFAULT 0,
            avg_total_a REAL, avg_total_b REAL, team_a_bat_first_wins INTEGER DEFAULT 0,
            team_b_bat_first_wins INTEGER DEFAULT 0, last_winner TEXT,
            last_match_date TEXT, venue_breakdown TEXT, updated_at TEXT,
            UNIQUE(team_a, team_b, league)
        )""",
        ["team_a","team_b","league","matches_played","team_a_wins","team_b_wins",
         "no_results","avg_total_a","avg_total_b","team_a_bat_first_wins",
         "team_b_bat_first_wins","last_winner","last_match_date","venue_breakdown","updated_at"])

    _fix_unique_constraint(conn, "team_ratings",
        """CREATE TABLE team_ratings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team TEXT NOT NULL, league TEXT DEFAULT 'psl',
            elo REAL DEFAULT 1500, elo_home REAL DEFAULT 1500, elo_away REAL DEFAULT 1500,
            batting_avg REAL, bowling_avg REAL, batting_sr REAL, bowling_economy REAL,
            powerplay_run_rate REAL, powerplay_wicket_rate REAL, middle_run_rate REAL,
            death_overs_economy REAL, death_overs_run_rate REAL,
            form_last5 REAL DEFAULT 0, form_last10 REAL DEFAULT 0,
            streak_type TEXT DEFAULT 'N', streak_length INTEGER DEFAULT 0,
            nrr REAL DEFAULT 0.0, matches_played INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0, no_results INTEGER DEFAULT 0,
            boundary_pct REAL, dot_ball_pct REAL, extras_conceded_avg REAL,
            collapse_rate REAL, updated_at TEXT,
            UNIQUE(team, league)
        )""",
        ["team","league","elo","elo_home","elo_away","batting_avg","bowling_avg","batting_sr",
         "bowling_economy","powerplay_run_rate","powerplay_wicket_rate","middle_run_rate",
         "death_overs_economy","death_overs_run_rate","form_last5","form_last10",
         "streak_type","streak_length","nrr","matches_played","wins","losses","no_results",
         "boundary_pct","dot_ball_pct","extras_conceded_avg","collapse_rate","updated_at"])

    conn.commit()


def _fix_unique_constraint(conn, table_name, create_sql, columns):
    """Recreate a table if its unique constraint doesn't include league. Safe to re-run."""
    # Check if constraint already correct by looking at table SQL
    table_sql = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", [table_name]
    ).fetchone()
    if table_sql and "league)" in table_sql[0]:
        return  # Already has league in unique constraint

    # Backup, drop, recreate, restore
    try:
        rows = conn.execute(f"SELECT * FROM {table_name}").fetchall()
    except Exception:
        rows = []

    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(create_sql)

    for r in rows:
        d = dict(r)
        vals = []
        valid_cols = []
        for c in columns:
            if c in d:
                valid_cols.append(c)
                vals.append(d[c])
            elif c == "league":
                valid_cols.append(c)
                vals.append("psl")
        placeholders = ",".join(["?"] * len(valid_cols))
        col_names = ",".join(valid_cols)
        try:
            conn.execute(f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})", vals)
        except Exception:
            pass


def migrate_add_data_refresh_log():
    """Create data_refresh_log table for tracking API update timestamps."""
    conn = get_connection()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS data_refresh_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                league TEXT DEFAULT 'psl',
                source TEXT NOT NULL,
                status TEXT DEFAULT 'ok',
                detail TEXT,
                refreshed_at TEXT NOT NULL,
                UNIQUE(league, source)
            )
        """)
        conn.commit()
    except sqlite3.OperationalError:
        pass
