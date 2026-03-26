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
        _local.connection.execute("PRAGMA journal_mode=DELETE")
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
    """Add league column to existing tables if not present (safe to re-run)."""
    conn = get_connection()
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
    conn.commit()


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
