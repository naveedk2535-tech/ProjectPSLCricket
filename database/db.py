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
    """Initialize database from schema.sql."""
    conn = get_connection()
    if os.path.exists(SCHEMA_PATH):
        with open(SCHEMA_PATH, "r") as f:
            conn.executescript(f.read())
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
