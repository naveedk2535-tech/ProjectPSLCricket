"""Load seed data into the database. Safe for PythonAnywhere."""
import os
import sys
import sqlite3

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "projectpslcricket.db")
SCHEMA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "database", "schema.sql")
SEED_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "seed_data.sql")

def main():
    # Delete old DB completely
    for ext in ["", "-wal", "-shm"]:
        p = DB_PATH + ext
        if os.path.exists(p):
            os.remove(p)
            print(f"Deleted {os.path.basename(p)}")

    # Create fresh DB (no WAL mode to avoid web worker conflicts)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=DELETE")  # NOT WAL — simpler, no -wal/-shm files

    # Create tables
    with open(SCHEMA_PATH) as f:
        schema = f.read()
    for stmt in schema.split(';'):
        stmt = stmt.strip()
        if stmt and ('CREATE' in stmt):
            try:
                conn.execute(stmt)
            except sqlite3.OperationalError:
                pass
    conn.commit()
    print("Tables created")

    # Load seed data
    loaded = 0
    errors = 0
    with open(SEED_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('--'):
                try:
                    conn.execute(line)
                    loaded += 1
                except Exception as e:
                    errors += 1
                    if errors <= 3:
                        print(f"Error: {e}")
    conn.commit()
    conn.close()

    # Verify
    conn = sqlite3.connect(DB_PATH)
    for table in ["matches", "fixtures", "predictions", "team_ratings", "venue_stats", "head_to_head"]:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count}")
    conn.close()

    print(f"\nDone! Loaded {loaded} rows ({errors} errors)")

if __name__ == "__main__":
    main()
