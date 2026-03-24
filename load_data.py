"""Load seed data into the database. Run this on PythonAnywhere."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import db

def main():
    print("Initializing database...")
    db.init_db()

    seed_path = os.path.join(os.path.dirname(__file__), "data", "seed_data.sql")
    if not os.path.exists(seed_path):
        print(f"Seed file not found: {seed_path}")
        return

    print("Loading seed data...")
    with open(seed_path, "r") as f:
        sql = f.read()

    conn = db.get_connection()
    conn.executescript(sql)
    conn.commit()

    # Verify
    count = db.fetch_one("SELECT COUNT(*) as cnt FROM matches")
    teams = db.fetch_all("SELECT team, elo, form_last5 FROM team_ratings ORDER BY elo DESC")
    venues = db.fetch_one("SELECT COUNT(*) as cnt FROM venue_stats")

    print(f"\nLoaded successfully!")
    print(f"  Matches: {count['cnt']}")
    print(f"  Venues: {venues['cnt']}")
    print(f"\nTeam Rankings:")
    for t in teams:
        print(f"  {t['team']:25s} Elo: {t['elo']:7.1f}  Form: {t['form_last5']:.0f}%")

if __name__ == "__main__":
    main()
