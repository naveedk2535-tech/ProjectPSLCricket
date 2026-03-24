"""
CricSheet.org historical data downloader and parser.
Downloads PSL ball-by-ball CSV data for model training.
Free, unlimited downloads — no API key needed.
"""

import os
import io
import zipfile
import csv
import glob
from datetime import datetime
from collections import defaultdict

import pandas as pd
import requests

import config
from database import db
from data.rate_limiter import can_call, record_call, check_cache, save_cache
from data.team_names import standardise, standardise_venue

CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")
CSV_DIR = os.path.join(CACHE_DIR, "cricsheet_psl")


def download_psl_data():
    """Download and extract PSL ball-by-ball CSV data from CricSheet."""
    if not can_call("cricsheet"):
        print("[CricSheet] Rate limit reached, skipping download")
        return False

    os.makedirs(CSV_DIR, exist_ok=True)
    zip_path = os.path.join(CACHE_DIR, "psl_male_csv2.zip")

    try:
        print("[CricSheet] Downloading PSL data...")
        resp = requests.get(config.CRICSHEET_PSL_URL, timeout=60, stream=True)
        record_call("cricsheet", "download", resp.status_code)

        if resp.status_code != 200:
            print(f"[CricSheet] Download failed: {resp.status_code}")
            return False

        with open(zip_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(CSV_DIR)

        print(f"[CricSheet] Extracted to {CSV_DIR}")
        return True

    except Exception as e:
        print(f"[CricSheet] Error: {e}")
        record_call("cricsheet", "download", 0)
        return False


def parse_match_info(info_path):
    """Parse a CricSheet match info CSV file."""
    info = {}
    try:
        with open(info_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 3 and row[0] == "info":
                    key = row[1].strip()
                    value = row[2].strip() if len(row) > 2 else ""
                    if key == "team":
                        info.setdefault("teams", []).append(value)
                    elif key == "date":
                        info["date"] = value
                    elif key == "venue":
                        info["venue"] = value
                    elif key == "toss_winner":
                        info["toss_winner"] = value
                    elif key == "toss_decision":
                        info["toss_decision"] = value
                    elif key == "winner":
                        info["winner"] = value
                    elif key == "winner_runs" or key == "win_by_runs":
                        info["win_margin"] = int(value) if value else 0
                        info["win_type"] = "runs"
                    elif key == "winner_wickets" or key == "win_by_wickets":
                        info["win_margin"] = int(value) if value else 0
                        info["win_type"] = "wickets"
                    elif key == "player_of_match":
                        info["player_of_match"] = value
                    elif key == "season":
                        info["season"] = value
                    elif key == "outcome" and "no result" in value.lower():
                        info["win_type"] = "no_result"
                        info["winner"] = None
    except Exception as e:
        print(f"[CricSheet] Error parsing info {info_path}: {e}")
    return info


def parse_ball_by_ball(csv_path):
    """Parse ball-by-ball CSV and extract aggregate stats per innings."""
    stats = {
        "innings1": {"runs": 0, "wickets": 0, "overs": 0, "balls": 0,
                      "fours": 0, "sixes": 0, "wides": 0, "noballs": 0, "extras": 0,
                      "dot_balls": 0, "powerplay_runs": 0, "powerplay_wickets": 0,
                      "middle_runs": 0, "middle_wickets": 0,
                      "death_runs": 0, "death_wickets": 0, "batting_team": ""},
        "innings2": {"runs": 0, "wickets": 0, "overs": 0, "balls": 0,
                      "fours": 0, "sixes": 0, "wides": 0, "noballs": 0, "extras": 0,
                      "dot_balls": 0, "powerplay_runs": 0, "powerplay_wickets": 0,
                      "middle_runs": 0, "middle_wickets": 0,
                      "death_runs": 0, "death_wickets": 0, "batting_team": ""},
    }

    try:
        df = pd.read_csv(csv_path, low_memory=False)
    except Exception:
        return stats

    for innings_num in [1, 2]:
        key = f"innings{innings_num}"
        inn_df = df[df["innings"] == innings_num] if "innings" in df.columns else pd.DataFrame()

        if inn_df.empty:
            continue

        if "batting_team" in inn_df.columns:
            stats[key]["batting_team"] = inn_df["batting_team"].iloc[0]

        # Total runs (batsman runs + extras)
        if "runs_off_bat" in inn_df.columns:
            stats[key]["runs"] += int(inn_df["runs_off_bat"].sum())
        if "extras" in inn_df.columns:
            stats[key]["extras"] = int(inn_df["extras"].sum())
            stats[key]["runs"] += stats[key]["extras"]

        # Wickets
        if "wicket_type" in inn_df.columns:
            stats[key]["wickets"] = int(inn_df["wicket_type"].notna().sum())

        # Balls and overs
        if "ball" in inn_df.columns:
            # Count legal deliveries (exclude wides and no-balls for ball count)
            legal = inn_df
            if "wides" in inn_df.columns:
                legal = legal[inn_df.get("wides", pd.Series(dtype=float)).isna() | (inn_df.get("wides", pd.Series(dtype=float)) == 0)]
            if "noballs" in inn_df.columns:
                no_nb = inn_df[inn_df.get("noballs", pd.Series(dtype=float)).isna() | (inn_df.get("noballs", pd.Series(dtype=float)) == 0)]
                stats[key]["balls"] = len(no_nb) if not no_nb.empty else len(inn_df)
            else:
                stats[key]["balls"] = len(inn_df)
            stats[key]["overs"] = round(stats[key]["balls"] / 6 + (stats[key]["balls"] % 6) / 10, 1)

            # Get max over number
            try:
                max_ball = inn_df["ball"].max()
                stats[key]["overs"] = max_ball
            except Exception:
                pass

        # Boundaries
        if "runs_off_bat" in inn_df.columns:
            stats[key]["fours"] = int((inn_df["runs_off_bat"] == 4).sum())
            stats[key]["sixes"] = int((inn_df["runs_off_bat"] == 6).sum())

        # Extras breakdown
        if "wides" in inn_df.columns:
            stats[key]["wides"] = int(inn_df["wides"].fillna(0).astype(int).sum())
        if "noballs" in inn_df.columns:
            stats[key]["noballs"] = int(inn_df["noballs"].fillna(0).astype(int).sum())

        # Dot balls
        if "runs_off_bat" in inn_df.columns:
            total_runs_per_ball = inn_df["runs_off_bat"].fillna(0) + inn_df.get("extras", pd.Series(0, index=inn_df.index)).fillna(0)
            stats[key]["dot_balls"] = int((total_runs_per_ball == 0).sum())

        # Phase breakdown (using ball column)
        if "ball" in inn_df.columns:
            try:
                over_num = inn_df["ball"].apply(lambda x: int(float(x)))
                runs_col = inn_df["runs_off_bat"].fillna(0) + inn_df.get("extras", pd.Series(0, index=inn_df.index)).fillna(0)
                wicket_col = inn_df["wicket_type"].notna() if "wicket_type" in inn_df.columns else pd.Series(False, index=inn_df.index)

                # Powerplay: overs 0-5
                pp = (over_num >= 0) & (over_num < 6)
                stats[key]["powerplay_runs"] = int(runs_col[pp].sum())
                stats[key]["powerplay_wickets"] = int(wicket_col[pp].sum())

                # Middle: overs 6-15
                mid = (over_num >= 6) & (over_num < 16)
                stats[key]["middle_runs"] = int(runs_col[mid].sum())
                stats[key]["middle_wickets"] = int(wicket_col[mid].sum())

                # Death: overs 16-19
                death = (over_num >= 16)
                stats[key]["death_runs"] = int(runs_col[death].sum())
                stats[key]["death_wickets"] = int(wicket_col[death].sum())
            except Exception:
                pass

    return stats


def import_all_matches():
    """Process all CricSheet CSVs and import into database."""
    if not os.path.exists(CSV_DIR):
        print("[CricSheet] No data directory found. Run download_psl_data() first.")
        return 0

    # Find all match CSV files
    csv_files = glob.glob(os.path.join(CSV_DIR, "*.csv"))
    if not csv_files:
        # Try subdirectories
        csv_files = glob.glob(os.path.join(CSV_DIR, "**", "*.csv"), recursive=True)

    # Separate info files from ball-by-ball files
    # CricSheet CSV2 format: each match has one CSV with both info and ball-by-ball
    imported = 0
    errors = 0

    for csv_file in csv_files:
        try:
            # Read the file to check if it's a combined format
            info = {}
            ball_data_start = 0

            with open(csv_file, "r", encoding="utf-8") as f:
                for i, line in enumerate(f):
                    if line.startswith("info,"):
                        parts = line.strip().split(",")
                        if len(parts) >= 3:
                            key = parts[1].strip()
                            value = parts[2].strip()
                            if key == "team":
                                info.setdefault("teams", []).append(value)
                            else:
                                info[key] = value
                    elif line.startswith("ball,") or line.startswith("innings,"):
                        ball_data_start = i
                        break

            if not info.get("teams") or len(info.get("teams", [])) < 2:
                continue

            # Parse ball-by-ball data
            stats = parse_ball_by_ball(csv_file)

            team_a = standardise(info["teams"][0])
            team_b = standardise(info["teams"][1])
            venue = standardise_venue(info.get("venue", ""))
            season = info.get("season", "")
            match_date = info.get("date", "")
            winner = standardise(info.get("winner", "")) if info.get("winner") else None
            toss_winner = standardise(info.get("toss_winner", "")) if info.get("toss_winner") else None

            # Determine which team batted first
            batting_first = stats["innings1"].get("batting_team", "")
            if batting_first:
                batting_first = standardise(batting_first)
                if batting_first == team_b:
                    # Swap so team_a always batted first in our data
                    # Actually keep original order, just track correctly
                    pass

            db.execute(
                """INSERT INTO matches (season, match_date, venue, team_a, team_b,
                   toss_winner, toss_decision, innings1_runs, innings1_wickets, innings1_overs,
                   innings2_runs, innings2_wickets, innings2_overs,
                   winner, win_margin, win_type, player_of_match,
                   powerplay_runs_a, powerplay_wickets_a, powerplay_runs_b, powerplay_wickets_b,
                   middle_runs_a, middle_wickets_a, middle_runs_b, middle_wickets_b,
                   death_runs_a, death_wickets_a, death_runs_b, death_wickets_b,
                   total_fours_a, total_sixes_a, total_fours_b, total_sixes_b,
                   total_wides_a, total_noballs_a, total_wides_b, total_noballs_b,
                   total_extras_a, total_extras_b, dot_balls_a, dot_balls_b)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(season, match_date, team_a, team_b) DO UPDATE SET
                   winner=excluded.winner, win_margin=excluded.win_margin,
                   innings1_runs=excluded.innings1_runs, innings2_runs=excluded.innings2_runs""",
                [season, match_date, venue, team_a, team_b,
                 toss_winner, info.get("toss_decision"),
                 stats["innings1"]["runs"], stats["innings1"]["wickets"], stats["innings1"]["overs"],
                 stats["innings2"]["runs"], stats["innings2"]["wickets"], stats["innings2"]["overs"],
                 winner, int(info.get("win_margin", info.get("winner_runs", info.get("winner_wickets", 0))) or 0),
                 info.get("win_type", ""),
                 info.get("player_of_match"),
                 stats["innings1"]["powerplay_runs"], stats["innings1"]["powerplay_wickets"],
                 stats["innings2"]["powerplay_runs"], stats["innings2"]["powerplay_wickets"],
                 stats["innings1"]["middle_runs"], stats["innings1"]["middle_wickets"],
                 stats["innings2"]["middle_runs"], stats["innings2"]["middle_wickets"],
                 stats["innings1"]["death_runs"], stats["innings1"]["death_wickets"],
                 stats["innings2"]["death_runs"], stats["innings2"]["death_wickets"],
                 stats["innings1"]["fours"], stats["innings1"]["sixes"],
                 stats["innings2"]["fours"], stats["innings2"]["sixes"],
                 stats["innings1"]["wides"], stats["innings1"]["noballs"],
                 stats["innings2"]["wides"], stats["innings2"]["noballs"],
                 stats["innings1"]["extras"], stats["innings2"]["extras"],
                 stats["innings1"]["dot_balls"], stats["innings2"]["dot_balls"]]
            )
            imported += 1

        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"[CricSheet] Error importing {csv_file}: {e}")

    print(f"[CricSheet] Imported {imported} matches ({errors} errors)")
    return imported


def update_venue_stats():
    """Calculate and update venue statistics from match history."""
    venues = db.fetch_all("SELECT DISTINCT venue FROM matches WHERE venue IS NOT NULL AND venue != ''")

    for v in venues:
        venue = v["venue"]
        matches = db.fetch_all("SELECT * FROM matches WHERE venue = ?", [venue])

        if not matches:
            continue

        first_innings_scores = [m["innings1_runs"] for m in matches if m["innings1_runs"]]
        second_innings_scores = [m["innings2_runs"] for m in matches if m["innings2_runs"]]
        chase_wins = sum(1 for m in matches if m["winner"] and m["innings2_runs"] and
                         m["winner"] != (m["team_a"] if m["innings1_runs"] and m["innings1_runs"] > (m["innings2_runs"] or 0) else m["team_b"]))
        toss_bat_first = sum(1 for m in matches if m["toss_decision"] and m["toss_decision"].lower() == "bat")

        wides = [m["total_wides_a"] + m["total_wides_b"] for m in matches
                 if m["total_wides_a"] is not None and m["total_wides_b"] is not None]
        noballs = [m["total_noballs_a"] + m["total_noballs_b"] for m in matches
                   if m["total_noballs_a"] is not None and m["total_noballs_b"] is not None]
        sixes = [m["total_sixes_a"] + m["total_sixes_b"] for m in matches
                 if m["total_sixes_a"] is not None and m["total_sixes_b"] is not None]
        fours = [m["total_fours_a"] + m["total_fours_b"] for m in matches
                 if m["total_fours_a"] is not None and m["total_fours_b"] is not None]

        total_matches = len(matches)
        chase_matches = len([m for m in matches if m["innings2_runs"]])

        db.execute(
            """INSERT INTO venue_stats (venue, city, matches_played,
               avg_first_innings, avg_second_innings, chase_win_pct, toss_bat_first_pct,
               avg_wides, avg_noballs, avg_sixes, avg_fours,
               highest_total, lowest_total, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(venue) DO UPDATE SET
               matches_played=excluded.matches_played, avg_first_innings=excluded.avg_first_innings,
               avg_second_innings=excluded.avg_second_innings, chase_win_pct=excluded.chase_win_pct,
               toss_bat_first_pct=excluded.toss_bat_first_pct, avg_wides=excluded.avg_wides,
               avg_noballs=excluded.avg_noballs, avg_sixes=excluded.avg_sixes, avg_fours=excluded.avg_fours,
               highest_total=excluded.highest_total, lowest_total=excluded.lowest_total,
               updated_at=excluded.updated_at""",
            [venue, standardise_venue(venue),
             total_matches,
             sum(first_innings_scores) / len(first_innings_scores) if first_innings_scores else None,
             sum(second_innings_scores) / len(second_innings_scores) if second_innings_scores else None,
             (chase_wins / chase_matches * 100) if chase_matches > 0 else 50.0,
             (toss_bat_first / total_matches * 100) if total_matches > 0 else 50.0,
             sum(wides) / len(wides) if wides else 0,
             sum(noballs) / len(noballs) if noballs else 0,
             sum(sixes) / len(sixes) if sixes else 0,
             sum(fours) / len(fours) if fours else 0,
             max(first_innings_scores + second_innings_scores) if first_innings_scores or second_innings_scores else None,
             min([s for s in first_innings_scores + second_innings_scores if s and s > 0]) if first_innings_scores or second_innings_scores else None,
             db.now_iso()]
        )

    print(f"[CricSheet] Updated venue stats for {len(venues)} venues")


def update_head_to_head():
    """Calculate head-to-head records between all team pairs."""
    teams = db.fetch_all("SELECT DISTINCT team_a FROM matches UNION SELECT DISTINCT team_b FROM matches")
    team_list = [t["team_a"] for t in teams]

    for i, team_a in enumerate(team_list):
        for team_b in team_list[i + 1:]:
            matches = db.fetch_all(
                """SELECT * FROM matches WHERE
                   (team_a = ? AND team_b = ?) OR (team_a = ? AND team_b = ?)
                   ORDER BY match_date""",
                [team_a, team_b, team_b, team_a]
            )

            if not matches:
                continue

            a_wins = sum(1 for m in matches if m["winner"] == team_a)
            b_wins = sum(1 for m in matches if m["winner"] == team_b)
            no_results = len(matches) - a_wins - b_wins

            totals_a = [m["innings1_runs"] if m["team_a"] == team_a else m["innings2_runs"]
                        for m in matches if m["innings1_runs"]]
            totals_b = [m["innings1_runs"] if m["team_a"] == team_b else m["innings2_runs"]
                        for m in matches if m["innings1_runs"]]

            last = matches[-1] if matches else None

            db.execute(
                """INSERT INTO head_to_head (team_a, team_b, matches_played,
                   team_a_wins, team_b_wins, no_results, avg_total_a, avg_total_b,
                   last_winner, last_match_date, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(team_a, team_b) DO UPDATE SET
                   matches_played=excluded.matches_played, team_a_wins=excluded.team_a_wins,
                   team_b_wins=excluded.team_b_wins, avg_total_a=excluded.avg_total_a,
                   avg_total_b=excluded.avg_total_b, last_winner=excluded.last_winner,
                   last_match_date=excluded.last_match_date, updated_at=excluded.updated_at""",
                [team_a, team_b, len(matches), a_wins, b_wins, no_results,
                 sum(t for t in totals_a if t) / len(totals_a) if totals_a else None,
                 sum(t for t in totals_b if t) / len(totals_b) if totals_b else None,
                 last["winner"] if last else None,
                 last["match_date"] if last else None,
                 db.now_iso()]
            )

    print(f"[CricSheet] Updated head-to-head records")
