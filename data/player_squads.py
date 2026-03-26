"""
PSL Player Stats — Extracted from CricSheet ball-by-ball CSV data.

Parses all PSL ball-by-ball CSVs from data/cache/cricsheet_psl/ to build
real player profiles with batting, bowling, and phase-specific stats.

Usage:
    from data.player_squads import seed_player_stats, get_team_players
    seed_player_stats()  # populates player_stats table from CSV data
"""

import glob
import os
from collections import defaultdict

import pandas as pd

from database.db import get_connection, now_iso
from data.team_names import standardise, standardise_venue

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CRICSHEET_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "cache", "cricsheet_psl"
)

MIN_INNINGS = 3  # minimum innings to include a player

KNOWN_WICKETKEEPERS = {
    "Kamran Akmal", "Sarfaraz Ahmed", "Mohammad Rizwan",
    "Quentin de Kock", "Q de Kock",
    "Phil Salt", "Azam Khan", "Rahmanullah Gurbaz",
    "Ben Duckett", "Luke Ronchi", "Chadwick Walton",
    "Zeeshan Ashraf",
}

NON_BOWLER_WICKET_TYPES = frozenset({
    "run out", "retired hurt", "retired out", "obstructing the field",
})


# ---------------------------------------------------------------------------
# Internal: load all CSV data once
# ---------------------------------------------------------------------------

_cached_all_data = None


def _load_all_csvs():
    """Load and concatenate all ball-by-ball CSVs. Cached after first call."""
    global _cached_all_data
    if _cached_all_data is not None:
        return _cached_all_data

    csv_pattern = os.path.join(CRICSHEET_DIR, "*.csv")
    all_files = sorted(glob.glob(csv_pattern))
    data_files = [f for f in all_files if "_info" not in os.path.basename(f)]

    print(f"[player_squads] Found {len(data_files)} ball-by-ball CSV files")
    if not data_files:
        print("[player_squads] WARNING: No CSV files found in", CRICSHEET_DIR)
        return None

    frames = []
    for i, fpath in enumerate(data_files):
        try:
            frames.append(pd.read_csv(fpath, low_memory=False))
        except Exception as e:
            print(f"  [WARN] Failed to read {os.path.basename(fpath)}: {e}")
        if (i + 1) % 100 == 0:
            print(f"  ... read {i + 1}/{len(data_files)} files")

    print(f"[player_squads] Concatenating {len(frames)} DataFrames ...")
    all_data = pd.concat(frames, ignore_index=True)
    print(f"[player_squads] Total deliveries: {len(all_data):,}")

    # Ensure numeric columns
    for col in ["runs_off_bat", "extras", "wides", "noballs", "byes", "legbyes", "penalty"]:
        all_data[col] = pd.to_numeric(all_data[col], errors="coerce").fillna(0).astype(int)

    # Derived columns
    all_data["over"] = all_data["ball"].apply(lambda b: int(float(b)))
    all_data["is_powerplay"] = all_data["over"].between(0, 5)
    all_data["is_death"] = all_data["over"].between(16, 19)
    all_data["is_wide"] = all_data["wides"] > 0
    all_data["is_noball"] = all_data["noballs"] > 0
    all_data["match_innings"] = all_data["match_id"].astype(str) + "_" + all_data["innings"].astype(str)
    all_data["match_id_str"] = all_data["match_id"].astype(str)
    # Runs conceded to bowler per delivery
    all_data["bowl_conceded"] = all_data["runs_off_bat"] + all_data["wides"] + all_data["noballs"]
    # Dot ball: no runs at all
    all_data["is_dot"] = (all_data["runs_off_bat"] == 0) & (all_data["extras"] == 0)
    # Bowler-credited wicket
    all_data["is_bowler_wicket"] = (
        all_data["wicket_type"].notna()
        & ~all_data["wicket_type"].isin(NON_BOWLER_WICKET_TYPES)
    )

    _cached_all_data = all_data
    return all_data


# ---------------------------------------------------------------------------
# 1. Extract player stats from CricSheet CSVs
# ---------------------------------------------------------------------------

def extract_player_stats_from_cricsheet():
    """
    Parse ALL ball-by-ball CSVs and build player profiles.

    Returns dict keyed by player name with full batting/bowling/phase stats.
    """
    all_data = _load_all_csvs()
    if all_data is None:
        return {}

    players = {}

    # ========== BATTING STATS ==========
    print("[player_squads] Computing batting stats ...")

    # Filter to deliveries faced by batsman (exclude wides)
    bat_df = all_data[~all_data["is_wide"]].copy()

    # --- Aggregated batting per player ---
    bat_agg = bat_df.groupby("striker").agg(
        runs_scored=("runs_off_bat", "sum"),
        balls_faced=("runs_off_bat", "count"),
        fours=("runs_off_bat", lambda x: (x == 4).sum()),
        sixes=("runs_off_bat", lambda x: (x == 6).sum()),
        dot_balls_faced=("is_dot", "sum"),
    ).reset_index()

    # Innings batted: distinct match_innings per striker
    bat_innings = bat_df.groupby("striker")["match_innings"].nunique().reset_index()
    bat_innings.columns = ["striker", "innings_batted"]

    # Matches played (batting side)
    bat_matches = bat_df.groupby("striker")["match_id_str"].nunique().reset_index()
    bat_matches.columns = ["striker", "bat_matches"]

    # Team: most recent batting_team per player
    bat_team = bat_df.sort_values("start_date").groupby("striker").agg(
        team=("batting_team", "last"),
        latest_date=("start_date", "last"),
    ).reset_index()

    # Merge batting aggregates
    bat_stats = bat_agg.merge(bat_innings, on="striker", how="left")
    bat_stats = bat_stats.merge(bat_matches, on="striker", how="left")
    bat_stats = bat_stats.merge(bat_team, on="striker", how="left")

    # --- Per-innings runs for fifties/hundreds ---
    innings_runs = bat_df.groupby(["striker", "match_innings"])["runs_off_bat"].sum().reset_index()
    innings_runs.columns = ["striker", "match_innings", "inns_runs"]
    fifties = innings_runs[(innings_runs["inns_runs"] >= 50) & (innings_runs["inns_runs"] < 100)].groupby("striker").size().reset_index(name="fifties")
    hundreds = innings_runs[innings_runs["inns_runs"] >= 100].groupby("striker").size().reset_index(name="hundreds")

    bat_stats = bat_stats.merge(fifties, on="striker", how="left")
    bat_stats = bat_stats.merge(hundreds, on="striker", how="left")
    bat_stats["fifties"] = bat_stats["fifties"].fillna(0).astype(int)
    bat_stats["hundreds"] = bat_stats["hundreds"].fillna(0).astype(int)

    # --- Phase batting ---
    pp_bat = bat_df[bat_df["is_powerplay"]].groupby("striker").agg(
        powerplay_runs=("runs_off_bat", "sum"),
        powerplay_balls=("runs_off_bat", "count"),
    ).reset_index()

    death_bat = bat_df[bat_df["is_death"]].groupby("striker").agg(
        death_runs=("runs_off_bat", "sum"),
        death_balls=("runs_off_bat", "count"),
    ).reset_index()

    bat_stats = bat_stats.merge(pp_bat, on="striker", how="left")
    bat_stats = bat_stats.merge(death_bat, on="striker", how="left")
    for col in ["powerplay_runs", "powerplay_balls", "death_runs", "death_balls"]:
        bat_stats[col] = bat_stats[col].fillna(0).astype(int)

    # --- Dismissals ---
    print("[player_squads] Computing dismissals ...")
    dismissed_df = all_data[all_data["player_dismissed"].notna()]
    dismissals = dismissed_df.groupby("player_dismissed").size().reset_index(name="dismissals")
    dismissals.columns = ["striker", "dismissals"]
    bat_stats = bat_stats.merge(dismissals, on="striker", how="left")
    bat_stats["dismissals"] = bat_stats["dismissals"].fillna(0).astype(int)

    # ========== BOWLING STATS ==========
    print("[player_squads] Computing bowling stats ...")

    # Legal deliveries for bowling (exclude wides and noballs)
    legal_bowl = all_data[~all_data["is_wide"] & ~all_data["is_noball"]]

    bowl_legal_agg = legal_bowl.groupby("bowler").agg(
        balls_bowled=("bowler", "count"),
        dot_balls_bowled=("is_dot", "sum"),
    ).reset_index()

    # All deliveries for runs conceded (including wides/noballs)
    bowl_all_agg = all_data.groupby("bowler").agg(
        runs_conceded=("bowl_conceded", "sum"),
    ).reset_index()

    # Wickets
    wicket_df = all_data[all_data["is_bowler_wicket"]]
    wickets_agg = wicket_df.groupby("bowler").size().reset_index(name="wickets_taken")

    # Innings bowled
    bowl_innings = all_data.groupby("bowler")["match_innings"].nunique().reset_index()
    bowl_innings.columns = ["bowler", "innings_bowled"]

    # Matches (bowling side)
    bowl_matches = all_data.groupby("bowler")["match_id_str"].nunique().reset_index()
    bowl_matches.columns = ["bowler", "bowl_matches"]

    # Team: most recent bowling_team
    bowl_team = all_data.sort_values("start_date").groupby("bowler").agg(
        bowl_team=("bowling_team", "last"),
        bowl_latest_date=("start_date", "last"),
    ).reset_index()

    # Three-wicket hauls
    wickets_per_innings = wicket_df.groupby(["bowler", "match_innings"]).size().reset_index(name="inns_wkts")
    three_wkt = wickets_per_innings[wickets_per_innings["inns_wkts"] >= 3].groupby("bowler").size().reset_index(name="three_wicket_hauls")

    # Phase bowling (legal deliveries only for balls, all for runs)
    pp_bowl_legal = legal_bowl[legal_bowl["is_powerplay"]].groupby("bowler")["bowler"].count().reset_index(name="pp_bowl_balls")
    pp_bowl_runs = all_data[all_data["is_powerplay"]].groupby("bowler")["bowl_conceded"].sum().reset_index()
    pp_bowl_runs.columns = ["bowler", "pp_bowl_runs"]

    death_bowl_legal = legal_bowl[legal_bowl["is_death"]].groupby("bowler")["bowler"].count().reset_index(name="death_bowl_balls")
    death_bowl_runs = all_data[all_data["is_death"]].groupby("bowler")["bowl_conceded"].sum().reset_index()
    death_bowl_runs.columns = ["bowler", "death_bowl_runs"]

    # Merge bowling
    bowl_stats = bowl_legal_agg.merge(bowl_all_agg, on="bowler", how="left")
    bowl_stats = bowl_stats.merge(wickets_agg, on="bowler", how="left")
    bowl_stats = bowl_stats.merge(bowl_innings, on="bowler", how="left")
    bowl_stats = bowl_stats.merge(bowl_matches, on="bowler", how="left")
    bowl_stats = bowl_stats.merge(bowl_team, on="bowler", how="left")
    bowl_stats = bowl_stats.merge(three_wkt, on="bowler", how="left")
    bowl_stats = bowl_stats.merge(pp_bowl_legal, on="bowler", how="left")
    bowl_stats = bowl_stats.merge(pp_bowl_runs, on="bowler", how="left")
    bowl_stats = bowl_stats.merge(death_bowl_legal, on="bowler", how="left")
    bowl_stats = bowl_stats.merge(death_bowl_runs, on="bowler", how="left")

    for col in ["wickets_taken", "three_wicket_hauls", "innings_bowled", "bowl_matches",
                "pp_bowl_balls", "pp_bowl_runs", "death_bowl_balls", "death_bowl_runs"]:
        if col in bowl_stats.columns:
            bowl_stats[col] = bowl_stats[col].fillna(0).astype(int)

    # ========== MERGE & BUILD PLAYER DICT ==========
    print("[player_squads] Building player profiles ...")

    # Index bat_stats by striker name
    bat_lookup = {}
    for _, row in bat_stats.iterrows():
        bat_lookup[row["striker"]] = row

    bowl_lookup = {}
    for _, row in bowl_stats.iterrows():
        bowl_lookup[row["bowler"]] = row

    all_names = set(bat_lookup.keys()) | set(bowl_lookup.keys())

    for name in all_names:
        br = bat_lookup.get(name)
        bw = bowl_lookup.get(name)

        innings_batted = int(br["innings_batted"]) if br is not None else 0
        innings_bowled = int(bw["innings_bowled"]) if bw is not None else 0

        # Filter by minimum innings
        if innings_batted < MIN_INNINGS and innings_bowled < MIN_INNINGS:
            continue

        # Determine team (most recent appearance)
        bat_date = str(br["latest_date"]) if br is not None and pd.notna(br.get("latest_date")) else ""
        bowl_date = str(bw["bowl_latest_date"]) if bw is not None and pd.notna(bw.get("bowl_latest_date")) else ""
        if bat_date >= bowl_date and br is not None:
            team = str(br["team"])
        elif bw is not None:
            team = str(bw["bowl_team"])
        else:
            team = "Unknown"

        # Matches played (union would be ideal, but sum is close enough and fast)
        bat_m = int(br["bat_matches"]) if br is not None else 0
        bowl_m = int(bw["bowl_matches"]) if bw is not None else 0
        matches_played = max(bat_m, bowl_m)

        # Batting stats
        runs = int(br["runs_scored"]) if br is not None else 0
        bf = int(br["balls_faced"]) if br is not None else 0
        fours = int(br["fours"]) if br is not None else 0
        sixes = int(br["sixes"]) if br is not None else 0
        dismissals = int(br["dismissals"]) if br is not None else 0
        dot_bf = int(br["dot_balls_faced"]) if br is not None else 0
        fifty = int(br["fifties"]) if br is not None else 0
        hundred = int(br["hundreds"]) if br is not None else 0
        pp_runs = int(br["powerplay_runs"]) if br is not None else 0
        pp_balls = int(br["powerplay_balls"]) if br is not None else 0
        d_runs = int(br["death_runs"]) if br is not None else 0
        d_balls = int(br["death_balls"]) if br is not None else 0

        batting_avg = runs / dismissals if dismissals > 0 else float(runs) if runs > 0 else 0.0
        batting_sr = (runs / bf) * 100 if bf > 0 else 0.0
        boundary_pct = ((fours + sixes) / bf) * 100 if bf > 0 else 0.0
        powerplay_sr = (pp_runs / pp_balls) * 100 if pp_balls > 0 else 0.0
        death_bat_sr = (d_runs / d_balls) * 100 if d_balls > 0 else 0.0

        # Bowling stats
        bb = int(bw["balls_bowled"]) if bw is not None else 0
        rc = int(bw["runs_conceded"]) if bw is not None else 0
        wk = int(bw["wickets_taken"]) if bw is not None else 0
        dot_bb = int(bw["dot_balls_bowled"]) if bw is not None else 0
        three_wh = int(bw["three_wicket_hauls"]) if bw is not None else 0
        ppb_balls = int(bw["pp_bowl_balls"]) if bw is not None else 0
        ppb_runs = int(bw["pp_bowl_runs"]) if bw is not None else 0
        db_balls = int(bw["death_bowl_balls"]) if bw is not None else 0
        db_runs = int(bw["death_bowl_runs"]) if bw is not None else 0

        bowling_avg = rc / wk if wk > 0 else 0.0
        bowling_economy = rc / (bb / 6) if bb > 0 else 0.0
        bowling_sr = bb / wk if wk > 0 else 0.0
        dot_ball_pct = (dot_bb / bb) * 100 if bb > 0 else 0.0
        pp_economy = ppb_runs / (ppb_balls / 6) if ppb_balls > 0 else 0.0
        death_economy = db_runs / (db_balls / 6) if db_balls > 0 else 0.0

        role = _determine_role(name, innings_batted, innings_bowled)

        players[name] = {
            "name": name,
            "team": team,
            "role": role,
            "matches_played": matches_played,
            "innings_batted": innings_batted,
            "innings_bowled": innings_bowled,
            # batting
            "runs_scored": runs,
            "balls_faced": bf,
            "fours": fours,
            "sixes": sixes,
            "dot_balls_faced": dot_bf,
            "dismissals": dismissals,
            "batting_avg": round(batting_avg, 2),
            "batting_sr": round(batting_sr, 2),
            "boundary_pct": round(boundary_pct, 2),
            "fifties": fifty,
            "hundreds": hundred,
            "powerplay_sr": round(powerplay_sr, 2),
            "death_sr": round(death_bat_sr, 2),
            # bowling
            "balls_bowled": bb,
            "runs_conceded": rc,
            "wickets_taken": wk,
            "bowling_avg": round(bowling_avg, 2),
            "bowling_economy": round(bowling_economy, 2),
            "bowling_sr": round(bowling_sr, 2),
            "dot_balls_bowled": dot_bb,
            "dot_ball_pct": round(dot_ball_pct, 2),
            "three_wicket_hauls": three_wh,
            "powerplay_economy": round(pp_economy, 2),
            "death_economy": round(death_economy, 2),
        }

    print(f"[player_squads] Extracted stats for {len(players)} players (min {MIN_INNINGS} innings)")
    return players


def _determine_role(name, innings_batted, innings_bowled):
    """Determine player role from stats and known WK list."""
    for wk_name in KNOWN_WICKETKEEPERS:
        if wk_name.lower() in name.lower() or name.lower() in wk_name.lower():
            return "wicket-keeper"

    if innings_bowled == 0 and innings_batted > 0:
        return "batsman"
    if innings_batted == 0 and innings_bowled > 0:
        return "bowler"
    if innings_batted > innings_bowled * 3:
        return "batsman"
    if innings_bowled > innings_batted * 2:
        return "bowler"
    return "all-rounder"


# ---------------------------------------------------------------------------
# 2. Calculate player impact score
# ---------------------------------------------------------------------------

def calculate_player_impact(player_dict):
    """
    Calculate impact_score (0-100) from player stats dict.

    Batting component (max 50):
        batting_avg weight 0.4, batting_sr weight 0.6
        35+ avg and 140+ SR = max 50

    Bowling component (max 50):
        bowling_avg weight 0.5 (lower better), economy weight 0.5 (lower better)
        avg < 20 and econ < 7 = max 50

    Scaled by role:
        batsman: full batting + 20% bowling
        bowler: 20% batting + full bowling
        all-rounder / wicket-keeper: full both

    Volume bonus: up to 5 points for 50+ matches.
    """
    bat_avg = player_dict.get("batting_avg", 0) or 0
    bat_sr = player_dict.get("batting_sr", 0) or 0
    bowl_avg = player_dict.get("bowling_avg", 0) or 0
    bowl_econ = player_dict.get("bowling_economy", 0) or 0
    matches = player_dict.get("matches_played", 0) or 0
    role = player_dict.get("role", "batsman")

    # Batting component (0-50)
    avg_norm = min(bat_avg / 35.0, 1.0)
    sr_norm = min(max(bat_sr - 100, 0) / 40.0, 1.0)  # 140+ = max
    bat_score = (avg_norm * 0.4 + sr_norm * 0.6) * 50

    # Bowling component (0-50)
    if bowl_avg > 0:
        avg_bowl_norm = min(max(1.0 - (bowl_avg - 20) / 30.0, 0), 1.0)
    else:
        avg_bowl_norm = 0

    if bowl_econ > 0:
        econ_norm = min(max(1.0 - (bowl_econ - 7) / 5.0, 0), 1.0)
    else:
        econ_norm = 0

    bowl_score = (avg_bowl_norm * 0.5 + econ_norm * 0.5) * 50

    # Scale by role
    if role == "batsman":
        total = bat_score + bowl_score * 0.2
    elif role == "bowler":
        total = bat_score * 0.2 + bowl_score
    else:
        total = bat_score + bowl_score

    # Volume bonus (up to 5 points)
    volume_bonus = min(matches / 50.0, 1.0) * 5

    impact = min(total + volume_bonus, 100.0)
    return round(impact, 1)


# ---------------------------------------------------------------------------
# 3. Seed player stats to DB
# ---------------------------------------------------------------------------

def seed_player_stats():
    """
    Extract stats from CricSheet CSVs, calculate impact scores,
    and upsert ALL players into player_stats table.
    """
    player_data = extract_player_stats_from_cricsheet()
    if not player_data:
        print("[player_squads] No player data extracted, aborting seed.")
        return

    conn = get_connection()
    timestamp = now_iso()
    count = 0

    for name, p in player_data.items():
        team = standardise(p["team"])
        impact = calculate_player_impact(p)

        try:
            conn.execute("""
                INSERT INTO player_stats (
                    name, team, role,
                    batting_avg, batting_sr, bowling_avg, bowling_economy, bowling_sr,
                    matches_played, innings_batted, innings_bowled,
                    runs_scored, wickets_taken, fifties, hundreds,
                    three_wicket_hauls, powerplay_sr, death_sr,
                    powerplay_economy, death_economy,
                    dot_ball_pct, boundary_pct,
                    impact_score, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name, team) DO UPDATE SET
                    role = excluded.role,
                    batting_avg = excluded.batting_avg,
                    batting_sr = excluded.batting_sr,
                    bowling_avg = excluded.bowling_avg,
                    bowling_economy = excluded.bowling_economy,
                    bowling_sr = excluded.bowling_sr,
                    matches_played = excluded.matches_played,
                    innings_batted = excluded.innings_batted,
                    innings_bowled = excluded.innings_bowled,
                    runs_scored = excluded.runs_scored,
                    wickets_taken = excluded.wickets_taken,
                    fifties = excluded.fifties,
                    hundreds = excluded.hundreds,
                    three_wicket_hauls = excluded.three_wicket_hauls,
                    powerplay_sr = excluded.powerplay_sr,
                    death_sr = excluded.death_sr,
                    powerplay_economy = excluded.powerplay_economy,
                    death_economy = excluded.death_economy,
                    dot_ball_pct = excluded.dot_ball_pct,
                    boundary_pct = excluded.boundary_pct,
                    impact_score = excluded.impact_score,
                    updated_at = excluded.updated_at
            """, (
                name, team, p["role"],
                p["batting_avg"], p["batting_sr"],
                p["bowling_avg"] if p["bowling_avg"] > 0 else None,
                p["bowling_economy"] if p["bowling_economy"] > 0 else None,
                p["bowling_sr"] if p["bowling_sr"] > 0 else None,
                p["matches_played"], p["innings_batted"], p["innings_bowled"],
                p["runs_scored"], p["wickets_taken"],
                p["fifties"], p["hundreds"],
                p["three_wicket_hauls"],
                p["powerplay_sr"] if p["powerplay_sr"] > 0 else None,
                p["death_sr"] if p["death_sr"] > 0 else None,
                p["powerplay_economy"] if p["powerplay_economy"] > 0 else None,
                p["death_economy"] if p["death_economy"] > 0 else None,
                p["dot_ball_pct"], p["boundary_pct"],
                impact, timestamp,
            ))
            count += 1
        except Exception as e:
            print(f"  [WARN] Failed to upsert {name}: {e}")

    conn.commit()
    print(f"[player_squads] Seeded {count} players into player_stats table")
    return count


# ---------------------------------------------------------------------------
# 4. Get team players from DB
# ---------------------------------------------------------------------------

def get_team_players(team_name):
    """Fetch all players for a team from the player_stats table."""
    team = standardise(team_name)
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM player_stats WHERE team = ? ORDER BY impact_score DESC",
        (team,)
    ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# 5. Player venue stats (from CSV)
# ---------------------------------------------------------------------------

def get_player_venue_stats(player_name, venue):
    """
    Extract player performance at a specific venue from CSV data.
    Returns dict with batting_avg, batting_sr, bowling_economy at that venue.
    """
    all_data = _load_all_csvs()
    if all_data is None:
        return {"batting_avg": 0, "batting_sr": 0, "bowling_economy": 0, "matches": 0}

    std_venue = standardise_venue(venue)
    venue_mask = all_data["venue"].apply(
        lambda v: standardise_venue(str(v)) == std_venue if pd.notna(v) else False
    )
    venue_data = all_data[venue_mask]

    if venue_data.empty:
        return {"batting_avg": 0, "batting_sr": 0, "bowling_economy": 0, "matches": 0}

    # Batting at venue
    bat_rows = venue_data[(venue_data["striker"] == player_name) & (~venue_data["is_wide"])]
    runs = int(bat_rows["runs_off_bat"].sum()) if not bat_rows.empty else 0
    balls = len(bat_rows)
    dismissed = len(venue_data[venue_data["player_dismissed"] == player_name])

    bat_avg = runs / dismissed if dismissed > 0 else float(runs)
    bat_sr = (runs / balls) * 100 if balls > 0 else 0.0

    # Bowling at venue
    bowl_rows = venue_data[venue_data["bowler"] == player_name]
    if not bowl_rows.empty:
        bowl_conceded = int(bowl_rows["bowl_conceded"].sum())
        legal = len(bowl_rows[~bowl_rows["is_wide"] & ~bowl_rows["is_noball"]])
        bowl_econ = bowl_conceded / (legal / 6) if legal > 0 else 0.0
    else:
        bowl_econ = 0.0

    matches = venue_data[
        (venue_data["striker"] == player_name) | (venue_data["bowler"] == player_name)
    ]["match_id"].nunique()

    return {
        "batting_avg": round(bat_avg, 2),
        "batting_sr": round(bat_sr, 2),
        "bowling_economy": round(bowl_econ, 2),
        "matches": int(matches),
    }


# ---------------------------------------------------------------------------
# 6. Player vs team stats (from CSV)
# ---------------------------------------------------------------------------

def get_player_vs_team(player_name, opponent_team):
    """
    Extract player performance against a specific team from CSV data.
    Returns dict with batting_avg, batting_sr, bowling_economy vs that team.
    """
    all_data = _load_all_csvs()
    if all_data is None:
        return {"batting_avg": 0, "batting_sr": 0, "bowling_economy": 0, "wickets": 0, "matches": 0}

    std_opp = standardise(opponent_team)

    # Pre-compute standardised team columns for efficiency
    opp_bowling = all_data["bowling_team"].apply(
        lambda t: standardise(str(t)) == std_opp if pd.notna(t) else False
    )
    opp_batting = all_data["batting_team"].apply(
        lambda t: standardise(str(t)) == std_opp if pd.notna(t) else False
    )

    # Batting against opponent (opponent is bowling_team)
    bat_mask = (all_data["striker"] == player_name) & opp_bowling & (~all_data["is_wide"])
    bat_rows = all_data[bat_mask]
    runs = int(bat_rows["runs_off_bat"].sum()) if not bat_rows.empty else 0
    balls = len(bat_rows)

    # Dismissals vs opponent
    dismissed = len(all_data[(all_data["player_dismissed"] == player_name) & opp_bowling])

    bat_avg = runs / dismissed if dismissed > 0 else float(runs)
    bat_sr = (runs / balls) * 100 if balls > 0 else 0.0

    # Bowling against opponent (opponent is batting_team)
    bowl_mask = (all_data["bowler"] == player_name) & opp_batting
    bowl_rows = all_data[bowl_mask]

    if not bowl_rows.empty:
        conceded = int(bowl_rows["bowl_conceded"].sum())
        legal = len(bowl_rows[~bowl_rows["is_wide"] & ~bowl_rows["is_noball"]])
        bowl_econ = conceded / (legal / 6) if legal > 0 else 0.0
        wickets = int(bowl_rows["is_bowler_wicket"].sum())
    else:
        bowl_econ = 0.0
        wickets = 0

    matches = all_data[
        ((all_data["striker"] == player_name) & opp_bowling)
        | ((all_data["bowler"] == player_name) & opp_batting)
    ]["match_id"].nunique()

    return {
        "batting_avg": round(bat_avg, 2),
        "batting_sr": round(bat_sr, 2),
        "bowling_economy": round(bowl_econ, 2),
        "wickets": wickets,
        "matches": int(matches),
    }


# ---------------------------------------------------------------------------
# Convenience aliases (backward compat)
# ---------------------------------------------------------------------------

def get_squads():
    """Return all players grouped by team from DB."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM player_stats ORDER BY team, impact_score DESC"
    ).fetchall()
    squads = defaultdict(list)
    for r in rows:
        squads[dict(r)["team"]].append(dict(r))
    return dict(squads)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 60)
    print("PSL Player Stats — CricSheet Extraction")
    print("=" * 60)

    data = seed_player_stats()

    if data:
        # Print top 20 by impact
        scored = []
        for name, p in data.items():
            impact = calculate_player_impact(p)
            scored.append((name, p["team"], p["role"], impact,
                           p["matches_played"], p["batting_avg"], p["batting_sr"],
                           p["bowling_economy"], p["wickets_taken"]))

        scored.sort(key=lambda x: x[3], reverse=True)

        print(f"\nTop 20 players by impact score:")
        print(f"{'Name':<25} {'Team':<22} {'Role':<15} {'Impact':>7} {'Mat':>4} "
              f"{'BatAvg':>7} {'BatSR':>7} {'Econ':>6} {'Wkt':>4}")
        print("-" * 120)

        for name, team, role, impact, mat, ba, bsr, econ, wkt in scored[:20]:
            econ_str = f"{econ:.1f}" if econ > 0 else "-"
            print(f"{name:<25} {team:<22} {role:<15} {impact:>7.1f} {mat:>4} "
                  f"{ba:>7.1f} {bsr:>7.1f} {econ_str:>6} {wkt:>4}")

        print(f"\nTotal players seeded: {len(data)}")
    else:
        print("No data extracted.")
