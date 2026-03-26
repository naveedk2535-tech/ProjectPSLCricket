"""
Batting/Bowling Strength Model — replaces Poisson for cricket.
Calculates team strengths based on batting/bowling averages relative to league average.
Projects innings totals and converts to win probabilities.
"""

import math
from database import db
import config


def calculate_team_strengths(league="psl"):
    """Calculate batting and bowling strength indices for all teams."""
    matches = db.fetch_all(
        "SELECT * FROM matches WHERE innings1_runs IS NOT NULL AND innings2_runs IS NOT NULL AND league = ? ORDER BY match_date",
        [league]
    )

    if not matches:
        return {}

    # League averages
    all_first = [m["innings1_runs"] for m in matches if m["innings1_runs"]]
    all_second = [m["innings2_runs"] for m in matches if m["innings2_runs"]]
    league_avg_first = sum(all_first) / len(all_first) if all_first else 165
    league_avg_second = sum(all_second) / len(all_second) if all_second else 155
    league_avg = (league_avg_first + league_avg_second) / 2

    # Per-team stats
    team_stats = {}
    for m in matches:
        for team, role_bat, role_bowl in [
            (m["team_a"], "innings1", "innings2"),
            (m["team_b"], "innings2", "innings1")
        ]:
            if team not in team_stats:
                team_stats[team] = {
                    "bat_runs": [], "bowl_runs_conceded": [],
                    "pp_runs": [], "pp_conceded": [],
                    "death_runs": [], "death_conceded": [],
                    "wides": [], "noballs": [],
                }

            bat_runs = m[f"{role_bat}_runs"]
            bowl_conceded = m[f"{role_bowl}_runs"]

            if bat_runs is not None:
                team_stats[team]["bat_runs"].append(bat_runs)
            if bowl_conceded is not None:
                team_stats[team]["bowl_runs_conceded"].append(bowl_conceded)

            # Phase stats
            pp_key = "powerplay_runs_a" if role_bat == "innings1" else "powerplay_runs_b"
            death_key = "death_runs_a" if role_bat == "innings1" else "death_runs_b"
            pp_conceded_key = "powerplay_runs_a" if role_bowl == "innings1" else "powerplay_runs_b"
            death_conceded_key = "death_runs_a" if role_bowl == "innings1" else "death_runs_b"

            if m.get(pp_key):
                team_stats[team]["pp_runs"].append(m[pp_key])
            if m.get(death_key):
                team_stats[team]["death_runs"].append(m[death_key])
            if m.get(pp_conceded_key):
                team_stats[team]["pp_conceded"].append(m[pp_conceded_key])
            if m.get(death_conceded_key):
                team_stats[team]["death_conceded"].append(m[death_conceded_key])

            # Extras
            wide_key = "total_wides_a" if role_bowl == "innings1" else "total_wides_b"
            nb_key = "total_noballs_a" if role_bowl == "innings1" else "total_noballs_b"
            if m.get(wide_key) is not None:
                team_stats[team]["wides"].append(m[wide_key])
            if m.get(nb_key) is not None:
                team_stats[team]["noballs"].append(m[nb_key])

    strengths = {}
    league_pp_avg = 45  # Default powerplay average
    league_death_avg = 50  # Default death overs average

    for team, stats in team_stats.items():
        batting_avg = sum(stats["bat_runs"]) / len(stats["bat_runs"]) if stats["bat_runs"] else league_avg
        bowling_avg = sum(stats["bowl_runs_conceded"]) / len(stats["bowl_runs_conceded"]) if stats["bowl_runs_conceded"] else league_avg
        pp_rate = sum(stats["pp_runs"]) / len(stats["pp_runs"]) if stats["pp_runs"] else league_pp_avg
        death_rate = sum(stats["death_runs"]) / len(stats["death_runs"]) if stats["death_runs"] else league_death_avg
        pp_economy = sum(stats["pp_conceded"]) / len(stats["pp_conceded"]) if stats["pp_conceded"] else league_pp_avg
        death_economy = sum(stats["death_conceded"]) / len(stats["death_conceded"]) if stats["death_conceded"] else league_death_avg

        # Recency weighting: last 10 matches count double
        recent_bat = stats["bat_runs"][-10:] if len(stats["bat_runs"]) > 10 else stats["bat_runs"]
        recent_bowl = stats["bowl_runs_conceded"][-10:] if len(stats["bowl_runs_conceded"]) > 10 else stats["bowl_runs_conceded"]
        recent_bat_avg = sum(recent_bat) / len(recent_bat) if recent_bat else batting_avg
        recent_bowl_avg = sum(recent_bowl) / len(recent_bowl) if recent_bowl else bowling_avg

        # Blend historical and recent (60% recent, 40% all-time)
        batting_avg = 0.6 * recent_bat_avg + 0.4 * batting_avg
        bowling_avg = 0.6 * recent_bowl_avg + 0.4 * bowling_avg

        strengths[team] = {
            "batting_strength": batting_avg / league_avg if league_avg > 0 else 1.0,
            "bowling_strength": bowling_avg / league_avg if league_avg > 0 else 1.0,
            "batting_avg": batting_avg,
            "bowling_avg": bowling_avg,
            "powerplay_attack": pp_rate / league_pp_avg if league_pp_avg > 0 else 1.0,
            "death_bowling": death_economy / league_death_avg if league_death_avg > 0 else 1.0,
            "powerplay_run_rate": pp_rate / 6,  # Per over
            "death_overs_economy": death_economy / 4,  # Per over
            "avg_wides": sum(stats["wides"]) / len(stats["wides"]) if stats["wides"] else 4.0,
            "avg_noballs": sum(stats["noballs"]) / len(stats["noballs"]) if stats["noballs"] else 1.0,
            "matches": len(stats["bat_runs"]),
        }

    return strengths


def predict(team_a, team_b, venue=None, league="psl"):
    """
    Predict match outcome using batting/bowling strengths.

    Returns: {team_a_win, team_b_win, predicted_total_a, predicted_total_b, confidence, details}
    """
    strengths = calculate_team_strengths(league=league)

    # Default strengths for new/unknown teams
    default = {"batting_strength": 1.0, "bowling_strength": 1.0,
               "batting_avg": 160, "bowling_avg": 160,
               "powerplay_attack": 1.0, "death_bowling": 1.0, "matches": 0}

    s_a = strengths.get(team_a, default)
    s_b = strengths.get(team_b, default)

    # Get venue stats
    venue_avg_first = 170  # Default
    venue_avg_second = 160
    if venue:
        v_stats = db.fetch_one("SELECT * FROM venue_stats WHERE venue = ? AND league = ?", [venue, league])
        if v_stats:
            venue_avg_first = v_stats["avg_first_innings"] or 170
            venue_avg_second = v_stats["avg_second_innings"] or 160
        elif venue in config.VENUES:
            venue_avg_first = config.VENUES[venue]["avg_first_innings"]
            venue_avg_second = venue_avg_first - 10

    # Project innings totals
    # Team A batting first: their batting strength vs B's bowling
    total_a = venue_avg_first * s_a["batting_strength"] / max(0.5, s_b["bowling_strength"])
    # Team B chasing: their batting strength vs A's bowling
    total_b = venue_avg_second * s_b["batting_strength"] / max(0.5, s_a["bowling_strength"])

    # Clamp to reasonable T20 range
    total_a = max(80, min(250, total_a))
    total_b = max(80, min(250, total_b))

    # Convert run difference to win probability using logistic function
    run_diff = total_a - total_b
    # Empirically, each 10-run advantage ≈ 6% win probability shift
    team_a_win = 1 / (1 + math.exp(-run_diff / 16))
    team_b_win = 1 - team_a_win

    # Confidence based on data availability
    min_matches = min(s_a["matches"], s_b["matches"])
    confidence = min(0.9, 0.4 + min_matches * 0.02)

    return {
        "team_a_win": round(team_a_win, 4),
        "team_b_win": round(team_b_win, 4),
        "predicted_total_a": round(total_a, 1),
        "predicted_total_b": round(total_b, 1),
        "confidence": round(confidence, 3),
        "details": {
            "model": "batting_bowling",
            "team_a_batting_strength": round(s_a["batting_strength"], 3),
            "team_a_bowling_strength": round(s_a["bowling_strength"], 3),
            "team_b_batting_strength": round(s_b["batting_strength"], 3),
            "team_b_bowling_strength": round(s_b["bowling_strength"], 3),
            "venue_avg_first": venue_avg_first,
            "venue_avg_second": venue_avg_second,
            "run_diff": round(run_diff, 1),
        }
    }


def save_ratings(strengths, league="psl"):
    """Save team strengths to team_ratings table."""
    for team, s in strengths.items():
        db.execute(
            """INSERT INTO team_ratings (team, league, batting_avg, bowling_avg, batting_sr, bowling_economy,
               powerplay_run_rate, death_overs_economy, boundary_pct, dot_ball_pct,
               extras_conceded_avg, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(team, league) DO UPDATE SET
               batting_avg=excluded.batting_avg, bowling_avg=excluded.bowling_avg,
               powerplay_run_rate=excluded.powerplay_run_rate,
               death_overs_economy=excluded.death_overs_economy,
               extras_conceded_avg=excluded.extras_conceded_avg,
               updated_at=excluded.updated_at""",
            [team, league, s["batting_avg"], s["bowling_avg"],
             s.get("batting_sr"), s.get("death_overs_economy"),
             s["powerplay_run_rate"], s["death_overs_economy"],
             None, None, s["avg_wides"] + s["avg_noballs"],
             db.now_iso()]
        )
