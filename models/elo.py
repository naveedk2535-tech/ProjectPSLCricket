"""
Elo Rating System for PSL Cricket.
Higher K-factor (32 vs soccer's 20) because T20 is more volatile.
New teams (Hyderabad, Rawalpindi) start at 1450 with boosted K=48.
"""

import math
from database import db
import config
from data.team_names import standardise


def build_ratings(league="psl"):
    """
    Build Elo ratings from all historical matches (chronological).
    Returns dict of {team: {elo, form_last5, streak_type, streak_length, ...}}
    """
    settings = config.ELO_SETTINGS
    matches = db.fetch_all(
        "SELECT * FROM matches WHERE winner IS NOT NULL AND league = ? ORDER BY match_date ASC",
        [league]
    )

    ratings = {}
    form_history = {}  # {team: [list of W/L results]}
    current_season = None

    for m in matches:
        team_a = m["team_a"]
        team_b = m["team_b"]
        winner = m["winner"]
        season = m.get("season", "")

        # Season decay: reset ratings toward mean at season boundaries
        if season and season != current_season:
            if current_season is not None:
                for team in ratings:
                    ratings[team]["elo"] = settings["initial_elo"] + \
                        (ratings[team]["elo"] - settings["initial_elo"]) * settings["season_decay"]
            current_season = season

        # Initialize new teams
        for team in [team_a, team_b]:
            if team not in ratings:
                is_new = any(t["name"] == team and t.get("is_new") for t in config.TEAMS.values())
                initial = settings["new_team_elo"] if is_new else settings["initial_elo"]
                ratings[team] = {
                    "elo": initial, "elo_home": initial, "elo_away": initial,
                    "matches_played": 0, "wins": 0, "losses": 0,
                }
                form_history[team] = []

        elo_a = ratings[team_a]["elo"]
        elo_b = ratings[team_b]["elo"]

        # Expected scores
        exp_a = 1 / (1 + 10 ** ((elo_b - elo_a - settings["home_advantage"]) / 400))
        exp_b = 1 - exp_a

        # Actual scores
        if winner == team_a:
            actual_a, actual_b = 1.0, 0.0
            form_history.setdefault(team_a, []).append("W")
            form_history.setdefault(team_b, []).append("L")
            ratings[team_a]["wins"] += 1
            ratings[team_b]["losses"] += 1
        elif winner == team_b:
            actual_a, actual_b = 0.0, 1.0
            form_history.setdefault(team_a, []).append("L")
            form_history.setdefault(team_b, []).append("W")
            ratings[team_b]["wins"] += 1
            ratings[team_a]["losses"] += 1
        else:
            actual_a, actual_b = 0.5, 0.5
            form_history.setdefault(team_a, []).append("N")
            form_history.setdefault(team_b, []).append("N")

        # K-factor: higher for new teams with few matches
        k_a = settings["k_factor_new_team"] if ratings[team_a]["matches_played"] < settings["new_team_threshold"] else settings["k_factor"]
        k_b = settings["k_factor_new_team"] if ratings[team_b]["matches_played"] < settings["new_team_threshold"] else settings["k_factor"]

        # Update ratings
        ratings[team_a]["elo"] += k_a * (actual_a - exp_a)
        ratings[team_b]["elo"] += k_b * (actual_b - exp_b)
        ratings[team_a]["matches_played"] += 1
        ratings[team_b]["matches_played"] += 1

    # Calculate form and streaks
    for team in ratings:
        history = form_history.get(team, [])

        # Form last 5
        last5 = history[-5:]
        ratings[team]["form_last5"] = sum(1 for r in last5 if r == "W") / len(last5) * 100 if last5 else 50

        # Form last 10
        last10 = history[-10:]
        ratings[team]["form_last10"] = sum(1 for r in last10 if r == "W") / len(last10) * 100 if last10 else 50

        # Streak
        if history:
            streak_type = history[-1]
            streak_length = 0
            for r in reversed(history):
                if r == streak_type:
                    streak_length += 1
                else:
                    break
            ratings[team]["streak_type"] = streak_type
            ratings[team]["streak_length"] = streak_length
        else:
            ratings[team]["streak_type"] = "N"
            ratings[team]["streak_length"] = 0

        # NRR placeholder (calculated from match data separately)
        ratings[team]["nrr"] = 0.0

    return ratings


def predict(team_a, team_b, venue=None, league="psl"):
    """
    Convert Elo difference to win probability.
    No draw in T20 → 2-way market only.
    """
    ratings = build_ratings(league=league)
    settings = config.ELO_SETTINGS

    default = {"elo": settings["initial_elo"], "form_last5": 50,
               "streak_type": "N", "streak_length": 0, "matches_played": 0}

    r_a = ratings.get(team_a, default)
    r_b = ratings.get(team_b, default)

    elo_a = r_a["elo"]
    elo_b = r_b["elo"]

    # Win probability (2-way, no draw)
    team_a_win = 1 / (1 + 10 ** ((elo_b - elo_a - settings["home_advantage"]) / 400))
    team_b_win = 1 - team_a_win

    # Confidence based on matches played
    min_matches = min(r_a["matches_played"], r_b["matches_played"])
    confidence = min(0.85, 0.3 + min_matches * 0.025)

    return {
        "team_a_win": round(team_a_win, 4),
        "team_b_win": round(team_b_win, 4),
        "confidence": round(confidence, 3),
        "details": {
            "model": "elo",
            "elo_a": round(elo_a, 1),
            "elo_b": round(elo_b, 1),
            "elo_diff": round(elo_a - elo_b, 1),
            "form_a": r_a["form_last5"],
            "form_b": r_b["form_last5"],
            "streak_a": f"{r_a['streak_type']}{r_a['streak_length']}",
            "streak_b": f"{r_b['streak_type']}{r_b['streak_length']}",
            "matches_a": r_a["matches_played"],
            "matches_b": r_b["matches_played"],
        }
    }


def save_ratings(ratings=None, league="psl"):
    """Save current Elo ratings to team_ratings table."""
    if ratings is None:
        ratings = build_ratings(league=league)

    for team, r in ratings.items():
        db.execute(
            """INSERT INTO team_ratings (team, league, elo, form_last5, form_last10,
               streak_type, streak_length, matches_played, wins, losses, nrr, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(team, league) DO UPDATE SET
               elo=excluded.elo, form_last5=excluded.form_last5, form_last10=excluded.form_last10,
               streak_type=excluded.streak_type, streak_length=excluded.streak_length,
               matches_played=excluded.matches_played, wins=excluded.wins, losses=excluded.losses,
               nrr=excluded.nrr, updated_at=excluded.updated_at""",
            [team, league, r["elo"], r["form_last5"], r.get("form_last10", 50),
             r["streak_type"], r["streak_length"],
             r["matches_played"], r["wins"], r["losses"],
             r.get("nrr", 0.0), db.now_iso()]
        )
