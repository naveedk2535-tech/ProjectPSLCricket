"""
Over/Under Total Runs Model + Prop Bet Predictions.
Predicts total match runs, wides, no-balls, sixes, fours.
"""

import math
from database import db
import config
from models import batting_bowling


def predict(team_a, team_b, venue=None, match_date=None, league="psl"):
    """
    Predict over/under total runs and prop bets.
    """
    # Get base prediction from batting/bowling model
    base = batting_bowling.predict(team_a, team_b, venue, league=league)
    total_a = base["predicted_total_a"]
    total_b = base["predicted_total_b"]

    # Weather adjustment
    dew_boost = 0
    if match_date and venue:
        weather = db.fetch_one(
            "SELECT * FROM weather WHERE venue = ? AND match_date = ?",
            [venue, match_date]
        )
        if weather and weather.get("heavy_dew"):
            dew_boost = 8  # Heavy dew = more runs (harder to bowl)
        elif weather and weather.get("dew_score", 0) > 0.3:
            dew_boost = 4

    expected_total = total_a + total_b + dew_boost

    # Set line at nearest 0.5
    line = round(expected_total) + 0.5 if expected_total % 1 < 0.5 else round(expected_total) - 0.5

    # Over/under probability using normal distribution
    # Standard deviation of T20 totals is roughly 25-30 runs
    std_dev = 28
    z = (line - expected_total) / std_dev
    over_prob = 1 - _normal_cdf(z)
    under_prob = _normal_cdf(z)

    # Prop bets from team averages
    strengths = batting_bowling.calculate_team_strengths(league=league)
    s_a = strengths.get(team_a, {"avg_wides": 4, "avg_noballs": 1})
    s_b = strengths.get(team_b, {"avg_wides": 4, "avg_noballs": 1})

    # Venue averages
    v_stats = db.fetch_one("SELECT * FROM venue_stats WHERE venue = ? AND league = ?", [venue, league]) if venue else None

    total_wides = s_a.get("avg_wides", 4) + s_b.get("avg_wides", 4)
    total_noballs = s_a.get("avg_noballs", 1) + s_b.get("avg_noballs", 1)

    # Sixes and fours from venue + team data
    if v_stats:
        total_sixes = v_stats.get("avg_sixes") or 12
        total_fours = v_stats.get("avg_fours") or 24
    else:
        total_sixes = 12  # T20 average
        total_fours = 24

    return {
        "line": round(line, 1),
        "over_prob": round(over_prob, 4),
        "under_prob": round(under_prob, 4),
        "expected_total": round(expected_total, 1),
        "predicted_total_a": round(total_a, 1),
        "predicted_total_b": round(total_b, 1),
        "prop_bets": {
            "total_wides": round(total_wides, 1),
            "total_noballs": round(total_noballs, 1),
            "total_sixes": round(total_sixes, 1),
            "total_fours": round(total_fours, 1),
        },
        "dew_boost": dew_boost,
        "details": {
            "model": "over_under",
            "std_dev": std_dev,
            "z_score": round(z, 3),
        }
    }


def _normal_cdf(x):
    """Standard normal cumulative distribution function."""
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))
