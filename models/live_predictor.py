"""
Live/In-Play Prediction Model.
Recalculates win probability during the match based on current score.
Supports both auto-update (CricAPI) and manual input modes.
"""

import math
from database import db
import config


def calculate_live_probability(team_a, team_b, venue, innings, score, wickets, overs, target=None):
    """
    Calculate live win probability based on current match state.

    INNINGS 1 (batting): Project final total → estimate chase probability
    INNINGS 2 (chasing): Compare required rate vs historical chase success
    """
    if overs <= 0:
        return {"team_a_win": 0.5, "team_b_win": 0.5, "projected_total": 0,
                "momentum": "neutral", "key_insight": "Match not started"}

    current_rr = score / overs if overs > 0 else 0
    remaining_overs = max(0, 20 - overs)
    wickets_in_hand = 10 - wickets

    # Get venue stats
    v_stats = db.fetch_one("SELECT * FROM venue_stats WHERE venue = ?", [venue]) if venue else None
    venue_avg_first = v_stats["avg_first_innings"] if v_stats and v_stats.get("avg_first_innings") else 170
    venue_avg_second = v_stats["avg_second_innings"] if v_stats and v_stats.get("avg_second_innings") else 160
    venue_chase_pct = v_stats["chase_win_pct"] if v_stats and v_stats.get("chase_win_pct") else 50

    if innings == 1:
        return _innings1_prediction(team_a, team_b, score, wickets, overs,
                                     remaining_overs, current_rr, wickets_in_hand,
                                     venue_avg_first, venue_avg_second, venue_chase_pct)
    else:
        return _innings2_prediction(team_a, team_b, score, wickets, overs,
                                     remaining_overs, current_rr, wickets_in_hand,
                                     target, venue_chase_pct)


def _innings1_prediction(team_a, team_b, score, wickets, overs,
                          remaining_overs, current_rr, wickets_in_hand,
                          venue_avg_first, venue_avg_second, venue_chase_pct):
    """Project final total from first innings position."""

    # Acceleration factor based on phase and wickets
    if overs <= 6:  # Powerplay
        # Project from powerplay: historically teams score ~2.5x their powerplay score
        projected_total = score * (20 / overs) * _wicket_factor(wickets_in_hand)
    elif overs <= 15:  # Middle overs
        # Middle overs RR typically 7-8, death overs 9-11
        middle_remaining = max(0, 15 - overs)
        death_overs = 5
        middle_rr = current_rr * 0.9  # Middle overs slightly slower
        death_rr = current_rr * 1.3 * _wicket_factor(wickets_in_hand)  # Death acceleration
        projected_total = score + (middle_remaining * middle_rr) + (death_overs * death_rr)
    else:  # Death overs
        death_rr = current_rr * 1.1 * _wicket_factor(wickets_in_hand)
        projected_total = score + (remaining_overs * death_rr)

    # Clamp to realistic range
    projected_total = max(score + remaining_overs * 4, min(280, projected_total))

    # Convert projected total to win probability
    # Higher projected total = better for batting team
    diff_from_avg = projected_total - venue_avg_first
    batting_first_win = _logistic(diff_from_avg / 15) * (1 - venue_chase_pct / 100) * 2
    batting_first_win = max(0.15, min(0.85, batting_first_win))

    # Par score at this stage
    par_score = venue_avg_first * (overs / 20)

    momentum = "improving" if score > par_score * 1.05 else ("declining" if score < par_score * 0.95 else "neutral")

    key_insight = _generate_insight_innings1(score, wickets, overs, projected_total, par_score, venue_avg_first)

    return {
        "team_a_win": round(batting_first_win, 4),
        "team_b_win": round(1 - batting_first_win, 4),
        "projected_total": round(projected_total, 0),
        "current_rate": round(current_rr, 2),
        "required_rate": None,
        "par_score": round(par_score, 0),
        "innings": 1,
        "score": score,
        "wickets": wickets,
        "overs": overs,
        "momentum": momentum,
        "key_insight": key_insight,
    }


def _innings2_prediction(team_a, team_b, score, wickets, overs,
                          remaining_overs, current_rr, wickets_in_hand,
                          target, venue_chase_pct):
    """Calculate chase probability from current position."""
    if not target or target <= 0:
        return {"team_a_win": 0.5, "team_b_win": 0.5, "projected_total": 0,
                "momentum": "neutral", "key_insight": "Target not set"}

    runs_needed = target - score
    required_rr = runs_needed / remaining_overs if remaining_overs > 0 else 999

    # Chase probability based on required rate and wickets in hand
    if runs_needed <= 0:
        # Already won
        return {"team_a_win": 0.0, "team_b_win": 1.0, "projected_total": score,
                "current_rate": current_rr, "required_rate": 0,
                "innings": 2, "score": score, "wickets": wickets, "overs": overs,
                "momentum": "won", "key_insight": "Chase complete!"}

    if wickets >= 10 or remaining_overs <= 0:
        return {"team_a_win": 1.0, "team_b_win": 0.0, "projected_total": score,
                "current_rate": current_rr, "required_rate": required_rr,
                "innings": 2, "score": score, "wickets": wickets, "overs": overs,
                "momentum": "lost", "key_insight": "All out / Overs exhausted"}

    # Required rate factor: higher required rate = lower chase probability
    rr_factor = _logistic(-(required_rr - 9) / 3)  # 9 RPO is neutral point

    # Wickets factor: more wickets in hand = higher probability
    wicket_factor = _wicket_factor(wickets_in_hand)

    # Phase factor: easier to score in death overs
    phase_factor = 1.0
    if overs >= 15:
        phase_factor = 1.1  # Death overs boost
    elif overs <= 6:
        phase_factor = 0.95  # Powerplay still early

    chase_prob = rr_factor * wicket_factor * phase_factor
    chase_prob = max(0.05, min(0.95, chase_prob))

    # DLS-like par score
    resources_remaining = wickets_in_hand * remaining_overs / 100  # Simplified
    par_score = target * (1 - resources_remaining / 2)

    momentum = "improving" if current_rr > required_rr else ("declining" if current_rr < required_rr * 0.85 else "neutral")

    key_insight = _generate_insight_innings2(score, wickets, overs, target, runs_needed, required_rr, wickets_in_hand)

    return {
        "team_a_win": round(1 - chase_prob, 4),  # team_a batted first
        "team_b_win": round(chase_prob, 4),        # team_b is chasing
        "projected_total": round(score + remaining_overs * current_rr, 0),
        "current_rate": round(current_rr, 2),
        "required_rate": round(required_rr, 2),
        "par_score": round(par_score, 0),
        "runs_needed": runs_needed,
        "balls_remaining": int(remaining_overs * 6),
        "innings": 2,
        "score": score,
        "wickets": wickets,
        "overs": overs,
        "momentum": momentum,
        "key_insight": key_insight,
    }


def what_if(team_a, team_b, venue, innings, score, wickets, overs, target, scenario):
    """
    What-if scenario calculator.
    Modifies inputs based on scenario and recalculates probability.
    """
    scenarios = {
        "wicket_next_over": {"wickets": wickets + 1, "overs": overs + 1, "score": score + 4},
        "boundary_spree": {"score": score + 18, "overs": overs + 1},  # 18-run over
        "dot_ball_over": {"score": score + 2, "overs": overs + 1},    # Only 2 runs
        "two_wickets": {"wickets": min(10, wickets + 2), "overs": overs + 2, "score": score + 10},
        "big_over": {"score": score + 22, "overs": overs + 1},        # 22-run over
        "maiden": {"score": score, "overs": overs + 1},               # Maiden over
    }

    if scenario not in scenarios:
        return None

    mods = scenarios[scenario]
    new_score = mods.get("score", score)
    new_wickets = mods.get("wickets", wickets)
    new_overs = mods.get("overs", overs)

    return calculate_live_probability(team_a, team_b, venue, innings,
                                      new_score, new_wickets, new_overs, target)


def _wicket_factor(wickets_in_hand):
    """Wickets in hand factor — more wickets = more aggressive batting."""
    if wickets_in_hand >= 8:
        return 1.1
    elif wickets_in_hand >= 6:
        return 1.0
    elif wickets_in_hand >= 4:
        return 0.85
    elif wickets_in_hand >= 2:
        return 0.65
    else:
        return 0.4


def _logistic(x):
    """Logistic sigmoid function."""
    return 1 / (1 + math.exp(-x))


def _generate_insight_innings1(score, wickets, overs, projected, par, venue_avg):
    """Generate human-readable insight for first innings."""
    if projected > venue_avg + 20:
        return f"Projected {int(projected)} — well above venue average ({int(venue_avg)}). Strong position."
    elif projected < venue_avg - 20:
        return f"Projected {int(projected)} — below venue average ({int(venue_avg)}). Under pressure."
    elif wickets >= 5 and overs <= 15:
        return f"Lost {wickets} wickets early. Collapse risk. May fall short of {int(venue_avg)}."
    elif score > par * 1.1:
        return f"Ahead of par by {int(score - par)} runs. Good platform for death overs."
    else:
        return f"Tracking near par. Projected total: {int(projected)}."


def _generate_insight_innings2(score, wickets, overs, target, needed, rr, wih):
    """Generate human-readable insight for chase."""
    if rr > 15:
        return f"Need {needed} off {int((20-overs)*6)} balls. Required rate {rr:.1f} — virtually impossible."
    elif rr > 12:
        return f"Need {needed} off {int((20-overs)*6)} balls at {rr:.1f} RPO. Very difficult but not impossible."
    elif rr > 9 and wih <= 4:
        return f"Need {rr:.1f} RPO with only {wih} wickets in hand. Tough ask."
    elif rr < 7 and wih >= 6:
        return f"Cruising! Need just {rr:.1f} RPO with {wih} wickets in hand."
    elif needed <= 30 and wih >= 4:
        return f"Just {needed} needed with {wih} wickets in hand. Strong position."
    else:
        return f"Need {needed} from {int((20-overs)*6)} balls at {rr:.1f} RPO. {wih} wickets in hand."
