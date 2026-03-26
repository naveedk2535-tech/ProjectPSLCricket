"""
Player Strength Model — calculates team batting/bowling strength from individual player stats
and contextual factors (venue conditions, weather/dew).

Aggregates individual player contributions to produce team-level strength metrics
that feed into the ensemble prediction model.
"""

import math
from database import db
from data.team_names import standardise


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe(val, default=0.0):
    """Return val if truthy number, else default."""
    if val is None:
        return default
    try:
        f = float(val)
        return f if f > 0 else default
    except (TypeError, ValueError):
        return default


def _get_venue_stats(venue):
    """Fetch venue_stats row for a venue. Returns dict or None."""
    if not venue:
        return None
    row = db.fetch_one(
        "SELECT pace_wicket_pct, spin_wicket_pct, avg_first_innings, "
        "avg_second_innings, dew_impact_score FROM venue_stats WHERE venue = ?",
        [venue],
    )
    return row


def _get_weather(venue, match_date):
    """Fetch weather row for venue/date. Returns dict or None."""
    if not venue:
        return None
    params = [venue]
    sql = "SELECT temperature, humidity, dew_point, dew_score, heavy_dew FROM weather WHERE venue = ?"
    if match_date:
        sql += " AND match_date = ?"
        params.append(str(match_date))
    sql += " ORDER BY match_date DESC LIMIT 1"
    return db.fetch_one(sql, params)


def _pace_bowling_quality(player):
    """Metric for a pace bowler's quality (higher = better)."""
    avg = _safe(player.get("bowling_avg"), 30)
    econ = _safe(player.get("bowling_economy"), 8.5)
    return (25.0 / max(avg, 15)) * (8.0 / max(econ, 6))


def _spin_bowling_quality(player):
    """Metric for a spin bowler's quality (higher = better)."""
    avg = _safe(player.get("bowling_avg"), 30)
    econ = _safe(player.get("bowling_economy"), 8.0)
    return (25.0 / max(avg, 15)) * (8.0 / max(econ, 6))


# ---------------------------------------------------------------------------
# 1. get_team_player_strength
# ---------------------------------------------------------------------------

def get_team_player_strength(team):
    """
    Fetch all available players for a team from player_stats and calculate
    composite batting, bowling, and overall strength metrics.

    Returns a dict with:
        batting_strength, bowling_strength, powerplay_batting, death_bowling,
        all_rounder_depth, star_player_impact, squad_depth, top_players,
        pace_bowling_quality, spin_bowling_quality
    """
    team = standardise(team)

    players = db.fetch_all(
        "SELECT * FROM player_stats WHERE team = ? AND availability = 'available' "
        "ORDER BY impact_score DESC",
        [team],
    )

    if not players:
        # Fallback: try without availability filter
        players = db.fetch_all(
            "SELECT * FROM player_stats WHERE team = ? ORDER BY impact_score DESC",
            [team],
        )

    if not players:
        return {
            "batting_strength": 0.5,
            "bowling_strength": 0.5,
            "powerplay_batting": 0.0,
            "death_bowling": 0.0,
            "all_rounder_depth": 0.0,
            "star_player_impact": 0.0,
            "squad_depth": 0.0,
            "pace_bowling_quality": 0.0,
            "spin_bowling_quality": 0.0,
            "top_players": [],
        }

    # ── Batting strength: weighted avg of top 7 batsmen ────────────────
    # quality = batting_avg * batting_sr / 130  (130 = league avg SR baseline)
    batsmen = [
        p for p in players
        if _safe(p.get("batting_avg")) > 0 and _safe(p.get("batting_sr")) > 0
        and p.get("role") in ("batsman", "all-rounder", "wicket-keeper")
    ]
    batsmen.sort(key=lambda p: _safe(p.get("impact_score")), reverse=True)
    top_batsmen = batsmen[:7]

    if top_batsmen:
        weights = [7 - i for i in range(len(top_batsmen))]
        total_weight = sum(weights)
        batting_strength = sum(
            w * (_safe(p["batting_avg"]) * _safe(p["batting_sr"]) / 130.0)
            for w, p in zip(weights, top_batsmen)
        ) / total_weight
    else:
        batting_strength = 0.5  # league-average fallback

    # ── Bowling strength: weighted avg of top 5 bowlers ────────────────
    # quality = (25 / max(bowling_avg, 15)) * (8.0 / max(economy, 6))
    bowlers = [
        p for p in players
        if _safe(p.get("bowling_avg")) > 0 and _safe(p.get("bowling_economy")) > 0
        and p.get("role") in ("bowler", "all-rounder")
    ]
    bowlers.sort(key=lambda p: _safe(p.get("impact_score")), reverse=True)
    top_bowlers = bowlers[:5]

    if top_bowlers:
        weights = [5 - i for i in range(len(top_bowlers))]
        total_weight = sum(weights)
        bowling_strength = sum(
            w * (25.0 / max(_safe(p["bowling_avg"]), 15))
              * (8.0 / max(_safe(p["bowling_economy"]), 6))
            for w, p in zip(weights, top_bowlers)
        ) / total_weight
    else:
        bowling_strength = 0.5  # league-average fallback

    # ── Powerplay batting: avg powerplay_sr of top-order batsmen ───────
    pp_batsmen = [
        p for p in top_batsmen
        if _safe(p.get("powerplay_sr")) > 0
    ][:4]
    powerplay_batting = (
        sum(_safe(p["powerplay_sr"]) for p in pp_batsmen) / len(pp_batsmen)
        if pp_batsmen else 0.0
    )

    # ── Death bowling: avg death_economy of death bowlers ──────────────
    death_bowlers = [
        p for p in players
        if _safe(p.get("death_economy")) > 0
        and p.get("role") in ("bowler", "all-rounder")
    ]
    death_bowlers.sort(key=lambda p: _safe(p["death_economy"]))  # lower = better
    top_death = death_bowlers[:4]
    death_bowling = (
        sum(_safe(p["death_economy"]) for p in top_death) / len(top_death)
        if top_death else 0.0
    )

    # ── All-rounder depth ──────────────────────────────────────────────
    all_rounders = [
        p for p in players
        if p.get("role") == "all-rounder"
        and _safe(p.get("batting_avg")) > 0 and _safe(p.get("bowling_avg")) > 0
    ]
    high_impact_ar = [p for p in all_rounders if _safe(p.get("impact_score")) > 30]
    ar_count = len(high_impact_ar)
    if all_rounders:
        ar_quality = sum(
            _safe(p.get("impact_score")) for p in all_rounders
        ) / (len(all_rounders) * 100.0)
    else:
        ar_quality = 0.0
    all_rounder_depth = min(ar_count * 0.2 + ar_quality, 1.0)

    # ── Star player impact: top 3 impact scores / 300 ─────────────────
    top_impact = sorted(players, key=lambda p: _safe(p.get("impact_score")), reverse=True)[:3]
    star_player_impact = sum(_safe(p.get("impact_score")) for p in top_impact) / 300.0
    star_player_impact = min(star_player_impact, 1.0)

    # ── Squad depth: weighted metric of total squad quality ────────────
    players_with_impact = [p for p in players if _safe(p.get("impact_score")) > 0]
    if players_with_impact:
        avg_impact = sum(_safe(p["impact_score"]) for p in players_with_impact) / len(players_with_impact)
        squad_depth = min(avg_impact / 80.0, 1.0) * min(len(players_with_impact) / 15.0, 1.0)
    else:
        squad_depth = 0.0

    # ── Pace vs spin bowling quality (for venue adjustment) ────────────
    pace_bowlers = [
        p for p in bowlers
        if p.get("role") in ("bowler", "all-rounder")
        and "spin" not in (p.get("name", "") + p.get("bowling_style", "")).lower()
    ]
    spin_bowlers_list = [
        p for p in bowlers
        if p.get("role") in ("bowler", "all-rounder")
        and "spin" in (p.get("name", "") + p.get("bowling_style", "")).lower()
    ]
    # If we can't distinguish, split bowlers roughly
    if not pace_bowlers and not spin_bowlers_list:
        half = max(len(top_bowlers) // 2, 1)
        pace_bowlers = top_bowlers[:half]
        spin_bowlers_list = top_bowlers[half:]

    pace_qual = (
        sum(_pace_bowling_quality(p) for p in pace_bowlers) / len(pace_bowlers)
        if pace_bowlers else 0.5
    )
    spin_qual = (
        sum(_spin_bowling_quality(p) for p in spin_bowlers_list) / len(spin_bowlers_list)
        if spin_bowlers_list else 0.5
    )

    # ── Top players list ───────────────────────────────────────────────
    top_players = [
        {
            "name": p.get("name", "Unknown"),
            "role": p.get("role", "unknown"),
            "impact_score": round(_safe(p.get("impact_score")), 1),
        }
        for p in players[:5]
    ]

    return {
        "batting_strength": round(batting_strength, 3),
        "bowling_strength": round(bowling_strength, 3),
        "powerplay_batting": round(powerplay_batting, 2),
        "death_bowling": round(death_bowling, 2),
        "all_rounder_depth": round(all_rounder_depth, 3),
        "star_player_impact": round(star_player_impact, 3),
        "squad_depth": round(squad_depth, 3),
        "pace_bowling_quality": round(pace_qual, 3),
        "spin_bowling_quality": round(spin_qual, 3),
        "top_players": top_players,
    }


# ---------------------------------------------------------------------------
# 2. predict  — standard model interface
# ---------------------------------------------------------------------------

def predict(team_a, team_b, venue=None, match_date=None):
    """
    Predict match outcome based on player-level strength comparison,
    adjusted for venue conditions and weather/dew.

    Returns dict with same interface as other models:
        team_a_win, team_b_win, confidence, details
    """
    team_a = standardise(team_a)
    team_b = standardise(team_b)

    strength_a = get_team_player_strength(team_a)
    strength_b = get_team_player_strength(team_b)

    # Start with raw scores
    bat_a = strength_a["batting_strength"]
    bat_b = strength_b["batting_strength"]
    bowl_a = strength_a["bowling_strength"]
    bowl_b = strength_b["bowling_strength"]
    star_a = strength_a["star_player_impact"]
    star_b = strength_b["star_player_impact"]

    # ── VENUE ADJUSTMENT ─────────────────────────────────────────────────
    venue_info = None
    venue_adjustment_a = 0.0
    venue_adjustment_b = 0.0
    if venue:
        venue_info = _get_venue_stats(venue)

    if venue_info:
        pace_pct = _safe(venue_info.get("pace_wicket_pct"), 50)
        spin_pct = _safe(venue_info.get("spin_wicket_pct"), 50)

        if pace_pct > 55:
            # Pace-friendly venue — boost teams with better pace bowlers
            pace_factor = (pace_pct - 50) / 100.0  # mild multiplier
            venue_adjustment_a += pace_factor * strength_a["pace_bowling_quality"]
            venue_adjustment_b += pace_factor * strength_b["pace_bowling_quality"]
        elif spin_pct > 55:
            # Spin-friendly venue — boost teams with better spin bowlers
            spin_factor = (spin_pct - 50) / 100.0
            venue_adjustment_a += spin_factor * strength_a["spin_bowling_quality"]
            venue_adjustment_b += spin_factor * strength_b["spin_bowling_quality"]

    # ── WEATHER / DEW ADJUSTMENT ─────────────────────────────────────────
    weather_info = None
    dew_boost_a = 0.0
    dew_boost_b = 0.0
    if venue:
        weather_info = _get_weather(venue, match_date)

    if weather_info:
        dew = _safe(weather_info.get("dew_score"), 0)
        if dew > 0.5:
            # Heavy dew makes batting easier (wet ball, harder to grip)
            # Slight boost to batting strength for both teams
            dew_factor = (dew - 0.5) * 0.10  # max ~0.05 boost
            dew_boost_a = dew_factor * bat_a
            dew_boost_b = dew_factor * bat_b

    # ── Composite team scores ────────────────────────────────────────────
    score_a = (
        0.45 * bat_a
        + 0.35 * bowl_a
        + 0.20 * star_a
        + venue_adjustment_a
        + dew_boost_a
    )
    score_b = (
        0.45 * bat_b
        + 0.35 * bowl_b
        + 0.20 * star_b
        + venue_adjustment_b
        + dew_boost_b
    )

    # ── Convert diff to probability via logistic function ────────────────
    # Scale factor proportional to average magnitude so probabilities stay reasonable
    avg_score = (abs(score_a) + abs(score_b)) / 2.0 if (score_a + score_b) > 0 else 1.0
    scale = max(avg_score * 0.3, 0.3)
    score_diff = score_a - score_b
    prob_a = 1.0 / (1.0 + math.exp(-score_diff / scale))
    prob_b = 1.0 - prob_a

    # ── Confidence based on data availability ────────────────────────────
    players_a = db.fetch_all(
        "SELECT COUNT(*) as cnt FROM player_stats WHERE team = ? AND impact_score IS NOT NULL",
        [team_a],
    )
    players_b = db.fetch_all(
        "SELECT COUNT(*) as cnt FROM player_stats WHERE team = ? AND impact_score IS NOT NULL",
        [team_b],
    )
    count_a = players_a[0]["cnt"] if players_a else 0
    count_b = players_b[0]["cnt"] if players_b else 0
    data_ratio = min((count_a + count_b) / 30.0, 1.0)
    confidence = 0.30 + 0.55 * data_ratio  # range [0.30, 0.85]

    return {
        "team_a_win": round(prob_a, 4),
        "team_b_win": round(prob_b, 4),
        "confidence": round(confidence, 3),
        "details": {
            "model": "player_strength",
            "team_a_batting_strength": strength_a["batting_strength"],
            "team_a_bowling_strength": strength_a["bowling_strength"],
            "team_a_star_impact": strength_a["star_player_impact"],
            "team_b_batting_strength": strength_b["batting_strength"],
            "team_b_bowling_strength": strength_b["bowling_strength"],
            "team_b_star_impact": strength_b["star_player_impact"],
            "venue_adjustment_a": round(venue_adjustment_a, 4),
            "venue_adjustment_b": round(venue_adjustment_b, 4),
            "dew_boost_a": round(dew_boost_a, 4),
            "dew_boost_b": round(dew_boost_b, 4),
            "team_a_players": strength_a["top_players"],
            "team_b_players": strength_b["top_players"],
        },
    }


# ---------------------------------------------------------------------------
# 3. get_matchup_analysis  — detailed head-to-head comparison
# ---------------------------------------------------------------------------

def get_matchup_analysis(team_a, team_b, venue=None):
    """
    Return a detailed head-to-head matchup analysis comparing player-level
    batting, bowling, and all-rounder metrics for both teams, including
    venue-specific advantages.
    """
    team_a = standardise(team_a)
    team_b = standardise(team_b)

    strength_a = get_team_player_strength(team_a)
    strength_b = get_team_player_strength(team_b)

    # ── Batting comparison ───────────────────────────────────────────────
    batting_comparison = {
        "team_a": {
            "batting_strength": strength_a["batting_strength"],
            "powerplay_sr": strength_a["powerplay_batting"],
        },
        "team_b": {
            "batting_strength": strength_b["batting_strength"],
            "powerplay_sr": strength_b["powerplay_batting"],
        },
        "advantage": team_a if strength_a["batting_strength"] > strength_b["batting_strength"] else team_b,
    }

    for label, team in [("team_a", team_a), ("team_b", team_b)]:
        row = db.fetch_one(
            "SELECT AVG(batting_avg) as avg_bat, AVG(batting_sr) as avg_sr, "
            "AVG(boundary_pct) as avg_boundary "
            "FROM player_stats WHERE team = ? AND batting_avg IS NOT NULL",
            [team],
        )
        if row:
            batting_comparison[label]["avg_batting_avg"] = round(_safe(row.get("avg_bat")), 2)
            batting_comparison[label]["avg_strike_rate"] = round(_safe(row.get("avg_sr")), 2)
            batting_comparison[label]["avg_boundary_pct"] = round(_safe(row.get("avg_boundary")), 2)

    # ── Bowling comparison ───────────────────────────────────────────────
    bowling_comparison = {
        "team_a": {
            "bowling_strength": strength_a["bowling_strength"],
            "death_economy": strength_a["death_bowling"],
        },
        "team_b": {
            "bowling_strength": strength_b["bowling_strength"],
            "death_economy": strength_b["death_bowling"],
        },
        "advantage": team_a if strength_a["bowling_strength"] > strength_b["bowling_strength"] else team_b,
    }

    for label, team in [("team_a", team_a), ("team_b", team_b)]:
        row = db.fetch_one(
            "SELECT AVG(bowling_avg) as avg_bowl_avg, AVG(bowling_economy) as avg_econ, "
            "AVG(dot_ball_pct) as avg_dot "
            "FROM player_stats WHERE team = ? AND bowling_avg IS NOT NULL AND bowling_avg > 0",
            [team],
        )
        if row:
            bowling_comparison[label]["avg_bowling_avg"] = round(_safe(row.get("avg_bowl_avg")), 2)
            bowling_comparison[label]["avg_economy"] = round(_safe(row.get("avg_econ")), 2)
            bowling_comparison[label]["avg_dot_ball_pct"] = round(_safe(row.get("avg_dot")), 2)

    # ── Star players: top 3-5 impact players per team with key stats ───
    star_players = {}
    for label, team in [("team_a_stars", team_a), ("team_b_stars", team_b)]:
        rows = db.fetch_all(
            "SELECT name, role, impact_score, batting_avg, batting_sr, "
            "bowling_avg, bowling_economy, boundary_pct, dot_ball_pct "
            "FROM player_stats WHERE team = ? ORDER BY impact_score DESC LIMIT 5",
            [team],
        )
        stars = []
        for r in (rows or []):
            entry = {
                "name": r.get("name", "Unknown"),
                "role": r.get("role", "unknown"),
                "impact_score": round(_safe(r.get("impact_score")), 1),
            }
            if r.get("role") in ("batsman", "wicket-keeper"):
                entry["batting_avg"] = round(_safe(r.get("batting_avg")), 1)
                entry["strike_rate"] = round(_safe(r.get("batting_sr")), 1)
                entry["boundary_pct"] = round(_safe(r.get("boundary_pct")), 1)
            elif r.get("role") == "bowler":
                entry["bowling_avg"] = round(_safe(r.get("bowling_avg")), 1)
                entry["economy"] = round(_safe(r.get("bowling_economy")), 2)
                entry["dot_ball_pct"] = round(_safe(r.get("dot_ball_pct")), 1)
            else:  # all-rounder
                entry["batting_avg"] = round(_safe(r.get("batting_avg")), 1)
                entry["strike_rate"] = round(_safe(r.get("batting_sr")), 1)
                entry["economy"] = round(_safe(r.get("bowling_economy")), 2)
            stars.append(entry)
        star_players[label] = stars

    # ── All-rounder depth comparison ─────────────────────────────────────
    all_rounder_comparison = {
        "team_a": {
            "depth": strength_a["all_rounder_depth"],
            "squad_depth": strength_a["squad_depth"],
        },
        "team_b": {
            "depth": strength_b["all_rounder_depth"],
            "squad_depth": strength_b["squad_depth"],
        },
        "advantage": team_a if strength_a["all_rounder_depth"] > strength_b["all_rounder_depth"] else team_b,
    }

    # ── Venue matchup: which team's composition suits the venue ─────────
    venue_matchup = {"venue": venue, "advantage": None, "reason": "No venue data"}
    if venue:
        venue_info = _get_venue_stats(venue)
        if venue_info:
            pace_pct = _safe(venue_info.get("pace_wicket_pct"), 50)
            spin_pct = _safe(venue_info.get("spin_wicket_pct"), 50)

            if pace_pct > 55:
                # Pace-friendly
                if strength_a["pace_bowling_quality"] > strength_b["pace_bowling_quality"]:
                    venue_matchup["advantage"] = team_a
                else:
                    venue_matchup["advantage"] = team_b
                venue_matchup["reason"] = (
                    f"Pace-friendly venue (pace {pace_pct:.0f}%) — favours stronger pace attack"
                )
            elif spin_pct > 55:
                # Spin-friendly
                if strength_a["spin_bowling_quality"] > strength_b["spin_bowling_quality"]:
                    venue_matchup["advantage"] = team_a
                else:
                    venue_matchup["advantage"] = team_b
                venue_matchup["reason"] = (
                    f"Spin-friendly venue (spin {spin_pct:.0f}%) — favours stronger spin attack"
                )
            else:
                venue_matchup["reason"] = "Balanced venue — no significant pace/spin bias"
                # Slight advantage to team with better overall bowling
                venue_matchup["advantage"] = (
                    team_a if strength_a["bowling_strength"] > strength_b["bowling_strength"]
                    else team_b
                )

            venue_matchup["avg_first_innings"] = _safe(venue_info.get("avg_first_innings"))
            venue_matchup["avg_second_innings"] = _safe(venue_info.get("avg_second_innings"))

    # ── Overall advantage ────────────────────────────────────────────────
    composite_a = (
        strength_a["batting_strength"]
        + strength_a["bowling_strength"]
        + strength_a["star_player_impact"]
    )
    composite_b = (
        strength_b["batting_strength"]
        + strength_b["bowling_strength"]
        + strength_b["star_player_impact"]
    )

    return {
        "team_a": team_a,
        "team_b": team_b,
        "batting": batting_comparison,
        "bowling": bowling_comparison,
        "key_players": star_players,
        "all_rounders": all_rounder_comparison,
        "venue_matchup": venue_matchup,
        "overall_advantage": team_a if composite_a > composite_b else team_b,
    }


# ---------------------------------------------------------------------------
# 4. get_player_detail_cards  — structured data for match detail display
# ---------------------------------------------------------------------------

def get_player_detail_cards(team_a, team_b):
    """
    Return structured player card data for the match detail page.

    For each team, returns a list of players with:
        name, role, impact_score, key_stat, form_indicator
    """
    team_a = standardise(team_a)
    team_b = standardise(team_b)

    result = {}

    for label, team in [("team_a", team_a), ("team_b", team_b)]:
        rows = db.fetch_all(
            "SELECT name, role, impact_score, batting_avg, batting_sr, "
            "bowling_avg, bowling_economy, matches_played, runs_scored, "
            "wickets_taken, fifties, hundreds, boundary_pct, dot_ball_pct, "
            "powerplay_sr, death_economy, availability "
            "FROM player_stats WHERE team = ? ORDER BY impact_score DESC",
            [team],
        )

        cards = []
        for p in (rows or []):
            role = p.get("role", "unknown")
            impact = _safe(p.get("impact_score"))

            # Key stat depends on role
            if role in ("batsman", "wicket-keeper"):
                bat_avg = _safe(p.get("batting_avg"))
                bat_sr = _safe(p.get("batting_sr"))
                key_stat = f"Avg {bat_avg:.1f} | SR {bat_sr:.1f}"
            elif role == "bowler":
                econ = _safe(p.get("bowling_economy"))
                bowl_avg = _safe(p.get("bowling_avg"))
                key_stat = f"Econ {econ:.2f} | Avg {bowl_avg:.1f}"
            else:  # all-rounder
                bat_avg = _safe(p.get("batting_avg"))
                econ = _safe(p.get("bowling_economy"))
                key_stat = f"Bat Avg {bat_avg:.1f} | Econ {econ:.2f}"

            # Form indicator based on impact score thresholds
            if impact >= 70:
                form_indicator = "elite"
            elif impact >= 50:
                form_indicator = "in-form"
            elif impact >= 30:
                form_indicator = "steady"
            elif impact >= 15:
                form_indicator = "moderate"
            else:
                form_indicator = "low"

            cards.append({
                "name": p.get("name", "Unknown"),
                "role": role,
                "impact_score": round(impact, 1),
                "key_stat": key_stat,
                "form_indicator": form_indicator,
                "matches_played": int(_safe(p.get("matches_played"))),
                "availability": p.get("availability", "unknown"),
            })

        result[label] = {
            "team": team,
            "players": cards,
        }

    return result
