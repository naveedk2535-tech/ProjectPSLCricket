"""
CricAPI client (cricketdata.org) for live scores and fixtures.
API Key: from config.CRICKET_API_KEY
Free tier: 100 requests/day
"""

import requests
from datetime import datetime

import config
from database import db
from data.rate_limiter import can_call, record_call, check_cache, save_cache
from data.team_names import standardise, standardise_venue


def _make_request(endpoint, params=None):
    """Make a rate-limited request to CricAPI."""
    if not can_call("cricket_api"):
        cached = check_cache(f"cricapi_{endpoint}", config.CACHE_TTL["fixtures"])
        if cached:
            record_call("cricket_api", endpoint, 200, cached=True)
            return cached
        return None

    url = f"{config.CRICKET_API_BASE}/{endpoint}"
    params = params or {}
    params["apikey"] = config.CRICKET_API_KEY

    try:
        resp = requests.get(url, params=params, timeout=15)
        record_call("cricket_api", endpoint, resp.status_code)

        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == "success":
                save_cache(f"cricapi_{endpoint}", data)
                return data
        return None
    except requests.RequestException:
        record_call("cricket_api", endpoint, 0)
        cached = check_cache(f"cricapi_{endpoint}")
        return cached


def get_fixtures(league="psl"):
    """Fetch upcoming fixtures for the given league (psl or ipl)."""
    league_filters = {
        "psl": {"keywords": ["psl", "pakistan super league", "super league"], "series_id": "psl-2026"},
        "ipl": {"keywords": ["ipl", "indian premier league"], "series_id": "ipl-2025"},
    }
    lf = league_filters.get(league, league_filters["psl"])

    # Try series-specific endpoint first
    data = _make_request("series_info", {"id": lf["series_id"]})

    if not data:
        # Fallback: get current matches and filter
        data = _make_request("currentMatches", {"offset": 0})

    cache_key = f"{league}_fixtures"
    if not data or "data" not in data:
        cached = check_cache(cache_key, config.CACHE_TTL["fixtures"])
        return cached.get("fixtures", []) if cached else []

    fixtures = []
    for match in data.get("data", []):
        match_name = (match.get("name", "") + " " + match.get("series", "")).lower()
        if any(kw in match_name for kw in lf["keywords"]):
            teams = match.get("teams", [])
            if len(teams) >= 2:
                fixture = {
                    "match_date": match.get("date", ""),
                    "match_time": match.get("dateTimeGMT", ""),
                    "venue": standardise_venue(match.get("venue", "")),
                    "team_a": standardise(teams[0]),
                    "team_b": standardise(teams[1]),
                    "status": _map_status(match.get("matchStarted", False), match.get("matchEnded", False)),
                    "cricapi_id": match.get("id", ""),
                    "match_number": match.get("matchNumber"),
                    "stage": "group",
                }
                fixtures.append(fixture)

    save_cache(cache_key, {"fixtures": fixtures, "fetched_at": db.now_iso()})
    return fixtures


# Backward compatibility
get_psl_fixtures = lambda: get_fixtures("psl")


def save_fixtures_to_db(fixtures, league="psl"):
    """Save fetched fixtures to database."""
    season = config.LEAGUES[league]["season"]
    for f in fixtures:
        db.execute(
            """INSERT INTO fixtures (season, match_date, match_time, venue, team_a, team_b,
               match_number, stage, status, cricapi_id, league, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(season, match_date, team_a, team_b)
               DO UPDATE SET status=excluded.status, match_time=excluded.match_time,
               venue=excluded.venue, cricapi_id=excluded.cricapi_id, league=excluded.league,
               updated_at=excluded.updated_at""",
            [season, f["match_date"], f.get("match_time"), f["venue"],
             f["team_a"], f["team_b"], f.get("match_number"), f.get("stage", "group"),
             f["status"], f.get("cricapi_id"), league, db.now_iso()]
        )


def get_live_score(cricapi_id):
    """Fetch live score for a specific match."""
    if not cricapi_id:
        return None

    cached = check_cache(f"live_{cricapi_id}", config.CACHE_TTL["live_score"])
    if cached:
        return cached

    data = _make_request("match_info", {"id": cricapi_id})
    if not data or "data" not in data:
        return None

    match = data["data"]
    score_data = {
        "cricapi_id": cricapi_id,
        "status": match.get("status", ""),
        "match_started": match.get("matchStarted", False),
        "match_ended": match.get("matchEnded", False),
        "scores": [],
    }

    for score in match.get("score", []):
        score_data["scores"].append({
            "team": standardise(score.get("inning", "").replace(" Inning 1", "").replace(" Inning 2", "")),
            "runs": score.get("r", 0),
            "wickets": score.get("w", 0),
            "overs": score.get("o", 0.0),
        })

    save_cache(f"live_{cricapi_id}", score_data)
    return score_data


def get_match_scorecard(cricapi_id):
    """Fetch detailed scorecard for completed match."""
    data = _make_request("match_scorecard", {"id": cricapi_id})
    if not data or "data" not in data:
        return None
    return data["data"]


def _map_status(started, ended):
    """Map CricAPI status to our status enum."""
    if ended:
        return "COMPLETED"
    if started:
        return "LIVE"
    return "SCHEDULED"


def get_player_info(player_id):
    """Fetch player statistics."""
    cached = check_cache(f"player_{player_id}", config.CACHE_TTL["player_stats"])
    if cached:
        return cached

    data = _make_request("players_info", {"id": player_id})
    if data and "data" in data:
        save_cache(f"player_{player_id}", data["data"])
        return data["data"]
    return None
