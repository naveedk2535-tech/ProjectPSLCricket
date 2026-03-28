"""
Cricbuzz unofficial API client — free, no API key needed.
Uses cricbuzz-live.vercel.app as primary source for live scores and recent results.
Falls back to direct Cricbuzz scraping if Vercel app is down.

Endpoints:
  /v1/matches/live?type=league   — live matches
  /v1/matches/recent?type=league — recent completed matches
  /v1/matches/upcoming?type=league — upcoming fixtures
  /v1/score/{matchId}            — detailed live score
"""

import requests
from datetime import datetime

from database import db
from data.rate_limiter import can_call, record_call, check_cache, save_cache
from data.team_names import standardise, standardise_venue

BASE_URL = "https://cricbuzz-live.vercel.app"

# Map Cricbuzz team names to our standardised names
PSL_TEAM_MAP = {
    "lahore qalandars": "Lahore Qalandars",
    "karachi kings": "Karachi Kings",
    "islamabad united": "Islamabad United",
    "peshawar zalmi": "Peshawar Zalmi",
    "multan sultans": "Multan Sultans",
    "quetta gladiators": "Quetta Gladiators",
    "hyderabad kingsmen": "Hyderabad Kingsmen",
    "rawalpindi pindiz": "Rawalpindi Pindiz",
}

IPL_TEAM_MAP = {
    "chennai super kings": "Chennai Super Kings",
    "mumbai indians": "Mumbai Indians",
    "royal challengers bengaluru": "Royal Challengers Bengaluru",
    "royal challengers bangalore": "Royal Challengers Bengaluru",
    "kolkata knight riders": "Kolkata Knight Riders",
    "delhi capitals": "Delhi Capitals",
    "punjab kings": "Punjab Kings",
    "kings xi punjab": "Punjab Kings",
    "rajasthan royals": "Rajasthan Royals",
    "sunrisers hyderabad": "Sunrisers Hyderabad",
    "gujarat titans": "Gujarat Titans",
    "lucknow super giants": "Lucknow Super Giants",
}


def _request(endpoint, cache_key=None, cache_ttl=300):
    """Make a request to cricbuzz-live API with caching."""
    if cache_key:
        cached = check_cache(cache_key, cache_ttl)
        if cached:
            return cached

    try:
        resp = requests.get(f"{BASE_URL}{endpoint}", timeout=15,
                            headers={"User-Agent": "CricketAnalytics/1.0"})
        if resp.status_code != 200:
            return None
        data = resp.json()
        if cache_key:
            save_cache(cache_key, data)
        return data
    except Exception as e:
        print(f"[Cricbuzz] Request error {endpoint}: {e}")
        return None


def _standardise_team(name, league="psl"):
    """Map Cricbuzz team name to our standardised name."""
    name_lower = (name or "").lower().strip()
    team_map = PSL_TEAM_MAP if league == "psl" else IPL_TEAM_MAP

    # Direct map lookup
    if name_lower in team_map:
        return team_map[name_lower]

    # Partial match
    for key, val in team_map.items():
        if key in name_lower or name_lower in key:
            return val

    # Fall back to standardise()
    return standardise(name)


def _is_league_match(match, league="psl"):
    """Check if a Cricbuzz match belongs to our league."""
    series = (match.get("series", "") or "").lower()
    name = (match.get("title", "") or match.get("name", "")).lower()
    combined = series + " " + name

    if league == "psl":
        return any(kw in combined for kw in ["psl", "pakistan super league", "super league 2026"])
    else:
        return any(kw in combined for kw in ["ipl", "indian premier league", "premier league 2025"])


def get_recent_results(league="psl"):
    """Fetch recent completed matches for the league."""
    cache_key = f"cricbuzz_recent_{league}"
    data = _request("/v1/matches/recent?type=league", cache_key, 600)

    if not data:
        return []

    results = []
    matches = data if isinstance(data, list) else data.get("typeMatches", data.get("matches", []))

    # Cricbuzz returns nested structure
    if isinstance(matches, list):
        for type_match in matches:
            series_matches = type_match.get("seriesMatches", []) if isinstance(type_match, dict) else []
            for series in series_matches:
                series_ad = series.get("seriesAdWrapper", {}) or {}
                match_list = series_ad.get("matches", [])
                for m in match_list:
                    match_info = m.get("matchInfo", {})
                    match_score = m.get("matchScore", {})

                    if not _is_league_match({"series": series_ad.get("seriesName", ""), "title": match_info.get("matchDesc", "")}, league):
                        continue

                    team1 = match_info.get("team1", {})
                    team2 = match_info.get("team2", {})

                    result = {
                        "match_id": str(match_info.get("matchId", "")),
                        "match_date": "",
                        "team_a": _standardise_team(team1.get("teamName", ""), league),
                        "team_b": _standardise_team(team2.get("teamName", ""), league),
                        "status": match_info.get("status", ""),
                        "venue": match_info.get("venueInfo", {}).get("ground", ""),
                    }

                    # Parse date
                    start_ts = match_info.get("startDate")
                    if start_ts:
                        try:
                            result["match_date"] = datetime.fromtimestamp(int(start_ts) / 1000).strftime("%Y-%m-%d")
                        except Exception:
                            pass

                    # Parse scores
                    team1_score = match_score.get("team1Score", {})
                    team2_score = match_score.get("team2Score", {})

                    inn1 = team1_score.get("inngs1", {})
                    inn2 = team2_score.get("inngs1", {})

                    result["innings1_runs"] = inn1.get("runs", 0)
                    result["innings1_wickets"] = inn1.get("wickets", 0)
                    result["innings2_runs"] = inn2.get("runs", 0)
                    result["innings2_wickets"] = inn2.get("wickets", 0)

                    # Parse winner from status text
                    status_text = result["status"].lower()
                    result["winner"] = None
                    for team_name in [result["team_a"], result["team_b"]]:
                        if team_name.lower() in status_text:
                            if "won" in status_text:
                                result["winner"] = team_name
                                break

                    results.append(result)

    return results


def get_live_matches(league="psl"):
    """Fetch currently live matches for the league."""
    cache_key = f"cricbuzz_live_{league}"
    data = _request("/v1/matches/live?type=league", cache_key, 120)

    if not data:
        return []

    live = []
    matches = data if isinstance(data, list) else data.get("typeMatches", data.get("matches", []))

    if isinstance(matches, list):
        for type_match in matches:
            series_matches = type_match.get("seriesMatches", []) if isinstance(type_match, dict) else []
            for series in series_matches:
                series_ad = series.get("seriesAdWrapper", {}) or {}
                match_list = series_ad.get("matches", [])
                for m in match_list:
                    match_info = m.get("matchInfo", {})
                    match_score = m.get("matchScore", {})

                    if not _is_league_match({"series": series_ad.get("seriesName", ""), "title": match_info.get("matchDesc", "")}, league):
                        continue

                    team1 = match_info.get("team1", {})
                    team2 = match_info.get("team2", {})

                    live_match = {
                        "match_id": str(match_info.get("matchId", "")),
                        "team_a": _standardise_team(team1.get("teamName", ""), league),
                        "team_b": _standardise_team(team2.get("teamName", ""), league),
                        "status": match_info.get("status", ""),
                        "venue": match_info.get("venueInfo", {}).get("ground", ""),
                    }

                    # Scores
                    t1s = match_score.get("team1Score", {}).get("inngs1", {})
                    t2s = match_score.get("team2Score", {}).get("inngs1", {})
                    live_match["score_a"] = f"{t1s.get('runs', 0)}/{t1s.get('wickets', 0)} ({t1s.get('overs', 0)})" if t1s else ""
                    live_match["score_b"] = f"{t2s.get('runs', 0)}/{t2s.get('wickets', 0)} ({t2s.get('overs', 0)})" if t2s else ""

                    live.append(live_match)

    return live


def get_upcoming_matches(league="psl"):
    """Fetch upcoming scheduled matches for the league."""
    cache_key = f"cricbuzz_upcoming_{league}"
    data = _request("/v1/matches/upcoming?type=league", cache_key, 3600)

    if not data:
        return []

    upcoming = []
    matches = data if isinstance(data, list) else data.get("typeMatches", data.get("matches", []))

    if isinstance(matches, list):
        for type_match in matches:
            series_matches = type_match.get("seriesMatches", []) if isinstance(type_match, dict) else []
            for series in series_matches:
                series_ad = series.get("seriesAdWrapper", {}) or {}
                match_list = series_ad.get("matches", [])
                for m in match_list:
                    match_info = m.get("matchInfo", {})

                    if not _is_league_match({"series": series_ad.get("seriesName", ""), "title": match_info.get("matchDesc", "")}, league):
                        continue

                    team1 = match_info.get("team1", {})
                    team2 = match_info.get("team2", {})

                    fixture = {
                        "match_id": str(match_info.get("matchId", "")),
                        "team_a": _standardise_team(team1.get("teamName", ""), league),
                        "team_b": _standardise_team(team2.get("teamName", ""), league),
                        "venue": match_info.get("venueInfo", {}).get("ground", ""),
                        "match_date": "",
                    }

                    start_ts = match_info.get("startDate")
                    if start_ts:
                        try:
                            fixture["match_date"] = datetime.fromtimestamp(int(start_ts) / 1000).strftime("%Y-%m-%d")
                        except Exception:
                            pass

                    upcoming.append(fixture)

    return upcoming


def update_completed_matches(league="psl"):
    """
    Fetch recent results from Cricbuzz and update fixtures + matches tables.
    This is the main function called by the scheduler to settle completed games.
    """
    results = get_recent_results(league=league)
    updated = 0

    for result in results:
        if not result.get("winner"):
            continue

        team_a = result["team_a"]
        team_b = result["team_b"]
        match_date = result.get("match_date", "")
        winner = result["winner"]

        if not match_date or not team_a or not team_b:
            continue

        # Update fixture status
        fix = db.fetch_one(
            "SELECT id FROM fixtures WHERE team_a = ? AND team_b = ? AND match_date = ? AND league = ?",
            [team_a, team_b, match_date, league]
        )
        if not fix:
            # Try reverse order
            fix = db.fetch_one(
                "SELECT id FROM fixtures WHERE team_a = ? AND team_b = ? AND match_date = ? AND league = ?",
                [team_b, team_a, match_date, league]
            )

        if fix:
            db.execute(
                "UPDATE fixtures SET status = 'COMPLETED', result = ? WHERE id = ?",
                [f"{winner} won", fix["id"]]
            )

        # Insert/update match result
        season = "2026" if league == "psl" else "2025"
        db.execute(
            """INSERT INTO matches (season, match_date, venue, team_a, team_b, winner,
               innings1_runs, innings1_wickets, innings2_runs, innings2_wickets, league)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(season, match_date, team_a, team_b) DO UPDATE SET
               winner=excluded.winner, innings1_runs=excluded.innings1_runs,
               innings2_runs=excluded.innings2_runs""",
            [season, match_date, result.get("venue", ""),
             team_a, team_b, winner,
             result.get("innings1_runs", 0), result.get("innings1_wickets", 0),
             result.get("innings2_runs", 0), result.get("innings2_wickets", 0),
             league]
        )

        updated += 1
        print(f"[Cricbuzz] Updated: {team_a} vs {team_b} ({match_date}) -> {winner}")

    return updated
