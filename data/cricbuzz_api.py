"""
Cricket match data — TheSportsDB API (primary) + ESPN fallback (IPL).

Sources:
  1. TheSportsDB (free, no key needed — use "3" as key)
     - PSL league ID: 5067
     - IPL league ID: 4460
  2. ESPN API (fallback for IPL only)

Replaces the old multi-source scraper (PSL site + Cricbuzz HTML).
"""

import json
import requests
from datetime import datetime

import config
from database import db
from data.rate_limiter import can_call, record_call, check_cache, save_cache
from data.team_names import standardise, standardise_venue

# ── TheSportsDB configuration ──────────────────────────────────────────────
SPORTSDB_BASE = "https://www.thesportsdb.com/api/v1/json/3"

LEAGUE_IDS = {
    "psl": 5067,
    "ipl": 4460,
}

SEASONS = {
    "psl": config.LEAGUES["psl"]["season"],   # "2026"
    "ipl": config.LEAGUES["ipl"]["season"],    # "2025"
}

# ── Team name pre-mapping (applied BEFORE standardise()) ────────────────────
# Maps TheSportsDB names that standardise() can't resolve on its own.
SPORTSDB_TEAM_MAP = {
    # PSL
    "Hyderabad Houston Kingsmen": "Hyderabad Kingsmen",
    "Pindiz": "Rawalpindi Pindiz",
    # IPL
    "Royal Challengers Bangalore": "Royal Challengers Bengaluru",
}

# ── IPL team mapping (for ESPN fallback) ────────────────────────────────────
IPL_TEAM_MAP = {
    "chennai super kings": "Chennai Super Kings",
    "csk": "Chennai Super Kings",
    "mumbai indians": "Mumbai Indians",
    "mi": "Mumbai Indians",
    "royal challengers bengaluru": "Royal Challengers Bengaluru",
    "royal challengers bangalore": "Royal Challengers Bengaluru",
    "rcb": "Royal Challengers Bengaluru",
    "kolkata knight riders": "Kolkata Knight Riders",
    "kkr": "Kolkata Knight Riders",
    "delhi capitals": "Delhi Capitals",
    "dc": "Delhi Capitals",
    "punjab kings": "Punjab Kings",
    "kings xi punjab": "Punjab Kings",
    "pbks": "Punjab Kings",
    "pk": "Punjab Kings",
    "rajasthan royals": "Rajasthan Royals",
    "rr": "Rajasthan Royals",
    "sunrisers hyderabad": "Sunrisers Hyderabad",
    "srh": "Sunrisers Hyderabad",
    "gujarat titans": "Gujarat Titans",
    "gt": "Gujarat Titans",
    "lucknow super giants": "Lucknow Super Giants",
    "lsg": "Lucknow Super Giants",
}

# ESPN endpoint for IPL fallback
ESPN_IPL_URL = "https://site.api.espn.com/apis/site/v2/sports/cricket/8048/scoreboard"

# Cache TTL: 10 minutes
CACHE_TTL = 600

# ── Helpers ─────────────────────────────────────────────────────────────────


def _map_team_name(name, league="psl"):
    """Map a TheSportsDB / ESPN team name to canonical form."""
    if not name:
        return name
    # Apply pre-mapping first
    mapped = SPORTSDB_TEAM_MAP.get(name, name)
    # Then run through standardise() for PSL teams
    if league == "psl":
        return standardise(mapped)
    # For IPL, try the IPL map, then return as-is
    lower = mapped.lower()
    if lower in IPL_TEAM_MAP:
        return IPL_TEAM_MAP[lower]
    return mapped


def _parse_score(raw):
    """Safely parse a score string to int (returns None for non-numeric)."""
    if raw is None:
        return None
    try:
        return int(raw)
    except (ValueError, TypeError):
        return None


def _is_live_status(status):
    """Check if a status string indicates a live/in-progress match."""
    if not status:
        return False
    live_keywords = ["1H", "2H", "HT", "innings", "In Progress",
                     "1st Innings", "2nd Innings", "Innings Break"]
    status_lower = status.lower()
    return any(kw.lower() in status_lower for kw in live_keywords)


# ── TheSportsDB API ────────────────────────────────────────────────────────


def _fetch_sportsdb_season(league="psl"):
    """
    Fetch all events for the current season from TheSportsDB.
    Returns raw list of event dicts, or [] on failure.
    Uses 10-minute cache.
    """
    league_id = LEAGUE_IDS.get(league)
    season = SEASONS.get(league)
    if not league_id or not season:
        print(f"[SportsDB] Unknown league: {league}")
        return []

    cache_key = f"sportsdb_{league}_{season}"
    cached = check_cache(cache_key, ttl_seconds=CACHE_TTL)
    if cached is not None:
        print(f"[SportsDB] Using cached data for {league.upper()} {season}")
        record_call("cricket_api", f"sportsdb_season_{league}", 200, cached=True)
        return cached

    if not can_call("cricket_api"):
        print("[SportsDB] Rate limit reached, falling back to cache")
        stale = check_cache(cache_key)
        return stale if stale else []

    url = f"{SPORTSDB_BASE}/eventsseason.php?id={league_id}&s={season}"
    print(f"[SportsDB] Fetching {league.upper()} {season}: {url}")

    try:
        resp = requests.get(url, timeout=15)
        record_call("cricket_api", url, resp.status_code)
        resp.raise_for_status()

        data = resp.json()
        events = data.get("events") or []
        save_cache(cache_key, events)
        print(f"[SportsDB] Got {len(events)} events for {league.upper()} {season}")
        return events

    except Exception as e:
        print(f"[SportsDB] Error fetching {league.upper()}: {e}")
        record_call("cricket_api", url, 500)
        stale = check_cache(cache_key)
        if stale:
            print("[SportsDB] Falling back to stale cache")
            return stale
        return []


def _parse_event(event, league="psl"):
    """Parse a single TheSportsDB event dict into a normalized match dict."""
    home_team = _map_team_name(event.get("strHomeTeam", ""), league)
    away_team = _map_team_name(event.get("strAwayTeam", ""), league)
    venue_raw = event.get("strVenue", "")
    venue = standardise_venue(venue_raw) if league == "psl" else venue_raw
    status = event.get("strStatus", "")
    home_score = _parse_score(event.get("intHomeScore"))
    away_score = _parse_score(event.get("intAwayScore"))
    match_date = event.get("dateEvent", "")
    match_time = event.get("strTime", "")
    event_id = event.get("idEvent", "")

    # Determine winner
    winner = None
    if home_score is not None and away_score is not None:
        if home_score > away_score:
            winner = home_team
        elif away_score > home_score:
            winner = away_team
        # Equal scores = super over or no result (winner stays None)

    # Determine our internal status
    if status == "Match Finished":
        internal_status = "COMPLETED"
    elif _is_live_status(status):
        internal_status = "LIVE"
    elif status == "Not Started" or status == "NS":
        internal_status = "SCHEDULED"
    elif "Postponed" in (status or ""):
        internal_status = "POSTPONED"
    elif "Cancelled" in (status or "") or "Abandoned" in (status or ""):
        internal_status = "ABANDONED"
    else:
        internal_status = "SCHEDULED"

    return {
        "event_id": str(event_id),
        "match_date": match_date,
        "match_time": match_time,
        "home_team": home_team,
        "away_team": away_team,
        "team_a": home_team,
        "team_b": away_team,
        "venue": venue,
        "home_score": home_score,
        "away_score": away_score,
        "winner": winner,
        "status": internal_status,
        "raw_status": status,
        "league": league,
        "season": SEASONS.get(league, ""),
    }


# ── ESPN fallback (IPL only) ───────────────────────────────────────────────


def _fetch_espn_ipl():
    """Fetch IPL scoreboard from ESPN API as fallback. Returns list of parsed match dicts."""
    cache_key = "espn_ipl_scoreboard"
    cached = check_cache(cache_key, ttl_seconds=CACHE_TTL)
    if cached is not None:
        print("[ESPN] Using cached IPL scoreboard")
        record_call("cricket_api", "espn_ipl", 200, cached=True)
        return cached

    if not can_call("cricket_api"):
        print("[ESPN] Rate limit reached")
        stale = check_cache(cache_key)
        return stale if stale else []

    print(f"[ESPN] Fetching IPL scoreboard: {ESPN_IPL_URL}")
    try:
        resp = requests.get(ESPN_IPL_URL, timeout=15)
        record_call("cricket_api", ESPN_IPL_URL, resp.status_code)
        resp.raise_for_status()
        data = resp.json()
        events = data.get("events", [])

        matches = []
        for ev in events:
            try:
                comp = ev["competitions"][0]
                teams = comp.get("competitors", [])
                if len(teams) < 2:
                    continue

                team_a_raw = teams[0].get("team", {}).get("displayName", "")
                team_b_raw = teams[1].get("team", {}).get("displayName", "")
                team_a = _map_team_name(team_a_raw, "ipl")
                team_b = _map_team_name(team_b_raw, "ipl")

                score_a = _parse_score(teams[0].get("score"))
                score_b = _parse_score(teams[1].get("score"))

                status_obj = ev.get("status", {}).get("type", {})
                completed = status_obj.get("completed", False)
                state = status_obj.get("state", "")

                venue_info = comp.get("venue", {})
                venue_name = venue_info.get("fullName", "")

                date_str = ev.get("date", "")[:10]

                winner = None
                if score_a is not None and score_b is not None and completed:
                    winner = team_a if score_a > score_b else team_b if score_b > score_a else None

                if completed:
                    internal_status = "COMPLETED"
                elif state == "in":
                    internal_status = "LIVE"
                else:
                    internal_status = "SCHEDULED"

                matches.append({
                    "event_id": ev.get("id", ""),
                    "match_date": date_str,
                    "match_time": "",
                    "home_team": team_a,
                    "away_team": team_b,
                    "team_a": team_a,
                    "team_b": team_b,
                    "venue": venue_name,
                    "home_score": score_a,
                    "away_score": score_b,
                    "winner": winner,
                    "status": internal_status,
                    "raw_status": state,
                    "league": "ipl",
                    "season": SEASONS.get("ipl", ""),
                })
            except (KeyError, IndexError):
                continue

        save_cache(cache_key, matches)
        print(f"[ESPN] Got {len(matches)} IPL matches")
        return matches

    except Exception as e:
        print(f"[ESPN] Error fetching IPL: {e}")
        record_call("cricket_api", ESPN_IPL_URL, 500)
        stale = check_cache(cache_key)
        return stale if stale else []


# ── Public API ──────────────────────────────────────────────────────────────


def get_season_matches(league="psl"):
    """
    Fetch ALL matches for the current season from TheSportsDB.
    Returns list of parsed match dicts.
    """
    events = _fetch_sportsdb_season(league)
    matches = [_parse_event(ev, league) for ev in events]
    print(f"[get_season_matches] {league.upper()}: {len(matches)} total matches")
    return matches


def get_recent_results(league="psl"):
    """
    Returns only completed matches with parsed scores and winner.
    Primary: TheSportsDB. Fallback for IPL: ESPN.
    """
    matches = get_season_matches(league)
    completed = [m for m in matches if m["status"] == "COMPLETED"]

    # If TheSportsDB returned nothing for IPL, try ESPN
    if not completed and league == "ipl":
        print("[get_recent_results] No IPL results from SportsDB, trying ESPN fallback")
        espn = _fetch_espn_ipl()
        completed = [m for m in espn if m["status"] == "COMPLETED"]

    # Sort by date descending (most recent first)
    completed.sort(key=lambda m: m["match_date"], reverse=True)
    print(f"[get_recent_results] {league.upper()}: {len(completed)} completed matches")
    return completed


def get_upcoming_fixtures(league="psl"):
    """
    Returns matches with status "Not Started" as upcoming fixtures.
    Primary: TheSportsDB. Fallback for IPL: ESPN.
    """
    matches = get_season_matches(league)
    upcoming = [m for m in matches if m["status"] == "SCHEDULED"]

    if not upcoming and league == "ipl":
        print("[get_upcoming_fixtures] No IPL fixtures from SportsDB, trying ESPN fallback")
        espn = _fetch_espn_ipl()
        upcoming = [m for m in espn if m["status"] == "SCHEDULED"]

    upcoming.sort(key=lambda m: m["match_date"])
    print(f"[get_upcoming_fixtures] {league.upper()}: {len(upcoming)} upcoming fixtures")
    return upcoming


def get_live_matches(league="psl"):
    """
    Returns matches currently in progress.
    Primary: TheSportsDB. Fallback for IPL: ESPN.
    """
    matches = get_season_matches(league)
    live = [m for m in matches if m["status"] == "LIVE"]

    if not live and league == "ipl":
        espn = _fetch_espn_ipl()
        live = [m for m in espn if m["status"] == "LIVE"]

    print(f"[get_live_matches] {league.upper()}: {len(live)} live matches")
    return live


def update_completed_matches(league="psl"):
    """
    Main update function: fetch completed results and upsert into DB.

    For each completed match:
      - Determine winner (higher score)
      - UPSERT into matches table
      - Update fixture status to COMPLETED
      - Log with print statements

    Returns count of updated matches.
    """
    results = get_recent_results(league)
    if not results:
        print(f"[update_completed_matches] No completed matches for {league.upper()}")
        return 0

    season = SEASONS.get(league, "")
    updated = 0

    for m in results:
        team_a = m["team_a"]
        team_b = m["team_b"]
        venue = m["venue"]
        match_date = m["match_date"]
        home_score = m["home_score"]
        away_score = m["away_score"]
        winner = m["winner"]

        if home_score is None or away_score is None:
            continue

        # Determine win margin and type
        if winner == team_a:
            win_margin = home_score - away_score
            win_type = "runs"  # Simplified — we don't have innings detail from this API
        elif winner == team_b:
            win_margin = away_score - home_score
            win_type = "runs"
        else:
            win_margin = 0
            win_type = "no_result"

        # UPSERT into matches table
        try:
            db.execute(
                """INSERT INTO matches (league, season, match_date, venue, team_a, team_b,
                                        innings1_runs, innings2_runs, winner, win_margin, win_type)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(season, match_date, team_a, team_b) DO UPDATE SET
                       venue = excluded.venue,
                       innings1_runs = excluded.innings1_runs,
                       innings2_runs = excluded.innings2_runs,
                       winner = excluded.winner,
                       win_margin = excluded.win_margin,
                       win_type = excluded.win_type,
                       league = excluded.league""",
                [league, season, match_date, venue, team_a, team_b,
                 home_score, away_score, winner, win_margin, win_type]
            )
            print(f"  [matches] {match_date}: {team_a} {home_score} vs {team_b} {away_score} → Winner: {winner}")
            updated += 1
        except Exception as e:
            print(f"  [matches] Error inserting {team_a} vs {team_b} ({match_date}): {e}")

        # Update fixture status to COMPLETED
        try:
            db.execute(
                """UPDATE fixtures SET status = 'COMPLETED', result = ?,
                       updated_at = ?
                   WHERE league = ? AND season = ? AND match_date = ?
                     AND team_a = ? AND team_b = ?
                     AND status != 'COMPLETED'""",
                [f"{winner} won by {win_margin} {win_type}" if winner else "No result",
                 db.now_iso(), league, season, match_date, team_a, team_b]
            )
        except Exception as e:
            print(f"  [fixtures] Error updating fixture status for {team_a} vs {team_b}: {e}")

        # Also try with teams swapped (fixture may have them in different order)
        try:
            db.execute(
                """UPDATE fixtures SET status = 'COMPLETED', result = ?,
                       updated_at = ?
                   WHERE league = ? AND season = ? AND match_date = ?
                     AND team_a = ? AND team_b = ?
                     AND status != 'COMPLETED'""",
                [f"{winner} won by {win_margin} {win_type}" if winner else "No result",
                 db.now_iso(), league, season, match_date, team_b, team_a]
            )
        except Exception:
            pass

    print(f"[update_completed_matches] {league.upper()}: {updated} matches updated")
    return updated


def sync_fixtures(league="psl"):
    """
    Sync upcoming fixtures from TheSportsDB into the fixtures table.
    Inserts new fixtures that don't exist yet. Doesn't overwrite existing ones.

    Returns count of newly inserted fixtures.
    """
    upcoming = get_upcoming_fixtures(league)
    if not upcoming:
        print(f"[sync_fixtures] No upcoming fixtures for {league.upper()}")
        return 0

    season = SEASONS.get(league, "")
    inserted = 0

    for m in upcoming:
        team_a = m["team_a"]
        team_b = m["team_b"]
        venue = m["venue"]
        match_date = m["match_date"]
        match_time = m.get("match_time", "")
        event_id = m.get("event_id", "")

        # Check if fixture already exists (either team order)
        existing = db.fetch_one(
            """SELECT id FROM fixtures
               WHERE league = ? AND season = ? AND match_date = ?
                 AND ((team_a = ? AND team_b = ?) OR (team_a = ? AND team_b = ?))""",
            [league, season, match_date, team_a, team_b, team_b, team_a]
        )

        if existing:
            continue

        try:
            db.execute(
                """INSERT INTO fixtures (league, season, match_date, match_time, venue,
                                         team_a, team_b, status, cricapi_id, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'SCHEDULED', ?, ?)""",
                [league, season, match_date, match_time, venue,
                 team_a, team_b, event_id, db.now_iso()]
            )
            print(f"  [fixtures] NEW: {match_date} {team_a} vs {team_b} @ {venue}")
            inserted += 1
        except Exception as e:
            print(f"  [fixtures] Error inserting {team_a} vs {team_b}: {e}")

    print(f"[sync_fixtures] {league.upper()}: {inserted} new fixtures inserted")
    return inserted


# ── Convenience / backward-compatible aliases ───────────────────────────────


def fetch_live_scores(league="psl"):
    """Alias for get_live_matches — used by live_predictor and scheduler."""
    return get_live_matches(league)


def fetch_recent_results(league="psl"):
    """Alias for get_recent_results — used by scheduler."""
    return get_recent_results(league)


def refresh_all(league="psl"):
    """
    Full refresh: sync fixtures + update completed matches.
    Called by scheduler and the Refresh Data button.
    """
    print(f"\n{'='*60}")
    print(f"[refresh_all] Starting full refresh for {league.upper()}")
    print(f"{'='*60}")

    fixtures_count = sync_fixtures(league)
    matches_count = update_completed_matches(league)

    print(f"[refresh_all] Done — {fixtures_count} new fixtures, {matches_count} match results updated")
    return {"fixtures_synced": fixtures_count, "matches_updated": matches_count}
