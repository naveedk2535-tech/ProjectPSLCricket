"""
The Odds API client for cricket betting odds.
Sport key: cricket_psl (or cricket_big_bash_league for testing)
Free tier: 500 requests/month
"""

import requests
from datetime import datetime

import config
from database import db
from data.rate_limiter import can_call, record_call, check_cache, save_cache
from data.team_names import standardise


def get_odds():
    """Fetch odds for all upcoming PSL matches."""
    cached = check_cache("odds_psl", config.CACHE_TTL["odds"])
    if cached:
        return cached.get("odds", [])

    if not can_call("odds_api"):
        return cached.get("odds", []) if cached else []

    url = f"{config.ODDS_API_BASE}/sports/{config.ODDS_API_SPORT}/odds"
    params = {
        "apiKey": config.ODDS_API_KEY,
        "regions": "us,uk,eu,au",
        "markets": "h2h,totals",
        "oddsFormat": "decimal",
    }

    try:
        resp = requests.get(url, params=params, timeout=15)
        record_call("odds_api", "odds", resp.status_code)

        if resp.status_code != 200:
            return []

        data = resp.json()
        odds_list = []

        for event in data:
            teams = [standardise(t) for t in event.get("teams", [])]
            if len(teams) < 2:
                commence = event.get("commence_time", "")
                bookmakers = event.get("bookmakers", [])

                # Find best odds across bookmakers
                best_odds = _find_best_odds(bookmakers, teams)
                if best_odds:
                    odds_entry = {
                        "match_date": commence[:10] if commence else "",
                        "team_a": teams[0] if teams else "",
                        "team_b": teams[1] if len(teams) > 1 else "",
                        **best_odds,
                        "fetched_at": db.now_iso(),
                    }
                    odds_list.append(odds_entry)
                continue

            commence = event.get("commence_time", "")
            bookmakers = event.get("bookmakers", [])

            best_odds = _find_best_odds(bookmakers, teams)
            if best_odds:
                odds_entry = {
                    "match_date": commence[:10] if commence else "",
                    "team_a": teams[0],
                    "team_b": teams[1],
                    **best_odds,
                    "fetched_at": db.now_iso(),
                }
                odds_list.append(odds_entry)

        save_cache("odds_psl", {"odds": odds_list, "fetched_at": db.now_iso()})
        return odds_list

    except requests.RequestException as e:
        print(f"[OddsAPI] Error: {e}")
        record_call("odds_api", "odds", 0)
        return []


def _find_best_odds(bookmakers, teams):
    """Find best available odds across all bookmakers."""
    if not bookmakers or len(teams) < 2:
        return None

    best_a = 0.0
    best_b = 0.0
    best_bookmaker_a = ""
    best_bookmaker_b = ""
    over_under_line = None
    over_odds = None
    under_odds = None

    for bm in bookmakers:
        bm_name = bm.get("title", "")
        for market in bm.get("markets", []):
            if market.get("key") == "h2h":
                outcomes = market.get("outcomes", [])
                for outcome in outcomes:
                    name = standardise(outcome.get("name", ""))
                    price = outcome.get("price", 0)
                    if name == teams[0] and price > best_a:
                        best_a = price
                        best_bookmaker_a = bm_name
                    elif name == teams[1] and price > best_b:
                        best_b = price
                        best_bookmaker_b = bm_name

            elif market.get("key") == "totals":
                outcomes = market.get("outcomes", [])
                for outcome in outcomes:
                    if outcome.get("name") == "Over":
                        over_under_line = outcome.get("point")
                        over_odds = outcome.get("price")
                    elif outcome.get("name") == "Under":
                        under_odds = outcome.get("price")

    if best_a <= 0 or best_b <= 0:
        return None

    implied_a = 1 / best_a if best_a > 0 else 0
    implied_b = 1 / best_b if best_b > 0 else 0
    margin = (implied_a + implied_b - 1) * 100

    return {
        "team_a_odds": best_a,
        "team_b_odds": best_b,
        "bookmaker": f"{best_bookmaker_a}/{best_bookmaker_b}",
        "implied_prob_a": round(implied_a, 4),
        "implied_prob_b": round(implied_b, 4),
        "margin": round(margin, 2),
        "over_under_line": over_under_line,
        "over_odds": over_odds,
        "under_odds": under_odds,
    }


def save_odds_to_db(odds_list):
    """Save odds to database."""
    for o in odds_list:
        db.execute(
            """INSERT INTO odds (match_date, team_a, team_b, team_a_odds, team_b_odds,
               over_under_line, over_odds, under_odds, bookmaker,
               implied_prob_a, implied_prob_b, margin, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(match_date, team_a, team_b, bookmaker) DO UPDATE SET
               team_a_odds=excluded.team_a_odds, team_b_odds=excluded.team_b_odds,
               over_under_line=excluded.over_under_line, over_odds=excluded.over_odds,
               under_odds=excluded.under_odds, implied_prob_a=excluded.implied_prob_a,
               implied_prob_b=excluded.implied_prob_b, margin=excluded.margin,
               fetched_at=excluded.fetched_at""",
            [o["match_date"], o["team_a"], o["team_b"],
             o["team_a_odds"], o["team_b_odds"],
             o.get("over_under_line"), o.get("over_odds"), o.get("under_odds"),
             o.get("bookmaker", "best"),
             o["implied_prob_a"], o["implied_prob_b"], o["margin"],
             o["fetched_at"]]
        )


def get_best_odds_for_match(team_a, team_b, match_date):
    """Get best available odds for a specific match."""
    return db.fetch_one(
        """SELECT * FROM odds WHERE match_date = ? AND team_a = ? AND team_b = ?
           ORDER BY team_a_odds DESC LIMIT 1""",
        [match_date, team_a, team_b]
    )
