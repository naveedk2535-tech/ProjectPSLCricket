"""
Cricket match results scraper — multi-source approach.

Sources (tried in order):
  1. PSL: hblpsl.com/matches (official PSL site, HTML scraping)
  2. IPL: ESPN API (site.api.espn.com, clean JSON)
  3. Fallback: Cricbuzz HTML scraping (live-scores, recent-matches)

Replaces the dead cricbuzz-live.vercel.app (402 errors).
"""

import re
import json
import requests
from datetime import datetime, timedelta

from database import db
from data.rate_limiter import can_call, record_call, check_cache, save_cache
from data.team_names import standardise, standardise_venue

# ── Browser-like headers to avoid blocks ──────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# ── PSL team name mapping (abbreviation / site variants → canonical) ──────────
PSL_TEAM_MAP = {
    # Full names
    "lahore qalandars": "Lahore Qalandars",
    "karachi kings": "Karachi Kings",
    "islamabad united": "Islamabad United",
    "peshawar zalmi": "Peshawar Zalmi",
    "multan sultans": "Multan Sultans",
    "quetta gladiators": "Quetta Gladiators",
    "hyderabad kingsmen": "Hyderabad Kingsmen",
    "rawalpindi pindiz": "Rawalpindi Pindiz",
    # Common abbreviations
    "lhq": "Lahore Qalandars",
    "lq": "Lahore Qalandars",
    "lahore": "Lahore Qalandars",
    "krk": "Karachi Kings",
    "kk": "Karachi Kings",
    "karachi": "Karachi Kings",
    "isu": "Islamabad United",
    "islamabad": "Islamabad United",
    "psz": "Peshawar Zalmi",
    "pz": "Peshawar Zalmi",
    "peshawar": "Peshawar Zalmi",
    "ms": "Multan Sultans",
    "multan": "Multan Sultans",
    "qtg": "Quetta Gladiators",
    "qg": "Quetta Gladiators",
    "quetta": "Quetta Gladiators",
    "hydk": "Hyderabad Kingsmen",
    "hk": "Hyderabad Kingsmen",
    "hyderabad kingsmen": "Hyderabad Kingsmen",
    "rwp": "Rawalpindi Pindiz",
    "rp": "Rawalpindi Pindiz",
    "rawalpindi": "Rawalpindi Pindiz",
}

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
    "rajasthan royals": "Rajasthan Royals",
    "rr": "Rajasthan Royals",
    "sunrisers hyderabad": "Sunrisers Hyderabad",
    "srh": "Sunrisers Hyderabad",
    "gujarat titans": "Gujarat Titans",
    "gt": "Gujarat Titans",
    "lucknow super giants": "Lucknow Super Giants",
    "lsg": "Lucknow Super Giants",
}

# Known Cricbuzz PSL 2026 match IDs (for direct scraping)
CRICBUZZ_PSL_MATCH_IDS = {
    148962: "lq-vs-hk",
    148963: "qg-vs-kk",
    148973: "pz-vs-rwp",
}

# ── ESPN league IDs ───────────────────────────────────────────────────────────
ESPN_LEAGUE_IDS = {
    "ipl": "8048",
    "psl": "8953",  # PSL on ESPN (may not always have data)
}

CACHE_TTL = 600  # 10 minutes


# ─────────────────────────────────────────────────────────────────────────────
# Helper functions
# ─────────────────────────────────────────────────────────────────────────────

def _standardise_team(name, league="psl"):
    """Map a scraped team name to our canonical name."""
    if not name:
        return ""
    name_clean = name.strip()
    name_lower = name_clean.lower().strip()
    team_map = PSL_TEAM_MAP if league == "psl" else IPL_TEAM_MAP

    # Direct lookup
    if name_lower in team_map:
        return team_map[name_lower]

    # Partial match — check if any key is contained in the name or vice versa
    for key, val in team_map.items():
        if len(key) > 2 and (key in name_lower or name_lower in key):
            return val

    # Fall back to the global standardise()
    return standardise(name_clean)


def _fetch_html(url, cache_key=None):
    """Fetch HTML from a URL with caching and browser headers."""
    if cache_key:
        cached = check_cache(cache_key, CACHE_TTL)
        if cached and isinstance(cached, dict) and cached.get("html"):
            return cached["html"]

    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            print(f"[CricScrape] HTTP {resp.status_code} from {url}")
            return None
        html = resp.text
        if cache_key:
            save_cache(cache_key, {"html": html, "fetched_at": datetime.now().isoformat()})
        return html
    except Exception as e:
        print(f"[CricScrape] Error fetching {url}: {e}")
        return None


def _fetch_json(url, cache_key=None):
    """Fetch JSON from a URL with caching."""
    if cache_key:
        cached = check_cache(cache_key, CACHE_TTL)
        if cached:
            return cached

    try:
        resp = requests.get(url, headers={**HEADERS, "Accept": "application/json"}, timeout=20)
        if resp.status_code != 200:
            print(f"[CricScrape] HTTP {resp.status_code} from {url}")
            return None
        data = resp.json()
        if cache_key:
            save_cache(cache_key, data)
        return data
    except Exception as e:
        print(f"[CricScrape] Error fetching {url}: {e}")
        return None


def _parse_score(score_str):
    """Parse a score string like '178/5' or '178/5 (20)' into (runs, wickets)."""
    if not score_str:
        return 0, 0
    score_str = str(score_str).strip()
    m = re.search(r'(\d+)[/-](\d+)', score_str)
    if m:
        return int(m.group(1)), int(m.group(2))
    # All out — just a number
    m2 = re.match(r'^(\d+)$', score_str.split('(')[0].strip())
    if m2:
        return int(m2.group(1)), 10
    return 0, 0


def _parse_overs(score_str):
    """Extract overs from a score string like '178/5 (19.3)'."""
    if not score_str:
        return 0.0
    m = re.search(r'\((\d+\.?\d*)\)', str(score_str))
    if m:
        return float(m.group(1))
    return 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Source 1: Official PSL site (hblpsl.com)
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_psl_official():
    """Scrape match results from hblpsl.com/matches."""
    url = "https://hblpsl.com/matches"
    html = _fetch_html(url, cache_key="psl_official_matches")
    if not html:
        return []

    results = []

    try:
        from bs4 import BeautifulSoup
        results = _parse_psl_with_bs4(html)
    except ImportError:
        results = _parse_psl_with_regex(html)

    print(f"[PSL Official] Scraped {len(results)} completed matches")
    return results


def _parse_psl_with_bs4(html):
    """Parse PSL official site HTML using BeautifulSoup."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    results = []

    # Look for match cards — the PSL site typically has match result sections
    # Try multiple selectors since site structure may vary
    match_cards = soup.find_all("div", class_=re.compile(r"match|fixture|result|card", re.I))
    if not match_cards:
        # Broader search: any section with score-like content
        match_cards = soup.find_all(["div", "section", "article"])

    for card in match_cards:
        text = card.get_text(" ", strip=True)
        # Must contain a score pattern like "123/4" and team names
        if not re.search(r'\d{2,3}[/-]\d{1,2}', text):
            continue

        result = _extract_match_from_text(text, "psl")
        if result and result.get("winner"):
            # Avoid duplicates
            dup = False
            for existing in results:
                if existing["team_a"] == result["team_a"] and existing["team_b"] == result["team_b"] and existing.get("match_date") == result.get("match_date"):
                    dup = True
                    break
            if not dup:
                results.append(result)

    # If structured parsing didn't work, try the full-page regex approach
    if not results:
        results = _parse_psl_with_regex(html)

    return results


def _parse_psl_with_regex(html):
    """Parse PSL official site HTML using regex patterns."""
    results = []

    # Strip HTML tags for text analysis
    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'\s+', ' ', text)

    # PSL team names to look for
    psl_teams = [
        "Lahore Qalandars", "Karachi Kings", "Islamabad United",
        "Peshawar Zalmi", "Multan Sultans", "Quetta Gladiators",
        "Hyderabad Kingsmen", "Rawalpindi Pindiz"
    ]

    # Pattern: Team1 score vs Team2 score (or similar)
    # E.g., "Lahore Qalandars 178/5 (20) ... Karachi Kings 165/8 (20)"
    for i, team_a_name in enumerate(psl_teams):
        for team_b_name in psl_teams[i + 1:]:
            # Look for both teams near each other with scores
            pattern = (
                rf'({re.escape(team_a_name)})\s+.*?'
                rf'(\d{{2,3}}[/-]\d{{1,2}}(?:\s*\(\d+\.?\d*\))?)\s+.*?'
                rf'({re.escape(team_b_name)})\s+.*?'
                rf'(\d{{2,3}}[/-]\d{{1,2}}(?:\s*\(\d+\.?\d*\))?)'
            )
            for m in re.finditer(pattern, text, re.IGNORECASE | re.DOTALL):
                ta = _standardise_team(m.group(1), "psl")
                score_a = m.group(2)
                tb = _standardise_team(m.group(3), "psl")
                score_b = m.group(4)

                runs_a, wkts_a = _parse_score(score_a)
                runs_b, wkts_b = _parse_score(score_b)

                if runs_a == 0 and runs_b == 0:
                    continue

                winner = ta if runs_a > runs_b else tb if runs_b > runs_a else None

                results.append({
                    "team_a": ta,
                    "team_b": tb,
                    "match_date": "",  # Hard to get from regex; will match by teams
                    "winner": winner,
                    "innings1_runs": runs_a,
                    "innings1_wickets": wkts_a,
                    "innings2_runs": runs_b,
                    "innings2_wickets": wkts_b,
                    "venue": "",
                    "status": "COMPLETED",
                    "source": "psl_official",
                })

            # Also try reverse order (team_b first)
            pattern_rev = (
                rf'({re.escape(team_b_name)})\s+.*?'
                rf'(\d{{2,3}}[/-]\d{{1,2}}(?:\s*\(\d+\.?\d*\))?)\s+.*?'
                rf'({re.escape(team_a_name)})\s+.*?'
                rf'(\d{{2,3}}[/-]\d{{1,2}}(?:\s*\(\d+\.?\d*\))?)'
            )
            for m in re.finditer(pattern_rev, text, re.IGNORECASE | re.DOTALL):
                tb2 = _standardise_team(m.group(1), "psl")
                score_b2 = m.group(2)
                ta2 = _standardise_team(m.group(3), "psl")
                score_a2 = m.group(4)

                runs_b2, wkts_b2 = _parse_score(score_b2)
                runs_a2, wkts_a2 = _parse_score(score_a2)

                if runs_a2 == 0 and runs_b2 == 0:
                    continue

                winner = ta2 if runs_a2 > runs_b2 else tb2 if runs_b2 > runs_a2 else None

                # Deduplicate
                dup = False
                for existing in results:
                    if set([existing["team_a"], existing["team_b"]]) == set([ta2, tb2]):
                        dup = True
                        break
                if not dup:
                    results.append({
                        "team_a": ta2,
                        "team_b": tb2,
                        "match_date": "",
                        "winner": winner,
                        "innings1_runs": runs_a2,
                        "innings1_wickets": wkts_a2,
                        "innings2_runs": runs_b2,
                        "innings2_wickets": wkts_b2,
                        "venue": "",
                        "status": "COMPLETED",
                        "source": "psl_official",
                    })

    # Also try to find dates near team mentions
    date_pattern = r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{4})'
    for result in results:
        # Look for dates near the team names in the text
        ta_pos = text.lower().find(result["team_a"].lower())
        if ta_pos >= 0:
            nearby = text[max(0, ta_pos - 200):ta_pos + 500]
            date_match = re.search(date_pattern, nearby, re.IGNORECASE)
            if date_match:
                try:
                    # Try multiple date formats
                    for fmt in ["%d %B %Y", "%d %b %Y"]:
                        try:
                            dt = datetime.strptime(date_match.group(1).strip(), fmt)
                            result["match_date"] = dt.strftime("%Y-%m-%d")
                            break
                        except ValueError:
                            continue
                except Exception:
                    pass

    return results


def _extract_match_from_text(text, league):
    """Extract match result from a block of text containing team names and scores."""
    psl_teams = list(PSL_TEAM_MAP.values()) if league == "psl" else list(IPL_TEAM_MAP.values())
    # Deduplicate
    psl_teams = list(set(psl_teams))

    found_teams = []
    for team in psl_teams:
        if team.lower() in text.lower():
            found_teams.append(team)

    if len(found_teams) < 2:
        return None

    # Get scores
    scores = re.findall(r'(\d{2,3})[/-](\d{1,2})', text)
    if len(scores) < 2:
        return None

    team_a = found_teams[0]
    team_b = found_teams[1]
    runs_a, wkts_a = int(scores[0][0]), int(scores[0][1])
    runs_b, wkts_b = int(scores[1][0]), int(scores[1][1])

    # Determine winner
    winner = None
    text_lower = text.lower()
    if "won" in text_lower:
        for t in found_teams:
            # Check if "TeamName won" appears
            if re.search(rf'{re.escape(t)}\s+won', text, re.IGNORECASE):
                winner = t
                break
    if not winner:
        winner = team_a if runs_a > runs_b else team_b if runs_b > runs_a else None

    # Try to find date
    match_date = ""
    date_match = re.search(r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{4})', text, re.IGNORECASE)
    if date_match:
        for fmt in ["%d %B %Y", "%d %b %Y"]:
            try:
                dt = datetime.strptime(date_match.group(1).strip(), fmt)
                match_date = dt.strftime("%Y-%m-%d")
                break
            except ValueError:
                continue

    return {
        "team_a": _standardise_team(team_a, league),
        "team_b": _standardise_team(team_b, league),
        "match_date": match_date,
        "winner": _standardise_team(winner, league) if winner else None,
        "innings1_runs": runs_a,
        "innings1_wickets": wkts_a,
        "innings2_runs": runs_b,
        "innings2_wickets": wkts_b,
        "venue": "",
        "status": "COMPLETED",
        "source": "psl_official",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Source 2: ESPN API (works great for IPL, may also have PSL)
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_espn_results(league="ipl"):
    """Fetch match results from ESPN cricket API."""
    league_id = ESPN_LEAGUE_IDS.get(league, ESPN_LEAGUE_IDS.get("ipl"))
    url = f"https://site.api.espn.com/apis/site/v2/sports/cricket/{league_id}/scoreboard"
    cache_key = f"espn_{league}_scoreboard"
    data = _fetch_json(url, cache_key)
    if not data:
        return []

    results = []
    events = data.get("events", [])

    for event in events:
        try:
            competition = event.get("competitions", [{}])[0]
            competitors = competition.get("competitors", [])
            if len(competitors) < 2:
                continue

            status_obj = event.get("status", {})
            status_type = status_obj.get("type", {})
            is_completed = status_type.get("completed", False)
            status_name = status_type.get("name", "")
            status_text = status_obj.get("type", {}).get("shortDetail", "")

            # Parse teams
            team_a_info = competitors[0]
            team_b_info = competitors[1]
            team_a_name = team_a_info.get("team", {}).get("displayName", "")
            team_b_name = team_b_info.get("team", {}).get("displayName", "")

            ta = _standardise_team(team_a_name, league)
            tb = _standardise_team(team_b_name, league)

            # Parse scores
            score_a = team_a_info.get("score", "0")
            score_b = team_b_info.get("score", "0")
            # ESPN sometimes gives just runs as the score
            runs_a, wkts_a = _parse_score(score_a)
            runs_b, wkts_b = _parse_score(score_b)

            # If score is just a number (ESPN often does this), wickets may not be present
            if runs_a == 0 and wkts_a == 0:
                try:
                    runs_a = int(score_a)
                except (ValueError, TypeError):
                    pass
            if runs_b == 0 and wkts_b == 0:
                try:
                    runs_b = int(score_b)
                except (ValueError, TypeError):
                    pass

            # Try to get detailed score from linescores
            linescores_a = team_a_info.get("linescores", [])
            linescores_b = team_b_info.get("linescores", [])
            if linescores_a:
                inn1 = linescores_a[0] if linescores_a else {}
                runs_a = inn1.get("runs", runs_a) or runs_a
                wkts_a = inn1.get("wickets", wkts_a) or wkts_a
            if linescores_b:
                inn1 = linescores_b[0] if linescores_b else {}
                runs_b = inn1.get("runs", runs_b) or runs_b
                wkts_b = inn1.get("wickets", wkts_b) or wkts_b

            # Parse date
            match_date = ""
            date_str = event.get("date", "")
            if date_str:
                try:
                    dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                    match_date = dt.strftime("%Y-%m-%d")
                except Exception:
                    pass

            # Determine winner
            winner = None
            if is_completed:
                a_winner = team_a_info.get("winner", False)
                b_winner = team_b_info.get("winner", False)
                if a_winner:
                    winner = ta
                elif b_winner:
                    winner = tb
                else:
                    # Fall back to comparing scores
                    if runs_a > runs_b:
                        winner = ta
                    elif runs_b > runs_a:
                        winner = tb

            venue = ""
            venue_info = competition.get("venue", {})
            if venue_info:
                venue = venue_info.get("fullName", venue_info.get("shortName", ""))

            match_result = {
                "match_id": str(event.get("id", "")),
                "team_a": ta,
                "team_b": tb,
                "match_date": match_date,
                "winner": winner,
                "innings1_runs": int(runs_a) if runs_a else 0,
                "innings1_wickets": int(wkts_a) if wkts_a else 0,
                "innings2_runs": int(runs_b) if runs_b else 0,
                "innings2_wickets": int(wkts_b) if wkts_b else 0,
                "venue": venue,
                "status": "COMPLETED" if is_completed else status_name,
                "source": "espn",
            }

            if is_completed and winner:
                results.append(match_result)
            elif not is_completed:
                # Still useful for live tracking; append with live status
                match_result["status"] = "LIVE"
                results.append(match_result)

        except Exception as e:
            print(f"[ESPN] Error parsing event: {e}")
            continue

    print(f"[ESPN] Found {len(results)} matches for {league}")
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Source 3: Cricbuzz HTML scraping (fallback)
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_cricbuzz_recent():
    """Scrape recent match results from Cricbuzz HTML."""
    url = "https://www.cricbuzz.com/cricket-match/live-scores/recent-matches"
    html = _fetch_html(url, cache_key="cricbuzz_recent_html")
    if not html:
        return []

    return _parse_cricbuzz_matches_html(html)


def _scrape_cricbuzz_live():
    """Scrape live matches from Cricbuzz HTML."""
    url = "https://www.cricbuzz.com/cricket-match/live-scores"
    html = _fetch_html(url, cache_key="cricbuzz_live_html")
    if not html:
        return []

    return _parse_cricbuzz_matches_html(html, live_only=True)


def _scrape_cricbuzz_match(match_id, slug="match"):
    """Scrape a specific Cricbuzz match page."""
    url = f"https://www.cricbuzz.com/live-cricket-scores/{match_id}/{slug}"
    cache_key = f"cricbuzz_match_{match_id}"
    html = _fetch_html(url, cache_key)
    if not html:
        return None

    return _parse_cricbuzz_match_page(html)


def _parse_cricbuzz_matches_html(html, live_only=False):
    """Parse Cricbuzz match listing pages (recent or live)."""
    results = []

    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")

        # Cricbuzz uses div.cb-mtch-lst for match listings
        match_sections = soup.find_all("div", class_=re.compile(r"cb-mtch-lst|cb-col-100|match-info", re.I))

        for section in match_sections:
            text = section.get_text(" ", strip=True)
            text_lower = text.lower()

            # Filter for PSL matches
            if not any(kw in text_lower for kw in ["psl", "pakistan super league", "super league"]):
                # Check if any PSL team is mentioned
                psl_teams_lower = [t.lower() for t in set(PSL_TEAM_MAP.values())]
                if not any(t in text_lower for t in psl_teams_lower):
                    continue

            result = _extract_match_from_text(text, "psl")
            if result:
                result["source"] = "cricbuzz"
                if live_only:
                    if "won" not in text_lower:
                        result["status"] = "LIVE"
                        results.append(result)
                else:
                    if result.get("winner"):
                        results.append(result)
    except ImportError:
        # Regex fallback
        # Strip tags
        text = re.sub(r'<[^>]+>', '\n', html)
        blocks = text.split('\n\n')
        for block in blocks:
            block_lower = block.lower()
            if not any(kw in block_lower for kw in ["psl", "pakistan super league"]):
                continue
            result = _extract_match_from_text(block, "psl")
            if result and result.get("winner"):
                result["source"] = "cricbuzz"
                results.append(result)

    print(f"[Cricbuzz HTML] Found {len(results)} matches")
    return results


def _parse_cricbuzz_match_page(html):
    """Parse a single Cricbuzz match detail page."""
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)
    except ImportError:
        text = re.sub(r'<[^>]+>', ' ', html)
        text = re.sub(r'\s+', ' ', text)

    return _extract_match_from_text(text, "psl")


# ─────────────────────────────────────────────────────────────────────────────
# Public API — main functions
# ─────────────────────────────────────────────────────────────────────────────

def get_recent_results(league="psl"):
    """
    Fetch recent completed matches for the given league.
    Tries multiple sources in order of reliability.

    Returns list of dicts with keys:
      team_a, team_b, match_date, winner,
      innings1_runs, innings1_wickets, innings2_runs, innings2_wickets
    """
    results = []

    if league == "psl":
        # Source 1: Official PSL site
        try:
            psl_results = _scrape_psl_official()
            if psl_results:
                results.extend(psl_results)
                print(f"[get_recent_results] Got {len(psl_results)} from PSL official")
        except Exception as e:
            print(f"[get_recent_results] PSL official scrape failed: {e}")

        # Source 2: ESPN (may have PSL data)
        if not results:
            try:
                espn_results = _fetch_espn_results(league="psl")
                completed = [r for r in espn_results if r.get("winner")]
                if completed:
                    results.extend(completed)
                    print(f"[get_recent_results] Got {len(completed)} from ESPN PSL")
            except Exception as e:
                print(f"[get_recent_results] ESPN PSL failed: {e}")

        # Source 3: Cricbuzz HTML scraping
        if not results:
            try:
                cb_results = _scrape_cricbuzz_recent()
                if cb_results:
                    results.extend(cb_results)
                    print(f"[get_recent_results] Got {len(cb_results)} from Cricbuzz HTML")
            except Exception as e:
                print(f"[get_recent_results] Cricbuzz scrape failed: {e}")

        # Source 4: Known Cricbuzz match IDs (direct scrape)
        if not results:
            for mid, slug in CRICBUZZ_PSL_MATCH_IDS.items():
                try:
                    m = _scrape_cricbuzz_match(mid, slug)
                    if m and m.get("winner"):
                        results.append(m)
                except Exception as e:
                    print(f"[get_recent_results] Cricbuzz match {mid} failed: {e}")

    elif league == "ipl":
        # IPL: ESPN is the primary (cleanest) source
        try:
            espn_results = _fetch_espn_results(league="ipl")
            completed = [r for r in espn_results if r.get("winner")]
            results.extend(completed)
            print(f"[get_recent_results] Got {len(completed)} IPL results from ESPN")
        except Exception as e:
            print(f"[get_recent_results] ESPN IPL failed: {e}")

    # Deduplicate by (team_a, team_b, date) — keep first occurrence
    seen = set()
    deduped = []
    for r in results:
        key = tuple(sorted([r["team_a"], r["team_b"]])) + (r.get("match_date", ""),)
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    record_call("cricbuzz_scrape", endpoint=f"get_recent_results/{league}", response_code=200)
    return deduped


def get_live_matches(league="psl"):
    """
    Fetch currently live matches for the given league.

    Returns list of dicts with keys:
      team_a, team_b, score_a, score_b, status, venue
    """
    live = []

    if league == "psl":
        # Try ESPN first (may have live PSL data)
        try:
            espn_results = _fetch_espn_results(league="psl")
            for r in espn_results:
                if r.get("status") == "LIVE":
                    live.append({
                        "match_id": r.get("match_id", ""),
                        "team_a": r["team_a"],
                        "team_b": r["team_b"],
                        "score_a": f"{r['innings1_runs']}/{r['innings1_wickets']}" if r.get("innings1_runs") else "",
                        "score_b": f"{r['innings2_runs']}/{r['innings2_wickets']}" if r.get("innings2_runs") else "",
                        "status": r.get("status", "LIVE"),
                        "venue": r.get("venue", ""),
                        "source": "espn",
                    })
        except Exception as e:
            print(f"[get_live_matches] ESPN PSL live failed: {e}")

        # Try Cricbuzz HTML
        if not live:
            try:
                cb_live = _scrape_cricbuzz_live()
                for r in cb_live:
                    live.append({
                        "match_id": r.get("match_id", ""),
                        "team_a": r["team_a"],
                        "team_b": r["team_b"],
                        "score_a": f"{r['innings1_runs']}/{r['innings1_wickets']}" if r.get("innings1_runs") else "",
                        "score_b": f"{r['innings2_runs']}/{r['innings2_wickets']}" if r.get("innings2_runs") else "",
                        "status": "LIVE",
                        "venue": r.get("venue", ""),
                        "source": "cricbuzz",
                    })
            except Exception as e:
                print(f"[get_live_matches] Cricbuzz live scrape failed: {e}")

        # Try known match IDs on Cricbuzz
        if not live:
            for mid, slug in CRICBUZZ_PSL_MATCH_IDS.items():
                try:
                    m = _scrape_cricbuzz_match(mid, slug)
                    if m and not m.get("winner"):
                        # Not completed = possibly live
                        live.append({
                            "match_id": str(mid),
                            "team_a": m["team_a"],
                            "team_b": m["team_b"],
                            "score_a": f"{m['innings1_runs']}/{m['innings1_wickets']}",
                            "score_b": f"{m['innings2_runs']}/{m['innings2_wickets']}",
                            "status": "LIVE",
                            "venue": m.get("venue", ""),
                            "source": "cricbuzz",
                        })
                except Exception:
                    continue

    elif league == "ipl":
        try:
            espn_results = _fetch_espn_results(league="ipl")
            for r in espn_results:
                if r.get("status") == "LIVE":
                    live.append({
                        "match_id": r.get("match_id", ""),
                        "team_a": r["team_a"],
                        "team_b": r["team_b"],
                        "score_a": f"{r['innings1_runs']}/{r['innings1_wickets']}" if r.get("innings1_runs") else "",
                        "score_b": f"{r['innings2_runs']}/{r['innings2_wickets']}" if r.get("innings2_runs") else "",
                        "status": "LIVE",
                        "venue": r.get("venue", ""),
                        "source": "espn",
                    })
        except Exception as e:
            print(f"[get_live_matches] ESPN IPL live failed: {e}")

    record_call("cricbuzz_scrape", endpoint=f"get_live_matches/{league}", response_code=200)
    return live


def update_completed_matches(league="psl"):
    """
    Main function: fetch results from working sources and update
    fixtures + matches tables in the database.

    Called by the scheduler to settle completed games.
    Returns the number of matches updated.
    """
    results = get_recent_results(league=league)
    updated = 0

    for result in results:
        if not result.get("winner"):
            continue

        team_a = standardise(result["team_a"])
        team_b = standardise(result["team_b"])
        match_date = result.get("match_date", "")
        winner = standardise(result["winner"])
        venue = standardise_venue(result.get("venue", "")) if result.get("venue") else ""

        if not team_a or not team_b:
            continue

        # ── Update fixture status ─────────────────────────────────────────
        fix = None
        if match_date:
            fix = db.fetch_one(
                "SELECT id FROM fixtures WHERE team_a = ? AND team_b = ? AND match_date = ? AND league = ?",
                [team_a, team_b, match_date, league]
            )
            if not fix:
                # Try reverse team order
                fix = db.fetch_one(
                    "SELECT id FROM fixtures WHERE team_a = ? AND team_b = ? AND match_date = ? AND league = ?",
                    [team_b, team_a, match_date, league]
                )

        if not fix and match_date:
            # Try fuzzy date match (+-1 day, timezone differences)
            try:
                dt = datetime.strptime(match_date, "%Y-%m-%d")
                for delta in [-1, 1]:
                    alt_date = (dt + timedelta(days=delta)).strftime("%Y-%m-%d")
                    fix = db.fetch_one(
                        "SELECT id FROM fixtures WHERE team_a = ? AND team_b = ? AND match_date = ? AND league = ?",
                        [team_a, team_b, alt_date, league]
                    )
                    if not fix:
                        fix = db.fetch_one(
                            "SELECT id FROM fixtures WHERE team_a = ? AND team_b = ? AND match_date = ? AND league = ?",
                            [team_b, team_a, alt_date, league]
                        )
                    if fix:
                        break
            except ValueError:
                pass

        if not fix:
            # Last resort: match by teams only (for undated results), pick most recent scheduled
            fix = db.fetch_one(
                """SELECT id FROM fixtures
                   WHERE ((team_a = ? AND team_b = ?) OR (team_a = ? AND team_b = ?))
                   AND league = ? AND status = 'SCHEDULED'
                   ORDER BY match_date DESC LIMIT 1""",
                [team_a, team_b, team_b, team_a, league]
            )

        if fix:
            db.execute(
                "UPDATE fixtures SET status = 'COMPLETED', result = ?, updated_at = ? WHERE id = ?",
                [f"{winner} won", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), fix["id"]]
            )

        # ── Insert/update match result ────────────────────────────────────
        season = "2026" if league == "psl" else "2025"

        # We need a date — if none, try to get from fixture
        if not match_date and fix:
            fix_row = db.fetch_one("SELECT match_date FROM fixtures WHERE id = ?", [fix["id"]])
            if fix_row:
                match_date = fix_row["match_date"]

        if not match_date:
            match_date = datetime.now().strftime("%Y-%m-%d")

        db.execute(
            """INSERT INTO matches (season, match_date, venue, team_a, team_b, winner,
               innings1_runs, innings1_wickets, innings2_runs, innings2_wickets, league)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(season, match_date, team_a, team_b) DO UPDATE SET
               winner=excluded.winner,
               innings1_runs=excluded.innings1_runs, innings1_wickets=excluded.innings1_wickets,
               innings2_runs=excluded.innings2_runs, innings2_wickets=excluded.innings2_wickets,
               venue=COALESCE(NULLIF(excluded.venue, ''), venue)""",
            [season, match_date, venue,
             team_a, team_b, winner,
             result.get("innings1_runs", 0), result.get("innings1_wickets", 0),
             result.get("innings2_runs", 0), result.get("innings2_wickets", 0),
             league]
        )

        updated += 1
        print(f"[CricScrape] Updated: {team_a} vs {team_b} ({match_date}) -> {winner}")

    print(f"[CricScrape] Total updated: {updated} matches for {league}")
    return updated


# ─────────────────────────────────────────────────────────────────────────────
# Convenience / backward-compatible functions
# ─────────────────────────────────────────────────────────────────────────────

def get_upcoming_matches(league="psl"):
    """
    Fetch upcoming matches. Currently relies on fixtures table
    since scraping upcoming is less reliable than results.
    Falls back to ESPN if available.
    """
    upcoming = []

    if league == "ipl":
        try:
            espn_results = _fetch_espn_results(league="ipl")
            for r in espn_results:
                if r.get("status") not in ("COMPLETED", "LIVE") and not r.get("winner"):
                    upcoming.append({
                        "match_id": r.get("match_id", ""),
                        "team_a": r["team_a"],
                        "team_b": r["team_b"],
                        "match_date": r.get("match_date", ""),
                        "venue": r.get("venue", ""),
                    })
        except Exception as e:
            print(f"[get_upcoming] ESPN failed: {e}")

    # For PSL, just pull from our fixtures table
    if not upcoming:
        rows = db.fetch_all(
            """SELECT team_a, team_b, match_date, venue
               FROM fixtures
               WHERE league = ? AND status = 'SCHEDULED' AND match_date >= date('now')
               ORDER BY match_date ASC LIMIT 20""",
            [league]
        )
        for row in (rows or []):
            upcoming.append({
                "team_a": row["team_a"],
                "team_b": row["team_b"],
                "match_date": row["match_date"],
                "venue": row.get("venue", ""),
            })

    return upcoming
