"""
NewsAPI client for PSL/IPL cricket headline sentiment.
Free tier: 100 requests/day.
Uses 1 batch call per league instead of 1 call per team.
"""

import requests
from datetime import datetime, timedelta
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

import config
from database import db
from data.rate_limiter import can_call, record_call, check_cache, save_cache
from data.team_names import standardise, get_all_teams, get_abbreviation

analyzer = SentimentIntensityAnalyzer()

CRICKET_KEYWORDS = {
    "negative": ["injury", "banned", "suspended", "dropped", "ruled out",
                  "unfit", "defeat", "collapse", "poor", "struggling", "controversy"],
    "positive": ["debut", "captain", "recalled", "century", "hat-trick",
                 "comeback", "winning", "dominant", "brilliant", "record"],
}


def fetch_all_teams(league="psl"):
    """
    Fetch news sentiment for all teams in ONE API call.
    Search for the league name, then parse which teams are mentioned.
    """
    cache_key = f"news_all_{league}"
    cached = check_cache(cache_key, config.CACHE_TTL["sentiment"])
    if cached:
        return cached

    if not can_call("newsapi"):
        return cached or {}

    # One batch query for the whole league
    league_query = "PSL OR \"Pakistan Super League\"" if league == "psl" else "IPL OR \"Indian Premier League\""
    from_date = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")

    try:
        resp = requests.get(
            f"{config.NEWSAPI_BASE}/everything",
            params={
                "q": league_query,
                "from": from_date,
                "sortBy": "publishedAt",
                "language": "en",
                "pageSize": 100,
                "apiKey": config.NEWSAPI_KEY,
            },
            timeout=15,
        )
        record_call("newsapi", f"everything/{league}", resp.status_code)

        if resp.status_code != 200:
            return {}

        articles = resp.json().get("articles", [])

    except requests.RequestException as e:
        print(f"[NewsAPI] Error: {e}")
        record_call("newsapi", f"everything/{league}", 0)
        return {}

    # Get team list for this league
    if league == "psl":
        team_names = config.TEAM_NAMES
    else:
        team_names = config.IPL_TEAM_NAMES

    # Parse articles and assign to teams based on mentions
    results = {}
    for team_name in team_names:
        team_std = standardise(team_name)
        abbrev = get_abbreviation(team_name) or ""
        # Find articles mentioning this team
        team_articles = []
        for article in articles:
            text = f"{article.get('title', '')} {article.get('description', '')}"
            text_lower = text.lower()
            if (team_name.lower() in text_lower or
                team_std.lower() in text_lower or
                (abbrev and f" {abbrev.lower()} " in f" {text_lower} ")):
                team_articles.append(text)

        scores = []
        keywords_found = []
        for text in team_articles:
            sentiment = analyzer.polarity_scores(text)
            scores.append(sentiment["compound"])
            text_lower = text.lower()
            for kw in CRICKET_KEYWORDS["negative"]:
                if kw in text_lower:
                    keywords_found.append(f"-{kw}")
            for kw in CRICKET_KEYWORDS["positive"]:
                if kw in text_lower:
                    keywords_found.append(f"+{kw}")

        if scores:
            avg_score = sum(scores) / len(scores)
            positive = sum(1 for s in scores if s > 0.05) / len(scores) * 100
            negative = sum(1 for s in scores if s < -0.05) / len(scores) * 100
            neutral = 100 - positive - negative
            signal = "bullish" if avg_score > 0.15 else ("bearish" if avg_score < -0.15 else "neutral")
        else:
            avg_score, positive, negative, neutral = 0.0, 0.0, 0.0, 100.0
            signal = "neutral"

        # Calculate trend vs previous
        prev = db.fetch_one(
            "SELECT score FROM sentiment WHERE team = ? AND source = 'news' ORDER BY scored_at DESC LIMIT 1",
            [team_std]
        )
        trend = round(avg_score - prev["score"], 3) if prev else 0.0

        result = {
            "team": team_std, "source": "news",
            "score": round(avg_score, 3), "trend": trend,
            "volume": len(team_articles),
            "positive_pct": round(positive, 1),
            "negative_pct": round(negative, 1),
            "neutral_pct": round(neutral, 1),
            "keywords": ",".join(list(set(keywords_found))[:10]),
            "signal": signal,
        }
        results[team_std] = result
        _save_sentiment(result)

    save_cache(cache_key, results)
    return results


# Keep backward compat
def fetch_team_news(team):
    """Fetch news for a single team (uses batch internally)."""
    all_results = fetch_all_teams()
    team_std = standardise(team)
    return all_results.get(team_std)


def _save_sentiment(data):
    """Save sentiment to database."""
    db.execute(
        """INSERT INTO sentiment (team, source, score, trend, volume,
           positive_pct, negative_pct, neutral_pct, keywords, signal, scored_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(team, source, scored_at) DO UPDATE SET
           score=excluded.score, trend=excluded.trend, volume=excluded.volume,
           keywords=excluded.keywords, signal=excluded.signal""",
        [data["team"], data["source"], data["score"], data["trend"],
         data["volume"], data["positive_pct"], data["negative_pct"],
         data["neutral_pct"], data["keywords"], data["signal"],
         datetime.utcnow().strftime("%Y-%m-%d")]
    )
