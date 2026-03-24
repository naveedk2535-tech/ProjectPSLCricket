"""
NewsAPI client for PSL cricket headline sentiment.
Free tier: 100 requests/day.
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


def fetch_team_news(team):
    """Fetch and analyze news sentiment for a PSL team."""
    team_name = standardise(team)
    cache_key = f"news_{team_name}".replace(" ", "_")
    cached = check_cache(cache_key, config.CACHE_TTL["sentiment"])
    if cached:
        return cached

    if not can_call("newsapi"):
        return cached

    query = f'"{team_name}" AND (cricket OR PSL)'
    from_date = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")

    try:
        resp = requests.get(
            f"{config.NEWSAPI_BASE}/everything",
            params={
                "q": query,
                "from": from_date,
                "sortBy": "publishedAt",
                "language": "en",
                "pageSize": 20,
                "apiKey": config.NEWSAPI_KEY,
            },
            timeout=10,
        )
        record_call("newsapi", f"everything/{team_name}", resp.status_code)

        if resp.status_code != 200:
            return None

        data = resp.json()
        articles = data.get("articles", [])

    except requests.RequestException as e:
        print(f"[NewsAPI] Error for {team_name}: {e}")
        record_call("newsapi", f"everything/{team_name}", 0)
        return None

    scores = []
    keywords_found = []

    for article in articles:
        text = f"{article.get('title', '')} {article.get('description', '')}"
        sentiment = analyzer.polarity_scores(text)
        scores.append(sentiment["compound"])

        text_lower = text.lower()
        for kw in CRICKET_KEYWORDS["negative"]:
            if kw in text_lower:
                keywords_found.append(f"-{kw}")
        for kw in CRICKET_KEYWORDS["positive"]:
            if kw in text_lower:
                keywords_found.append(f"+{kw}")

    if not scores:
        result = {
            "team": team_name, "source": "news",
            "score": 0.0, "trend": 0.0, "volume": 0,
            "positive_pct": 0.0, "negative_pct": 0.0, "neutral_pct": 100.0,
            "keywords": "", "signal": "neutral",
        }
    else:
        avg_score = sum(scores) / len(scores)
        positive = sum(1 for s in scores if s > 0.05) / len(scores) * 100
        negative = sum(1 for s in scores if s < -0.05) / len(scores) * 100
        neutral = 100 - positive - negative

        signal = "neutral"
        if avg_score > 0.15:
            signal = "bullish"
        elif avg_score < -0.15:
            signal = "bearish"

        result = {
            "team": team_name, "source": "news",
            "score": round(avg_score, 3),
            "trend": 0.0,
            "volume": len(articles),
            "positive_pct": round(positive, 1),
            "negative_pct": round(negative, 1),
            "neutral_pct": round(neutral, 1),
            "keywords": ",".join(list(set(keywords_found))[:10]),
            "signal": signal,
        }

    prev = db.fetch_one(
        "SELECT score FROM sentiment WHERE team = ? AND source = 'news' ORDER BY scored_at DESC LIMIT 1",
        [team_name]
    )
    if prev:
        result["trend"] = round(result["score"] - prev["score"], 3)

    save_cache(cache_key, result)
    return result


def fetch_all_teams():
    """Fetch news sentiment for all PSL teams."""
    results = {}
    for team in get_all_teams():
        sentiment = fetch_team_news(team)
        if sentiment:
            results[team] = sentiment
            _save_sentiment(sentiment)
    return results


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
