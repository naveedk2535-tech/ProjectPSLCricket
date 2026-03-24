"""
Reddit sentiment via PRAW for PSL cricket teams.
Subreddits: r/Cricket, r/PakCricket, r/PSL
Uses VADER sentiment scoring.
"""

import praw
from datetime import datetime, timedelta
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

import config
from database import db
from data.rate_limiter import can_call, record_call, check_cache, save_cache
from data.team_names import standardise, get_all_teams, get_abbreviation

analyzer = SentimentIntensityAnalyzer()

# Keywords that signal important events
SIGNAL_KEYWORDS = {
    "negative": ["injury", "injured", "dropped", "suspended", "banned", "out of",
                  "ruled out", "hamstring", "fracture", "strain", "unfit", "miss",
                  "poor form", "struggling", "collapse", "defeat", "thrashed"],
    "positive": ["comeback", "return", "fit", "selected", "recalled", "captain",
                 "debut", "century", "hat-trick", "five-wicket", "winning streak",
                 "dominant", "incredible", "brilliant", "unstoppable"],
}


def _get_reddit():
    """Initialize PRAW Reddit client."""
    try:
        return praw.Reddit(
            client_id=config.REDDIT_CLIENT_ID,
            client_secret=config.REDDIT_CLIENT_SECRET,
            user_agent=config.REDDIT_USER_AGENT,
        )
    except Exception as e:
        print(f"[Reddit] Init error: {e}")
        return None


def fetch_team_sentiment(team):
    """Fetch and analyze Reddit sentiment for a team."""
    cache_key = f"reddit_{team}".replace(" ", "_")
    cached = check_cache(cache_key, config.CACHE_TTL["sentiment"])
    if cached:
        return cached

    if not can_call("reddit"):
        return cached

    reddit = _get_reddit()
    if not reddit:
        return None

    team_name = standardise(team)
    abbrev = get_abbreviation(team_name)
    search_terms = [team_name, abbrev]

    scores = []
    volume = 0
    keywords_found = []

    try:
        for subreddit_name in config.REDDIT_SUBREDDITS:
            try:
                subreddit = reddit.subreddit(subreddit_name)
                for term in search_terms:
                    for post in subreddit.search(term, time_filter="week", limit=10):
                        text = f"{post.title} {post.selftext}"
                        sentiment = analyzer.polarity_scores(text)
                        scores.append(sentiment["compound"])
                        volume += 1

                        # Check for signal keywords
                        text_lower = text.lower()
                        for kw in SIGNAL_KEYWORDS["negative"]:
                            if kw in text_lower:
                                keywords_found.append(f"-{kw}")
                        for kw in SIGNAL_KEYWORDS["positive"]:
                            if kw in text_lower:
                                keywords_found.append(f"+{kw}")

                        # Top comments
                        post.comments.replace_more(limit=0)
                        for comment in post.comments[:5]:
                            c_sentiment = analyzer.polarity_scores(comment.body)
                            scores.append(c_sentiment["compound"])
                            volume += 1
            except Exception:
                continue

        record_call("reddit", f"sentiment/{team_name}", 200)

    except Exception as e:
        print(f"[Reddit] Error for {team_name}: {e}")
        record_call("reddit", f"sentiment/{team_name}", 0)
        return None

    if not scores:
        result = {
            "team": team_name, "source": "reddit",
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
            "team": team_name, "source": "reddit",
            "score": round(avg_score, 3),
            "trend": 0.0,
            "volume": volume,
            "positive_pct": round(positive, 1),
            "negative_pct": round(negative, 1),
            "neutral_pct": round(neutral, 1),
            "keywords": ",".join(list(set(keywords_found))[:10]),
            "signal": signal,
        }

    # Calculate trend vs previous
    prev = db.fetch_one(
        "SELECT score FROM sentiment WHERE team = ? AND source = 'reddit' ORDER BY scored_at DESC LIMIT 1",
        [team_name]
    )
    if prev:
        result["trend"] = round(result["score"] - prev["score"], 3)

    save_cache(cache_key, result)
    return result


def fetch_all_teams():
    """Fetch sentiment for all PSL teams."""
    results = {}
    for team in get_all_teams():
        sentiment = fetch_team_sentiment(team)
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
