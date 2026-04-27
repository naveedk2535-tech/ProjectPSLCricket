"""
Reddit sentiment via PRAW for PSL/IPL cricket teams.
Uses 1 batch search per subreddit instead of per-team searches.
VADER sentiment scoring with signal keyword detection.
"""

import praw
from datetime import datetime, timedelta
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

import config
from database import db
from data.rate_limiter import can_call, record_call, check_cache, save_cache
from data.team_names import standardise, get_all_teams, get_abbreviation

analyzer = SentimentIntensityAnalyzer()

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


def fetch_all_teams(league="psl"):
    """
    Fetch sentiment for all teams using 1 batch search per subreddit.
    Instead of 8 separate searches (one per team), we search for the league
    name once and parse which teams are mentioned.
    """
    cache_key = f"reddit_all_{league}"
    cached = check_cache(cache_key, config.CACHE_TTL["sentiment"])
    if cached:
        return cached

    if not can_call("reddit"):
        return cached or {}

    reddit = _get_reddit()
    if not reddit:
        return {}

    # Determine league search term and subreddits
    if league == "psl":
        search_term = "PSL"
        subreddits = config.REDDIT_SUBREDDITS
        team_names = config.TEAM_NAMES
    else:
        search_term = "IPL"
        subreddits = getattr(config, "REDDIT_SUBREDDITS_IPL", ["Cricket", "ipl"])
        team_names = config.IPL_TEAM_NAMES

    # Collect all posts from batch search (1 call per subreddit)
    all_posts = []
    try:
        for sub_name in subreddits:
            try:
                subreddit = reddit.subreddit(sub_name)
                for post in subreddit.search(search_term, time_filter="week", limit=50):
                    text = f"{post.title} {post.selftext}"
                    # Get top comments too
                    comments_text = []
                    try:
                        post.comments.replace_more(limit=0)
                        for comment in post.comments[:3]:
                            comments_text.append(comment.body)
                    except Exception:
                        pass
                    all_posts.append({
                        "text": text,
                        "comments": comments_text,
                    })
            except Exception:
                continue

        record_call("reddit", f"sentiment_batch/{league}", 200)

    except Exception as e:
        print(f"[Reddit] Batch error: {e}")
        record_call("reddit", f"sentiment_batch/{league}", 0)
        return {}

    # Now parse team mentions and score per team
    results = {}
    for team_name in team_names:
        team_std = standardise(team_name)
        abbrev = get_abbreviation(team_name) or ""

        scores = []
        volume = 0
        keywords_found = []

        for post in all_posts:
            text = post["text"]
            text_lower = text.lower()

            # Check if this post mentions this team
            mentioned = (
                team_name.lower() in text_lower or
                team_std.lower() in text_lower or
                (abbrev and len(abbrev) >= 2 and f" {abbrev.lower()} " in f" {text_lower} ")
            )

            if not mentioned:
                continue

            sentiment = analyzer.polarity_scores(text)
            scores.append(sentiment["compound"])
            volume += 1

            # Signal keywords
            for kw in SIGNAL_KEYWORDS["negative"]:
                if kw in text_lower:
                    keywords_found.append(f"-{kw}")
            for kw in SIGNAL_KEYWORDS["positive"]:
                if kw in text_lower:
                    keywords_found.append(f"+{kw}")

            # Score comments too
            for comment in post["comments"]:
                if team_name.lower() in comment.lower() or team_std.lower() in comment.lower():
                    c_sentiment = analyzer.polarity_scores(comment)
                    scores.append(c_sentiment["compound"])
                    volume += 1

        if scores:
            avg_score = sum(scores) / len(scores)
            positive = sum(1 for s in scores if s > 0.05) / len(scores) * 100
            negative = sum(1 for s in scores if s < -0.05) / len(scores) * 100
            neutral = 100 - positive - negative
            signal = "confident" if avg_score > 0.15 else ("struggling" if avg_score < -0.15 else "neutral")
        else:
            avg_score, positive, negative, neutral = 0.0, 0.0, 0.0, 100.0
            signal = "neutral"

        # Trend vs previous
        prev = db.fetch_one(
            "SELECT score FROM sentiment WHERE team = ? AND source = 'reddit' ORDER BY scored_at DESC LIMIT 1",
            [team_std]
        )
        trend = round(avg_score - prev["score"], 3) if prev else 0.0

        result = {
            "team": team_std, "source": "reddit",
            "score": round(avg_score, 3), "trend": trend,
            "volume": volume,
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
def fetch_team_sentiment(team):
    """Fetch sentiment for one team (uses batch internally)."""
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
