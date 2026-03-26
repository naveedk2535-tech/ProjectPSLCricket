"""
Sentiment Model — small adjustment based on Reddit/News sentiment.
Max ±5% probability adjustment. Sentiment is a weak signal but real.
"""

from database import db
from data.team_names import standardise


def predict(team_a, team_b, league="psl"):
    """
    Adjust base probability using team sentiment differential.
    T20 is 2-way (no draw), so base is 50/50.
    """
    team_a = standardise(team_a)
    team_b = standardise(team_b)

    sent_a = _get_team_sentiment(team_a, league=league)
    sent_b = _get_team_sentiment(team_b, league=league)

    # Calculate differential
    diff = sent_a["combined_score"] - sent_b["combined_score"]

    # Convert to small probability adjustment (max ±5%)
    adjustment = max(-0.05, min(0.05, diff * 0.15))

    team_a_win = 0.50 + adjustment
    team_b_win = 1.0 - team_a_win

    # Confidence is low — sentiment is a weak signal
    confidence = 0.35

    return {
        "team_a_win": round(team_a_win, 4),
        "team_b_win": round(team_b_win, 4),
        "confidence": confidence,
        "details": {
            "model": "sentiment",
            "sentiment_a": sent_a,
            "sentiment_b": sent_b,
            "differential": round(diff, 3),
            "adjustment": round(adjustment, 4),
        }
    }


def _get_team_sentiment(team, league="psl"):
    """Get latest combined sentiment for a team."""
    # Try combined first
    result = db.fetch_one(
        """SELECT * FROM sentiment WHERE team = ? AND source = 'combined' AND league = ?
           ORDER BY scored_at DESC LIMIT 1""",
        [team, league]
    )

    if result:
        return {
            "combined_score": result["score"],
            "trend": result["trend"],
            "volume": result["volume"],
            "signal": result["signal"],
            "keywords": result["keywords"],
        }

    # Blend reddit and news
    reddit = db.fetch_one(
        "SELECT * FROM sentiment WHERE team = ? AND source = 'reddit' AND league = ? ORDER BY scored_at DESC LIMIT 1",
        [team, league]
    )
    news = db.fetch_one(
        "SELECT * FROM sentiment WHERE team = ? AND source = 'news' AND league = ? ORDER BY scored_at DESC LIMIT 1",
        [team, league]
    )

    reddit_score = reddit["score"] if reddit else 0.0
    news_score = news["score"] if news else 0.0
    reddit_vol = reddit["volume"] if reddit else 0
    news_vol = news["volume"] if news else 0

    # Weight by volume
    total_vol = reddit_vol + news_vol
    if total_vol > 0:
        combined = (reddit_score * reddit_vol + news_score * news_vol) / total_vol
    else:
        combined = 0.0

    signal = "neutral"
    if combined > 0.15:
        signal = "bullish"
    elif combined < -0.15:
        signal = "bearish"

    keywords = ""
    if reddit and reddit.get("keywords"):
        keywords = reddit["keywords"]
    if news and news.get("keywords"):
        keywords = f"{keywords},{news['keywords']}" if keywords else news["keywords"]

    return {
        "combined_score": combined,
        "trend": (reddit["trend"] if reddit else 0) + (news["trend"] if news else 0),
        "volume": total_vol,
        "signal": signal,
        "keywords": keywords,
    }
