"""
Centralized API rate limiting with persistent tracking.
Every external API call MUST go through this module.
"""

import os
import json
import time
from datetime import datetime, timedelta

import config
from database import db

CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")
os.makedirs(CACHE_DIR, exist_ok=True)


def can_call(api_name):
    """Check if an API call is allowed within the rate limit window."""
    if api_name not in config.RATE_LIMITS:
        return True

    limit = config.RATE_LIMITS[api_name]
    cutoff = (datetime.utcnow() - timedelta(seconds=limit["period_seconds"])).isoformat()

    result = db.fetch_one(
        "SELECT COUNT(*) as cnt FROM api_calls WHERE api_name = ? AND called_at > ? AND cached = 0",
        [api_name, cutoff]
    )
    count = result["cnt"] if result else 0
    return count < limit["calls"]


def record_call(api_name, endpoint="", response_code=200, cached=False):
    """Log an API call for rate limit tracking."""
    db.execute(
        "INSERT INTO api_calls (api_name, endpoint, called_at, response_code, cached) VALUES (?, ?, ?, ?, ?)",
        [api_name, endpoint, db.now_iso(), response_code, 1 if cached else 0]
    )


def get_usage_summary():
    """Get API usage summary for dashboard display."""
    summary = {}
    for api_name, limit in config.RATE_LIMITS.items():
        cutoff = (datetime.utcnow() - timedelta(seconds=limit["period_seconds"])).isoformat()
        result = db.fetch_one(
            "SELECT COUNT(*) as cnt FROM api_calls WHERE api_name = ? AND called_at > ? AND cached = 0",
            [api_name, cutoff]
        )
        used = result["cnt"] if result else 0
        remaining = max(0, limit["calls"] - used)

        last_call = db.fetch_one(
            "SELECT called_at FROM api_calls WHERE api_name = ? ORDER BY called_at DESC LIMIT 1",
            [api_name]
        )

        summary[api_name] = {
            "used": used,
            "limit": limit["calls"],
            "remaining": remaining,
            "period_hours": limit["period_seconds"] / 3600,
            "last_call": last_call["called_at"] if last_call else None,
        }
    return summary


def remaining_calls(api_name):
    """Get remaining calls for a specific API."""
    if api_name not in config.RATE_LIMITS:
        return 999
    limit = config.RATE_LIMITS[api_name]
    cutoff = (datetime.utcnow() - timedelta(seconds=limit["period_seconds"])).isoformat()
    result = db.fetch_one(
        "SELECT COUNT(*) as cnt FROM api_calls WHERE api_name = ? AND called_at > ? AND cached = 0",
        [api_name, cutoff]
    )
    used = result["cnt"] if result else 0
    return max(0, limit["calls"] - used)


def check_cache(cache_key, ttl_seconds=None):
    """Return cached JSON data if it exists and is fresh."""
    cache_file = os.path.join(CACHE_DIR, f"{cache_key}.json")
    if not os.path.exists(cache_file):
        return None

    if ttl_seconds is not None:
        age = time.time() - os.path.getmtime(cache_file)
        if age > ttl_seconds:
            return None

    try:
        with open(cache_file, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def save_cache(cache_key, data):
    """Save data to cache as JSON."""
    cache_file = os.path.join(CACHE_DIR, f"{cache_key}.json")
    try:
        with open(cache_file, "w") as f:
            json.dump(data, f, default=str, indent=2)
    except IOError:
        pass


def clear_cache(cache_key=None):
    """Clear specific or all cache files."""
    if cache_key:
        cache_file = os.path.join(CACHE_DIR, f"{cache_key}.json")
        if os.path.exists(cache_file):
            os.remove(cache_file)
    else:
        for f in os.listdir(CACHE_DIR):
            if f.endswith(".json"):
                os.remove(os.path.join(CACHE_DIR, f))


def get_cache_size_mb():
    """Get total cache directory size in MB."""
    total = 0
    for f in os.listdir(CACHE_DIR):
        fp = os.path.join(CACHE_DIR, f)
        if os.path.isfile(fp):
            total += os.path.getsize(fp)
    return total / (1024 * 1024)
