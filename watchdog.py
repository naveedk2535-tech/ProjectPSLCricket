#!/usr/bin/env python3
"""
PSL Cricket Prediction Engine -- 28-Point Health Monitoring System
===================================================================
Checks data freshness, model health, data integrity, and API/system status.

Usage:
    python watchdog.py              # Run all checks, print summary
    python watchdog.py --json       # Output results as JSON
    python watchdog.py --category data_freshness  # Run one category only

Returns exit code 0 (healthy), 1 (degraded), 2 (critical).
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from database import db
from data import rate_limiter


# ============================================================================
#  Check result helper
# ============================================================================

def _result(check_name, category, status, message, suggestion=None):
    """Build a standardized check result dict."""
    return {
        "check_name": check_name,
        "category": category,
        "status": status,          # "ok", "warning", "critical"
        "message": message,
        "suggestion": suggestion,
    }


def _parse_timestamp(ts_string):
    """Parse an ISO-ish timestamp string to a datetime, returning None on failure."""
    if not ts_string:
        return None
    try:
        clean = ts_string.replace("Z", "").split("+")[0].strip()
        # Handle both "2026-03-24T12:00:00" and "2026-03-24 12:00:00"
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(clean[:len(fmt.replace("%", "X"))], fmt)
            except ValueError:
                continue
        return datetime.fromisoformat(clean)
    except (ValueError, TypeError):
        return None


def _hours_since(ts_string):
    """Return hours since a timestamp string, or None if unparseable."""
    dt = _parse_timestamp(ts_string)
    if dt is None:
        return None
    return (datetime.utcnow() - dt).total_seconds() / 3600


# ============================================================================
#  Category 1: Data Freshness (7 checks)
# ============================================================================

def check_fixtures_freshness():
    """Check if fixture data is up to date."""
    row = db.fetch_one("SELECT MAX(updated_at) as last FROM fixtures")
    threshold = config.WATCHDOG_SETTINGS["data_freshness_hours"]["fixtures"]
    if not row or not row["last"]:
        return _result("fixtures_freshness", "data_freshness", "critical",
                        "No fixture data found",
                        "Run: python scheduler.py --task fixtures")
    hours = _hours_since(row["last"])
    if hours is None:
        return _result("fixtures_freshness", "data_freshness", "warning",
                        f"Cannot parse fixture timestamp: {row['last']}")
    if hours > threshold:
        return _result("fixtures_freshness", "data_freshness", "warning",
                        f"Fixtures last updated {hours:.0f}h ago (threshold: {threshold}h)",
                        "Run: python scheduler.py --task fixtures")
    return _result("fixtures_freshness", "data_freshness", "ok",
                    f"Fixtures updated {hours:.1f}h ago")


def check_odds_freshness():
    """Check if odds data is recent enough."""
    row = db.fetch_one("SELECT MAX(fetched_at) as last FROM odds")
    threshold = config.WATCHDOG_SETTINGS["data_freshness_hours"]["odds"]
    if not row or not row["last"]:
        return _result("odds_freshness", "data_freshness", "warning",
                        "No odds data found",
                        "Run: python scheduler.py --task odds")
    hours = _hours_since(row["last"])
    if hours is None:
        return _result("odds_freshness", "data_freshness", "warning",
                        f"Cannot parse odds timestamp: {row['last']}")
    if hours > threshold:
        return _result("odds_freshness", "data_freshness", "warning",
                        f"Odds last fetched {hours:.0f}h ago (threshold: {threshold}h)",
                        "Run: python scheduler.py --task odds")
    return _result("odds_freshness", "data_freshness", "ok",
                    f"Odds updated {hours:.1f}h ago")


def check_sentiment_freshness():
    """Check if sentiment scores are recent."""
    row = db.fetch_one("SELECT MAX(scored_at) as last FROM sentiment")
    threshold = config.WATCHDOG_SETTINGS["data_freshness_hours"]["sentiment"]
    if not row or not row["last"]:
        return _result("sentiment_freshness", "data_freshness", "warning",
                        "No sentiment data found",
                        "Run: python scheduler.py --task sentiment")
    hours = _hours_since(row["last"])
    if hours is None:
        return _result("sentiment_freshness", "data_freshness", "warning",
                        f"Cannot parse sentiment date: {row['last']}")
    if hours > threshold:
        return _result("sentiment_freshness", "data_freshness", "warning",
                        f"Sentiment data {hours:.0f}h old (threshold: {threshold}h)",
                        "Run: python scheduler.py --task sentiment")
    return _result("sentiment_freshness", "data_freshness", "ok",
                    f"Sentiment updated {hours:.1f}h ago")


def check_weather_freshness():
    """Check if weather data is recent."""
    row = db.fetch_one("SELECT MAX(fetched_at) as last FROM weather")
    threshold = config.WATCHDOG_SETTINGS["data_freshness_hours"]["weather"]
    if not row or not row["last"]:
        return _result("weather_freshness", "data_freshness", "warning",
                        "No weather data found",
                        "Run: python scheduler.py --task weather")
    hours = _hours_since(row["last"])
    if hours is None:
        return _result("weather_freshness", "data_freshness", "warning",
                        f"Cannot parse weather timestamp: {row['last']}")
    if hours > threshold:
        return _result("weather_freshness", "data_freshness", "warning",
                        f"Weather data {hours:.0f}h old (threshold: {threshold}h)",
                        "Run: python scheduler.py --task weather")
    return _result("weather_freshness", "data_freshness", "ok",
                    f"Weather updated {hours:.1f}h ago")


def check_historical_freshness():
    """Check if historical match data is up to date."""
    row = db.fetch_one("SELECT MAX(match_date) as last FROM matches")
    threshold = config.WATCHDOG_SETTINGS["data_freshness_hours"]["historical"]
    if not row or not row["last"]:
        return _result("historical_freshness", "data_freshness", "critical",
                        "No historical match data found",
                        "Run: python scheduler.py --task weekly")
    hours = _hours_since(row["last"])
    if hours is None:
        return _result("historical_freshness", "data_freshness", "warning",
                        f"Cannot parse match date: {row['last']}")
    if hours > threshold:
        return _result("historical_freshness", "data_freshness", "warning",
                        f"Latest match data is {hours / 24:.0f} days old (threshold: {threshold / 24:.0f}d)",
                        "Run: python scheduler.py --task weekly")
    return _result("historical_freshness", "data_freshness", "ok",
                    f"Latest match: {row['last']}")


def check_ratings_freshness():
    """Check if team ratings have been updated recently."""
    row = db.fetch_one("SELECT MAX(updated_at) as last FROM team_ratings")
    threshold = config.WATCHDOG_SETTINGS["data_freshness_hours"]["ratings"]
    if not row or not row["last"]:
        return _result("ratings_freshness", "data_freshness", "warning",
                        "No team ratings found",
                        "Run: python scheduler.py --task ratings")
    hours = _hours_since(row["last"])
    if hours is None:
        return _result("ratings_freshness", "data_freshness", "warning",
                        f"Cannot parse ratings timestamp: {row['last']}")
    if hours > threshold:
        return _result("ratings_freshness", "data_freshness", "warning",
                        f"Ratings {hours / 24:.0f} days old (threshold: {threshold / 24:.0f}d)",
                        "Run: python scheduler.py --task ratings")
    return _result("ratings_freshness", "data_freshness", "ok",
                    f"Ratings updated {hours:.1f}h ago")


def check_venue_stats_freshness():
    """Check if venue statistics have been updated recently."""
    row = db.fetch_one("SELECT MAX(updated_at) as last FROM venue_stats")
    threshold = config.WATCHDOG_SETTINGS["data_freshness_hours"]["venue_stats"]
    if not row or not row["last"]:
        return _result("venue_stats_freshness", "data_freshness", "warning",
                        "No venue statistics found",
                        "Run: python scheduler.py --task weekly")
    hours = _hours_since(row["last"])
    if hours is None:
        return _result("venue_stats_freshness", "data_freshness", "warning",
                        f"Cannot parse venue stats timestamp: {row['last']}")
    if hours > threshold:
        return _result("venue_stats_freshness", "data_freshness", "warning",
                        f"Venue stats {hours / 24:.0f} days old (threshold: {threshold / 24:.0f}d)",
                        "Run: python scheduler.py --task weekly")
    return _result("venue_stats_freshness", "data_freshness", "ok",
                    f"Venue stats updated {hours:.1f}h ago")


# ============================================================================
#  Category 2: Model Health (7 checks)
# ============================================================================

def check_xgboost_exists():
    """Check if trained XGBoost model file exists."""
    model_path = os.path.join(config.CACHE_DIR, "xgboost_model_psl.pkl")
    if not os.path.exists(model_path):
        return _result("xgboost_exists", "model_health", "critical",
                        "No trained XGBoost model found",
                        "Run: python scheduler.py --task retrain")
    return _result("xgboost_exists", "model_health", "ok",
                    "XGBoost model file present")


def check_xgboost_age():
    """Check if XGBoost model is not too old."""
    model_path = os.path.join(config.CACHE_DIR, "xgboost_model_psl.pkl")
    max_age = config.WATCHDOG_SETTINGS["model_max_age_days"]
    if not os.path.exists(model_path):
        return _result("xgboost_age", "model_health", "critical",
                        "XGBoost model missing -- cannot check age",
                        "Run: python scheduler.py --task retrain")
    age_days = (time.time() - os.path.getmtime(model_path)) / 86400
    if age_days > max_age:
        return _result("xgboost_age", "model_health", "warning",
                        f"XGBoost model is {age_days:.0f} days old (max: {max_age}d)",
                        "Run: python scheduler.py --task retrain")
    return _result("xgboost_age", "model_health", "ok",
                    f"XGBoost model is {age_days:.1f} days old")


def check_stacker_exists():
    """Check if stacking meta-model exists."""
    stacker_path = os.path.join(config.CACHE_DIR, "stacker_model.pkl")
    if not os.path.exists(stacker_path):
        return _result("stacker_exists", "model_health", "warning",
                        "No stacking meta-model found (ensemble uses weighted average fallback)",
                        "Run: python scheduler.py --task retrain")
    return _result("stacker_exists", "model_health", "ok",
                    "Stacker model file present")


def check_weights_optimized():
    """Check if ensemble weights have been optimized."""
    weights_path = os.path.join(config.CACHE_DIR, "optimized_weights.json")
    if not os.path.exists(weights_path):
        return _result("weights_optimized", "model_health", "warning",
                        "Using default model weights (not optimized)",
                        "Run: python scheduler.py --task retrain")
    try:
        with open(weights_path) as f:
            weights = json.load(f)
        total = sum(weights.values())
        if abs(total - 1.0) > 0.01:
            return _result("weights_optimized", "model_health", "warning",
                            f"Weights sum to {total:.3f} instead of 1.0")
        return _result("weights_optimized", "model_health", "ok",
                        f"Optimized weights: {json.dumps({k: round(v, 3) for k, v in weights.items()})}")
    except (json.JSONDecodeError, IOError) as e:
        return _result("weights_optimized", "model_health", "warning",
                        f"Cannot read weights file: {e}")


def check_accuracy():
    """Check if recent prediction accuracy is above threshold."""
    min_accuracy = config.WATCHDOG_SETTINGS["min_accuracy"]
    recent = db.fetch_all(
        "SELECT * FROM model_tracker WHERE status = 'settled' ORDER BY settled_at DESC LIMIT 20"
    )
    if len(recent) < 5:
        return _result("accuracy", "model_health", "warning",
                        f"Only {len(recent)} settled predictions -- insufficient for accuracy check",
                        "Wait for more matches to complete")
    correct = sum(1 for r in recent if r.get("top_pick_correct") == 1)
    accuracy = correct / len(recent)
    if accuracy < min_accuracy:
        return _result("accuracy", "model_health", "critical",
                        f"Recent accuracy {accuracy:.1%} ({correct}/{len(recent)}) below {min_accuracy:.0%}",
                        "Run: python scheduler.py --task retrain")
    return _result("accuracy", "model_health", "ok",
                    f"Recent accuracy: {accuracy:.1%} ({correct}/{len(recent)})")


def check_brier_score():
    """Check if Brier score is below acceptable threshold."""
    max_brier = config.WATCHDOG_SETTINGS["max_brier"]
    recent = db.fetch_all(
        """SELECT * FROM model_tracker
           WHERE status = 'settled' AND team_a_prob IS NOT NULL
           ORDER BY settled_at DESC LIMIT 20"""
    )
    if len(recent) < 5:
        return _result("brier_score", "model_health", "warning",
                        f"Only {len(recent)} settled predictions -- insufficient for Brier check")
    brier_sum = 0.0
    for r in recent:
        actual = 1 if r["actual_winner"] == r["team_a"] else 0
        brier_sum += (r["team_a_prob"] - actual) ** 2
    brier = brier_sum / len(recent)
    if brier > max_brier:
        return _result("brier_score", "model_health", "warning",
                        f"Brier score {brier:.4f} exceeds threshold {max_brier}",
                        "Run: python scheduler.py --task retrain")
    return _result("brier_score", "model_health", "ok",
                    f"Brier score: {brier:.4f} (threshold: {max_brier})")


def check_no_degradation():
    """Check that model performance has not degraded over the last 20 settled predictions."""
    recent = db.fetch_all(
        "SELECT * FROM model_tracker WHERE status = 'settled' ORDER BY settled_at DESC LIMIT 20"
    )
    if len(recent) < 20:
        return _result("no_degradation", "model_health", "ok",
                        f"Only {len(recent)} settled -- degradation check needs 20")
    last10 = recent[:10]
    prev10 = recent[10:20]
    acc_last = sum(1 for r in last10 if r.get("top_pick_correct") == 1) / 10
    acc_prev = sum(1 for r in prev10 if r.get("top_pick_correct") == 1) / 10
    drop = acc_prev - acc_last
    if drop > 0.15:
        return _result("no_degradation", "model_health", "warning",
                        f"Accuracy dropped from {acc_prev:.0%} to {acc_last:.0%} (-{drop:.0%})",
                        "Run: python scheduler.py --task retrain")
    return _result("no_degradation", "model_health", "ok",
                    f"No degradation (last10: {acc_last:.0%}, prev10: {acc_prev:.0%})")


def check_predictions_exist():
    """Check that predictions exist for upcoming matches."""
    upcoming = db.fetch_one(
        "SELECT COUNT(*) as cnt FROM fixtures WHERE status = 'SCHEDULED' AND match_date >= date('now')"
    )
    predicted = db.fetch_one(
        """SELECT COUNT(*) as cnt FROM predictions p
           JOIN fixtures f ON p.match_date = f.match_date AND p.team_a = f.team_a AND p.team_b = f.team_b
           WHERE f.status = 'SCHEDULED' AND f.match_date >= date('now')"""
    )
    uc = upcoming["cnt"] if upcoming else 0
    pc = predicted["cnt"] if predicted else 0
    if uc > 0 and pc == 0:
        return _result("predictions_exist", "model_health", "warning",
                        f"{uc} upcoming fixtures but 0 predictions",
                        "Run: python scheduler.py --task predictions")
    if uc > 0 and pc < uc:
        return _result("predictions_exist", "model_health", "warning",
                        f"Only {pc}/{uc} upcoming fixtures have predictions",
                        "Run: python scheduler.py --task predictions")
    return _result("predictions_exist", "model_health", "ok",
                    f"{pc}/{uc} upcoming fixtures have predictions")


# ============================================================================
#  Category 3: Data Integrity (7 checks)
# ============================================================================

def check_team_names_canonical():
    """Verify all team names in key tables use canonical names."""
    from data.team_names import CANONICAL_TEAMS
    tables_cols = [
        ("fixtures", "team_a"), ("fixtures", "team_b"),
        ("predictions", "team_a"), ("predictions", "team_b"),
        ("matches", "team_a"), ("matches", "team_b"),
    ]
    bad = []
    for table, col in tables_cols:
        if not db.table_exists(table):
            continue
        rows = db.fetch_all(f"SELECT DISTINCT {col} as name FROM {table} WHERE {col} IS NOT NULL")
        for r in rows:
            if r["name"] and r["name"] not in CANONICAL_TEAMS:
                bad.append(f"{table}.{col}='{r['name']}'")
    if bad:
        return _result("team_names_canonical", "data_integrity", "warning",
                        f"{len(bad)} non-canonical names: {', '.join(bad[:5])}",
                        "Pipe all team names through data.team_names.standardise()")
    return _result("team_names_canonical", "data_integrity", "ok",
                    "All team names are canonical")


def check_no_orphan_predictions():
    """Check for predictions referencing non-existent fixtures."""
    orphans = db.fetch_one(
        """SELECT COUNT(*) as cnt FROM predictions p
           WHERE p.fixture_id IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM fixtures f WHERE f.id = p.fixture_id)"""
    )
    count = orphans["cnt"] if orphans else 0
    if count > 0:
        return _result("no_orphans", "data_integrity", "warning",
                        f"{count} predictions reference missing fixtures",
                        "Clean up orphaned prediction rows")
    return _result("no_orphans", "data_integrity", "ok",
                    "No orphaned predictions")


def check_no_duplicate_fixtures():
    """Check for duplicate fixtures (same date + teams)."""
    dupes = db.fetch_one(
        """SELECT COUNT(*) as cnt FROM (
               SELECT match_date, team_a, team_b, COUNT(*) as c
               FROM fixtures GROUP BY match_date, team_a, team_b HAVING c > 1
           )"""
    )
    count = dupes["cnt"] if dupes else 0
    if count > 0:
        return _result("no_duplicates", "data_integrity", "warning",
                        f"{count} duplicate fixture entries found",
                        "De-duplicate fixtures table")
    return _result("no_duplicates", "data_integrity", "ok",
                    "No duplicate fixtures")


def check_odds_range():
    """Verify odds are within reasonable range (1.01 - 50.0)."""
    bad = db.fetch_one(
        """SELECT COUNT(*) as cnt FROM odds
           WHERE (team_a_odds < 1.01 OR team_a_odds > 50.0
                  OR team_b_odds < 1.01 OR team_b_odds > 50.0)
           AND team_a_odds IS NOT NULL AND team_b_odds IS NOT NULL"""
    )
    count = bad["cnt"] if bad else 0
    if count > 0:
        return _result("odds_range", "data_integrity", "warning",
                        f"{count} odds outside valid range [1.01, 50.0]",
                        "Check odds API parsing logic")
    return _result("odds_range", "data_integrity", "ok",
                    "All odds within valid range")


def check_probability_sums():
    """Verify prediction probabilities sum to ~1.0."""
    bad = db.fetch_one(
        """SELECT COUNT(*) as cnt FROM predictions
           WHERE ABS(team_a_win + team_b_win - 1.0) > 0.01
           AND team_a_win IS NOT NULL AND team_b_win IS NOT NULL"""
    )
    count = bad["cnt"] if bad else 0
    if count > 0:
        return _result("probability_sums", "data_integrity", "warning",
                        f"{count} predictions have probabilities not summing to ~1.0",
                        "Check ensemble normalization logic")
    return _result("probability_sums", "data_integrity", "ok",
                    "All prediction probabilities sum correctly")


def check_no_future_completed():
    """Check for COMPLETED fixtures with future dates."""
    bad = db.fetch_one(
        """SELECT COUNT(*) as cnt FROM fixtures
           WHERE status = 'COMPLETED' AND match_date > date('now', '+1 day')"""
    )
    count = bad["cnt"] if bad else 0
    if count > 0:
        return _result("no_future_completed", "data_integrity", "critical",
                        f"{count} fixtures marked COMPLETED with future dates",
                        "Investigate fixture status update logic")
    return _result("no_future_completed", "data_integrity", "ok",
                    "No future-dated COMPLETED fixtures")


def check_tracker_status_valid():
    """Verify all model_tracker entries have valid status values."""
    bad = db.fetch_one(
        "SELECT COUNT(*) as cnt FROM model_tracker WHERE status NOT IN ('pending', 'settled')"
    )
    count = bad["cnt"] if bad else 0
    if count > 0:
        return _result("tracker_status_valid", "data_integrity", "warning",
                        f"{count} tracker entries have invalid status",
                        "Valid statuses: pending, settled")
    return _result("tracker_status_valid", "data_integrity", "ok",
                    "All tracker entries have valid status")


# ============================================================================
#  Category 4: API & System (7 checks)
# ============================================================================

def check_rate_limits_ok():
    """Check that no API rate limits are exhausted."""
    usage = rate_limiter.get_usage_summary()
    exhausted = []
    for api_name, info in usage.items():
        if info["remaining"] <= 0:
            exhausted.append(f"{api_name} ({info['used']}/{info['limit']})")
    if exhausted:
        return _result("rate_limits_ok", "api_system", "warning",
                        f"Rate limits exhausted: {', '.join(exhausted)}",
                        "Wait for rate limit window to reset")
    low = [f"{k}({v['remaining']} left)" for k, v in usage.items()
           if v["remaining"] <= 2 and v["limit"] > 0]
    msg = "All API rate limits have headroom"
    if low:
        msg = f"Rate limits OK but low: {', '.join(low)}"
    return _result("rate_limits_ok", "api_system", "ok", msg)


def check_db_size():
    """Check database file size is within limits."""
    max_mb = config.WATCHDOG_SETTINGS["max_db_size_mb"]
    if not os.path.exists(config.DB_PATH):
        return _result("db_size", "api_system", "critical",
                        "Database file not found",
                        "Run: python -c 'from database import db; db.init_db()'")
    size_mb = os.path.getsize(config.DB_PATH) / (1024 * 1024)
    if size_mb > max_mb:
        return _result("db_size", "api_system", "warning",
                        f"Database is {size_mb:.1f}MB (max: {max_mb}MB)",
                        "Prune old api_calls and cache data")
    return _result("db_size", "api_system", "ok",
                    f"Database size: {size_mb:.1f}MB (max: {max_mb}MB)")


def check_cache_size():
    """Check cache directory size is within limits."""
    max_mb = config.WATCHDOG_SETTINGS["max_cache_size_mb"]
    try:
        cache_mb = rate_limiter.get_cache_size_mb()
    except Exception:
        cache_mb = 0.0
    if cache_mb > max_mb:
        return _result("cache_size", "api_system", "warning",
                        f"Cache is {cache_mb:.1f}MB (max: {max_mb}MB)",
                        "Clear stale cache: from data.rate_limiter import clear_cache; clear_cache()")
    return _result("cache_size", "api_system", "ok",
                    f"Cache size: {cache_mb:.1f}MB (max: {max_mb}MB)")


def check_env_vars_set():
    """Check that critical environment variables are configured."""
    required = {
        "CRICKET_API_KEY": config.CRICKET_API_KEY,
        "ODDS_API_KEY": config.ODDS_API_KEY,
    }
    optional = {
        "REDDIT_CLIENT_ID": config.REDDIT_CLIENT_ID,
        "REDDIT_CLIENT_SECRET": config.REDDIT_CLIENT_SECRET,
        "NEWSAPI_KEY": config.NEWSAPI_KEY,
    }
    missing_req = [k for k, v in required.items() if not v]
    missing_opt = [k for k, v in optional.items() if not v]

    if missing_req:
        return _result("env_vars_set", "api_system", "critical",
                        f"Missing required env vars: {', '.join(missing_req)}",
                        "Set variables in .env or PythonAnywhere env settings")
    if missing_opt:
        return _result("env_vars_set", "api_system", "warning",
                        f"Missing optional env vars: {', '.join(missing_opt)}",
                        "Sentiment features will be limited without these")
    return _result("env_vars_set", "api_system", "ok",
                    "All environment variables are set")


def check_db_connection():
    """Verify database connection is working."""
    try:
        result = db.fetch_one("SELECT 1 as ok")
        if result and result["ok"] == 1:
            return _result("db_connection", "api_system", "ok",
                            "Database connection healthy")
        return _result("db_connection", "api_system", "critical",
                        "Database query returned unexpected result")
    except Exception as e:
        return _result("db_connection", "api_system", "critical",
                        f"Database connection failed: {e}",
                        "Check DB_PATH in config and file permissions")


def check_tables_exist():
    """Verify all required database tables exist."""
    required_tables = [
        "matches", "fixtures", "predictions", "odds", "value_bets",
        "team_ratings", "venue_stats", "head_to_head", "sentiment",
        "weather", "model_tracker", "model_performance", "api_calls",
        "live_matches", "backtest_results",
    ]
    missing = [t for t in required_tables if not db.table_exists(t)]
    if missing:
        return _result("tables_exist", "api_system", "critical",
                        f"Missing tables: {', '.join(missing)}",
                        "Run: python -c 'from database import db; db.init_db()'")
    return _result("tables_exist", "api_system", "ok",
                    f"All {len(required_tables)} required tables exist")


def check_no_hardcoded_keys():
    """Scan source files for hardcoded API keys (security check)."""
    src_dir = os.path.dirname(os.path.abspath(__file__))
    suspect_pattern = re.compile(r'["\'][a-f0-9]{32,}["\']')
    flagged = set()

    for root, dirs, files in os.walk(src_dir):
        dirs[:] = [d for d in dirs if d not in (
            "__pycache__", ".git", "venv", "env", "node_modules", "static", "templates"
        )]
        for fname in files:
            if not fname.endswith(".py"):
                continue
            if fname in ("config.py", "watchdog.py"):
                continue  # config reads from env; watchdog contains the regex
            fpath = os.path.join(root, fname)
            try:
                with open(fpath, "r", errors="ignore") as fh:
                    for line_num, line in enumerate(fh, 1):
                        matches = suspect_pattern.findall(line)
                        for m in matches:
                            inner = m[1:-1]  # strip quotes
                            if len(inner) >= 32 and "example" not in inner.lower():
                                flagged.add(os.path.relpath(fpath, src_dir))
            except IOError:
                continue

    if flagged:
        return _result("no_hardcoded_keys", "api_system", "warning",
                        f"Possible hardcoded keys in: {', '.join(sorted(flagged)[:5])}",
                        "Move all API keys to environment variables")
    return _result("no_hardcoded_keys", "api_system", "ok",
                    "No hardcoded API keys detected")


# ============================================================================
#  Registry  (28 checks total: 7 + 7 + 7 + 7)
# ============================================================================

ALL_CHECKS = [
    # Data Freshness (7)
    check_fixtures_freshness,
    check_odds_freshness,
    check_sentiment_freshness,
    check_weather_freshness,
    check_historical_freshness,
    check_ratings_freshness,
    check_venue_stats_freshness,
    # Model Health (7)
    check_xgboost_exists,
    check_xgboost_age,
    check_stacker_exists,
    check_weights_optimized,
    check_accuracy,
    check_brier_score,
    check_no_degradation,
    # note: check_predictions_exist is the 7th model_health check
    # Data Integrity (7)
    check_predictions_exist,
    check_team_names_canonical,
    check_no_orphan_predictions,
    check_no_duplicate_fixtures,
    check_odds_range,
    check_probability_sums,
    check_no_future_completed,
    check_tracker_status_valid,
    # API & System (7)
    check_rate_limits_ok,
    check_db_size,
    check_cache_size,
    check_env_vars_set,
    check_db_connection,
    check_tables_exist,
    check_no_hardcoded_keys,
]

CATEGORY_MAP = {
    "data_freshness": [
        check_fixtures_freshness, check_odds_freshness, check_sentiment_freshness,
        check_weather_freshness, check_historical_freshness, check_ratings_freshness,
        check_venue_stats_freshness,
    ],
    "model_health": [
        check_xgboost_exists, check_xgboost_age, check_stacker_exists,
        check_weights_optimized, check_accuracy, check_brier_score,
        check_no_degradation,
    ],
    "data_integrity": [
        check_predictions_exist, check_team_names_canonical, check_no_orphan_predictions,
        check_no_duplicate_fixtures, check_odds_range, check_probability_sums,
        check_no_future_completed, check_tracker_status_valid,
    ],
    "api_system": [
        check_rate_limits_ok, check_db_size, check_cache_size,
        check_env_vars_set, check_db_connection, check_tables_exist,
        check_no_hardcoded_keys,
    ],
}


# ============================================================================
#  Public API
# ============================================================================

def run_all_checks(category=None):
    """
    Run all 28 health checks (or a single category).

    Args:
        category: Optional category name to filter checks.

    Returns:
        list of dicts, each with keys:
            check_name, category, status, message, suggestion
    """
    db.init_db()
    checks = CATEGORY_MAP.get(category, ALL_CHECKS) if category else ALL_CHECKS
    results = []
    for fn in checks:
        try:
            results.append(fn())
        except Exception as e:
            results.append(_result(
                fn.__name__.replace("check_", ""), "unknown", "critical",
                f"Check raised exception: {e}",
                "Investigate the check function"
            ))
    return results


def get_summary(results=None):
    """
    Return overall system health status.

    Returns:
        "healthy"   -- all checks OK
        "degraded"  -- some warnings but no criticals
        "critical"  -- at least one critical failure
    """
    if results is None:
        results = run_all_checks()
    statuses = [r["status"] for r in results]
    if "critical" in statuses:
        return "critical"
    if "warning" in statuses:
        return "degraded"
    return "healthy"


# ============================================================================
#  CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="PSL Cricket Prediction Engine -- 28-Point Health Monitor"
    )
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--category", choices=list(CATEGORY_MAP.keys()),
                        help="Run checks for one category only")
    args = parser.parse_args()

    results = run_all_checks(category=args.category)
    summary = get_summary(results)

    if args.json:
        print(json.dumps({
            "summary": summary,
            "timestamp": datetime.utcnow().isoformat(),
            "total_checks": len(results),
            "ok": sum(1 for r in results if r["status"] == "ok"),
            "warnings": sum(1 for r in results if r["status"] == "warning"),
            "criticals": sum(1 for r in results if r["status"] == "critical"),
            "checks": results,
        }, indent=2))
    else:
        icon_map = {"ok": "[OK]", "warning": "[WARN]", "critical": "[CRIT]"}
        current_cat = None

        print("=" * 70)
        print("  PSL Cricket Engine -- Health Report")
        print(f"  {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print("=" * 70)

        for r in results:
            cat = r["category"]
            if cat != current_cat:
                current_cat = cat
                print(f"\n--- {cat.upper().replace('_', ' ')} ---")
            icon = icon_map.get(r["status"], "[??]")
            print(f"  {icon:6s} {r['check_name']:30s} {r['message']}")
            if r.get("suggestion") and r["status"] != "ok":
                print(f"         {'':30s} -> {r['suggestion']}")

        ok_c = sum(1 for r in results if r["status"] == "ok")
        warn_c = sum(1 for r in results if r["status"] == "warning")
        crit_c = sum(1 for r in results if r["status"] == "critical")
        print("\n" + "=" * 70)
        print(f"  SUMMARY: {summary.upper()}")
        print(f"  OK: {ok_c} | Warnings: {warn_c} | Critical: {crit_c} | Total: {len(results)}")
        print("=" * 70)

    if summary == "critical":
        sys.exit(2)
    elif summary == "degraded":
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
