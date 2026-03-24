#!/usr/bin/env python3
"""
PSL Cricket Prediction Engine — 28-Point Health Monitoring System (Watchdog)
============================================================================
Usage:
    python watchdog.py                    # Run all 28 checks
    python watchdog.py --check fixtures   # Run a single check
    python watchdog.py --category model   # Run checks in a category
    python watchdog.py --json             # Output as JSON
    python watchdog.py --alert            # Send email on critical findings

This is the control room for the betting operation. Every check returns a
structured result with status (ok/warning/critical), a human-readable
message, and a suggestion for remediation.
"""

import argparse
import json
import logging
import os
import re
import smtplib
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "database" / "psl_cricket.db"
MODELS_DIR = BASE_DIR / "models"
DATA_DIR = BASE_DIR / "data"
CACHE_DIR = DATA_DIR / "cache"
CONFIG_PATH = BASE_DIR / "watchdog_config.json"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("watchdog")

# ---------------------------------------------------------------------------
# Default thresholds (overridden by watchdog_config.json if present)
# ---------------------------------------------------------------------------
DEFAULT_THRESHOLDS = {
    "fixtures_freshness_hours": 24,
    "odds_freshness_hours": 12,
    "sentiment_freshness_hours": 48,
    "historical_freshness_days": 7,
    "ratings_freshness_days": 7,
    "venue_freshness_days": 14,
    "model_max_age_days": 14,
    "min_accuracy_pct": 55.0,
    "max_brier_score": 0.28,
    "prob_sum_tolerance": 0.02,
    "odds_min": 1.01,
    "odds_max": 100.0,
    "db_max_size_mb": 100,
    "cache_max_size_mb": 50,
    "rate_limit_warning_pct": 10,
    "rolling_window": 10,
}

REQUIRED_ENV_VARS = [
    "CRICAPI_KEY",
    "ODDS_API_KEY",
    "NEWSAPI_KEY",
]

REQUIRED_TABLES = [
    "fixtures", "odds", "weather", "sentiment", "predictions",
    "tracker", "live_matches", "elo_ratings", "team_strengths",
    "venue_stats", "head_to_head", "player_stats", "model_performance",
    "api_rate_limits", "value_bets", "user_bets", "historical_matches",
    "watchdog_history",
]

PSL_TEAMS = [
    "Islamabad United", "Karachi Kings", "Lahore Qalandars",
    "Multan Sultans", "Peshawar Zalmi", "Quetta Gladiators",
    "Rawalpindi Hawks", "Abbottabad Falcons",
]


# ===========================================================================
# Configuration
# ===========================================================================
def load_thresholds() -> dict:
    """Load thresholds from config file or use defaults."""
    thresholds = dict(DEFAULT_THRESHOLDS)
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH) as f:
                overrides = json.load(f)
            thresholds.update(overrides)
            log.info(f"Loaded config overrides from {CONFIG_PATH}")
        except Exception as e:
            log.warning(f"Failed to load watchdog config: {e}")
    return thresholds


# ===========================================================================
# Check Result
# ===========================================================================
class CheckResult:
    """Structured result from a single health check."""

    def __init__(self, check_name: str, category: str):
        self.check_name = check_name
        self.category = category
        self.status = "ok"  # ok | warning | critical
        self.message = ""
        self.details = ""
        self.suggestion = ""
        self.timestamp = datetime.now().isoformat()

    def ok(self, msg: str, details: str = ""):
        self.status = "ok"
        self.message = msg
        self.details = details
        return self

    def warning(self, msg: str, suggestion: str = "", details: str = ""):
        self.status = "warning"
        self.message = msg
        self.suggestion = suggestion
        self.details = details
        return self

    def critical(self, msg: str, suggestion: str = "", details: str = ""):
        self.status = "critical"
        self.message = msg
        self.suggestion = suggestion
        self.details = details
        return self

    def to_dict(self) -> dict:
        return {
            "check_name": self.check_name,
            "category": self.category,
            "status": self.status,
            "message": self.message,
            "details": self.details,
            "suggestion": self.suggestion,
            "timestamp": self.timestamp,
        }


# ===========================================================================
# Database Helper
# ===========================================================================
def get_db():
    """Return a sqlite3 connection with row factory."""
    if not DB_PATH.exists():
        return None
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _hours_since(iso_timestamp: str | None) -> float:
    """Calculate hours elapsed since an ISO timestamp."""
    if not iso_timestamp:
        return float("inf")
    try:
        dt = datetime.fromisoformat(iso_timestamp)
        delta = datetime.now() - dt
        return delta.total_seconds() / 3600
    except Exception:
        return float("inf")


def _days_since(iso_timestamp: str | None) -> float:
    """Calculate days elapsed since an ISO timestamp."""
    return _hours_since(iso_timestamp) / 24.0


# ===========================================================================
# DATA FRESHNESS CHECKS (1-7)
# ===========================================================================

def check_fixtures_freshness(thresholds: dict) -> CheckResult:
    """Check 1: Fixtures updated within threshold hours."""
    result = CheckResult("fixtures_freshness", "data_freshness")
    conn = get_db()
    if not conn:
        return result.critical("Database not found", "Run scheduler.py --task daily to initialize")
    try:
        row = conn.execute(
            "SELECT MAX(updated_at) as last_update, COUNT(*) as total FROM fixtures"
        ).fetchone()
        if not row or row["total"] == 0:
            return result.critical(
                "No fixtures in database",
                "Run: python scheduler.py --task fixtures",
            )
        hours = _hours_since(row["last_update"])
        limit = thresholds["fixtures_freshness_hours"]
        if hours > limit * 2:
            return result.critical(
                f"Fixtures {hours:.0f}h stale (limit: {limit}h)",
                "Run: python scheduler.py --task fixtures",
                f"Last update: {row['last_update']}",
            )
        elif hours > limit:
            return result.warning(
                f"Fixtures {hours:.0f}h since last update (limit: {limit}h)",
                "Schedule more frequent fixture fetches",
                f"Last update: {row['last_update']}",
            )
        return result.ok(
            f"Fixtures fresh ({hours:.0f}h ago, {row['total']} total)",
            f"Last update: {row['last_update']}",
        )
    finally:
        conn.close()


def check_odds_freshness(thresholds: dict) -> CheckResult:
    """Check 2: Odds updated within threshold hours on match days."""
    result = CheckResult("odds_freshness", "data_freshness")
    conn = get_db()
    if not conn:
        return result.critical("Database not found")
    try:
        row = conn.execute("SELECT MAX(fetched_at) as last_fetch, COUNT(*) as total FROM odds").fetchone()
        if not row or row["total"] == 0:
            # Check if there are upcoming matches
            upcoming = conn.execute(
                "SELECT COUNT(*) as cnt FROM fixtures WHERE status = 'UPCOMING'"
            ).fetchone()
            if upcoming and upcoming["cnt"] > 0:
                return result.warning(
                    "No odds data but upcoming matches exist",
                    "Run: python scheduler.py --task odds",
                )
            return result.ok("No odds data (no upcoming matches)")

        hours = _hours_since(row["last_fetch"])
        limit = thresholds["odds_freshness_hours"]
        if hours > limit * 2:
            return result.critical(
                f"Odds {hours:.0f}h stale (limit: {limit}h)",
                "Run: python scheduler.py --task odds",
            )
        elif hours > limit:
            return result.warning(
                f"Odds {hours:.0f}h since last fetch (limit: {limit}h)",
                "Increase odds fetch frequency on match days",
            )
        return result.ok(f"Odds fresh ({hours:.0f}h ago, {row['total']} records)")
    finally:
        conn.close()


def check_sentiment_freshness(thresholds: dict) -> CheckResult:
    """Check 3: Sentiment data updated within threshold hours."""
    result = CheckResult("sentiment_freshness", "data_freshness")
    conn = get_db()
    if not conn:
        return result.critical("Database not found")
    try:
        row = conn.execute("SELECT MAX(fetched_at) as last_fetch, COUNT(*) as total FROM sentiment").fetchone()
        if not row or row["total"] == 0:
            return result.warning(
                "No sentiment data collected",
                "Run: python scheduler.py --task sentiment",
            )
        hours = _hours_since(row["last_fetch"])
        limit = thresholds["sentiment_freshness_hours"]
        if hours > limit:
            return result.warning(
                f"Sentiment {hours:.0f}h stale (limit: {limit}h)",
                "Run: python scheduler.py --task sentiment",
            )
        return result.ok(f"Sentiment fresh ({hours:.0f}h ago, {row['total']} records)")
    finally:
        conn.close()


def check_weather_freshness(thresholds: dict) -> CheckResult:
    """Check 4: Weather data exists for the next match."""
    result = CheckResult("weather_freshness", "data_freshness")
    conn = get_db()
    if not conn:
        return result.critical("Database not found")
    try:
        next_match = conn.execute(
            "SELECT match_id, match_date FROM fixtures WHERE status = 'UPCOMING' ORDER BY match_date LIMIT 1"
        ).fetchone()
        if not next_match:
            return result.ok("No upcoming matches — weather not needed")

        weather = conn.execute(
            "SELECT * FROM weather WHERE match_id = ?", (next_match["match_id"],)
        ).fetchone()
        if not weather:
            return result.warning(
                f"No weather data for next match ({next_match['match_date']})",
                "Run: python scheduler.py --task weather",
            )
        return result.ok(
            f"Weather available for next match ({next_match['match_date']})",
            f"Conditions: {weather['conditions']}",
        )
    finally:
        conn.close()


def check_historical_freshness(thresholds: dict) -> CheckResult:
    """Check 5: Historical CricSheet data updated within threshold days."""
    result = CheckResult("historical_freshness", "data_freshness")
    conn = get_db()
    if not conn:
        return result.critical("Database not found")
    try:
        row = conn.execute(
            "SELECT MAX(imported_at) as last_import, COUNT(*) as total FROM historical_matches"
        ).fetchone()
        if not row or row["total"] == 0:
            return result.warning(
                "No historical match data imported",
                "Run: python scheduler.py --task weekly",
            )
        days = _days_since(row["last_import"])
        limit = thresholds["historical_freshness_days"]
        if days > limit:
            return result.warning(
                f"Historical data {days:.0f} days old (limit: {limit}d)",
                "Run: python scheduler.py --task weekly",
            )
        return result.ok(f"Historical data fresh ({days:.0f}d ago, {row['total']} matches)")
    finally:
        conn.close()


def check_ratings_freshness(thresholds: dict) -> CheckResult:
    """Check 6: Team Elo ratings recalculated within threshold days."""
    result = CheckResult("ratings_freshness", "data_freshness")
    conn = get_db()
    if not conn:
        return result.critical("Database not found")
    try:
        row = conn.execute(
            "SELECT MAX(last_updated) as last_update, COUNT(*) as total FROM elo_ratings"
        ).fetchone()
        if not row or row["total"] == 0:
            return result.warning(
                "No Elo ratings calculated",
                "Run: python scheduler.py --task ratings",
            )
        days = _days_since(row["last_update"])
        limit = thresholds["ratings_freshness_days"]
        if days > limit:
            return result.warning(
                f"Elo ratings {days:.0f} days old (limit: {limit}d)",
                "Run: python scheduler.py --task ratings",
            )
        return result.ok(f"Ratings fresh ({days:.0f}d ago, {row['total']} teams)")
    finally:
        conn.close()


def check_venue_freshness(thresholds: dict) -> CheckResult:
    """Check 7: Venue stats updated within threshold days."""
    result = CheckResult("venue_freshness", "data_freshness")
    conn = get_db()
    if not conn:
        return result.critical("Database not found")
    try:
        row = conn.execute(
            "SELECT MAX(last_updated) as last_update, COUNT(*) as total FROM venue_stats"
        ).fetchone()
        if not row or row["total"] == 0:
            return result.warning(
                "No venue stats calculated",
                "Run: python scheduler.py --task weekly",
            )
        days = _days_since(row["last_update"])
        limit = thresholds["venue_freshness_days"]
        if days > limit:
            return result.warning(
                f"Venue stats {days:.0f} days old (limit: {limit}d)",
                "Run venue_update task",
            )
        return result.ok(f"Venue stats fresh ({days:.0f}d ago, {row['total']} venues)")
    finally:
        conn.close()


# ===========================================================================
# MODEL HEALTH CHECKS (8-14)
# ===========================================================================

def check_xgboost_model(thresholds: dict) -> CheckResult:
    """Check 8: XGBoost model exists and was trained recently."""
    result = CheckResult("xgboost_model", "model_health")
    model_path = MODELS_DIR / "xgboost_psl.pkl"
    if not model_path.exists():
        return result.warning(
            "XGBoost model not found",
            "Run: python scheduler.py --task retrain",
        )
    try:
        import pickle
        with open(model_path, "rb") as f:
            data = pickle.load(f)
        trained_at = data.get("trained_at", "")
        days = _days_since(trained_at)
        limit = thresholds["model_max_age_days"]
        brier = data.get("brier_score", None)
        accuracy = data.get("accuracy", None)

        details = f"Trained: {trained_at}, Brier: {brier:.4f}, Accuracy: {accuracy:.1%}" if brier else f"Trained: {trained_at}"
        if days > limit:
            return result.warning(
                f"XGBoost model {days:.0f} days old (limit: {limit}d)",
                "Run: python scheduler.py --task retrain",
                details,
            )
        return result.ok(f"XGBoost model healthy ({days:.0f}d old)", details)
    except Exception as e:
        return result.critical(f"Failed to load XGBoost model: {e}", "Model file may be corrupted — retrain")


def check_stacker_model(thresholds: dict) -> CheckResult:
    """Check 9: Stacker model exists and was trained recently."""
    result = CheckResult("stacker_model", "model_health")
    model_path = MODELS_DIR / "stacker_model.pkl"
    if not model_path.exists():
        return result.warning(
            "Stacker model not found",
            "Run: python scheduler.py --task retrain",
        )
    try:
        import pickle
        with open(model_path, "rb") as f:
            data = pickle.load(f)
        trained_at = data.get("trained_at", "")
        days = _days_since(trained_at)
        limit = thresholds["model_max_age_days"]
        if days > limit:
            return result.warning(
                f"Stacker model {days:.0f} days old (limit: {limit}d)",
                "Run: python scheduler.py --task retrain",
            )
        return result.ok(f"Stacker model healthy ({days:.0f}d old)")
    except Exception as e:
        return result.critical(f"Failed to load stacker model: {e}", "Retrain the stacker")


def check_ensemble_weights(thresholds: dict) -> CheckResult:
    """Check 10: Ensemble weights optimized recently."""
    result = CheckResult("ensemble_weights", "model_health")
    weights_path = MODELS_DIR / "ensemble_weights.json"
    if not weights_path.exists():
        return result.warning(
            "Ensemble weights not optimized — using defaults",
            "Run: python scheduler.py --task retrain",
        )
    try:
        with open(weights_path) as f:
            data = json.load(f)
        optimized_at = data.get("optimized_at", "")
        days = _days_since(optimized_at)
        limit = thresholds["model_max_age_days"]
        weights = data.get("weights", {})
        if days > limit:
            return result.warning(
                f"Ensemble weights {days:.0f} days old (limit: {limit}d)",
                "Run weight optimization",
                f"Weights: {json.dumps(weights)}",
            )
        return result.ok(
            f"Ensemble weights fresh ({days:.0f}d old)",
            f"Weights: {json.dumps(weights)}",
        )
    except Exception as e:
        return result.critical(f"Failed to load ensemble weights: {e}")


def check_model_accuracy(thresholds: dict) -> CheckResult:
    """Check 11: Model accuracy above minimum threshold."""
    result = CheckResult("model_accuracy", "model_health")
    conn = get_db()
    if not conn:
        return result.critical("Database not found")
    try:
        row = conn.execute(
            """SELECT metric_value, sample_size FROM model_performance
               WHERE model_name = 'xgboost' AND metric_name = 'accuracy'
               ORDER BY logged_at DESC LIMIT 1"""
        ).fetchone()
        if not row:
            return result.warning("No accuracy data logged", "Run predictions and settle matches first")

        accuracy = row["metric_value"] * 100
        minimum = thresholds["min_accuracy_pct"]
        if accuracy < minimum:
            return result.critical(
                f"Model accuracy {accuracy:.1f}% below minimum {minimum}%",
                "Investigate feature engineering or retrain with more data",
                f"Sample size: {row['sample_size']}",
            )
        return result.ok(
            f"Model accuracy {accuracy:.1f}% (minimum: {minimum}%)",
            f"Sample size: {row['sample_size']}",
        )
    finally:
        conn.close()


def check_brier_score(thresholds: dict) -> CheckResult:
    """Check 12: Brier score below maximum threshold."""
    result = CheckResult("brier_score", "model_health")
    conn = get_db()
    if not conn:
        return result.critical("Database not found")
    try:
        row = conn.execute(
            """SELECT metric_value, sample_size FROM model_performance
               WHERE metric_name = 'brier_score'
               ORDER BY logged_at DESC LIMIT 1"""
        ).fetchone()
        if not row:
            return result.warning("No Brier score data", "Run backtest or settle predictions")

        brier = row["metric_value"]
        maximum = thresholds["max_brier_score"]
        if brier >= 0.25:
            return result.critical(
                f"Brier score {brier:.4f} >= 0.25 — model is WORSE than random!",
                "Urgently investigate model — consider reverting to simpler approach",
            )
        elif brier > maximum:
            return result.warning(
                f"Brier score {brier:.4f} above threshold {maximum}",
                "Retrain models and optimize ensemble weights",
            )
        return result.ok(f"Brier score {brier:.4f} (max: {maximum})")
    finally:
        conn.close()


def check_model_degradation(thresholds: dict) -> CheckResult:
    """Check 13: No model shows degradation in rolling accuracy."""
    result = CheckResult("model_degradation", "model_health")
    conn = get_db()
    if not conn:
        return result.critical("Database not found")
    try:
        models = conn.execute(
            "SELECT DISTINCT model_name FROM model_performance WHERE metric_name = 'rolling_accuracy'"
        ).fetchall()

        if not models:
            return result.warning("No rolling accuracy data", "Run performance_log task after settling matches")

        degraded = []
        for m in models:
            rows = conn.execute(
                """SELECT metric_value FROM model_performance
                   WHERE model_name = ? AND metric_name = 'rolling_accuracy'
                   ORDER BY logged_at DESC LIMIT 3""",
                (m["model_name"],),
            ).fetchall()
            if len(rows) >= 3:
                values = [r["metric_value"] for r in rows]
                # Check if accuracy is declining (each value lower than the one before)
                if values[0] < values[1] < values[2]:
                    degraded.append(f"{m['model_name']}: {values[2]:.1%} -> {values[1]:.1%} -> {values[0]:.1%}")

        if degraded:
            return result.warning(
                f"{len(degraded)} model(s) showing degradation",
                "Consider retraining affected models",
                "; ".join(degraded),
            )
        return result.ok("No models showing degradation")
    finally:
        conn.close()


def check_predictions_coverage(thresholds: dict) -> CheckResult:
    """Check 14: Predictions exist for all upcoming fixtures."""
    result = CheckResult("predictions_coverage", "model_health")
    conn = get_db()
    if not conn:
        return result.critical("Database not found")
    try:
        upcoming = conn.execute(
            "SELECT COUNT(*) as cnt FROM fixtures WHERE status = 'UPCOMING'"
        ).fetchone()
        predicted = conn.execute(
            """SELECT COUNT(DISTINCT f.match_id) as cnt
               FROM fixtures f
               JOIN predictions p ON f.match_id = p.match_id AND p.model_name = 'ensemble'
               WHERE f.status = 'UPCOMING'"""
        ).fetchone()

        total = upcoming["cnt"] if upcoming else 0
        covered = predicted["cnt"] if predicted else 0

        if total == 0:
            return result.ok("No upcoming fixtures")
        if covered < total:
            missing = total - covered
            return result.warning(
                f"{missing} upcoming match(es) missing predictions ({covered}/{total})",
                "Run: python scheduler.py --task predictions",
            )
        return result.ok(f"All {total} upcoming matches have predictions")
    finally:
        conn.close()


# ===========================================================================
# DATA INTEGRITY CHECKS (15-21)
# ===========================================================================

def check_team_names(thresholds: dict) -> CheckResult:
    """Check 15: All team names in predictions match canonical names."""
    result = CheckResult("team_names", "data_integrity")
    conn = get_db()
    if not conn:
        return result.critical("Database not found")
    try:
        teams_in_predictions = conn.execute(
            "SELECT DISTINCT team1 FROM predictions UNION SELECT DISTINCT team2 FROM predictions"
        ).fetchall()

        canonical = set(PSL_TEAMS)
        invalid = []
        for row in teams_in_predictions:
            name = row[0]
            if name and name not in canonical:
                invalid.append(name)

        if invalid:
            return result.warning(
                f"{len(invalid)} non-canonical team name(s) in predictions",
                "Run name normalization or check fuzzy matching",
                f"Invalid: {', '.join(invalid)}",
            )
        return result.ok("All team names match canonical list")
    finally:
        conn.close()


def check_orphaned_predictions(thresholds: dict) -> CheckResult:
    """Check 16: No predictions without matching fixtures."""
    result = CheckResult("orphaned_predictions", "data_integrity")
    conn = get_db()
    if not conn:
        return result.critical("Database not found")
    try:
        orphaned = conn.execute(
            """SELECT COUNT(*) as cnt FROM predictions p
               LEFT JOIN fixtures f ON p.match_id = f.match_id
               WHERE f.match_id IS NULL"""
        ).fetchone()
        count = orphaned["cnt"] if orphaned else 0
        if count > 0:
            return result.warning(
                f"{count} orphaned prediction(s) without matching fixtures",
                "Clean up: DELETE FROM predictions WHERE match_id NOT IN (SELECT match_id FROM fixtures)",
            )
        return result.ok("No orphaned predictions")
    finally:
        conn.close()


def check_duplicates(thresholds: dict) -> CheckResult:
    """Check 17: No duplicate entries in key tables."""
    result = CheckResult("duplicates", "data_integrity")
    conn = get_db()
    if not conn:
        return result.critical("Database not found")
    try:
        issues = []
        # Check predictions: same match_id + model_name should be unique-ish
        dups = conn.execute(
            """SELECT match_id, model_name, COUNT(*) as cnt
               FROM predictions
               GROUP BY match_id, model_name
               HAVING cnt > 1"""
        ).fetchall()
        if dups:
            issues.append(f"predictions: {len(dups)} duplicate match+model combos")

        # Check tracker duplicates
        dups = conn.execute(
            "SELECT match_id, COUNT(*) as cnt FROM tracker GROUP BY match_id HAVING cnt > 1"
        ).fetchall()
        if dups:
            issues.append(f"tracker: {len(dups)} duplicate match entries")

        # Check fixtures duplicates
        dups = conn.execute(
            "SELECT match_id, COUNT(*) as cnt FROM fixtures GROUP BY match_id HAVING cnt > 1"
        ).fetchall()
        if dups:
            issues.append(f"fixtures: {len(dups)} duplicate match_ids")

        if issues:
            return result.warning(
                f"Duplicates found in {len(issues)} table(s)",
                "Deduplicate affected tables",
                "; ".join(issues),
            )
        return result.ok("No duplicates found in key tables")
    finally:
        conn.close()


def check_odds_range(thresholds: dict) -> CheckResult:
    """Check 18: Odds are within reasonable range."""
    result = CheckResult("odds_range", "data_integrity")
    conn = get_db()
    if not conn:
        return result.critical("Database not found")
    try:
        count = conn.execute("SELECT COUNT(*) as cnt FROM odds").fetchone()
        if not count or count["cnt"] == 0:
            return result.ok("No odds data to validate")

        lo = thresholds["odds_min"]
        hi = thresholds["odds_max"]
        bad = conn.execute(
            """SELECT COUNT(*) as cnt FROM odds
               WHERE (team1_odds IS NOT NULL AND (team1_odds < ? OR team1_odds > ?))
                  OR (team2_odds IS NOT NULL AND (team2_odds < ? OR team2_odds > ?))""",
            (lo, hi, lo, hi),
        ).fetchone()
        bad_count = bad["cnt"] if bad else 0
        if bad_count > 0:
            return result.warning(
                f"{bad_count} odds record(s) outside range [{lo}, {hi}]",
                "Review odds data for data entry or API parsing errors",
            )
        return result.ok(f"All odds within [{lo}, {hi}] range")
    finally:
        conn.close()


def check_probability_sums(thresholds: dict) -> CheckResult:
    """Check 19: Probabilities sum to ~1.0 for all predictions."""
    result = CheckResult("probability_sums", "data_integrity")
    conn = get_db()
    if not conn:
        return result.critical("Database not found")
    try:
        tol = thresholds["prob_sum_tolerance"]
        bad = conn.execute(
            f"""SELECT COUNT(*) as cnt FROM predictions
                WHERE ABS(team1_win_prob + team2_win_prob - 1.0) > {tol}"""
        ).fetchone()
        bad_count = bad["cnt"] if bad else 0
        if bad_count > 0:
            return result.warning(
                f"{bad_count} prediction(s) where P(team1) + P(team2) deviates from 1.0 by > {tol}",
                "Check prediction generation logic for normalization errors",
            )
        total = conn.execute("SELECT COUNT(*) as cnt FROM predictions").fetchone()
        return result.ok(f"All {total['cnt']} predictions sum to ~1.0 (tolerance: {tol})")
    finally:
        conn.close()


def check_future_completed(thresholds: dict) -> CheckResult:
    """Check 20: No future matches marked as COMPLETED."""
    result = CheckResult("future_completed", "data_integrity")
    conn = get_db()
    if not conn:
        return result.critical("Database not found")
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        bad = conn.execute(
            "SELECT COUNT(*) as cnt FROM fixtures WHERE status = 'COMPLETED' AND match_date > ?",
            (today,),
        ).fetchone()
        bad_count = bad["cnt"] if bad else 0
        if bad_count > 0:
            return result.critical(
                f"{bad_count} future match(es) incorrectly marked COMPLETED",
                "Fix status: UPDATE fixtures SET status='UPCOMING' WHERE match_date > date('now') AND status='COMPLETED'",
            )
        return result.ok("No future matches marked as COMPLETED")
    finally:
        conn.close()


def check_tracker_status(thresholds: dict) -> CheckResult:
    """Check 21: Tracker entries have valid status transitions."""
    result = CheckResult("tracker_status", "data_integrity")
    conn = get_db()
    if not conn:
        return result.critical("Database not found")
    try:
        valid_statuses = {"PENDING", "SETTLED", "VOID"}
        rows = conn.execute("SELECT DISTINCT status FROM tracker").fetchall()
        invalid = [r["status"] for r in rows if r["status"] not in valid_statuses]
        if invalid:
            return result.warning(
                f"Invalid tracker status values: {', '.join(invalid)}",
                f"Valid statuses are: {', '.join(valid_statuses)}",
            )

        # Check: settled trackers should have actual_winner and settled_at
        incomplete = conn.execute(
            "SELECT COUNT(*) as cnt FROM tracker WHERE status = 'SETTLED' AND (actual_winner IS NULL OR settled_at IS NULL)"
        ).fetchone()
        if incomplete and incomplete["cnt"] > 0:
            return result.warning(
                f"{incomplete['cnt']} SETTLED tracker(s) missing actual_winner or settled_at",
                "Review and fix incomplete settlement records",
            )

        return result.ok("All tracker status transitions valid")
    finally:
        conn.close()


# ===========================================================================
# API & SYSTEM HEALTH CHECKS (22-28)
# ===========================================================================

def check_rate_limits(thresholds: dict) -> CheckResult:
    """Check 22: API rate limits not exceeded."""
    result = CheckResult("rate_limits", "api_system")
    conn = get_db()
    if not conn:
        return result.critical("Database not found")
    try:
        rows = conn.execute("SELECT * FROM api_rate_limits").fetchall()
        if not rows:
            return result.ok("No API rate limit data (APIs not yet called)")

        issues = []
        for row in rows:
            used = row["calls_today"] or 0
            limit = row["daily_limit"] or 100
            remaining_pct = ((limit - used) / limit) * 100 if limit > 0 else 0
            warn_pct = thresholds["rate_limit_warning_pct"]
            if remaining_pct < warn_pct:
                issues.append(f"{row['api_name']}: {used}/{limit} used ({remaining_pct:.0f}% remaining)")

        if issues:
            return result.warning(
                f"{len(issues)} API(s) near rate limit",
                "Reduce API call frequency or wait for daily reset",
                "; ".join(issues),
            )
        return result.ok("All API rate limits healthy")
    finally:
        conn.close()


def check_database_size(thresholds: dict) -> CheckResult:
    """Check 23: Database size under limit."""
    result = CheckResult("database_size", "api_system")
    if not DB_PATH.exists():
        return result.critical("Database file not found")
    size_mb = DB_PATH.stat().st_size / (1024 * 1024)
    limit = thresholds["db_max_size_mb"]
    if size_mb > limit:
        return result.warning(
            f"Database size {size_mb:.1f}MB exceeds {limit}MB limit",
            "Consider archiving old data or running VACUUM",
        )
    return result.ok(f"Database size {size_mb:.1f}MB (limit: {limit}MB)")


def check_cache_size(thresholds: dict) -> CheckResult:
    """Check 24: Cache directory size under limit."""
    result = CheckResult("cache_size", "api_system")
    if not CACHE_DIR.exists():
        return result.ok("Cache directory does not exist yet")
    total = sum(f.stat().st_size for f in CACHE_DIR.rglob("*") if f.is_file())
    size_mb = total / (1024 * 1024)
    limit = thresholds["cache_max_size_mb"]
    if size_mb > limit:
        return result.warning(
            f"Cache size {size_mb:.1f}MB exceeds {limit}MB limit",
            "Clear old cache files",
        )
    return result.ok(f"Cache size {size_mb:.1f}MB (limit: {limit}MB)")


def check_env_vars(thresholds: dict) -> CheckResult:
    """Check 25: All required environment variables are set."""
    result = CheckResult("env_vars", "api_system")
    missing = [v for v in REQUIRED_ENV_VARS if not os.environ.get(v)]
    if missing:
        return result.warning(
            f"{len(missing)} required env var(s) not set",
            f"Set: {', '.join(missing)}",
            f"Missing: {', '.join(missing)}",
        )
    return result.ok(f"All {len(REQUIRED_ENV_VARS)} required env vars set")


def check_db_connection(thresholds: dict) -> CheckResult:
    """Check 26: Database connection is healthy."""
    result = CheckResult("db_connection", "api_system")
    try:
        conn = get_db()
        if not conn:
            return result.critical(
                "Cannot connect to database",
                f"Ensure {DB_PATH} exists and is a valid SQLite file",
            )
        # Test with a simple query
        conn.execute("SELECT 1").fetchone()
        # Check integrity
        integrity = conn.execute("PRAGMA integrity_check").fetchone()
        conn.close()
        if integrity[0] != "ok":
            return result.critical(
                f"Database integrity check failed: {integrity[0]}",
                "Database may be corrupted — restore from backup",
            )
        return result.ok("Database connection healthy, integrity OK")
    except Exception as e:
        return result.critical(f"Database error: {e}", "Check database file permissions and integrity")


def check_required_tables(thresholds: dict) -> CheckResult:
    """Check 27: All required tables exist with expected columns."""
    result = CheckResult("required_tables", "api_system")
    conn = get_db()
    if not conn:
        return result.critical("Database not found")
    try:
        existing = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        existing_names = {r["name"] for r in existing}
        missing = [t for t in REQUIRED_TABLES if t not in existing_names]
        if missing:
            return result.critical(
                f"{len(missing)} required table(s) missing",
                "Run scheduler.py --task daily to initialize tables",
                f"Missing: {', '.join(missing)}",
            )
        return result.ok(f"All {len(REQUIRED_TABLES)} required tables exist")
    finally:
        conn.close()


def check_hardcoded_keys(thresholds: dict) -> CheckResult:
    """Check 28: No hardcoded API keys in source files."""
    result = CheckResult("hardcoded_keys", "api_system")
    # Patterns that look like API keys (long alphanumeric strings assigned to variables)
    key_patterns = [
        re.compile(r'(?:api_?key|apikey|secret|password|token)\s*=\s*["\'][a-zA-Z0-9]{16,}["\']', re.IGNORECASE),
        re.compile(r'(?:api_?key|apikey|secret|password|token)\s*:\s*["\'][a-zA-Z0-9]{16,}["\']', re.IGNORECASE),
    ]

    py_files = list(BASE_DIR.glob("*.py"))
    py_files.extend(BASE_DIR.glob("**/*.py"))
    py_files = list(set(py_files))  # deduplicate

    findings = []
    for pyf in py_files:
        try:
            content = pyf.read_text(errors="ignore")
            for pattern in key_patterns:
                matches = pattern.findall(content)
                for match in matches:
                    # Exclude obvious test/placeholder values
                    if any(placeholder in match.lower() for placeholder in
                           ["your_api_key", "xxx", "placeholder", "example", "test", "dummy"]):
                        continue
                    findings.append(f"{pyf.name}: {match[:60]}...")
        except Exception:
            continue

    if findings:
        return result.critical(
            f"{len(findings)} potential hardcoded key(s) found!",
            "Move API keys to environment variables immediately",
            "; ".join(findings[:5]),
        )
    return result.ok(f"No hardcoded API keys found in {len(py_files)} Python file(s)")


# ===========================================================================
# Check Registry
# ===========================================================================

ALL_CHECKS = {
    # Data Freshness
    "fixtures_freshness": (check_fixtures_freshness, "data_freshness"),
    "odds_freshness": (check_odds_freshness, "data_freshness"),
    "sentiment_freshness": (check_sentiment_freshness, "data_freshness"),
    "weather_freshness": (check_weather_freshness, "data_freshness"),
    "historical_freshness": (check_historical_freshness, "data_freshness"),
    "ratings_freshness": (check_ratings_freshness, "data_freshness"),
    "venue_freshness": (check_venue_freshness, "data_freshness"),
    # Model Health
    "xgboost_model": (check_xgboost_model, "model_health"),
    "stacker_model": (check_stacker_model, "model_health"),
    "ensemble_weights": (check_ensemble_weights, "model_health"),
    "model_accuracy": (check_model_accuracy, "model_health"),
    "brier_score": (check_brier_score, "model_health"),
    "model_degradation": (check_model_degradation, "model_health"),
    "predictions_coverage": (check_predictions_coverage, "model_health"),
    # Data Integrity
    "team_names": (check_team_names, "data_integrity"),
    "orphaned_predictions": (check_orphaned_predictions, "data_integrity"),
    "duplicates": (check_duplicates, "data_integrity"),
    "odds_range": (check_odds_range, "data_integrity"),
    "probability_sums": (check_probability_sums, "data_integrity"),
    "future_completed": (check_future_completed, "data_integrity"),
    "tracker_status": (check_tracker_status, "data_integrity"),
    # API & System
    "rate_limits": (check_rate_limits, "api_system"),
    "database_size": (check_database_size, "api_system"),
    "cache_size": (check_cache_size, "api_system"),
    "env_vars": (check_env_vars, "api_system"),
    "db_connection": (check_db_connection, "api_system"),
    "required_tables": (check_required_tables, "api_system"),
    "hardcoded_keys": (check_hardcoded_keys, "api_system"),
}

CATEGORY_NAMES = {
    "data_freshness": "Data Freshness",
    "model_health": "Model Health",
    "data_integrity": "Data Integrity",
    "api_system": "API & System Health",
}


# ===========================================================================
# Runner Functions
# ===========================================================================

def run_check(check_name: str, thresholds: dict = None) -> CheckResult:
    """Run a single check by name."""
    if thresholds is None:
        thresholds = load_thresholds()
    if check_name not in ALL_CHECKS:
        r = CheckResult(check_name, "unknown")
        return r.critical(f"Unknown check: {check_name}")
    func, _ = ALL_CHECKS[check_name]
    try:
        return func(thresholds)
    except Exception as e:
        r = CheckResult(check_name, ALL_CHECKS[check_name][1])
        return r.critical(f"Check crashed: {e}")


def run_category(category: str, thresholds: dict = None) -> list[CheckResult]:
    """Run all checks in a category."""
    if thresholds is None:
        thresholds = load_thresholds()
    results = []
    for name, (func, cat) in ALL_CHECKS.items():
        if cat == category:
            try:
                results.append(func(thresholds))
            except Exception as e:
                r = CheckResult(name, cat)
                results.append(r.critical(f"Check crashed: {e}"))
    return results


def run_all_checks(thresholds: dict = None) -> list[CheckResult]:
    """Run all 28 health checks."""
    if thresholds is None:
        thresholds = load_thresholds()
    results = []
    for name, (func, cat) in ALL_CHECKS.items():
        try:
            results.append(func(thresholds))
        except Exception as e:
            r = CheckResult(name, cat)
            results.append(r.critical(f"Check crashed: {e}"))
    return results


def get_summary(results: list[CheckResult]) -> dict:
    """Get overall health summary from check results."""
    total = len(results)
    ok_count = sum(1 for r in results if r.status == "ok")
    warn_count = sum(1 for r in results if r.status == "warning")
    crit_count = sum(1 for r in results if r.status == "critical")

    if crit_count > 0:
        overall = "critical"
    elif warn_count > 3:
        overall = "degraded"
    elif warn_count > 0:
        overall = "healthy_with_warnings"
    else:
        overall = "healthy"

    return {
        "overall_status": overall,
        "total_checks": total,
        "ok": ok_count,
        "warnings": warn_count,
        "critical": crit_count,
        "score": round(ok_count / max(total, 1) * 100, 1),
        "checked_at": datetime.now().isoformat(),
    }


def store_results(results: list[CheckResult]):
    """Store check results in database for trend analysis."""
    conn = get_db()
    if not conn:
        return
    try:
        for r in results:
            conn.execute(
                """INSERT INTO watchdog_history
                   (check_name, category, status, message, details)
                   VALUES (?, ?, ?, ?, ?)""",
                (r.check_name, r.category, r.status, r.message, r.details),
            )
        conn.commit()
    except Exception as e:
        log.warning(f"Failed to store watchdog results: {e}")
    finally:
        conn.close()


def send_alert_email(summary: dict, critical_results: list[CheckResult]):
    """Send email alert for critical findings."""
    email_from = os.environ.get("ALERT_EMAIL_FROM", "")
    email_to = os.environ.get("ALERT_EMAIL_TO", "")
    if not email_from or not email_to:
        log.warning("Email alert skipped — ALERT_EMAIL_FROM or ALERT_EMAIL_TO not set")
        return

    body_lines = [
        f"PSL Prediction Engine Health Report",
        f"Overall: {summary['overall_status'].upper()}",
        f"Score: {summary['score']}%",
        f"OK: {summary['ok']} | Warnings: {summary['warnings']} | Critical: {summary['critical']}",
        "",
        "CRITICAL ISSUES:",
    ]
    for r in critical_results:
        body_lines.append(f"  [{r.category}] {r.check_name}: {r.message}")
        if r.suggestion:
            body_lines.append(f"    Fix: {r.suggestion}")

    body = "\n".join(body_lines)

    try:
        msg = MIMEText(body)
        msg["Subject"] = f"[PSL Engine] HEALTH {summary['overall_status'].upper()} — {summary['critical']} critical"
        msg["From"] = email_from
        msg["To"] = email_to
        smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
        smtp_port = int(os.environ.get("SMTP_PORT", "587"))
        smtp_pass = os.environ.get("SMTP_PASSWORD", "")
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(email_from, smtp_pass)
            server.send_message(msg)
        log.info("Alert email sent successfully")
    except Exception as e:
        log.error(f"Failed to send alert email: {e}")


def print_report(results: list[CheckResult], summary: dict):
    """Print a formatted health report to console."""
    status_icons = {"ok": "[OK]  ", "warning": "[WARN]", "critical": "[CRIT]"}
    status_colors = {"ok": "\033[92m", "warning": "\033[93m", "critical": "\033[91m"}
    reset = "\033[0m"

    print()
    print("=" * 72)
    print("  PSL CRICKET PREDICTION ENGINE — HEALTH REPORT")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 72)

    overall_color = {
        "healthy": "\033[92m",
        "healthy_with_warnings": "\033[93m",
        "degraded": "\033[93m",
        "critical": "\033[91m",
    }.get(summary["overall_status"], "")

    print(f"\n  Overall: {overall_color}{summary['overall_status'].upper()}{reset}")
    print(f"  Score:   {summary['score']}% ({summary['ok']}/{summary['total_checks']} checks passing)")
    print(f"  OK: {summary['ok']} | Warnings: {summary['warnings']} | Critical: {summary['critical']}")
    print()

    # Group by category
    by_category = {}
    for r in results:
        by_category.setdefault(r.category, []).append(r)

    for cat_key in ["data_freshness", "model_health", "data_integrity", "api_system"]:
        cat_results = by_category.get(cat_key, [])
        if not cat_results:
            continue
        cat_name = CATEGORY_NAMES.get(cat_key, cat_key)
        cat_ok = sum(1 for r in cat_results if r.status == "ok")
        print(f"  --- {cat_name} ({cat_ok}/{len(cat_results)}) ---")
        for r in cat_results:
            icon = status_icons.get(r.status, "[??]  ")
            color = status_colors.get(r.status, "")
            print(f"    {color}{icon}{reset} {r.check_name}: {r.message}")
            if r.suggestion and r.status != "ok":
                print(f"           Fix: {r.suggestion}")
        print()

    print("=" * 72)
    print()


# ===========================================================================
# CLI Entry Point
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="PSL Cricket Prediction Engine — 28-Point Health Monitor",
    )
    parser.add_argument("--check", help="Run a single check by name")
    parser.add_argument(
        "--category",
        choices=["data_freshness", "model_health", "data_integrity", "api_system"],
        help="Run all checks in a category",
    )
    parser.add_argument("--json", action="store_true", help="Output results as JSON")
    parser.add_argument("--alert", action="store_true", help="Send email alert on critical findings")
    parser.add_argument("--store", action="store_true", help="Store results in database for trend analysis")
    parser.add_argument("--list", action="store_true", help="List all available checks")
    args = parser.parse_args()

    if args.list:
        print("\nAvailable checks:")
        for name, (_, cat) in sorted(ALL_CHECKS.items()):
            cat_name = CATEGORY_NAMES.get(cat, cat)
            print(f"  {name:30s} [{cat_name}]")
        print()
        sys.exit(0)

    thresholds = load_thresholds()

    if args.check:
        results = [run_check(args.check, thresholds)]
    elif args.category:
        results = run_category(args.category, thresholds)
    else:
        results = run_all_checks(thresholds)

    summary = get_summary(results)

    if args.store:
        store_results(results)

    if args.json:
        output = {
            "summary": summary,
            "checks": [r.to_dict() for r in results],
        }
        print(json.dumps(output, indent=2))
    else:
        print_report(results, summary)

    critical_results = [r for r in results if r.status == "critical"]
    if args.alert and critical_results:
        send_alert_email(summary, critical_results)

    # Exit code
    if critical_results:
        sys.exit(2)
    elif any(r.status == "warning" for r in results):
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
