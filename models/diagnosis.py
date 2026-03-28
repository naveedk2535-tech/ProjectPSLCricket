"""
Model self-monitoring and performance tracking.
Detects degradation, logs metrics, suggests improvements.
"""

import json
from datetime import datetime, timedelta
from database import db
import config


def check_model_health():
    """Run all model health checks."""
    checks = []

    # Check XGBoost model age
    import os
    model_path = os.path.join(config.CACHE_DIR, "xgboost_model.pkl")
    if os.path.exists(model_path):
        age_days = (datetime.now().timestamp() - os.path.getmtime(model_path)) / 86400
        checks.append({
            "check": "xgboost_model_age",
            "status": "ok" if age_days < 14 else "warning",
            "message": f"XGBoost model is {age_days:.0f} days old",
            "suggestion": "Retrain with: python scheduler.py --task retrain" if age_days >= 14 else None,
        })
    else:
        checks.append({
            "check": "xgboost_model_exists",
            "status": "warning",
            "message": "No trained XGBoost model found",
            "suggestion": "Train with: python scheduler.py --task retrain",
        })

    # Check prediction accuracy (last 10 settled)
    recent = db.fetch_all(
        "SELECT * FROM model_tracker WHERE status = 'settled' ORDER BY settled_at DESC LIMIT 10"
    )
    if recent:
        correct = sum(1 for r in recent if r.get("top_pick_correct") == 1)
        accuracy = correct / len(recent) * 100
        checks.append({
            "check": "recent_accuracy",
            "status": "ok" if accuracy >= 55 else ("warning" if accuracy >= 45 else "critical"),
            "message": f"Last {len(recent)} predictions: {accuracy:.0f}% accurate ({correct}/{len(recent)})",
            "suggestion": "Consider retraining models" if accuracy < 50 else None,
        })

    # Check data freshness
    last_match = db.fetch_one("SELECT MAX(match_date) as last FROM matches")
    if last_match and last_match["last"]:
        checks.append({
            "check": "data_freshness",
            "status": "ok",
            "message": f"Latest match data: {last_match['last']}",
        })

    # Check prediction count
    pred_count = db.row_count("predictions")
    fixture_count = db.fetch_one(
        "SELECT COUNT(*) as cnt FROM fixtures WHERE status = 'SCHEDULED'"
    )
    upcoming = fixture_count["cnt"] if fixture_count else 0

    if upcoming > 0 and pred_count == 0:
        checks.append({
            "check": "predictions_exist",
            "status": "warning",
            "message": f"{upcoming} upcoming fixtures but no predictions generated",
            "suggestion": "Run: python scheduler.py --task predictions",
        })

    return checks


def log_performance(model_name, predicted_prob, actual_result, league="psl"):
    """
    Log a single prediction result for performance tracking.
    actual_result: 1 if team_a won, 0 if team_b won
    Updates both the monthly period AND the 'overall' period.
    """
    brier = (predicted_prob - actual_result) ** 2
    correct = 1 if (predicted_prob > 0.5 and actual_result == 1) or \
                    (predicted_prob < 0.5 and actual_result == 0) else 0

    month_period = datetime.utcnow().strftime("%Y-%m")

    for period in [month_period, "overall"]:
        existing = db.fetch_one(
            "SELECT * FROM model_performance WHERE model_name = ? AND period = ? AND league = ?",
            [model_name, period, league]
        )

        if existing:
            total = existing["total_predictions"] + 1
            correct_total = existing["correct_predictions"] + correct
            avg_brier = ((existing["brier_score"] or 0) * existing["total_predictions"] + brier) / total
            accuracy = correct_total / total

            db.execute(
                """UPDATE model_performance SET
                   accuracy = ?, brier_score = ?, total_predictions = ?,
                   correct_predictions = ?, evaluated_at = ?
                   WHERE model_name = ? AND period = ? AND league = ?""",
                [accuracy, avg_brier, total, correct_total, db.now_iso(), model_name, period, league]
            )
        else:
            db.execute(
                """INSERT INTO model_performance (model_name, period, accuracy, brier_score,
                   total_predictions, correct_predictions, league, evaluated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [model_name, period, correct, brier, 1, correct, league, db.now_iso()]
            )


def evaluate_all_models_for_match(tracker_entry, league="psl"):
    """
    After settling a match, evaluate ALL individual models by parsing
    model_details from the predictions table.
    """
    match_date = tracker_entry.get("match_date")
    team_a = tracker_entry.get("team_a")
    team_b = tracker_entry.get("team_b")
    actual_winner = tracker_entry.get("actual_winner")

    if not actual_winner:
        return

    actual_result = 1 if actual_winner == team_a else 0

    # Get the prediction with model_details
    pred = db.fetch_one(
        "SELECT model_details FROM predictions WHERE match_date = ? AND "
        "((team_a = ? AND team_b = ?) OR (team_a = ? AND team_b = ?)) AND league = ?",
        [match_date, team_a, team_b, team_b, team_a, league]
    )

    if not pred or not pred.get("model_details"):
        # Just log ensemble
        log_performance("ensemble", tracker_entry.get("team_a_prob", 0.5), actual_result, league)
        return

    try:
        details = json.loads(pred["model_details"]) if isinstance(pred["model_details"], str) else pred["model_details"]
    except (json.JSONDecodeError, TypeError):
        log_performance("ensemble", tracker_entry.get("team_a_prob", 0.5), actual_result, league)
        return

    # Log each model's performance
    for model_name in ["batting_bowling", "elo", "xgboost", "sentiment", "player_strength"]:
        model_data = details.get(model_name, {})
        if isinstance(model_data, dict):
            model_prob = model_data.get("team_a_win", 0.5)
            if isinstance(model_prob, (int, float)):
                log_performance(model_name, model_prob, actual_result, league)

    # Log ensemble
    log_performance("ensemble", tracker_entry.get("team_a_prob", 0.5), actual_result, league)


def get_performance_summary():
    """Get performance summary for all models."""
    models = db.fetch_all(
        """SELECT model_name,
           SUM(total_predictions) as total,
           SUM(correct_predictions) as correct,
           AVG(brier_score) as avg_brier,
           AVG(accuracy) as avg_accuracy
           FROM model_performance
           GROUP BY model_name"""
    )

    summary = {}
    for m in models:
        summary[m["model_name"]] = {
            "total_predictions": m["total"],
            "correct_predictions": m["correct"],
            "accuracy": round(m["avg_accuracy"], 1) if m["avg_accuracy"] else 0,
            "brier_score": round(m["avg_brier"], 4) if m["avg_brier"] else 0,
        }

    return summary
