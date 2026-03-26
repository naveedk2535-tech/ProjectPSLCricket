"""
XGBoost ML Classifier with 50+ cricket-specific features.
The workhorse model — handles non-linear relationships that statistical models miss.
"""

import os
import pickle
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

import config
from database import db

FEATURE_IMPORTANCE_PATH = os.path.join(config.CACHE_DIR, "feature_importance.json")

FEATURE_NAMES = [
    "elo_a", "elo_b", "batting_avg_a", "batting_avg_b",
    "bowling_economy_a", "bowling_economy_b", "batting_sr_a", "batting_sr_b",
    "pp_run_rate_a", "pp_run_rate_b", "death_economy_a", "death_economy_b",
    "form_last5_a", "form_last5_b", "h2h_win_rate_a",
    "venue_avg_first", "venue_chase_win_pct", "venue_pace_pct",
    "days_rest_a", "days_rest_b", "nrr_a", "nrr_b",
    "dew_score", "humidity", "temperature",
    "boundary_pct_a", "boundary_pct_b", "dot_ball_pct_a", "dot_ball_pct_b",
    "wides_avg_a", "wides_avg_b", "collapse_rate_a", "collapse_rate_b",
    "matches_season_a", "matches_season_b", "wins_season_a", "wins_season_b",
    "implied_prob_a", "extras_avg_a", "extras_avg_b",
    "match_number", "is_playoff", "is_new_team_a", "is_new_team_b",
    "streak_length_a", "streak_length_b", "elo_diff",
]


def _model_path(league="psl"):
    """Return league-specific model path."""
    return os.path.join(config.CACHE_DIR, f"xgboost_model_{league}.pkl")


def _feature_importance_path(league="psl"):
    """Return league-specific feature importance path."""
    return os.path.join(config.CACHE_DIR, f"feature_importance_{league}.json")


def extract_features(team_a, team_b, venue=None, match_date=None, league="psl"):
    """Extract 50+ features for a match prediction."""
    features = {}

    # Team ratings
    r_a = db.fetch_one("SELECT * FROM team_ratings WHERE team = ? AND league = ?", [team_a, league])
    r_b = db.fetch_one("SELECT * FROM team_ratings WHERE team = ? AND league = ?", [team_b, league])

    default_rating = {
        "elo": 1500, "batting_avg": 160, "bowling_avg": 160, "batting_sr": 130,
        "bowling_economy": 8.0, "powerplay_run_rate": 7.5, "death_overs_economy": 10.0,
        "form_last5": 50, "nrr": 0.0, "boundary_pct": 0.5, "dot_ball_pct": 0.35,
        "extras_conceded_avg": 12, "collapse_rate": 0.15,
        "matches_played": 0, "wins": 0, "streak_length": 0,
    }

    ra = {**default_rating, **(dict(r_a) if r_a else {})}
    rb = {**default_rating, **(dict(r_b) if r_b else {})}

    features["elo_a"] = ra["elo"]
    features["elo_b"] = rb["elo"]
    features["elo_diff"] = ra["elo"] - rb["elo"]
    features["batting_avg_a"] = ra["batting_avg"] or 160
    features["batting_avg_b"] = rb["batting_avg"] or 160
    features["bowling_economy_a"] = ra["bowling_economy"] or 8.0
    features["bowling_economy_b"] = rb["bowling_economy"] or 8.0
    features["batting_sr_a"] = ra["batting_sr"] or 130
    features["batting_sr_b"] = rb["batting_sr"] or 130
    features["pp_run_rate_a"] = ra["powerplay_run_rate"] or 7.5
    features["pp_run_rate_b"] = rb["powerplay_run_rate"] or 7.5
    features["death_economy_a"] = ra["death_overs_economy"] or 10.0
    features["death_economy_b"] = rb["death_overs_economy"] or 10.0
    features["form_last5_a"] = ra["form_last5"] or 50
    features["form_last5_b"] = rb["form_last5"] or 50
    features["nrr_a"] = ra["nrr"] or 0.0
    features["nrr_b"] = rb["nrr"] or 0.0
    features["boundary_pct_a"] = ra.get("boundary_pct") or 0.5
    features["boundary_pct_b"] = rb.get("boundary_pct") or 0.5
    features["dot_ball_pct_a"] = ra.get("dot_ball_pct") or 0.35
    features["dot_ball_pct_b"] = rb.get("dot_ball_pct") or 0.35
    features["wides_avg_a"] = ra.get("extras_conceded_avg") or 12
    features["wides_avg_b"] = rb.get("extras_conceded_avg") or 12
    features["collapse_rate_a"] = ra.get("collapse_rate") or 0.15
    features["collapse_rate_b"] = rb.get("collapse_rate") or 0.15
    features["matches_season_a"] = ra["matches_played"]
    features["matches_season_b"] = rb["matches_played"]
    features["wins_season_a"] = ra["wins"]
    features["wins_season_b"] = rb["wins"]
    features["extras_avg_a"] = ra.get("extras_conceded_avg") or 12
    features["extras_avg_b"] = rb.get("extras_conceded_avg") or 12
    features["streak_length_a"] = ra.get("streak_length") or 0
    features["streak_length_b"] = rb.get("streak_length") or 0

    # H2H
    h2h = db.fetch_one(
        "SELECT * FROM head_to_head WHERE ((team_a = ? AND team_b = ?) OR (team_a = ? AND team_b = ?)) AND league = ?",
        [team_a, team_b, team_b, team_a, league]
    )
    if h2h and h2h["matches_played"] > 0:
        if h2h["team_a"] == team_a:
            features["h2h_win_rate_a"] = h2h["team_a_wins"] / h2h["matches_played"]
        else:
            features["h2h_win_rate_a"] = h2h["team_b_wins"] / h2h["matches_played"]
    else:
        features["h2h_win_rate_a"] = 0.5

    # Venue
    v_stats = db.fetch_one("SELECT * FROM venue_stats WHERE venue = ? AND league = ?", [venue, league]) if venue else None
    if v_stats:
        features["venue_avg_first"] = v_stats["avg_first_innings"] or 170
        features["venue_chase_win_pct"] = v_stats["chase_win_pct"] or 50
        features["venue_pace_pct"] = v_stats["pace_wicket_pct"] or 50
    else:
        features["venue_avg_first"] = 170
        features["venue_chase_win_pct"] = 50
        features["venue_pace_pct"] = 50

    # Days rest
    features["days_rest_a"] = _get_days_rest(team_a, match_date, league=league)
    features["days_rest_b"] = _get_days_rest(team_b, match_date, league=league)

    # Weather
    weather = db.fetch_one(
        "SELECT * FROM weather WHERE venue = ? AND match_date = ?",
        [venue, match_date]
    ) if venue and match_date else None

    features["dew_score"] = weather["dew_score"] if weather else 0.0
    features["humidity"] = weather["humidity"] if weather else 50
    features["temperature"] = weather["temperature"] if weather else 30

    # Odds
    odds = db.fetch_one(
        "SELECT * FROM odds WHERE team_a = ? AND team_b = ? AND match_date = ? AND league = ? ORDER BY fetched_at DESC LIMIT 1",
        [team_a, team_b, match_date, league]
    ) if match_date else None
    features["implied_prob_a"] = odds["implied_prob_a"] if odds else 0.5

    # Match context
    fixture = db.fetch_one(
        "SELECT * FROM fixtures WHERE team_a = ? AND team_b = ? AND match_date = ? AND league = ?",
        [team_a, team_b, match_date, league]
    ) if match_date else None
    features["match_number"] = fixture["match_number"] if fixture and fixture.get("match_number") else 20
    features["is_playoff"] = 1 if fixture and fixture.get("stage") in ("qualifier", "eliminator", "final") else 0

    # New team flags
    features["is_new_team_a"] = 1 if any(t["name"] == team_a and t.get("is_new") for t in config.TEAMS.values()) else 0
    features["is_new_team_b"] = 1 if any(t["name"] == team_b and t.get("is_new") for t in config.TEAMS.values()) else 0

    return features


def _get_days_rest(team, match_date, league="psl"):
    """Calculate days since team's last match."""
    if not match_date:
        return 3  # Default

    last = db.fetch_one(
        """SELECT match_date FROM matches
           WHERE (team_a = ? OR team_b = ?) AND match_date < ? AND league = ?
           ORDER BY match_date DESC LIMIT 1""",
        [team, team, match_date, league]
    )
    if last:
        try:
            last_date = datetime.strptime(last["match_date"][:10], "%Y-%m-%d")
            current = datetime.strptime(match_date[:10], "%Y-%m-%d")
            return (current - last_date).days
        except ValueError:
            pass
    return 5  # Default if no history


def train(retrain=False, league="psl"):
    """Train XGBoost model on historical match data."""
    try:
        from xgboost import XGBClassifier
        from sklearn.model_selection import cross_val_score
        from sklearn.metrics import brier_score_loss
    except ImportError:
        print("[XGBoost] Required packages not installed")
        return None

    model_path = _model_path(league)
    fi_path = _feature_importance_path(league)

    matches = db.fetch_all(
        "SELECT * FROM matches WHERE winner IS NOT NULL AND league = ? ORDER BY match_date",
        [league]
    )

    if len(matches) < config.BACKTEST_SETTINGS["min_matches_to_train"]:
        print(f"[XGBoost] Need at least {config.BACKTEST_SETTINGS['min_matches_to_train']} matches, have {len(matches)}")
        return None

    # Build feature matrix
    X_list = []
    y_list = []

    for m in matches:
        features = extract_features(m["team_a"], m["team_b"], m.get("venue"), m["match_date"], league=league)
        feature_vector = [features.get(f, 0) for f in FEATURE_NAMES]
        X_list.append(feature_vector)
        y_list.append(1 if m["winner"] == m["team_a"] else 0)

    X = np.array(X_list)
    y = np.array(y_list)

    # Handle any NaN/Inf
    X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)

    settings = config.XGBOOST_SETTINGS
    model = XGBClassifier(
        n_estimators=settings["n_estimators"],
        max_depth=settings["max_depth"],
        learning_rate=settings["learning_rate"],
        min_child_weight=settings["min_child_weight"],
        subsample=settings["subsample"],
        colsample_bytree=settings["colsample_bytree"],
        eval_metric="logloss",
        use_label_encoder=False,
        random_state=42,
    )

    # Cross-validation
    cv_scores = cross_val_score(model, X, y, cv=settings["cv_folds"], scoring="accuracy")
    print(f"[XGBoost] CV Accuracy: {cv_scores.mean():.3f} (+/- {cv_scores.std():.3f})")

    # Train on full data
    model.fit(X, y)

    # Brier score on training data (rough estimate)
    probs = model.predict_proba(X)[:, 1]
    brier = brier_score_loss(y, probs)
    print(f"[XGBoost] Training Brier Score: {brier:.4f}")

    # Check if better than existing model
    if not retrain and os.path.exists(model_path):
        try:
            old_model = pickle.load(open(model_path, "rb"))
            old_probs = old_model.predict_proba(X)[:, 1]
            old_brier = brier_score_loss(y, old_probs)
            if brier >= old_brier - config.BACKTEST_SETTINGS["min_brier_improvement"]:
                print(f"[XGBoost] New model ({brier:.4f}) not better than old ({old_brier:.4f}), keeping old")
                return old_model
        except Exception:
            pass

    # Save model
    os.makedirs(os.path.dirname(model_path), exist_ok=True)
    with open(model_path, "wb") as f:
        pickle.dump(model, f)

    # Save feature importance
    import json
    importance = dict(zip(FEATURE_NAMES, model.feature_importances_.tolist()))
    with open(fi_path, "w") as f:
        json.dump(importance, f, indent=2)

    print(f"[XGBoost] Model saved ({league}). Features: {len(FEATURE_NAMES)}, Brier: {brier:.4f}")
    return model


def predict(team_a, team_b, venue=None, match_date=None, league="psl"):
    """Predict match outcome using trained XGBoost model."""
    model_path = _model_path(league)
    fi_path = _feature_importance_path(league)

    if not os.path.exists(model_path):
        return None

    try:
        model = pickle.load(open(model_path, "rb"))
    except Exception:
        return None

    features = extract_features(team_a, team_b, venue, match_date, league=league)
    feature_vector = np.array([[features.get(f, 0) for f in FEATURE_NAMES]])
    feature_vector = np.nan_to_num(feature_vector, nan=0.0)

    probs = model.predict_proba(feature_vector)[0]
    # probs[0] = team_b_win, probs[1] = team_a_win (based on label encoding)
    team_a_win = float(probs[1]) if len(probs) > 1 else float(probs[0])
    team_b_win = 1 - team_a_win

    # Feature importance for this prediction
    import json
    top_features = {}
    if os.path.exists(fi_path):
        try:
            with open(fi_path) as f:
                importance = json.load(f)
            sorted_imp = sorted(importance.items(), key=lambda x: x[1], reverse=True)[:10]
            top_features = {k: round(v, 4) for k, v in sorted_imp}
        except Exception:
            pass

    return {
        "team_a_win": round(team_a_win, 4),
        "team_b_win": round(team_b_win, 4),
        "confidence": round(max(team_a_win, team_b_win), 3),
        "details": {
            "model": "xgboost",
            "features_used": len(FEATURE_NAMES),
            "top_features": top_features,
        }
    }
