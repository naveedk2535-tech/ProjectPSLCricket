"""
Ensemble Model — blends all individual models into final prediction.
Supports weighted average and stacking meta-model (LogisticRegression).
Also handles value bet identification using Kelly criterion.
"""

import os
import json
import pickle
import math
import numpy as np
from datetime import datetime

import config
from database import db
from models import batting_bowling, elo, xgboost_model, sentiment as sentiment_model, over_under, player_strength

STACKER_PATH = os.path.join(config.CACHE_DIR, "stacker_model.pkl")
WEIGHTS_PATH = os.path.join(config.CACHE_DIR, "optimized_weights.json")


def predict(team_a, team_b, venue=None, match_date=None, league="psl"):
    """
    Generate ensemble prediction by blending all models.
    1. Get individual model predictions
    2. Try stacking meta-model
    3. Fallback to weighted average
    4. Add over/under and prop bets
    5. Add weather/dew context
    """
    predictions = {}

    # Get individual predictions (graceful degradation)
    try:
        predictions["batting_bowling"] = batting_bowling.predict(team_a, team_b, venue, league=league)
    except Exception as e:
        predictions["batting_bowling"] = {"team_a_win": 0.5, "team_b_win": 0.5, "confidence": 0.3,
                                           "details": {"model": "batting_bowling", "error": str(e)}}

    try:
        predictions["elo"] = elo.predict(team_a, team_b, venue, league=league)
    except Exception as e:
        predictions["elo"] = {"team_a_win": 0.5, "team_b_win": 0.5, "confidence": 0.3,
                               "details": {"model": "elo", "error": str(e)}}

    xgb_pred = None
    try:
        xgb_pred = xgboost_model.predict(team_a, team_b, venue, match_date, league=league)
    except Exception:
        pass

    if xgb_pred:
        predictions["xgboost"] = xgb_pred
    else:
        predictions["xgboost"] = {"team_a_win": 0.5, "team_b_win": 0.5, "confidence": 0.3,
                                    "details": {"model": "xgboost", "error": "not trained"}}

    try:
        predictions["sentiment"] = sentiment_model.predict(team_a, team_b, league=league)
    except Exception as e:
        predictions["sentiment"] = {"team_a_win": 0.5, "team_b_win": 0.5, "confidence": 0.3,
                                     "details": {"model": "sentiment", "error": str(e)}}

    try:
        predictions["player_strength"] = player_strength.predict(team_a, team_b, venue, match_date, league=league)
    except Exception as e:
        predictions["player_strength"] = {"team_a_win": 0.5, "team_b_win": 0.5, "confidence": 0.3,
                                           "details": {"model": "player_strength", "error": str(e)}}

    # Try stacking ensemble
    stacked = _stacker_predict(predictions, team_a, team_b)
    if stacked:
        team_a_win = stacked["team_a_win"]
        team_b_win = stacked["team_b_win"]
        blend_method = "stacking"
    else:
        # Weighted average
        weights = _load_weights()
        team_a_win = sum(weights[m] * predictions[m]["team_a_win"] for m in weights if m in predictions)
        team_b_win = sum(weights[m] * predictions[m]["team_b_win"] for m in weights if m in predictions)

        # Normalize
        total = team_a_win + team_b_win
        if total > 0:
            team_a_win /= total
            team_b_win /= total
        blend_method = "weighted_average"

    # Apply dew adjustment
    dew_adjustment = 0.0
    weather = None
    if venue and match_date:
        weather = db.fetch_one(
            "SELECT * FROM weather WHERE venue = ? AND match_date = ?",
            [venue, match_date]
        )
        if weather and weather.get("dew_score", 0) > 0:
            # Dew favors team batting second (which could be either team)
            # This is a toss-dependent adjustment — flag it as advice
            dew_adjustment = weather.get("dew_score", 0) * 0.05

    # Model agreement score (how much do models agree?)
    all_a = [predictions[m]["team_a_win"] for m in predictions]
    agreement = 1 - np.std(all_a) * 2 if len(all_a) > 1 else 0.5

    # Confidence
    confidence = max(team_a_win, team_b_win) * agreement
    confidence = min(0.95, max(0.35, confidence))

    # Over/under and prop bets
    ou_pred = None
    try:
        ou_pred = over_under.predict(team_a, team_b, venue, match_date, league=league)
    except Exception:
        pass

    # Predicted totals (from batting/bowling model)
    bb = predictions.get("batting_bowling", {})
    predicted_total_a = bb.get("predicted_total_a", 165)
    predicted_total_b = bb.get("predicted_total_b", 155)

    # Toss advantage assessment
    toss_advantage = "neutral"
    if weather and weather.get("heavy_dew"):
        toss_advantage = "bowl_first"
    elif venue:
        v_stats = db.fetch_one("SELECT chase_win_pct FROM venue_stats WHERE venue = ? AND league = ?", [venue, league])
        if v_stats and v_stats["chase_win_pct"]:
            if v_stats["chase_win_pct"] > 55:
                toss_advantage = "bowl_first"
            elif v_stats["chase_win_pct"] < 45:
                toss_advantage = "bat_first"

    result = {
        "team_a_win": round(team_a_win, 4),
        "team_b_win": round(team_b_win, 4),
        "predicted_total_a": round(predicted_total_a, 1),
        "predicted_total_b": round(predicted_total_b, 1),
        "confidence": round(confidence, 3),
        "blend_method": blend_method,
        "toss_advantage": toss_advantage,
        "dew_factor": round(dew_adjustment, 3),
        "venue_bias": toss_advantage,
        "model_agreement": round(agreement, 3),
        "model_details": {m: predictions[m] for m in predictions},
    }

    # Add over/under
    if ou_pred:
        result["over_under"] = {
            "line": ou_pred["line"],
            "over_prob": ou_pred["over_prob"],
            "under_prob": ou_pred["under_prob"],
            "expected_total": ou_pred["expected_total"],
        }
        result["prop_bets"] = ou_pred.get("prop_bets", {})
        result["total_wides_pred"] = ou_pred["prop_bets"].get("total_wides", 8)
        result["total_noballs_pred"] = ou_pred["prop_bets"].get("total_noballs", 2)
        result["total_sixes_pred"] = ou_pred["prop_bets"].get("total_sixes", 12)
        result["total_fours_pred"] = ou_pred["prop_bets"].get("total_fours", 24)
        result["over_under_line"] = ou_pred["line"]
        result["over_prob"] = ou_pred["over_prob"]
        result["under_prob"] = ou_pred["under_prob"]

    # Weather summary
    if weather:
        result["weather"] = {
            "temperature": weather.get("temperature"),
            "humidity": weather.get("humidity"),
            "dew_point": weather.get("dew_point"),
            "dew_score": weather.get("dew_score", 0),
            "heavy_dew": weather.get("heavy_dew", 0),
            "summary": weather.get("weather_summary", ""),
        }

    return result


def _load_weights():
    """Load model weights (optimized or default)."""
    if os.path.exists(WEIGHTS_PATH):
        try:
            with open(WEIGHTS_PATH) as f:
                return json.load(f)
        except Exception:
            pass
    return config.MODEL_WEIGHTS.copy()


def _stacker_predict(predictions, team_a, team_b):
    """Use stacking meta-model if available."""
    if not os.path.exists(STACKER_PATH):
        return None

    try:
        stacker = pickle.load(open(STACKER_PATH, "rb"))

        # Build feature vector for stacker
        features = []
        for model in ["batting_bowling", "elo", "xgboost", "sentiment", "player_strength"]:
            p = predictions.get(model, {"team_a_win": 0.5, "team_b_win": 0.5})
            features.extend([p["team_a_win"], p["team_b_win"]])

        # Additional features
        elo_diff = predictions["elo"]["details"].get("elo_diff", 0) if "details" in predictions.get("elo", {}) else 0
        all_a = [predictions[m]["team_a_win"] for m in predictions]
        agreement = 1 - np.std(all_a) * 2 if len(all_a) > 1 else 0.5
        max_prob = max(all_a) if all_a else 0.5

        features.extend([elo_diff, agreement, max_prob])

        X = np.array([features])
        probs = stacker.predict_proba(X)[0]

        return {
            "team_a_win": float(probs[1]) if len(probs) > 1 else float(probs[0]),
            "team_b_win": float(probs[0]) if len(probs) > 1 else 1 - float(probs[0]),
        }
    except Exception:
        return None


def calculate_value(prediction, odds_data):
    """
    Identify value bets by comparing model probability vs bookmaker implied probability.
    Uses Kelly criterion for stake sizing.
    """
    if not odds_data:
        return []

    value_bets = []
    settings = config.VALUE_BET_SETTINGS

    bet_types = [
        ("team_a_win", prediction["team_a_win"], odds_data.get("team_a_odds"), odds_data.get("implied_prob_a")),
        ("team_b_win", prediction["team_b_win"], odds_data.get("team_b_odds"), odds_data.get("implied_prob_b")),
    ]

    # Over/Under if available
    if prediction.get("over_prob") and odds_data.get("over_odds"):
        bet_types.append(("over", prediction["over_prob"], odds_data["over_odds"],
                          1 / odds_data["over_odds"] if odds_data["over_odds"] > 0 else 0))
    if prediction.get("under_prob") and odds_data.get("under_odds"):
        bet_types.append(("under", prediction["under_prob"], odds_data["under_odds"],
                          1 / odds_data["under_odds"] if odds_data["under_odds"] > 0 else 0))

    for bet_type, model_prob, best_odds, implied_prob in bet_types:
        if not best_odds or not implied_prob or best_odds <= 0:
            continue

        if best_odds < settings["min_odds"] or best_odds > settings["max_odds"]:
            continue

        edge = model_prob - implied_prob
        edge_pct = edge * 100

        if edge_pct >= settings["min_edge_percent"]:
            # Kelly criterion
            kelly = (model_prob * best_odds - 1) / (best_odds - 1) if best_odds > 1 else 0
            kelly_stake = kelly * settings["kelly_fraction"]
            kelly_stake = min(kelly_stake, settings["max_stake_percent"] / 100)
            kelly_stake = max(0, kelly_stake)

            value_bets.append({
                "bet_type": bet_type,
                "model_prob": round(model_prob, 4),
                "implied_prob": round(implied_prob, 4),
                "edge_pct": round(edge_pct, 2),
                "kelly_stake": round(kelly_stake * 100, 2),
                "best_odds": best_odds,
                "bookmaker": odds_data.get("bookmaker", ""),
            })

    return value_bets


def save_prediction(prediction, team_a, team_b, match_date, venue=None, fixture_id=None, league="psl"):
    """Save ensemble prediction to database."""
    db.execute(
        """INSERT INTO predictions (fixture_id, match_date, team_a, team_b, venue, league,
           team_a_win, team_b_win, predicted_total_a, predicted_total_b,
           over_under_line, over_prob, under_prob,
           total_wides_pred, total_noballs_pred, total_sixes_pred, total_fours_pred,
           confidence, model_details, toss_advantage, dew_factor, venue_bias,
           created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(match_date, team_a, team_b) DO UPDATE SET
           team_a_win=excluded.team_a_win, team_b_win=excluded.team_b_win,
           predicted_total_a=excluded.predicted_total_a, predicted_total_b=excluded.predicted_total_b,
           over_under_line=excluded.over_under_line, over_prob=excluded.over_prob,
           confidence=excluded.confidence, model_details=excluded.model_details,
           dew_factor=excluded.dew_factor, updated_at=excluded.updated_at""",
        [fixture_id, match_date, team_a, team_b, venue, league,
         prediction["team_a_win"], prediction["team_b_win"],
         prediction.get("predicted_total_a"), prediction.get("predicted_total_b"),
         prediction.get("over_under_line"), prediction.get("over_prob"), prediction.get("under_prob"),
         prediction.get("total_wides_pred"), prediction.get("total_noballs_pred"),
         prediction.get("total_sixes_pred"), prediction.get("total_fours_pred"),
         prediction["confidence"], json.dumps(prediction.get("model_details", {})),
         prediction.get("toss_advantage"), prediction.get("dew_factor"),
         prediction.get("venue_bias"), db.now_iso(), db.now_iso()]
    )


def optimize_weights(league="psl"):
    """Optimize model weights using historical predictions to minimize Brier score."""
    from scipy.optimize import minimize

    tracker = db.fetch_all("SELECT * FROM model_tracker WHERE status = 'settled' AND league = ?", [league])
    if len(tracker) < 20:
        print("[Ensemble] Need at least 20 settled predictions for optimization")
        return None

    model_names = ["batting_bowling", "elo", "xgboost", "sentiment", "player_strength"]

    predictions_data = []
    actuals = []

    for t in tracker:
        details = json.loads(t.get("model_details", "{}")) if isinstance(t.get("model_details"), str) else {}
        if not details:
            continue

        pred_dict = {}
        for model in model_names:
            if model in details:
                pred_dict[model] = details[model].get("team_a_win", 0.5)
            else:
                pred_dict[model] = 0.5

        predictions_data.append(pred_dict)
        actuals.append(1 if t["actual_winner"] == t["team_a"] else 0)

    if not predictions_data:
        return None

    def brier_objective(weights):
        w = dict(zip(model_names, weights))
        total_w = sum(weights)
        if total_w == 0:
            return 1.0

        brier = 0
        for pred, actual in zip(predictions_data, actuals):
            ensemble_prob = sum(w[m] * pred[m] for m in w) / total_w
            brier += (ensemble_prob - actual) ** 2
        return brier / len(actuals)

    result = minimize(
        brier_objective,
        x0=[0.10, 0.15, 0.40, 0.10, 0.25],
        method="SLSQP",
        bounds=[(0, 1)] * 5,
        constraints={"type": "eq", "fun": lambda w: sum(w) - 1},
    )

    if result.success:
        optimized = dict(zip(model_names, result.x.tolist()))
        optimized = {k: round(v, 4) for k, v in optimized.items()}

        with open(WEIGHTS_PATH, "w") as f:
            json.dump(optimized, f, indent=2)

        print(f"[Ensemble] Optimized weights: {optimized}, Brier: {result.fun:.4f}")
        return optimized

    return None


def train_stacker(league="psl"):
    """Train stacking meta-model (LogisticRegression) on historical predictions."""
    from sklearn.linear_model import LogisticRegression

    tracker = db.fetch_all("SELECT * FROM model_tracker WHERE status = 'settled' AND league = ?", [league])
    if len(tracker) < 30:
        print("[Ensemble] Need at least 30 settled predictions for stacker training")
        return None

    X_list = []
    y_list = []

    for t in tracker:
        details = json.loads(t.get("model_details", "{}")) if isinstance(t.get("model_details"), str) else {}
        if not details:
            continue

        features = []
        for model in ["batting_bowling", "elo", "xgboost", "sentiment", "player_strength"]:
            d = details.get(model, {"team_a_win": 0.5, "team_b_win": 0.5})
            features.extend([d.get("team_a_win", 0.5), d.get("team_b_win", 0.5)])

        # Additional features
        elo_diff = details.get("elo", {}).get("details", {}).get("elo_diff", 0)
        all_a = [details.get(m, {}).get("team_a_win", 0.5) for m in ["batting_bowling", "elo", "xgboost", "sentiment"]]
        agreement = 1 - np.std(all_a) * 2
        max_prob = max(all_a)

        features.extend([elo_diff, agreement, max_prob])
        X_list.append(features)
        y_list.append(1 if t["actual_winner"] == t["team_a"] else 0)

    X = np.array(X_list)
    y = np.array(y_list)

    stacker = LogisticRegression(max_iter=1000, random_state=42)
    stacker.fit(X, y)

    os.makedirs(os.path.dirname(STACKER_PATH), exist_ok=True)
    with open(STACKER_PATH, "wb") as f:
        pickle.dump(stacker, f)

    print(f"[Ensemble] Stacker trained on {len(y)} matches")
    return stacker
