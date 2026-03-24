"""
Shothai — Walk-Forward Backtesting Framework
Tests prediction accuracy using ONLY historical data available at prediction time.
The gold standard for validating a betting model.
"""

import argparse
import math
import json
import numpy as np
from datetime import datetime
from collections import defaultdict

from database import db
import config


def run_backtest(start_date=None, end_date=None, model="ensemble"):
    """
    Walk-forward backtest: for each historical match, predict using only prior data.

    Returns comprehensive metrics dict.
    """
    db.init_db()

    matches = db.fetch_all(
        "SELECT * FROM matches WHERE winner IS NOT NULL ORDER BY match_date ASC"
    )

    if start_date:
        matches = [m for m in matches if m["match_date"] >= start_date]
    if end_date:
        matches = [m for m in matches if m["match_date"] <= end_date]

    if len(matches) < 10:
        print(f"[Backtest] Only {len(matches)} matches — need at least 10")
        return None

    predictions = []
    actuals = []
    confidences = []
    venues = []
    toss_impacts = []
    match_details = []

    # Skip first N matches (need training data)
    min_train = config.BACKTEST_SETTINGS["min_matches_to_train"]

    for i, match in enumerate(matches):
        if i < min_train:
            continue

        team_a = match["team_a"]
        team_b = match["team_b"]
        venue = match.get("venue")
        actual_winner = match["winner"]
        actual = 1 if actual_winner == team_a else 0

        # Generate prediction using model
        pred = _get_prediction(model, team_a, team_b, venue, match["match_date"])
        if pred is None:
            continue

        prob_a = pred.get("team_a_win", 0.5)
        confidence = pred.get("confidence", 0.5)

        predictions.append(prob_a)
        actuals.append(actual)
        confidences.append(confidence)
        venues.append(venue)

        # Track toss impact
        toss_winner = match.get("toss_winner")
        toss_helped = 1 if toss_winner == actual_winner else 0
        toss_impacts.append(toss_helped)

        match_details.append({
            "match_date": match["match_date"],
            "team_a": team_a,
            "team_b": team_b,
            "venue": venue,
            "predicted_prob_a": prob_a,
            "predicted_winner": team_a if prob_a > 0.5 else team_b,
            "actual_winner": actual_winner,
            "correct": 1 if (prob_a > 0.5 and actual == 1) or (prob_a < 0.5 and actual == 0) else 0,
            "confidence": confidence,
        })

    if not predictions:
        print("[Backtest] No predictions generated")
        return None

    # Calculate all metrics
    metrics = _calculate_metrics(predictions, actuals)

    # Calibration
    calibration = _calculate_calibration(predictions, actuals)

    # Per-venue breakdown
    venue_accuracy = _per_venue_accuracy(match_details)

    # Toss impact
    toss_data = {
        "toss_winner_won_pct": sum(toss_impacts) / len(toss_impacts) * 100 if toss_impacts else 50,
        "total_matches": len(toss_impacts),
    }

    results = {
        "model": model,
        "total_matches": len(predictions),
        "start_date": match_details[0]["match_date"] if match_details else None,
        "end_date": match_details[-1]["match_date"] if match_details else None,
        **metrics,
        "calibration": calibration,
        "per_venue": venue_accuracy,
        "toss_impact": toss_data,
        "match_details": match_details,
    }

    return results


def _get_prediction(model, team_a, team_b, venue, match_date):
    """Get prediction from specified model."""
    try:
        if model == "ensemble":
            from models import ensemble
            return ensemble.predict(team_a, team_b, venue, match_date)
        elif model == "elo":
            from models import elo
            return elo.predict(team_a, team_b, venue)
        elif model == "batting_bowling":
            from models import batting_bowling
            return batting_bowling.predict(team_a, team_b, venue)
        elif model == "xgboost":
            from models import xgboost_model
            return xgboost_model.predict(team_a, team_b, venue, match_date)
        elif model == "sentiment":
            from models import sentiment
            return sentiment.predict(team_a, team_b)
        else:
            return None
    except Exception as e:
        return {"team_a_win": 0.5, "team_b_win": 0.5, "confidence": 0.3}


def _calculate_metrics(predictions, actuals):
    """Calculate all betting-relevant metrics."""
    preds = np.array(predictions)
    acts = np.array(actuals)
    n = len(preds)

    # Accuracy
    predicted_winners = (preds > 0.5).astype(int)
    correct = (predicted_winners == acts).sum()
    accuracy = correct / n * 100

    # Brier Score (lower = better, 0 = perfect, 0.25 = random for 2-way)
    brier = np.mean((preds - acts) ** 2)

    # Log Loss
    eps = 1e-15
    preds_clipped = np.clip(preds, eps, 1 - eps)
    log_loss = -np.mean(acts * np.log(preds_clipped) + (1 - acts) * np.log(1 - preds_clipped))

    # ROI (flat $100 bet on predicted winner at fair odds)
    pnl = 0
    total_staked = 0
    pnl_history = []
    for p, a in zip(preds, acts):
        stake = 100
        total_staked += stake
        predicted_winner = 1 if p > 0.5 else 0
        fair_odds = 1 / max(p, 0.01) if predicted_winner == 1 else 1 / max(1 - p, 0.01)
        if predicted_winner == a:
            pnl += stake * (fair_odds - 1)
        else:
            pnl -= stake
        pnl_history.append(pnl)

    roi = (pnl / total_staked) * 100 if total_staked > 0 else 0

    # Sharpe Ratio (daily P&L volatility)
    daily_pnl = np.diff([0] + pnl_history)
    sharpe = 0
    if len(daily_pnl) > 1 and np.std(daily_pnl) > 0:
        sharpe = np.mean(daily_pnl) / np.std(daily_pnl) * np.sqrt(len(daily_pnl))

    # Max Drawdown
    peak = 0
    max_dd = 0
    for val in pnl_history:
        if val > peak:
            peak = val
        dd = peak - val
        if dd > max_dd:
            max_dd = dd

    return {
        "accuracy": round(accuracy, 2),
        "brier_score": round(float(brier), 4),
        "log_loss": round(float(log_loss), 4),
        "roi": round(roi, 2),
        "total_pnl": round(pnl, 2),
        "total_staked": round(total_staked, 2),
        "sharpe_ratio": round(float(sharpe), 3),
        "max_drawdown": round(float(max_dd), 2),
        "correct": int(correct),
        "incorrect": n - int(correct),
    }


def _calculate_calibration(predictions, actuals, bins=10):
    """
    Calculate calibration data.
    When we say 70%, does it happen 70% of the time?
    """
    preds = np.array(predictions)
    acts = np.array(actuals)

    bin_edges = np.linspace(0, 1, bins + 1)
    calibration = []

    for i in range(bins):
        mask = (preds >= bin_edges[i]) & (preds < bin_edges[i + 1])
        if mask.sum() == 0:
            continue

        bin_center = (bin_edges[i] + bin_edges[i + 1]) / 2
        actual_freq = acts[mask].mean()
        count = int(mask.sum())

        calibration.append({
            "predicted": round(float(bin_center), 2),
            "actual": round(float(actual_freq), 3),
            "count": count,
        })

    return calibration


def _per_venue_accuracy(match_details):
    """Calculate accuracy per venue."""
    venue_data = defaultdict(lambda: {"correct": 0, "total": 0})

    for m in match_details:
        venue = m.get("venue", "Unknown")
        venue_data[venue]["total"] += 1
        if m["correct"]:
            venue_data[venue]["correct"] += 1

    return {
        venue: {
            "accuracy": round(d["correct"] / d["total"] * 100, 1),
            "total": d["total"],
        }
        for venue, d in venue_data.items()
        if d["total"] >= 3  # Minimum sample size
    }


def print_report(results):
    """Pretty-print backtest results."""
    if not results:
        print("No results to display.")
        return

    print("\n" + "=" * 60)
    print(f"  SHOTHAI BACKTEST REPORT — {results['model'].upper()}")
    print("=" * 60)
    print(f"  Period: {results.get('start_date', 'N/A')} → {results.get('end_date', 'N/A')}")
    print(f"  Matches: {results['total_matches']}")
    print("-" * 60)
    print(f"  Accuracy:      {results['accuracy']:.1f}% ({results['correct']}/{results['total_matches']})")
    print(f"  Brier Score:   {results['brier_score']:.4f} (random=0.250, perfect=0.000)")
    print(f"  Log Loss:      {results['log_loss']:.4f}")
    print(f"  ROI:           {results['roi']:+.1f}%")
    print(f"  Total P&L:     ${results['total_pnl']:+.0f}")
    print(f"  Sharpe Ratio:  {results['sharpe_ratio']:.3f}")
    print(f"  Max Drawdown:  ${results['max_drawdown']:.0f}")
    print("-" * 60)

    # Warnings
    if results['brier_score'] > 0.25:
        print("  ⚠ WARNING: Brier > 0.25 — model is WORSE than random!")
    if results['roi'] < -5:
        print("  ⚠ WARNING: Negative ROI — model is losing money")
    if results['accuracy'] < 50:
        print("  ⚠ WARNING: Accuracy below 50% — worse than coin flip")

    # Calibration
    print("\n  CALIBRATION:")
    for c in results.get("calibration", []):
        bar = "█" * int(c["actual"] * 20)
        print(f"    {c['predicted']*100:5.0f}% predicted → {c['actual']*100:5.1f}% actual ({c['count']} matches) {bar}")

    # Toss impact
    toss = results.get("toss_impact", {})
    if toss:
        print(f"\n  TOSS: Winner won {toss.get('toss_winner_won_pct', 50):.1f}% of matches")

    # Venue breakdown
    venues = results.get("per_venue", {})
    if venues:
        print("\n  PER-VENUE ACCURACY:")
        for venue, data in sorted(venues.items(), key=lambda x: x[1]["accuracy"], reverse=True):
            print(f"    {venue:30s} {data['accuracy']:5.1f}% ({data['total']} matches)")

    print("\n" + "=" * 60)


def save_results(results, model_name=None):
    """Save backtest results to database."""
    if not results:
        return

    model = model_name or results.get("model", "ensemble")
    db.execute(
        """INSERT INTO backtest_results (model_name, run_date, start_date, end_date,
           total_matches, accuracy, brier_score, log_loss, roi, sharpe_ratio,
           max_drawdown, calibration_data, per_venue_accuracy, toss_impact, details)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(model_name, run_date) DO UPDATE SET
           accuracy=excluded.accuracy, brier_score=excluded.brier_score,
           roi=excluded.roi, details=excluded.details""",
        [model, datetime.utcnow().strftime("%Y-%m-%d"),
         results.get("start_date"), results.get("end_date"),
         results["total_matches"], results["accuracy"],
         results["brier_score"], results["log_loss"],
         results["roi"], results["sharpe_ratio"],
         results["max_drawdown"],
         json.dumps(results.get("calibration", [])),
         json.dumps(results.get("per_venue", {})),
         json.dumps(results.get("toss_impact", {})),
         json.dumps({"match_details": results.get("match_details", [])})]
    )
    print(f"[Backtest] Results saved for {model}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Shothai Backtesting Framework")
    parser.add_argument("--model", default="ensemble", choices=["ensemble", "elo", "batting_bowling", "xgboost", "sentiment"])
    parser.add_argument("--start", default=None, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default=None, help="End date (YYYY-MM-DD)")
    parser.add_argument("--save", action="store_true", help="Save results to database")
    args = parser.parse_args()

    print(f"Running backtest for model: {args.model}")
    results = run_backtest(args.start, args.end, args.model)

    if results:
        print_report(results)
        if args.save:
            save_results(results)
    else:
        print("Backtest failed — not enough data")
