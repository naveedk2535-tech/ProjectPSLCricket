#!/usr/bin/env python3
"""
PSL Cricket Prediction Engine -- Walk-Forward Backtesting Framework
=====================================================================
Tests prediction accuracy using ONLY historical data available at
prediction time.  The gold standard for validating a betting model.

Usage:
    python backtest.py                           # Full backtest with ensemble
    python backtest.py --model elo               # Backtest Elo only
    python backtest.py --start 2020-01-01        # Custom date range
    python backtest.py --save                    # Save results to DB
"""

import argparse
import json
import math
import os
import sys
import time
from datetime import datetime

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from database import db
from data.team_names import standardise
from models import batting_bowling, elo, xgboost_model, over_under
from models import ensemble
from models import sentiment as sentiment_model


# ============================================================================
#  Core backtesting engine
# ============================================================================

def run_backtest(start_date=None, end_date=None, model="ensemble"):
    """
    Walk-forward backtest: iterate through historical matches chronologically,
    generate a prediction for each match using only prior data, then compare
    to the actual result.

    Args:
        start_date: First match date to include (YYYY-MM-DD). Defaults to
                     earliest match after minimum training window.
        end_date:    Last match date to include. Defaults to latest match.
        model:       Model to test -- 'ensemble', 'elo', 'batting_bowling',
                     'xgboost', 'sentiment'.

    Returns:
        dict with keys: accuracy, brier_score, log_loss, roi, sharpe_ratio,
                        max_drawdown, calibration, total_matches, predictions,
                        per_venue, model_name, start_date, end_date.
    """
    db.init_db()

    # Fetch all completed matches in chronological order
    matches = db.fetch_all(
        "SELECT * FROM matches WHERE winner IS NOT NULL ORDER BY match_date ASC"
    )
    if not matches:
        print("[Backtest] No completed matches found in database")
        return None

    min_train = config.BACKTEST_SETTINGS["min_matches_to_train"]

    # Apply date filters
    if start_date:
        matches = [m for m in matches if m["match_date"] >= start_date]
    if end_date:
        matches = [m for m in matches if m["match_date"] <= end_date]

    if len(matches) < min_train + 5:
        print(f"[Backtest] Need at least {min_train + 5} matches, have {len(matches)}")
        return None

    # We need at least min_train matches before we start predicting
    all_matches = db.fetch_all(
        "SELECT * FROM matches WHERE winner IS NOT NULL ORDER BY match_date ASC"
    )

    # Build index of match dates for the training window
    predictions_list = []
    actuals_list = []
    odds_list = []
    venues = []
    dates = []

    print(f"[Backtest] Model: {model}")
    print(f"[Backtest] Matches available: {len(all_matches)}")
    print(f"[Backtest] Testing on: {len(matches)} matches")
    print(f"[Backtest] Training window: first {min_train} matches")
    print()

    for i, match in enumerate(matches):
        # Find how many matches occurred before this one (across all history)
        prior_count = sum(1 for m in all_matches if m["match_date"] < match["match_date"])
        if prior_count < min_train:
            continue

        team_a = match["team_a"]
        team_b = match["team_b"]
        venue = match.get("venue")
        match_date = match["match_date"]
        actual_winner = match["winner"]

        # Generate prediction using the chosen model
        prob_a = _get_prediction(model, team_a, team_b, venue, match_date)
        if prob_a is None:
            continue

        actual = 1 if actual_winner == team_a else 0
        predictions_list.append(prob_a)
        actuals_list.append(actual)
        venues.append(venue)
        dates.append(match_date)

        # Fetch odds for ROI calculation
        odds_row = db.fetch_one(
            """SELECT * FROM odds WHERE match_date = ? AND team_a = ? AND team_b = ?
               ORDER BY fetched_at DESC LIMIT 1""",
            [match_date, team_a, team_b]
        )
        if odds_row:
            odds_list.append({
                "team_a_odds": odds_row.get("team_a_odds"),
                "team_b_odds": odds_row.get("team_b_odds"),
            })
        else:
            odds_list.append(None)

        # Progress indicator
        if (len(predictions_list)) % 25 == 0:
            running_acc = sum(
                1 for p, a in zip(predictions_list, actuals_list)
                if (p > 0.5 and a == 1) or (p < 0.5 and a == 0)
            ) / len(predictions_list)
            print(f"  ... {len(predictions_list)} predictions | running accuracy: {running_acc:.1%}")

    if not predictions_list:
        print("[Backtest] No predictions generated")
        return None

    # Calculate all metrics
    preds = np.array(predictions_list)
    acts = np.array(actuals_list)
    metrics = _calculate_metrics(preds, acts, odds_list)
    calibration = _calculate_calibration(preds, acts)

    # Per-venue breakdown
    per_venue = {}
    for p, a, v in zip(predictions_list, actuals_list, venues):
        vname = v or "Unknown"
        if vname not in per_venue:
            per_venue[vname] = {"predictions": [], "actuals": []}
        per_venue[vname]["predictions"].append(p)
        per_venue[vname]["actuals"].append(a)

    venue_results = {}
    for v, data in per_venue.items():
        vp = np.array(data["predictions"])
        va = np.array(data["actuals"])
        correct = sum(1 for p, a in zip(vp, va) if (p > 0.5 and a == 1) or (p < 0.5 and a == 0))
        venue_results[v] = {
            "matches": len(vp),
            "accuracy": round(correct / len(vp) * 100, 1) if len(vp) > 0 else 0,
            "avg_confidence": round(float(np.mean(np.maximum(vp, 1 - vp))) * 100, 1),
        }

    results = {
        "model_name": model,
        "start_date": dates[0] if dates else start_date,
        "end_date": dates[-1] if dates else end_date,
        "total_matches": len(predictions_list),
        **metrics,
        "calibration": calibration,
        "per_venue": venue_results,
        "predictions": predictions_list,
        "actuals": actuals_list,
    }

    return results


def _get_prediction(model, team_a, team_b, venue, match_date):
    """
    Get a win probability for team_a from the specified model.
    Returns float in [0, 1] or None on failure.
    """
    try:
        if model == "elo":
            pred = elo.predict(team_a, team_b, venue)
            return pred["team_a_win"] if pred else None

        elif model == "batting_bowling":
            pred = batting_bowling.predict(team_a, team_b, venue)
            return pred["team_a_win"] if pred else None

        elif model == "xgboost":
            pred = xgboost_model.predict(team_a, team_b, venue, match_date)
            return pred["team_a_win"] if pred else None

        elif model == "sentiment":
            pred = sentiment_model.predict(team_a, team_b)
            return pred["team_a_win"] if pred else None

        elif model == "ensemble":
            pred = ensemble.predict(team_a, team_b, venue=venue, match_date=match_date)
            return pred["team_a_win"] if pred else None

        else:
            print(f"[Backtest] Unknown model: {model}")
            return None

    except Exception:
        # Silently skip failures during backtest
        return None


# ============================================================================
#  Metrics calculation
# ============================================================================

def _calculate_metrics(predictions, actuals, odds_list):
    """
    Compute all backtest metrics from arrays of predictions, actuals, and odds.

    Args:
        predictions: np.array of predicted P(team_a wins)
        actuals:     np.array of 1 (team_a won) / 0 (team_b won)
        odds_list:   list of dicts with team_a_odds/team_b_odds (or None)

    Returns:
        dict with accuracy, brier_score, log_loss, roi, sharpe_ratio, max_drawdown
    """
    n = len(predictions)
    if n == 0:
        return {
            "accuracy": 0.0, "brier_score": 1.0, "log_loss": 999.0,
            "roi": 0.0, "sharpe_ratio": 0.0, "max_drawdown": 0.0,
        }

    # Accuracy
    correct = sum(
        1 for p, a in zip(predictions, actuals)
        if (p > 0.5 and a == 1) or (p < 0.5 and a == 0)
    )
    accuracy = correct / n * 100

    # Brier score
    brier = float(np.mean((predictions - actuals) ** 2))

    # Log loss (with clipping to avoid log(0))
    eps = 1e-15
    clipped = np.clip(predictions, eps, 1 - eps)
    log_loss = -float(np.mean(
        actuals * np.log(clipped) + (1 - actuals) * np.log(1 - clipped)
    ))

    # ROI calculation: flat $100 stake on model's predicted winner at best available odds
    returns = []
    for i in range(n):
        odds_data = odds_list[i] if i < len(odds_list) else None
        if odds_data is None:
            continue

        pred = predictions[i]
        actual = actuals[i]

        # Bet on team_a if pred > 0.5, else team_b
        if pred > 0.5:
            bet_odds = odds_data.get("team_a_odds")
            won = actual == 1
        else:
            bet_odds = odds_data.get("team_b_odds")
            won = actual == 0

        if bet_odds is None or bet_odds <= 1.0:
            continue

        if won:
            returns.append((bet_odds - 1) * 100)  # profit
        else:
            returns.append(-100)  # loss

    total_staked = len(returns) * 100
    total_return = sum(returns)
    roi = (total_return / total_staked * 100) if total_staked > 0 else 0.0

    # Sharpe ratio (annualized, assuming ~2 matches per week = ~100/year)
    if returns and len(returns) > 1:
        returns_arr = np.array(returns) / 100  # normalize to fraction of stake
        mean_ret = np.mean(returns_arr)
        std_ret = np.std(returns_arr, ddof=1)
        sharpe = (mean_ret / std_ret * math.sqrt(100)) if std_ret > 0 else 0.0
    else:
        sharpe = 0.0

    # Max drawdown
    max_drawdown = _calculate_max_drawdown(returns)

    return {
        "accuracy": round(accuracy, 2),
        "brier_score": round(brier, 4),
        "log_loss": round(log_loss, 4),
        "roi": round(roi, 2),
        "sharpe_ratio": round(sharpe, 3),
        "max_drawdown": round(max_drawdown, 2),
    }


def _calculate_max_drawdown(returns):
    """
    Calculate maximum drawdown from a list of bet returns.
    Returns the max drawdown as a positive dollar amount.
    """
    if not returns:
        return 0.0

    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0

    for ret in returns:
        cumulative += ret
        if cumulative > peak:
            peak = cumulative
        drawdown = peak - cumulative
        if drawdown > max_dd:
            max_dd = drawdown

    return max_dd


def _calculate_calibration(predictions, actuals, bins=10):
    """
    Group predictions into probability bins and compute the actual
    frequency in each bin.  Perfect calibration: predicted prob == actual freq.

    Args:
        predictions: np.array of predicted probabilities
        actuals:     np.array of binary outcomes
        bins:        number of bins (default 10)

    Returns:
        list of dicts with keys: bin_start, bin_end, bin_center,
                                  predicted_avg, actual_freq, count
    """
    bin_edges = np.linspace(0, 1, bins + 1)
    calibration = []

    for i in range(bins):
        lo = bin_edges[i]
        hi = bin_edges[i + 1]
        mask = (predictions >= lo) & (predictions < hi) if i < bins - 1 else (predictions >= lo) & (predictions <= hi)
        count = int(mask.sum())

        if count == 0:
            calibration.append({
                "bin_start": round(float(lo), 2),
                "bin_end": round(float(hi), 2),
                "bin_center": round((float(lo) + float(hi)) / 2, 2),
                "predicted_avg": 0.0,
                "actual_freq": 0.0,
                "count": 0,
            })
            continue

        pred_avg = float(predictions[mask].mean())
        actual_freq = float(actuals[mask].mean())

        calibration.append({
            "bin_start": round(float(lo), 2),
            "bin_end": round(float(hi), 2),
            "bin_center": round((float(lo) + float(hi)) / 2, 2),
            "predicted_avg": round(pred_avg, 4),
            "actual_freq": round(actual_freq, 4),
            "count": count,
        })

    return calibration


# ============================================================================
#  Reporting
# ============================================================================

def print_report(results):
    """Pretty-print the backtest results to console."""
    if not results:
        print("[Backtest] No results to display")
        return

    random_brier = config.BACKTEST_SETTINGS["random_brier_2way"]

    print()
    print("=" * 70)
    print(f"  BACKTEST REPORT: {results['model_name'].upper()}")
    print(f"  Period: {results['start_date']} to {results['end_date']}")
    print(f"  Matches tested: {results['total_matches']}")
    print("=" * 70)

    print()
    print("--- CORE METRICS ---")
    print(f"  Accuracy:         {results['accuracy']:.1f}%")
    print(f"  Brier Score:      {results['brier_score']:.4f}  (random baseline: {random_brier})")
    brier_skill = 1 - results['brier_score'] / random_brier
    print(f"  Brier Skill:      {brier_skill:.3f}  (>0 = better than random)")
    print(f"  Log Loss:         {results['log_loss']:.4f}")

    print()
    print("--- BETTING METRICS ---")
    print(f"  ROI:              {results['roi']:+.1f}%")
    print(f"  Sharpe Ratio:     {results['sharpe_ratio']:.3f}")
    print(f"  Max Drawdown:     ${results['max_drawdown']:.0f}")

    print()
    print("--- CALIBRATION ---")
    cal = results.get("calibration", [])
    if cal:
        print(f"  {'Bin':>10s}  {'Predicted':>10s}  {'Actual':>10s}  {'Count':>6s}  {'Gap':>8s}")
        for c in cal:
            if c["count"] == 0:
                continue
            gap = abs(c["predicted_avg"] - c["actual_freq"])
            flag = " ***" if gap > 0.10 else ""
            print(f"  {c['bin_start']:.1f}-{c['bin_end']:.1f}  "
                  f"{c['predicted_avg']:10.3f}  {c['actual_freq']:10.3f}  "
                  f"{c['count']:6d}  {gap:8.3f}{flag}")

    print()
    print("--- PER-VENUE BREAKDOWN ---")
    per_venue = results.get("per_venue", {})
    if per_venue:
        for v, vr in sorted(per_venue.items(), key=lambda x: -x[1]["matches"]):
            print(f"  {v:35s}  {vr['matches']:3d} matches  {vr['accuracy']:.1f}% acc  "
                  f"avg conf: {vr['avg_confidence']:.1f}%")

    # Assessment
    print()
    print("--- ASSESSMENT ---")
    if results["accuracy"] >= 60 and results["brier_score"] < 0.22:
        print("  EXCELLENT: Model is well-calibrated and profitable")
    elif results["accuracy"] >= 55 and results["brier_score"] < random_brier:
        print("  GOOD: Model beats random baseline, consider live deployment")
    elif results["accuracy"] >= 50:
        print("  MARGINAL: Model is barely better than coin flip")
    else:
        print("  POOR: Model underperforms random -- do NOT deploy for betting")

    print("=" * 70)
    print()


# ============================================================================
#  Persistence
# ============================================================================

def save_results(results, model_name=None):
    """
    Save backtest results to the backtest_results table.

    Args:
        results: dict returned by run_backtest()
        model_name: Override model name (defaults to results['model_name'])
    """
    if not results:
        print("[Backtest] No results to save")
        return

    db.init_db()
    name = model_name or results.get("model_name", "unknown")
    run_date = datetime.utcnow().strftime("%Y-%m-%d")

    # Serialize calibration and per-venue data
    calibration_json = json.dumps(results.get("calibration", []))
    per_venue_json = json.dumps(results.get("per_venue", {}))

    # Full details (exclude raw predictions/actuals arrays to keep size reasonable)
    details = {k: v for k, v in results.items()
               if k not in ("predictions", "actuals", "calibration", "per_venue")}
    details_json = json.dumps(details, default=str)

    db.execute(
        """INSERT INTO backtest_results (model_name, run_date, start_date, end_date,
           total_matches, accuracy, brier_score, log_loss, roi, sharpe_ratio,
           max_drawdown, calibration_data, per_venue_accuracy, details)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(model_name, run_date) DO UPDATE SET
           total_matches=excluded.total_matches, accuracy=excluded.accuracy,
           brier_score=excluded.brier_score, log_loss=excluded.log_loss,
           roi=excluded.roi, sharpe_ratio=excluded.sharpe_ratio,
           max_drawdown=excluded.max_drawdown, calibration_data=excluded.calibration_data,
           per_venue_accuracy=excluded.per_venue_accuracy, details=excluded.details""",
        [name, run_date, results.get("start_date"), results.get("end_date"),
         results["total_matches"], results["accuracy"], results["brier_score"],
         results["log_loss"], results["roi"], results["sharpe_ratio"],
         results["max_drawdown"], calibration_json, per_venue_json, details_json]
    )

    print(f"[Backtest] Results saved for model={name}, date={run_date}")


# ============================================================================
#  CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="PSL Cricket Prediction Engine -- Walk-Forward Backtesting"
    )
    parser.add_argument("--model", default="ensemble",
                        choices=["ensemble", "elo", "batting_bowling", "xgboost", "sentiment"],
                        help="Model to backtest (default: ensemble)")
    parser.add_argument("--start", default=None, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default=None, help="End date (YYYY-MM-DD)")
    parser.add_argument("--save", action="store_true", help="Save results to database")
    parser.add_argument("--json", action="store_true", help="Output results as JSON")
    args = parser.parse_args()

    start_time = time.time()
    results = run_backtest(start_date=args.start, end_date=args.end, model=args.model)
    elapsed = time.time() - start_time

    if not results:
        print("[Backtest] Backtest failed or returned no results")
        sys.exit(1)

    if args.json:
        # Remove numpy arrays for JSON serialization
        output = {k: v for k, v in results.items() if k not in ("predictions", "actuals")}
        print(json.dumps(output, indent=2, default=str))
    else:
        print_report(results)
        print(f"  Elapsed: {elapsed:.1f}s")

    if args.save:
        save_results(results)
        print(f"[Backtest] Results saved to backtest_results table")


if __name__ == "__main__":
    main()
