#!/usr/bin/env python3
"""
PSL Cricket Prediction Engine -- Automated Task Scheduler
==========================================================
Usage: python scheduler.py --task [daily|weekly|live|ratings|predictions|tracker|retrain|fixtures|odds|sentiment|weather|settle]

Designed for PythonAnywhere cron jobs.  Each task is wrapped in
try/except with timestamp logging, rate-limit awareness, and
structured exit codes.

Exit codes:  0 = success | 1 = warnings | 2 = critical error
"""

import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Project imports
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from database import db
from data import cricket_api, cricsheet, odds_api, weather_api, reddit_client, news_client, rate_limiter
from data.team_names import standardise, get_all_teams
from models import ensemble, batting_bowling, elo, xgboost_model, over_under, diagnosis

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------
_warnings_count = 0
_errors_count = 0


def _ts():
    """Return current UTC timestamp string."""
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def log_info(task, msg):
    print(f"[{_ts()}] [{task}] {msg}")


def log_warn(task, msg):
    global _warnings_count
    _warnings_count += 1
    print(f"[{_ts()}] [{task}] WARNING: {msg}")


def log_error(task, msg):
    global _errors_count
    _errors_count += 1
    print(f"[{_ts()}] [{task}] ERROR: {msg}")


# ============================================================================
#  Individual task implementations
# ============================================================================

def task_fixtures():
    """Fetch upcoming PSL fixtures from CricAPI and save to DB."""
    task = "fixtures"
    log_info(task, "Fetching PSL fixtures...")
    try:
        fixtures = cricket_api.get_psl_fixtures()
        if not fixtures:
            log_warn(task, "No fixtures returned from API (may be cached or rate-limited)")
            return
        cricket_api.save_fixtures_to_db(fixtures)
        log_info(task, f"Saved {len(fixtures)} fixtures to database")
    except Exception as e:
        log_error(task, f"Failed: {e}")
        traceback.print_exc()


def task_odds():
    """Fetch bookmaker odds for upcoming PSL matches."""
    task = "odds"
    log_info(task, "Fetching odds...")
    try:
        odds_list = odds_api.get_odds()
        if not odds_list:
            log_warn(task, "No odds returned (may be off-season or rate-limited)")
            return
        odds_api.save_odds_to_db(odds_list)
        log_info(task, f"Saved odds for {len(odds_list)} matches")
    except Exception as e:
        log_error(task, f"Failed: {e}")
        traceback.print_exc()


def task_weather():
    """Fetch weather forecasts for all scheduled fixtures."""
    task = "weather"
    log_info(task, "Fetching weather data for upcoming fixtures...")
    try:
        fixtures = db.fetch_all(
            "SELECT * FROM fixtures WHERE status = 'SCHEDULED' AND match_date >= date('now')"
        )
        if not fixtures:
            log_info(task, "No upcoming scheduled fixtures found")
            return
        fetched = 0
        for f in fixtures:
            if not f["venue"] or not f["match_date"]:
                continue
            weather = weather_api.get_match_weather(
                f["venue"], f["match_date"], f.get("match_time")
            )
            if weather:
                weather_api.save_weather_to_db(weather)
                fetched += 1
        log_info(task, f"Fetched weather for {fetched}/{len(fixtures)} fixtures")
    except Exception as e:
        log_error(task, f"Failed: {e}")
        traceback.print_exc()


def task_sentiment():
    """Run sentiment analysis (Reddit + News) for all teams."""
    task = "sentiment"
    log_info(task, "Running sentiment analysis...")
    try:
        reddit_results = reddit_client.fetch_all_teams()
        news_results = news_client.fetch_all_teams()

        # Build combined sentiment per team
        for team in get_all_teams():
            reddit_s = reddit_results.get(team)
            news_s = news_results.get(team)
            r_score = reddit_s["score"] if reddit_s else 0.0
            n_score = news_s["score"] if news_s else 0.0
            r_vol = reddit_s["volume"] if reddit_s else 0
            n_vol = news_s["volume"] if news_s else 0
            total_vol = r_vol + n_vol
            combined = (r_score * r_vol + n_score * n_vol) / total_vol if total_vol > 0 else 0.0

            signal = "neutral"
            if combined > 0.15:
                signal = "bullish"
            elif combined < -0.15:
                signal = "bearish"

            keywords = ""
            if reddit_s and reddit_s.get("keywords"):
                keywords = reddit_s["keywords"]
            if news_s and news_s.get("keywords"):
                keywords = f"{keywords},{news_s['keywords']}" if keywords else news_s["keywords"]

            db.execute(
                """INSERT INTO sentiment (team, source, score, trend, volume,
                   positive_pct, negative_pct, neutral_pct, keywords, signal, scored_at)
                   VALUES (?, 'combined', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(team, source, scored_at) DO UPDATE SET
                   score=excluded.score, trend=excluded.trend, volume=excluded.volume,
                   keywords=excluded.keywords, signal=excluded.signal""",
                [team, round(combined, 3), 0.0, total_vol,
                 0.0, 0.0, 0.0, keywords, signal,
                 datetime.utcnow().strftime("%Y-%m-%d")]
            )

        total = len(reddit_results) + len(news_results)
        log_info(task, f"Sentiment updated for {len(reddit_results)} Reddit + {len(news_results)} News sources")
    except Exception as e:
        log_error(task, f"Failed: {e}")
        traceback.print_exc()


def task_predictions():
    """Generate ensemble predictions for all upcoming fixtures."""
    task = "predictions"
    log_info(task, "Generating predictions...")
    try:
        fixtures = db.fetch_all(
            "SELECT * FROM fixtures WHERE status = 'SCHEDULED' AND match_date >= date('now') ORDER BY match_date"
        )
        if not fixtures:
            log_info(task, "No upcoming fixtures to predict")
            return
        generated = 0
        for f in fixtures:
            try:
                prediction = ensemble.predict(
                    f["team_a"], f["team_b"],
                    venue=f.get("venue"),
                    match_date=f["match_date"]
                )
                if prediction:
                    ensemble.save_prediction(
                        prediction, f["team_a"], f["team_b"],
                        f["match_date"], venue=f.get("venue"),
                        fixture_id=f["id"]
                    )
                    generated += 1
            except Exception as e:
                log_warn(task, f"Prediction failed for {f['team_a']} vs {f['team_b']}: {e}")
        log_info(task, f"Generated {generated}/{len(fixtures)} predictions")
    except Exception as e:
        log_error(task, f"Failed: {e}")
        traceback.print_exc()


def task_tracker_generate():
    """Create model_tracker entries for upcoming matches (lock-in predictions)."""
    task = "tracker_generate"
    log_info(task, "Generating tracker entries...")
    try:
        fixtures = db.fetch_all(
            """SELECT f.*, p.team_a_win, p.team_b_win, p.confidence, p.model_details,
                      p.predicted_total_a, p.predicted_total_b
               FROM fixtures f
               LEFT JOIN predictions p ON f.match_date = p.match_date
                   AND f.team_a = p.team_a AND f.team_b = p.team_b
               WHERE f.status = 'SCHEDULED' AND f.match_date >= date('now')"""
        )
        created = 0
        for f in fixtures:
            # Skip if already tracked
            existing = db.fetch_one(
                "SELECT id FROM model_tracker WHERE match_date = ? AND team_a = ? AND team_b = ?",
                [f["match_date"], f["team_a"], f["team_b"]]
            )
            if existing:
                continue

            team_a_prob = f.get("team_a_win") or 0.5
            team_b_prob = f.get("team_b_win") or 0.5
            predicted_winner = f["team_a"] if team_a_prob >= team_b_prob else f["team_b"]
            confidence = f.get("confidence") or 0.5
            predicted_total = (f.get("predicted_total_a") or 165) + (f.get("predicted_total_b") or 155)

            # Check for value bet
            odds_data = odds_api.get_best_odds_for_match(f["team_a"], f["team_b"], f["match_date"])
            is_value = 0
            value_type = None
            value_edge = None
            value_odds = None
            if odds_data:
                edge_a = team_a_prob - (odds_data.get("implied_prob_a") or 0.5)
                edge_b = team_b_prob - (odds_data.get("implied_prob_b") or 0.5)
                min_edge = config.VALUE_BET_SETTINGS["min_edge_percent"] / 100.0
                if edge_a >= min_edge:
                    is_value = 1
                    value_type = "team_a_win"
                    value_edge = round(edge_a * 100, 2)
                    value_odds = odds_data.get("team_a_odds")
                elif edge_b >= min_edge:
                    is_value = 1
                    value_type = "team_b_win"
                    value_edge = round(edge_b * 100, 2)
                    value_odds = odds_data.get("team_b_odds")

            db.execute(
                """INSERT INTO model_tracker (match_date, team_a, team_b, venue,
                   predicted_winner, team_a_prob, team_b_prob, predicted_total,
                   confidence, is_value_bet, value_bet_type, value_edge, value_odds,
                   status, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
                   ON CONFLICT(match_date, team_a, team_b) DO NOTHING""",
                [f["match_date"], f["team_a"], f["team_b"], f.get("venue"),
                 predicted_winner, team_a_prob, team_b_prob, round(predicted_total, 1),
                 confidence, is_value, value_type, value_edge, value_odds,
                 db.now_iso()]
            )
            created += 1
        log_info(task, f"Created {created} new tracker entries")
    except Exception as e:
        log_error(task, f"Failed: {e}")
        traceback.print_exc()


def _fetch_completed_scorecards():
    """For completed fixtures without match results, try to fetch scorecards from CricAPI."""
    completed = db.fetch_all(
        "SELECT * FROM fixtures WHERE status = 'COMPLETED' AND cricapi_id IS NOT NULL"
    )
    fetched = 0
    for fix in completed:
        # Check if match already in matches table
        existing = db.fetch_one(
            "SELECT id FROM matches WHERE match_date = ? AND team_a = ? AND team_b = ?",
            [fix["match_date"], fix["team_a"], fix["team_b"]]
        )
        if existing:
            continue

        # Try to get scorecard from CricAPI
        try:
            from data.cricket_api import get_match_scorecard as get_scorecard
            from data.rate_limiter import can_call
            if not can_call("cricket_api"):
                break
            scorecard = get_scorecard(fix["cricapi_id"])
            if scorecard and scorecard.get("status") in ("Match Over", "completed", "Complete"):
                winner = standardise(scorecard.get("matchWinner", "")) if scorecard.get("matchWinner") else None
                if not winner:
                    # Try to parse from score data
                    for team_score in scorecard.get("score", []):
                        pass  # CricAPI format varies

                # Insert into matches
                league = fix.get("league", "psl")
                db.execute(
                    """INSERT OR IGNORE INTO matches (season, match_date, venue, team_a, team_b, winner, league)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    [fix.get("season", "2026"), fix["match_date"], fix.get("venue"),
                     fix["team_a"], fix["team_b"], winner, league]
                )
                # Update fixture result
                if winner:
                    db.execute(
                        "UPDATE fixtures SET result = ? WHERE id = ?",
                        [f"{winner} won", fix["id"]]
                    )
                    fetched += 1
                    print(f"[settle] Fetched scorecard: {fix['team_a']} vs {fix['team_b']} -> {winner}")
        except Exception as e:
            print(f"[settle] Scorecard fetch error for {fix['team_a']} vs {fix['team_b']}: {e}")

    return fetched


def task_tracker_settle():
    """Settle pending tracker entries against actual match results."""
    task = "tracker_settle"
    log_info(task, "Settling tracker entries...")

    # First, try to fetch scorecards for completed fixtures without match records
    try:
        fetched = _fetch_completed_scorecards()
        if fetched:
            log_info(task, f"Fetched {fetched} scorecards from CricAPI")
    except Exception as e:
        log_warn(task, f"Scorecard fetch failed: {e}")

    try:
        pending = db.fetch_all(
            "SELECT * FROM model_tracker WHERE status = 'pending'"
        )
        if not pending:
            log_info(task, "No pending tracker entries")
            return

        settled = 0
        total_pnl = 0.0
        for entry in pending:
            # Look for completed match
            match = db.fetch_one(
                """SELECT * FROM matches WHERE match_date = ? AND
                   ((team_a = ? AND team_b = ?) OR (team_a = ? AND team_b = ?))
                   AND winner IS NOT NULL""",
                [entry["match_date"], entry["team_a"], entry["team_b"],
                 entry["team_b"], entry["team_a"]]
            )
            if not match:
                # Also check fixtures table
                fixture = db.fetch_one(
                    "SELECT * FROM fixtures WHERE match_date = ? AND team_a = ? AND team_b = ? AND status = 'COMPLETED'",
                    [entry["match_date"], entry["team_a"], entry["team_b"]]
                )
                if not fixture or not fixture.get("result"):
                    continue

            actual_winner = match["winner"] if match else None
            if not actual_winner:
                continue

            top_pick_correct = 1 if entry["predicted_winner"] == actual_winner else 0
            # P&L for top pick: flat $100 stake at implied 1.90 odds
            top_pick_pnl = 90.0 if top_pick_correct else -100.0

            # Value bet P&L
            vb_correct = None
            vb_pnl = None
            if entry["is_value_bet"] and entry["value_odds"]:
                if entry["value_bet_type"] == "team_a_win":
                    vb_correct = 1 if actual_winner == entry["team_a"] else 0
                elif entry["value_bet_type"] == "team_b_win":
                    vb_correct = 1 if actual_winner == entry["team_b"] else 0
                if vb_correct is not None:
                    vb_pnl = (entry["value_odds"] - 1) * 100.0 if vb_correct else -100.0

            actual_total_a = match.get("innings1_runs") if match else None
            actual_total_b = match.get("innings2_runs") if match else None

            db.execute(
                """UPDATE model_tracker SET
                   actual_winner = ?, actual_total_a = ?, actual_total_b = ?,
                   top_pick_correct = ?, top_pick_pnl = ?,
                   value_bet_correct = ?, value_bet_pnl = ?,
                   status = 'settled', settled_at = ?
                   WHERE id = ?""",
                [actual_winner, actual_total_a, actual_total_b,
                 top_pick_correct, top_pick_pnl,
                 vb_correct, vb_pnl,
                 db.now_iso(), entry["id"]]
            )

            # Log performance per model
            diagnosis.log_performance("ensemble", entry["team_a_prob"],
                                      1 if actual_winner == entry["team_a"] else 0)

            total_pnl += top_pick_pnl + (vb_pnl or 0.0)
            settled += 1

        log_info(task, f"Settled {settled}/{len(pending)} entries | Session P&L: ${total_pnl:+.2f}")
    except Exception as e:
        log_error(task, f"Failed: {e}")
        traceback.print_exc()


def task_value_bets():
    """Identify and record value bets for upcoming matches."""
    task = "value_bets"
    log_info(task, "Scanning for value bets...")
    try:
        fixtures = db.fetch_all(
            "SELECT * FROM fixtures WHERE status = 'SCHEDULED' AND match_date >= date('now')"
        )
        found = 0
        for f in fixtures:
            prediction = db.fetch_one(
                "SELECT * FROM predictions WHERE match_date = ? AND team_a = ? AND team_b = ?",
                [f["match_date"], f["team_a"], f["team_b"]]
            )
            if not prediction:
                continue

            odds_data = odds_api.get_best_odds_for_match(f["team_a"], f["team_b"], f["match_date"])
            if not odds_data:
                continue

            value_bets = ensemble.calculate_value(prediction, odds_data)
            for vb in value_bets:
                db.execute(
                    """INSERT INTO value_bets (fixture_id, match_date, team_a, team_b,
                       bet_type, model_prob, implied_prob, edge_pct, kelly_stake,
                       best_odds, bookmaker, status, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
                       ON CONFLICT(match_date, team_a, team_b, bet_type) DO UPDATE SET
                       model_prob=excluded.model_prob, implied_prob=excluded.implied_prob,
                       edge_pct=excluded.edge_pct, kelly_stake=excluded.kelly_stake,
                       best_odds=excluded.best_odds""",
                    [f["id"], f["match_date"], f["team_a"], f["team_b"],
                     vb["bet_type"], vb["model_prob"], vb["implied_prob"],
                     vb["edge_pct"], vb["kelly_stake"], vb["best_odds"],
                     vb.get("bookmaker", ""), db.now_iso()]
                )
                found += 1
        log_info(task, f"Found {found} value bets across {len(fixtures)} fixtures")
    except Exception as e:
        log_error(task, f"Failed: {e}")
        traceback.print_exc()


def task_historical():
    """Download and import historical match data from CricSheet."""
    task = "historical"
    log_info(task, "Downloading CricSheet historical data...")
    try:
        downloaded = cricsheet.download_psl_data()
        if not downloaded:
            log_warn(task, "Download skipped (rate-limited or failed)")
            return
        imported = cricsheet.import_all_matches()
        log_info(task, f"Imported {imported} historical matches")
    except Exception as e:
        log_error(task, f"Failed: {e}")
        traceback.print_exc()


def task_player_stats():
    """Extract player stats from CricSheet ball-by-ball data for both leagues."""
    task = "player_stats"
    try:
        from data.player_squads import seed_player_stats
        for league in ("psl", "ipl"):
            log_info(task, f"Extracting {league.upper()} player stats...")
            count = seed_player_stats(league=league)
            log_info(task, f"Seeded {count} {league.upper()} players")
    except Exception as e:
        log_error(task, f"Failed: {e}")
        traceback.print_exc()


def task_ratings():
    """Rebuild Elo ratings from historical match data."""
    task = "ratings"
    log_info(task, "Building Elo ratings...")
    try:
        ratings = elo.build_ratings()
        if not ratings:
            log_warn(task, "No ratings built (no match data)")
            return
        elo.save_ratings(ratings)
        log_info(task, f"Saved Elo ratings for {len(ratings)} teams")
        for team, r in sorted(ratings.items(), key=lambda x: -x[1]["elo"]):
            log_info(task, f"  {team}: Elo={r['elo']:.0f} Form={r['form_last5']:.0f}%")
    except Exception as e:
        log_error(task, f"Failed: {e}")
        traceback.print_exc()


def task_team_strengths():
    """Calculate batting/bowling team strength indices."""
    task = "team_strengths"
    log_info(task, "Calculating team strengths...")
    try:
        strengths = batting_bowling.calculate_team_strengths()
        if not strengths:
            log_warn(task, "No strengths calculated (no match data)")
            return
        batting_bowling.save_ratings(strengths)
        log_info(task, f"Saved strengths for {len(strengths)} teams")
    except Exception as e:
        log_error(task, f"Failed: {e}")
        traceback.print_exc()


def task_venue_update():
    """Recalculate venue statistics from historical matches."""
    task = "venue_update"
    log_info(task, "Updating venue statistics...")
    try:
        cricsheet.update_venue_stats()
        log_info(task, "Venue statistics updated")
    except Exception as e:
        log_error(task, f"Failed: {e}")
        traceback.print_exc()


def task_h2h():
    """Update head-to-head records between all team pairs."""
    task = "h2h"
    log_info(task, "Updating head-to-head records...")
    try:
        cricsheet.update_head_to_head()
        log_info(task, "Head-to-head records updated")
    except Exception as e:
        log_error(task, f"Failed: {e}")
        traceback.print_exc()


def task_retrain():
    """Retrain XGBoost model and optimize ensemble weights."""
    task = "retrain"
    log_info(task, "Retraining models...")
    try:
        # XGBoost
        log_info(task, "Training XGBoost classifier...")
        model = xgboost_model.train(retrain=True)
        if model:
            log_info(task, "XGBoost model retrained successfully")
        else:
            log_warn(task, "XGBoost training returned None (insufficient data?)")

        # Optimize weights
        log_info(task, "Optimizing ensemble weights...")
        weights = ensemble.optimize_weights()
        if weights:
            log_info(task, f"Optimized weights: {weights}")
        else:
            log_warn(task, "Weight optimization skipped (insufficient settled predictions)")

        # Train stacker
        log_info(task, "Training stacking meta-model...")
        stacker = ensemble.train_stacker()
        if stacker:
            log_info(task, "Stacker model trained")
        else:
            log_warn(task, "Stacker training skipped (insufficient data)")

    except Exception as e:
        log_error(task, f"Failed: {e}")
        traceback.print_exc()


def task_live():
    """Monitor live PSL matches and update probabilities in real-time."""
    task = "live"
    log_info(task, "Checking for live matches...")
    try:
        from models import live_predictor

        live_fixtures = db.fetch_all(
            "SELECT * FROM fixtures WHERE status = 'LIVE'"
        )
        if not live_fixtures:
            log_info(task, "No live matches currently")
            return

        for f in live_fixtures:
            cricapi_id = f.get("cricapi_id")
            if not cricapi_id:
                log_warn(task, f"No CricAPI ID for {f['team_a']} vs {f['team_b']}")
                continue

            score_data = cricket_api.get_live_score(cricapi_id)
            if not score_data:
                log_warn(task, f"No live score data for match {cricapi_id}")
                continue

            # Determine current batting state from score data
            scores = score_data.get("scores", [])
            if not scores:
                continue

            innings = len(scores)
            latest = scores[-1]
            current_score = latest.get("runs", 0)
            current_wickets = latest.get("wickets", 0)
            current_overs = latest.get("overs", 0.0)

            target = None
            if innings == 2 and len(scores) >= 2:
                target = scores[0].get("runs", 0) + 1

            # Calculate live probability
            live_prob = live_predictor.calculate_live_probability(
                f["team_a"], f["team_b"], f.get("venue"),
                innings, current_score, current_wickets, current_overs, target
            )

            # Update live_matches table
            db.execute(
                """INSERT INTO live_matches (fixture_id, match_date, team_a, team_b, venue,
                   current_batting, current_score, current_wickets, current_overs,
                   current_run_rate, target, projected_total, required_rate,
                   innings, live_win_prob_a, live_win_prob_b, last_updated)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(fixture_id) DO UPDATE SET
                   current_score=excluded.current_score, current_wickets=excluded.current_wickets,
                   current_overs=excluded.current_overs, current_run_rate=excluded.current_run_rate,
                   target=excluded.target, projected_total=excluded.projected_total,
                   required_rate=excluded.required_rate, innings=excluded.innings,
                   live_win_prob_a=excluded.live_win_prob_a, live_win_prob_b=excluded.live_win_prob_b,
                   last_updated=excluded.last_updated""",
                [f["id"], f["match_date"], f["team_a"], f["team_b"], f.get("venue"),
                 latest.get("team"), current_score, current_wickets, current_overs,
                 live_prob.get("current_rate", 0), target,
                 live_prob.get("projected_total"), live_prob.get("required_rate"),
                 innings, live_prob["team_a_win"], live_prob["team_b_win"],
                 db.now_iso()]
            )

            log_info(task,
                f"LIVE: {f['team_a']} vs {f['team_b']} | "
                f"Inn {innings}: {current_score}/{current_wickets} ({current_overs} ov) | "
                f"P({f['team_a']}): {live_prob['team_a_win']:.1%}")

            # Check if match ended
            if score_data.get("match_ended"):
                db.execute(
                    "UPDATE fixtures SET status = 'COMPLETED' WHERE id = ?",
                    [f["id"]]
                )
                db.execute(
                    "DELETE FROM live_matches WHERE fixture_id = ?",
                    [f["id"]]
                )
                log_info(task, f"Match completed: {f['team_a']} vs {f['team_b']}")

    except Exception as e:
        log_error(task, f"Failed: {e}")
        traceback.print_exc()


def task_settle():
    """Settle all pending items: tracker entries, value bets, and user bets."""
    task = "settle"
    log_info(task, "Running full settlement...")
    try:
        task_tracker_settle()

        # Settle value bets
        pending_vb = db.fetch_all(
            "SELECT * FROM value_bets WHERE status = 'pending'"
        )
        settled_vb = 0
        for vb in pending_vb:
            match = db.fetch_one(
                """SELECT * FROM matches WHERE match_date = ? AND
                   ((team_a = ? AND team_b = ?) OR (team_a = ? AND team_b = ?))
                   AND winner IS NOT NULL""",
                [vb["match_date"], vb["team_a"], vb["team_b"],
                 vb["team_b"], vb["team_a"]]
            )
            if not match:
                continue

            winner = match["winner"]
            won = False
            if vb["bet_type"] == "team_a_win" and winner == vb["team_a"]:
                won = True
            elif vb["bet_type"] == "team_b_win" and winner == vb["team_b"]:
                won = True
            elif vb["bet_type"] == "over":
                total = (match.get("innings1_runs") or 0) + (match.get("innings2_runs") or 0)
                line = vb.get("best_odds")  # stored separately; check context
                # Use over_under_line from odds if available
                ou = db.fetch_one(
                    "SELECT over_under_line FROM odds WHERE match_date = ? AND team_a = ? AND team_b = ?",
                    [vb["match_date"], vb["team_a"], vb["team_b"]]
                )
                if ou and ou["over_under_line"]:
                    won = total > ou["over_under_line"]
            elif vb["bet_type"] == "under":
                total = (match.get("innings1_runs") or 0) + (match.get("innings2_runs") or 0)
                ou = db.fetch_one(
                    "SELECT over_under_line FROM odds WHERE match_date = ? AND team_a = ? AND team_b = ?",
                    [vb["match_date"], vb["team_a"], vb["team_b"]]
                )
                if ou and ou["over_under_line"]:
                    won = total < ou["over_under_line"]

            status = "won" if won else "lost"
            pnl = (vb["best_odds"] - 1) * 100.0 if won else -100.0

            db.execute(
                "UPDATE value_bets SET status = ?, pnl = ?, settled_at = ? WHERE id = ?",
                [status, pnl, db.now_iso(), vb["id"]]
            )
            settled_vb += 1

        # Settle user bets
        pending_ub = db.fetch_all(
            "SELECT * FROM user_bets WHERE status = 'pending'"
        )
        settled_ub = 0
        for ub in pending_ub:
            match = db.fetch_one(
                """SELECT * FROM matches WHERE match_date = ? AND
                   ((team_a = ? AND team_b = ?) OR (team_a = ? AND team_b = ?))
                   AND winner IS NOT NULL""",
                [ub["match_date"], ub["team_a"], ub["team_b"],
                 ub["team_b"], ub["team_a"]]
            )
            if not match:
                continue

            won = False
            if ub["bet_type"] == "match_winner":
                won = match["winner"] == ub["selection"]

            status = "won" if won else "lost"
            actual_pnl = ub["stake"] * (ub["odds"] - 1) if won else -ub["stake"]

            db.execute(
                "UPDATE user_bets SET status = ?, actual_pnl = ?, settled_at = ? WHERE id = ?",
                [status, actual_pnl, db.now_iso(), ub["id"]]
            )
            settled_ub += 1

        log_info(task, f"Settled {settled_vb} value bets, {settled_ub} user bets")
    except Exception as e:
        log_error(task, f"Failed: {e}")
        traceback.print_exc()


# ============================================================================
#  Pipeline orchestrators
# ============================================================================

def run_daily():
    """Execute the full daily pipeline."""
    log_info("daily", "=" * 60)
    log_info("daily", "DAILY PIPELINE START")
    log_info("daily", "=" * 60)

    task_fixtures()
    task_odds()
    task_weather()
    task_sentiment()
    task_predictions()
    task_tracker_generate()
    task_tracker_settle()
    task_value_bets()

    log_info("daily", "DAILY PIPELINE COMPLETE")


def run_weekly():
    """Execute the full weekly pipeline."""
    log_info("weekly", "=" * 60)
    log_info("weekly", "WEEKLY PIPELINE START")
    log_info("weekly", "=" * 60)

    task_historical()
    task_player_stats()
    task_ratings()
    task_team_strengths()
    task_venue_update()
    task_h2h()
    task_retrain()
    task_predictions()

    log_info("weekly", "WEEKLY PIPELINE COMPLETE")


# ============================================================================
#  Task dispatch map
# ============================================================================

TASK_MAP = {
    "daily": run_daily,
    "weekly": run_weekly,
    "live": task_live,
    "fixtures": task_fixtures,
    "odds": task_odds,
    "weather": task_weather,
    "sentiment": task_sentiment,
    "ratings": task_ratings,
    "player_stats": task_player_stats,
    "predictions": task_predictions,
    "tracker": lambda: (task_tracker_generate(), task_tracker_settle()),
    "retrain": task_retrain,
    "settle": task_settle,
}


# ============================================================================
#  Main entry point
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="PSL Cricket Prediction Engine -- Task Scheduler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Tasks:
  daily        Full daily pipeline (fixtures -> odds -> weather -> sentiment -> predictions -> tracker -> value_bets)
  weekly       Full weekly pipeline (historical -> ratings -> strengths -> venues -> h2h -> retrain -> predictions)
  live         Check for live matches, fetch scores, recalculate probabilities
  fixtures     Fetch upcoming fixtures only
  odds         Fetch bookmaker odds only
  weather      Fetch weather forecasts for scheduled fixtures
  sentiment    Run Reddit + News sentiment analysis
  ratings      Rebuild Elo ratings from historical data
  predictions  Generate ensemble predictions for upcoming fixtures
  tracker      Generate tracker entries and settle completed matches
  retrain      Retrain XGBoost, optimize weights, train stacker
  settle       Settle all pending trackers, value bets, and user bets
        """,
    )
    parser.add_argument(
        "--task",
        required=True,
        choices=list(TASK_MAP.keys()),
        help="Task to execute",
    )
    args = parser.parse_args()

    # Ensure database is initialized
    db.init_db()

    log_info("main", f"Scheduler invoked: --task {args.task}")
    start = time.time()

    TASK_MAP[args.task]()

    elapsed = time.time() - start
    log_info("main", "=" * 60)
    log_info("main", f"COMPLETE: --task {args.task} in {elapsed:.1f}s")
    log_info("main", f"  Warnings: {_warnings_count} | Errors: {_errors_count}")
    log_info("main", "=" * 60)

    if _errors_count > 0:
        sys.exit(2)
    elif _warnings_count > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
