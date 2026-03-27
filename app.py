"""
Shothai — PSL Cricket Prediction & Betting Analytics Engine
Flask Application
"""

import os
import json
import hashlib
from datetime import datetime, timedelta
from functools import wraps
from flask import (
    Flask, render_template, request, redirect, url_for,
    session, flash, jsonify,
)

import config
from database import db
from data.team_names import standardise, standardise_venue, get_all_teams, get_abbreviation
from data import rate_limiter

# ---------------------------------------------------------------------------
# App Setup
# ---------------------------------------------------------------------------
app = Flask(__name__)
app.secret_key = config.FLASK_SECRET_KEY
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=7)

# ---------------------------------------------------------------------------
# User helpers (file-based auth)
# ---------------------------------------------------------------------------

def _load_users():
    """Read users from users.json."""
    path = config.USERS_FILE
    if not os.path.exists(path):
        default = {
            "admin": {
                "password": _hash_password("admin123"),
                "role": "admin",
                "email": "",
                "created_at": datetime.utcnow().strftime("%Y-%m-%d"),
            }
        }
        _save_users(default)
        return default
    with open(path, "r") as fh:
        return json.load(fh)


def _save_users(users):
    """Write users dict to users.json."""
    with open(config.USERS_FILE, "w") as fh:
        json.dump(users, fh, indent=4, default=str)


def _hash_password(password):
    """SHA-256 hex digest."""
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Auth decorators
# ---------------------------------------------------------------------------

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            flash("Please log in to access this page.", "warning")
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            flash("Please log in to access this page.", "warning")
            return redirect(url_for("login"))
        if session.get("role") != "admin":
            flash("Admin access required.", "danger")
            return redirect(url_for("dashboard"))
        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# Before-request: ensure DB is initialised once
# ---------------------------------------------------------------------------

@app.before_request
def ensure_db():
    if not hasattr(app, "_db_initialized"):
        db.init_db()
        db.migrate_add_league_column()
        db.migrate_add_data_refresh_log()
        # Pre-seed refresh log from api_calls if empty
        try:
            existing = db.fetch_one("SELECT COUNT(*) as c FROM data_refresh_log")
            if existing and existing["c"] == 0:
                api_sources = db.fetch_all(
                    "SELECT api_name, MAX(called_at) as last_call FROM api_calls GROUP BY api_name"
                )
                for src in api_sources:
                    db.execute(
                        "INSERT OR IGNORE INTO data_refresh_log (league, source, status, detail, refreshed_at) VALUES (?, ?, ?, ?, ?)",
                        ["psl", src["api_name"], "ok", "auto-detected from api_calls", src["last_call"]]
                    )
        except Exception:
            pass
        app._db_initialized = True


# ---------------------------------------------------------------------------
# Template filters / context processors
# ---------------------------------------------------------------------------

@app.template_filter("team_color")
def team_color_filter(name):
    """Return hex colour for a team."""
    canonical = standardise(name) if name else name
    abbrev = get_abbreviation(canonical) if canonical else None
    if abbrev and abbrev in config.TEAMS:
        return config.TEAMS[abbrev]["color"]
    if abbrev and abbrev in config.IPL_TEAMS:
        return config.IPL_TEAMS[abbrev]["color"]
    return "#6B7280"


@app.template_filter("team_abbrev")
def team_abbrev_filter(name):
    """Return short abbreviation."""
    if not name:
        return ""
    return get_abbreviation(name)


@app.template_filter("format_prob")
def format_prob_filter(value):
    """Format a 0-1 probability as a percentage string."""
    try:
        return f"{float(value) * 100:.1f}%"
    except (TypeError, ValueError):
        return "N/A"


def _get_team_color(name):
    """Helper to get team colour."""
    return team_color_filter(name)


@app.context_processor
def inject_league_and_refresh():
    """Make league info and data refresh timestamps available to all templates."""
    league = session.get("league", "psl") if session else "psl"
    refresh_log = []
    try:
        refresh_log = db.fetch_all(
            "SELECT source, status, detail, refreshed_at FROM data_refresh_log WHERE league = ? ORDER BY refreshed_at DESC",
            [league]
        )
    except Exception:
        pass
    teams_config = config.TEAMS if league == "psl" else config.IPL_TEAMS
    return {
        "current_league": league,
        "league_config": config.LEAGUES.get(league, config.LEAGUES["psl"]),
        "teams_config": teams_config,
        "data_refresh_log": refresh_log,
    }


# ---------------------------------------------------------------------------
# AUTH ROUTES
# ---------------------------------------------------------------------------

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()

        users = _load_users()
        user = users.get(username)
        if not user:
            flash("Invalid username or password.", "danger")
            return render_template("login.html")

        stored = user["password"]
        hashed_input = _hash_password(password)

        # First-login check: if stored password is plaintext (not 64-char hex),
        # hash it and persist so future logins use hashed comparison.
        if len(stored) != 64 or not all(c in "0123456789abcdef" for c in stored):
            if password == stored:
                users[username]["password"] = hashed_input
                _save_users(users)
                stored = hashed_input
            else:
                flash("Invalid username or password.", "danger")
                return render_template("login.html")

        if hashed_input != stored:
            flash("Invalid username or password.", "danger")
            return render_template("login.html")

        session.permanent = True
        session["logged_in"] = True
        session["username"] = username
        session["role"] = user.get("role", "viewer")
        flash(f"Welcome back, {username}!", "success")
        return redirect(url_for("dashboard"))

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    flash("You have been logged out.", "info")
    return redirect(url_for("login"))


@app.route("/league/<league>")
@login_required
def switch_league(league):
    """Switch between PSL and IPL."""
    if league in ("psl", "ipl"):
        session["league"] = league
        flash(f"Switched to {league.upper()}", "success")
    return redirect(url_for("dashboard"))


def _current_league():
    """Get current league from session, default to psl."""
    return session.get("league", "psl")


def _league_teams(league=None):
    """Return teams config dict for the current league."""
    league = league or _current_league()
    return config.IPL_TEAMS if league == "ipl" else config.TEAMS


def _league_team_names(league=None):
    """Return list of team names for the current league."""
    league = league or _current_league()
    if league == "ipl":
        return config.IPL_TEAM_NAMES
    return config.TEAM_NAMES


def _league_season(league=None):
    """Return current season string for the league."""
    league = league or _current_league()
    return config.LEAGUES.get(league, {}).get("season", config.CURRENT_SEASON)


# ---------------------------------------------------------------------------
# MAIN VIEW ROUTES
# ---------------------------------------------------------------------------

@app.route("/")
@login_required
def dashboard():
    """Command centre — upcoming fixtures, live matches, recent results, value bets."""
    league = _current_league()
    raw_fixtures = db.fetch_all(
        "SELECT * FROM fixtures WHERE status = 'SCHEDULED' AND league = ? ORDER BY match_date ASC LIMIT 7",
        [league]
    )

    # Enrich fixtures with prediction data for template
    upcoming = []
    predictions = {}
    for f in raw_fixtures:
        pred = db.fetch_one(
            "SELECT * FROM predictions WHERE team_a = ? AND team_b = ? AND match_date = ? AND league = ?",
            [f["team_a"], f["team_b"], f["match_date"], league]
        )
        if not pred:
            pred = db.fetch_one("SELECT * FROM predictions WHERE fixture_id = ? AND league = ?", [f["id"], league])

        match_data = dict(f)
        if pred:
            predictions[f["id"]] = pred
            match_data["team_a_win"] = round((pred["team_a_win"] or 0.5) * 100, 1)
            match_data["team_b_win"] = round((pred["team_b_win"] or 0.5) * 100, 1)
            conf_val = pred.get("confidence") or 0.5
            match_data["confidence"] = "high" if conf_val > 0.65 else ("medium" if conf_val > 0.45 else "low")
        else:
            match_data["team_a_win"] = 50
            match_data["team_b_win"] = 50
            match_data["confidence"] = "low"

        match_data["team_a_color"] = _get_team_color(f["team_a"])
        match_data["team_b_color"] = _get_team_color(f["team_b"])

        # Check if this match has a value bet
        vb = db.fetch_one(
            "SELECT id FROM value_bets WHERE team_a = ? AND team_b = ? AND match_date = ? AND status = 'pending' AND league = ? LIMIT 1",
            [f["team_a"], f["team_b"], f["match_date"], league],
        )
        match_data["is_value_bet"] = bool(vb)
        upcoming.append(match_data)

    # Live matches — pull from live_matches table for richer data
    raw_live = db.fetch_all(
        "SELECT * FROM live_matches WHERE league = ? ORDER BY last_updated DESC",
        [league]
    )
    live_matches = []
    for lm in raw_live:
        live_item = dict(lm)
        live_item["id"] = lm.get("id")
        live_item["team_a_score"] = f"{lm.get('current_score', 0)}/{lm.get('current_wickets', 0)}" if lm.get("innings") == 1 else ""
        live_item["team_b_score"] = f"{lm.get('current_score', 0)}/{lm.get('current_wickets', 0)}" if lm.get("innings") == 2 else ""
        live_item["team_a_win"] = (lm.get("live_win_prob_a") or 0.5) * 100
        live_item["team_b_win"] = (lm.get("live_win_prob_b") or 0.5) * 100
        live_matches.append(live_item)

    # Recent results from completed fixtures
    raw_results = db.fetch_all(
        "SELECT * FROM fixtures WHERE status = 'COMPLETED' AND league = ? ORDER BY match_date DESC LIMIT 5",
        [league]
    )
    recent_results = []
    for r in raw_results:
        result_data = dict(r)
        # Build score string from matches table if available
        match_row = db.fetch_one(
            "SELECT innings1_runs, innings1_wickets, innings2_runs, innings2_wickets FROM matches "
            "WHERE team_a = ? AND team_b = ? AND match_date = ? AND league = ?",
            [r["team_a"], r["team_b"], r["match_date"], league],
        )
        if match_row:
            result_data["score"] = (
                f"{match_row.get('innings1_runs', 0)}/{match_row.get('innings1_wickets', 0)} vs "
                f"{match_row.get('innings2_runs', 0)}/{match_row.get('innings2_wickets', 0)}"
            )
        else:
            result_data["score"] = r.get("result", "-")

        # Check if our prediction was correct
        tracker_row = db.fetch_one(
            "SELECT top_pick_correct FROM model_tracker WHERE team_a = ? AND team_b = ? AND match_date = ? AND league = ?",
            [r["team_a"], r["team_b"], r["match_date"], league],
        )
        result_data["prediction_correct"] = bool(tracker_row and tracker_row.get("top_pick_correct"))
        result_data["winner"] = r.get("result", "").split(" won")[0] if r.get("result") else ""
        # Try to find winner from matches table
        match_winner = db.fetch_one(
            "SELECT winner FROM matches WHERE team_a = ? AND team_b = ? AND match_date = ? AND league = ?",
            [r["team_a"], r["team_b"], r["match_date"], league],
        )
        if match_winner and match_winner.get("winner"):
            result_data["winner"] = match_winner["winner"]
        recent_results.append(result_data)

    # Value bets - format for dashboard template
    raw_value_bets = db.fetch_all(
        "SELECT * FROM value_bets WHERE status = 'pending' AND league = ? ORDER BY edge_pct DESC LIMIT 5",
        [league]
    )
    value_bets = []
    for vb in raw_value_bets:
        value_bets.append({
            "match": f"{vb['team_a']} vs {vb['team_b']}",
            "edge": vb.get("edge_pct", 0) or 0,
            "bet_type": (vb.get("bet_type", "") or "").replace("_", " ").title(),
            "odds": vb.get("best_odds", 0) or 0,
            "recommended_stake": f"{(vb.get('kelly_stake', 0) or 0):.0f}",
        })

    api_usage = rate_limiter.get_usage_summary()

    # Season stats
    season = _league_season(league)
    total_played = db.fetch_one(
        "SELECT COUNT(*) as cnt FROM fixtures WHERE status = 'COMPLETED' AND season = ? AND league = ?",
        [season, league]
    )
    total_fixtures = db.fetch_one(
        "SELECT COUNT(*) as cnt FROM fixtures WHERE season = ? AND league = ?",
        [season, league]
    )

    played_count = total_played["cnt"] if total_played else 0
    total_count = total_fixtures["cnt"] if total_fixtures else 44

    # Find current leader
    current_leader = "TBD"
    ratings = db.fetch_all("SELECT team, elo FROM team_ratings WHERE league = ? ORDER BY elo DESC LIMIT 1", [league])
    if ratings:
        current_leader = ratings[0]["team"]

    season = {
        "matches_played": played_count,
        "matches_remaining": total_count - played_count,
        "total_matches": total_count,
        "current_leader": current_leader,
    }

    # Stats for cards
    total_matches_all = db.fetch_one("SELECT COUNT(*) as cnt FROM matches WHERE league = ?", [league])
    pred_count = db.fetch_one("SELECT COUNT(*) as cnt FROM predictions WHERE league = ?", [league])
    vb_count = db.fetch_one("SELECT COUNT(*) as cnt FROM value_bets WHERE status = 'pending' AND league = ?", [league])
    tracker_settled = db.fetch_all("SELECT top_pick_correct FROM model_tracker WHERE status = 'settled' AND league = ?", [league])
    accuracy = 0.0
    if tracker_settled:
        correct = sum(1 for t in tracker_settled if t["top_pick_correct"] == 1)
        accuracy = (correct / len(tracker_settled) * 100) if tracker_settled else 0

    stats = {
        "total_matches": total_matches_all["cnt"] if total_matches_all else 0,
        "predictions_made": pred_count["cnt"] if pred_count else 0,
        "value_bets": vb_count["cnt"] if vb_count else 0,
        "model_accuracy": accuracy,
    }

    # Team standings (Elo-based) for the current league
    team_standings = db.fetch_all(
        "SELECT team, elo, matches_played, wins, losses, form_last5, streak_type, streak_length "
        "FROM team_ratings WHERE league = ? ORDER BY elo DESC",
        [league]
    )

    # Top players for the current league
    top_players = db.fetch_all(
        "SELECT name, team, role, impact_score, batting_avg, batting_sr, bowling_avg, bowling_economy, matches_played "
        "FROM player_stats WHERE league = ? AND impact_score IS NOT NULL "
        "ORDER BY impact_score DESC LIMIT 15",
        [league]
    )

    # Recent matches (from matches table) for current league
    recent_matches = db.fetch_all(
        "SELECT match_date, team_a, team_b, winner, win_margin, win_type, innings1_runs, innings2_runs "
        "FROM matches WHERE league = ? ORDER BY match_date DESC LIMIT 10",
        [league]
    )

    return render_template(
        "dashboard.html",
        upcoming=upcoming,
        predictions=predictions,
        live_matches=live_matches,
        recent_results=recent_results,
        value_bets=value_bets,
        api_usage=api_usage,
        season=season,
        stats=stats,
        teams=config.TEAMS if league == "psl" else config.IPL_TEAMS,
        now=datetime.utcnow(),
        team_standings=team_standings,
        top_players=top_players,
        recent_matches=recent_matches,
    )


# ---------------------------------------------------------------------------
# MATCH DETAIL
# ---------------------------------------------------------------------------

@app.route("/match/<team_a>/<team_b>/<match_date>")
@login_required
def match_detail(team_a, team_b, match_date):
    """Detailed match analysis page."""
    league = _current_league()
    team_a = standardise(team_a.replace("_", " "))
    team_b = standardise(team_b.replace("_", " "))

    # Prediction (raw DB row)
    raw_pred = db.fetch_one(
        "SELECT * FROM predictions WHERE team_a = ? AND team_b = ? AND match_date = ? AND league = ?",
        [team_a, team_b, match_date, league],
    )
    if not raw_pred:
        # Try reverse order
        raw_pred = db.fetch_one(
            "SELECT * FROM predictions WHERE team_a = ? AND team_b = ? AND match_date = ? AND league = ?",
            [team_b, team_a, match_date, league],
        )

    if not raw_pred:
        try:
            from models import ensemble
            venue_row = db.fetch_one(
                "SELECT venue FROM fixtures WHERE team_a = ? AND team_b = ? AND match_date = ? AND league = ?",
                [team_a, team_b, match_date, league],
            )
            venue = venue_row["venue"] if venue_row else None
            pred_data = ensemble.predict(team_a, team_b, venue, match_date)
            if pred_data:
                ensemble.save_prediction(pred_data, team_a, team_b, match_date, venue, league=league)
                raw_pred = db.fetch_one(
                    "SELECT * FROM predictions WHERE team_a = ? AND team_b = ? AND match_date = ? AND league = ?",
                    [team_a, team_b, match_date, league],
                )
        except Exception:
            raw_pred = None

    # Fixture info for venue/time
    fixture = db.fetch_one(
        "SELECT * FROM fixtures WHERE team_a = ? AND team_b = ? AND match_date = ? AND league = ?",
        [team_a, team_b, match_date, league],
    )
    if not fixture:
        fixture = db.fetch_one(
            "SELECT * FROM fixtures WHERE team_a = ? AND team_b = ? AND match_date = ? AND league = ?",
            [team_b, team_a, match_date, league],
        )

    venue = fixture["venue"] if fixture else (raw_pred.get("venue") if raw_pred else None)

    # Build the rich prediction dict that match.html expects
    team_a_win_pct = round((raw_pred["team_a_win"] or 0.5) * 100, 1) if raw_pred else 50
    team_b_win_pct = round((raw_pred["team_b_win"] or 0.5) * 100, 1) if raw_pred else 50
    conf_val = (raw_pred.get("confidence") or 0.5) if raw_pred else 0.5
    conf_label = "high" if conf_val > 0.65 else ("medium" if conf_val > 0.45 else "low")
    predicted_winner = team_a if team_a_win_pct >= team_b_win_pct else team_b

    # Parse model_details JSON if available
    model_details = {}
    if raw_pred and raw_pred.get("model_details"):
        try:
            model_details = json.loads(raw_pred["model_details"]) if isinstance(raw_pred["model_details"], str) else raw_pred["model_details"]
        except (json.JSONDecodeError, TypeError):
            model_details = {}

    # Team ratings for Elo breakdown
    rating_a = db.fetch_one("SELECT * FROM team_ratings WHERE team = ? AND league = ?", [team_a, league])
    rating_b = db.fetch_one("SELECT * FROM team_ratings WHERE team = ? AND league = ?", [team_b, league])

    # Toss recommendation
    toss_rec = None
    if raw_pred and raw_pred.get("toss_advantage"):
        toss_rec = raw_pred["toss_advantage"]
    elif venue:
        vs = db.fetch_one("SELECT * FROM venue_stats WHERE venue = ? AND league = ?", [venue, league])
        if vs and vs.get("chase_win_pct"):
            toss_rec = "Bat second (chase)" if vs["chase_win_pct"] > 55 else "Bat first"

    prediction = {
        "team_a": team_a,
        "team_b": team_b,
        "date": match_date,
        "time": fixture.get("match_time", "") if fixture else "",
        "venue": venue or "",
        "team_a_win": team_a_win_pct,
        "team_b_win": team_b_win_pct,
        "predicted_winner": predicted_winner,
        "confidence": conf_label,
        "team_a_color": _get_team_color(team_a),
        "team_b_color": _get_team_color(team_b),
        "toss_recommendation": toss_rec,
        "batting_bowling": {
            "team_a_batting": f"{rating_a.get('batting_avg', 0):.1f}" if rating_a and rating_a.get("batting_avg") else "-",
            "team_a_bowling": f"{rating_a.get('bowling_avg', 0):.1f}" if rating_a and rating_a.get("bowling_avg") else "-",
            "team_a_projected": f"{raw_pred.get('predicted_total_a', 0):.0f}" if raw_pred and raw_pred.get("predicted_total_a") else "-",
            "team_b_batting": f"{rating_b.get('batting_avg', 0):.1f}" if rating_b and rating_b.get("batting_avg") else "-",
            "team_b_bowling": f"{rating_b.get('bowling_avg', 0):.1f}" if rating_b and rating_b.get("bowling_avg") else "-",
            "team_b_projected": f"{raw_pred.get('predicted_total_b', 0):.0f}" if raw_pred and raw_pred.get("predicted_total_b") else "-",
        },
        "elo": {
            "team_a_rating": f"{rating_a.get('elo', 1500):.0f}" if rating_a else "1500",
            "team_b_rating": f"{rating_b.get('elo', 1500):.0f}" if rating_b else "1500",
            "team_a_form": f"{rating_a.get('form_last5', 0):.0f}%" if rating_a and rating_a.get("form_last5") is not None else "-",
            "team_b_form": f"{rating_b.get('form_last5', 0):.0f}%" if rating_b and rating_b.get("form_last5") is not None else "-",
            "diff": f"{abs((rating_a.get('elo', 1500) if rating_a else 1500) - (rating_b.get('elo', 1500) if rating_b else 1500)):.0f}",
        },
        "sentiment": {
            "team_a_score": 0,
            "team_a_trend": "neutral",
            "team_b_score": 0,
            "team_b_trend": "neutral",
            "keywords": [],
        },
        "ensemble_weights": model_details.get("weights", config.MODEL_WEIGHTS),
        "blend_method": "weighted average",
        "xgboost_features": model_details.get("xgboost_features", []),
    }

    # Sentiment data for the match detail
    sent_a = db.fetch_one(
        "SELECT * FROM sentiment WHERE team = ? AND league = ? ORDER BY scored_at DESC LIMIT 1",
        [team_a, league],
    )
    sent_b = db.fetch_one(
        "SELECT * FROM sentiment WHERE team = ? AND league = ? ORDER BY scored_at DESC LIMIT 1",
        [team_b, league],
    )
    if sent_a:
        prediction["sentiment"]["team_a_score"] = sent_a.get("score", 0) or 0
        trend_val = sent_a.get("trend", 0) or 0
        prediction["sentiment"]["team_a_trend"] = "bullish" if trend_val > 0 else ("bearish" if trend_val < 0 else "neutral")
        if sent_a.get("keywords"):
            try:
                kws = json.loads(sent_a["keywords"]) if isinstance(sent_a["keywords"], str) else sent_a["keywords"]
                if isinstance(kws, list):
                    prediction["sentiment"]["keywords"] = kws[:8]
            except (json.JSONDecodeError, TypeError):
                pass
    if sent_b:
        prediction["sentiment"]["team_b_score"] = sent_b.get("score", 0) or 0
        trend_val = sent_b.get("trend", 0) or 0
        prediction["sentiment"]["team_b_trend"] = "bullish" if trend_val > 0 else ("bearish" if trend_val < 0 else "neutral")

    # Odds - the template expects a list of odds objects
    raw_odds = db.fetch_all(
        "SELECT * FROM odds WHERE team_a = ? AND team_b = ? AND match_date = ? AND league = ? ORDER BY fetched_at DESC",
        [team_a, team_b, match_date, league],
    )
    odds_list = []
    for o in raw_odds:
        implied = (o.get("implied_prob_a") or 0) * 100
        our_prob = team_a_win_pct
        odds_list.append({
            "bookmaker": o.get("bookmaker", "Best"),
            "team_a_odds": o.get("team_a_odds", 0),
            "team_b_odds": o.get("team_b_odds", 0),
            "implied_prob": implied,
            "our_prob": our_prob,
            "edge": our_prob - implied,
        })

    # Head-to-head record
    h2h_row = db.fetch_one(
        "SELECT * FROM head_to_head WHERE ((team_a = ? AND team_b = ?) OR (team_a = ? AND team_b = ?)) AND league = ?",
        [team_a, team_b, team_b, team_a, league],
    )
    h2h = None
    if h2h_row:
        # Normalise so team_a_wins corresponds to our team_a
        if h2h_row["team_a"] == team_a:
            h2h = {
                "team_a_wins": h2h_row.get("team_a_wins", 0),
                "team_b_wins": h2h_row.get("team_b_wins", 0),
                "total": h2h_row.get("matches_played", 0),
            }
        else:
            h2h = {
                "team_a_wins": h2h_row.get("team_b_wins", 0),
                "team_b_wins": h2h_row.get("team_a_wins", 0),
                "total": h2h_row.get("matches_played", 0),
            }
        # Last 5 meetings from matches table
        last_5_raw = db.fetch_all(
            "SELECT match_date, winner FROM matches "
            "WHERE ((team_a = ? AND team_b = ?) OR (team_a = ? AND team_b = ?)) AND league = ? "
            "ORDER BY match_date DESC LIMIT 5",
            [team_a, team_b, team_b, team_a, league],
        )
        h2h["last_5"] = [{"date": m["match_date"], "winner": m.get("winner", "")} for m in last_5_raw]

    # Recent form (last 5 results for each team — W/L/NR list)
    form_a_raw = db.fetch_all(
        "SELECT winner, team_a, team_b FROM matches WHERE (team_a = ? OR team_b = ?) AND league = ? "
        "ORDER BY match_date DESC LIMIT 5",
        [team_a, team_a, league],
    )
    form_b_raw = db.fetch_all(
        "SELECT winner, team_a, team_b FROM matches WHERE (team_a = ? OR team_b = ?) AND league = ? "
        "ORDER BY match_date DESC LIMIT 5",
        [team_b, team_b, league],
    )

    def _form_list(team, matches):
        result = []
        for m in matches:
            winner = m.get("winner", "")
            if not winner:
                result.append("NR")
            elif standardise(winner) == standardise(team):
                result.append("W")
            else:
                result.append("L")
        return result

    form = {
        "team_a": _form_list(team_a, form_a_raw),
        "team_b": _form_list(team_b, form_b_raw),
    }

    # Venue stats — template expects specific keys
    venue_stats_data = None
    if venue:
        vs = db.fetch_one("SELECT * FROM venue_stats WHERE venue = ? AND league = ?", [venue, league])
        if vs:
            venue_stats_data = {
                "avg_first_innings": f"{vs.get('avg_first_innings', 0):.0f}" if vs.get("avg_first_innings") else "-",
                "avg_second_innings": f"{vs.get('avg_second_innings', 0):.0f}" if vs.get("avg_second_innings") else "-",
                "chase_pct": f"{vs.get('chase_win_pct', 0):.0f}" if vs.get("chase_win_pct") else "-",
                "pace_pct": f"{vs.get('pace_wicket_pct', 50):.0f}" if vs.get("pace_wicket_pct") else "50",
                "spin_pct": f"{vs.get('spin_wicket_pct', 50):.0f}" if vs.get("spin_wicket_pct") else "50",
            }

    # Weather
    weather = None
    if venue:
        weather_row = db.fetch_one(
            "SELECT * FROM weather WHERE venue = ? AND match_date = ? AND league = ?",
            [venue, match_date, league],
        )
        if weather_row:
            weather = dict(weather_row)
            # Add recommendation based on dew
            dew_score = weather.get("dew_score", 0) or 0
            if dew_score > 0.7:
                weather["recommendation"] = "Heavy dew expected — strong advantage batting second."
            elif dew_score > 0.4:
                weather["recommendation"] = "Moderate dew expected — slight advantage batting second."
            else:
                weather["recommendation"] = "Low dew — conditions fairly neutral."

    # Prop bets from prediction data
    prop_bets = None
    if raw_pred:
        ou_line = raw_pred.get("over_under_line")
        if ou_line:
            prop_bets = {
                "total_runs_line": f"{ou_line:.0f}" if ou_line else "-",
                "over_pct": (raw_pred.get("over_prob") or 0.5) * 100,
                "under_pct": (raw_pred.get("under_prob") or 0.5) * 100,
                "wides": f"{raw_pred.get('total_wides_pred', 0):.1f}" if raw_pred.get("total_wides_pred") else "-",
                "no_balls": f"{raw_pred.get('total_noballs_pred', 0):.1f}" if raw_pred.get("total_noballs_pred") else "-",
                "sixes": f"{raw_pred.get('total_sixes_pred', 0):.1f}" if raw_pred.get("total_sixes_pred") else "-",
                "fours": f"{raw_pred.get('total_fours_pred', 0):.1f}" if raw_pred.get("total_fours_pred") else "-",
            }

    # Key signals
    key_signals = []
    if rating_a and rating_b:
        elo_diff = (rating_a.get("elo", 1500) or 1500) - (rating_b.get("elo", 1500) or 1500)
        if abs(elo_diff) > 100:
            stronger = team_a if elo_diff > 0 else team_b
            key_signals.append(f"{stronger} has a significant Elo advantage ({abs(elo_diff):.0f} points)")
        if rating_a.get("streak_type") == "W" and (rating_a.get("streak_length", 0) or 0) >= 3:
            key_signals.append(f"{team_a} on a {rating_a['streak_length']}-match winning streak")
        if rating_b.get("streak_type") == "W" and (rating_b.get("streak_length", 0) or 0) >= 3:
            key_signals.append(f"{team_b} on a {rating_b['streak_length']}-match winning streak")
    if weather and (weather.get("dew_score", 0) or 0) > 0.6:
        key_signals.append("Heavy dew factor — team batting second has an advantage")
    if h2h and h2h.get("total", 0) > 0:
        dominant = team_a if h2h["team_a_wins"] > h2h["team_b_wins"] else team_b
        key_signals.append(f"{dominant} leads the head-to-head record")

    # Player strength data and matchup analysis
    player_cards = None
    matchup_analysis = None
    try:
        from models import player_strength
        player_cards = player_strength.get_player_detail_cards(team_a, team_b)
        matchup_analysis = player_strength.get_matchup_analysis(team_a, team_b, venue)

        # Add player-related key signals
        if matchup_analysis:
            if matchup_analysis.get("batting", {}).get("advantage"):
                key_signals.append(f"{matchup_analysis['batting']['advantage']} has stronger batting lineup (player stats)")
            if matchup_analysis.get("bowling", {}).get("advantage"):
                key_signals.append(f"{matchup_analysis['bowling']['advantage']} has stronger bowling attack (player stats)")
            vm = matchup_analysis.get("venue_matchup", {})
            if vm.get("advantage") and vm.get("reason"):
                key_signals.append(f"Venue favors {vm['advantage']}: {vm['reason']}")
    except Exception:
        pass

    # Build factor cards — all triggers being monitored
    factor_cards = []

    # Elo factor
    if rating_a and rating_b:
        elo_a = rating_a.get("elo", 1500) or 1500
        elo_b = rating_b.get("elo", 1500) or 1500
        factor_cards.append({
            "title": "Elo Rating",
            "icon": "chart-bar",
            "color": "emerald",
            "metrics": [
                {"label": team_a, "value": f"{elo_a:.0f}"},
                {"label": team_b, "value": f"{elo_b:.0f}"},
                {"label": "Difference", "value": f"{abs(elo_a - elo_b):.0f} pts"},
            ],
            "verdict": f"{team_a if elo_a > elo_b else team_b} advantage" if abs(elo_a - elo_b) > 30 else "Even",
        })

    # Form factor
    if rating_a and rating_b:
        form_a_val = rating_a.get("form_last5", 0) or 0
        form_b_val = rating_b.get("form_last5", 0) or 0
        streak_a = f"{rating_a.get('streak_length', 0) or 0}{rating_a.get('streak_type', 'N')}"
        streak_b = f"{rating_b.get('streak_length', 0) or 0}{rating_b.get('streak_type', 'N')}"
        factor_cards.append({
            "title": "Recent Form",
            "icon": "trending-up",
            "color": "blue",
            "metrics": [
                {"label": f"{team_a} Form", "value": f"{form_a_val:.0f}%"},
                {"label": f"{team_b} Form", "value": f"{form_b_val:.0f}%"},
                {"label": f"{team_a} Streak", "value": streak_a},
                {"label": f"{team_b} Streak", "value": streak_b},
            ],
            "verdict": f"{team_a if form_a_val > form_b_val else team_b} in better form" if abs(form_a_val - form_b_val) > 10 else "Similar form",
        })

    # Weather & Dew factor
    if weather:
        dew_s = weather.get("dew_score", 0) or 0
        factor_cards.append({
            "title": "Weather & Dew",
            "icon": "cloud",
            "color": "cyan",
            "metrics": [
                {"label": "Temperature", "value": f"{weather.get('temperature', '-')}°C"},
                {"label": "Humidity", "value": f"{weather.get('humidity', '-')}%"},
                {"label": "Dew Score", "value": f"{dew_s:.1f}"},
                {"label": "Wind", "value": f"{weather.get('wind_speed', '-')} km/h"},
            ],
            "verdict": "Heavy dew — bat second" if dew_s > 0.6 else ("Moderate dew" if dew_s > 0.3 else "Low dew — neutral"),
        })

    # Venue factor
    if venue_stats_data:
        factor_cards.append({
            "title": "Venue Analysis",
            "icon": "building",
            "color": "purple",
            "metrics": [
                {"label": "Avg 1st Innings", "value": venue_stats_data.get("avg_first_innings", "-")},
                {"label": "Avg 2nd Innings", "value": venue_stats_data.get("avg_second_innings", "-")},
                {"label": "Chase Win %", "value": f"{venue_stats_data.get('chase_pct', '-')}%"},
                {"label": "Pace / Spin", "value": f"{venue_stats_data.get('pace_pct', '50')}% / {venue_stats_data.get('spin_pct', '50')}%"},
            ],
            "verdict": f"Chase-friendly" if venue_stats_data.get("chase_pct", "50") != "-" and int(venue_stats_data.get("chase_pct", "50")) > 55 else "Bat first venue" if venue_stats_data.get("chase_pct", "50") != "-" and int(venue_stats_data.get("chase_pct", "50")) < 45 else "Balanced venue",
        })

    # H2H factor
    if h2h and h2h.get("total", 0) > 0:
        factor_cards.append({
            "title": "Head to Head",
            "icon": "arrows-right-left",
            "color": "amber",
            "metrics": [
                {"label": f"{team_a} Wins", "value": str(h2h.get("team_a_wins", 0))},
                {"label": f"{team_b} Wins", "value": str(h2h.get("team_b_wins", 0))},
                {"label": "Total Meetings", "value": str(h2h.get("total", 0))},
            ],
            "verdict": f"{team_a if h2h['team_a_wins'] > h2h['team_b_wins'] else team_b} dominant" if h2h["team_a_wins"] != h2h["team_b_wins"] else "Evenly matched",
        })

    # Sentiment factor
    sent_score_a = prediction["sentiment"]["team_a_score"]
    sent_score_b = prediction["sentiment"]["team_b_score"]
    if sent_score_a != 0 or sent_score_b != 0:
        factor_cards.append({
            "title": "Sentiment",
            "icon": "chat-bubble",
            "color": "pink",
            "metrics": [
                {"label": f"{team_a}", "value": f"{sent_score_a:.2f}"},
                {"label": f"{team_b}", "value": f"{sent_score_b:.2f}"},
                {"label": f"{team_a} Trend", "value": prediction["sentiment"]["team_a_trend"]},
                {"label": f"{team_b} Trend", "value": prediction["sentiment"]["team_b_trend"]},
            ],
            "verdict": f"{team_a if sent_score_a > sent_score_b else team_b} more positive buzz" if abs(sent_score_a - sent_score_b) > 0.05 else "Neutral sentiment",
        })

    # Batting strength factor
    if matchup_analysis and matchup_analysis.get("batting"):
        bat = matchup_analysis["batting"]
        factor_cards.append({
            "title": "Batting Strength",
            "icon": "bolt",
            "color": "lime",
            "metrics": [
                {"label": f"{team_a} Strength", "value": f"{bat.get('team_a', {}).get('batting_strength', '-'):.2f}" if isinstance(bat.get('team_a', {}).get('batting_strength'), (int, float)) else "-"},
                {"label": f"{team_b} Strength", "value": f"{bat.get('team_b', {}).get('batting_strength', '-'):.2f}" if isinstance(bat.get('team_b', {}).get('batting_strength'), (int, float)) else "-"},
                {"label": f"{team_a} PP SR", "value": f"{bat.get('team_a', {}).get('powerplay_sr', 0):.1f}"},
                {"label": f"{team_b} PP SR", "value": f"{bat.get('team_b', {}).get('powerplay_sr', 0):.1f}"},
            ],
            "verdict": f"{bat.get('advantage', 'Even')} has batting edge",
        })

    # Bowling strength factor
    if matchup_analysis and matchup_analysis.get("bowling"):
        bowl = matchup_analysis["bowling"]
        factor_cards.append({
            "title": "Bowling Strength",
            "icon": "fire",
            "color": "red",
            "metrics": [
                {"label": f"{team_a} Strength", "value": f"{bowl.get('team_a', {}).get('bowling_strength', '-'):.2f}" if isinstance(bowl.get('team_a', {}).get('bowling_strength'), (int, float)) else "-"},
                {"label": f"{team_b} Strength", "value": f"{bowl.get('team_b', {}).get('bowling_strength', '-'):.2f}" if isinstance(bowl.get('team_b', {}).get('bowling_strength'), (int, float)) else "-"},
                {"label": f"{team_a} Death Econ", "value": f"{bowl.get('team_a', {}).get('death_economy', 0):.1f}"},
                {"label": f"{team_b} Death Econ", "value": f"{bowl.get('team_b', {}).get('death_economy', 0):.1f}"},
            ],
            "verdict": f"{bowl.get('advantage', 'Even')} has bowling edge",
        })

    # Odds/Value factor
    if odds_list:
        best_edge = max(odds_list, key=lambda o: abs(o.get("edge", 0)))
        factor_cards.append({
            "title": "Bookmaker Odds",
            "icon": "currency-dollar",
            "color": "yellow",
            "metrics": [
                {"label": f"{team_a} Odds", "value": f"{odds_list[0].get('team_a_odds', '-')}"},
                {"label": f"{team_b} Odds", "value": f"{odds_list[0].get('team_b_odds', '-')}"},
                {"label": "Best Edge", "value": f"{best_edge.get('edge', 0):+.1f}%"},
                {"label": "Bookmaker", "value": odds_list[0].get("bookmaker", "-")},
            ],
            "verdict": f"Value on {team_a if best_edge.get('edge', 0) > 3 else team_b}" if abs(best_edge.get("edge", 0)) > 3 else "No clear value",
        })

    # Toss factor
    if toss_rec:
        factor_cards.append({
            "title": "Toss Impact",
            "icon": "arrow-path",
            "color": "indigo",
            "metrics": [
                {"label": "Recommendation", "value": str(toss_rec)},
            ],
            "verdict": f"Win toss & {toss_rec}" if toss_rec not in ("neutral",) else "Toss neutral",
        })

    return render_template(
        "match.html",
        prediction=prediction,
        odds=odds_list if odds_list else None,
        h2h=h2h,
        form=form,
        venue_stats=venue_stats_data,
        weather=weather,
        prop_bets=prop_bets,
        key_signals=key_signals if key_signals else None,
        player_cards=player_cards,
        matchup_analysis=matchup_analysis,
        factor_cards=factor_cards,
    )


# ---------------------------------------------------------------------------
# LIVE HUB + LIVE MATCH
# ---------------------------------------------------------------------------

@app.route("/live")
@login_required
def live_hub():
    """Live hub — shows today's matches (live, upcoming today, completed today)."""
    league = _current_league()
    from datetime import date
    today = date.today().isoformat()

    # Today's matches
    todays_matches = db.fetch_all(
        "SELECT * FROM fixtures WHERE match_date = ? AND league = ? ORDER BY match_time ASC",
        [today, league]
    )

    # Enrich with predictions and live data
    matches = []
    for f in todays_matches:
        match = dict(f)
        # Get prediction
        pred = db.fetch_one(
            "SELECT * FROM predictions WHERE team_a = ? AND team_b = ? AND match_date = ? AND league = ?",
            [f["team_a"], f["team_b"], f["match_date"], league]
        )
        if pred:
            match["team_a_win"] = round((pred["team_a_win"] or 0.5) * 100, 1)
            match["team_b_win"] = round((pred["team_b_win"] or 0.5) * 100, 1)
        else:
            match["team_a_win"] = 50
            match["team_b_win"] = 50

        # Get live data if match is live
        live_row = db.fetch_one("SELECT * FROM live_matches WHERE fixture_id = ? AND league = ?", [f["id"], league])
        if live_row:
            match["live"] = dict(live_row)
        else:
            match["live"] = None

        match["team_a_color"] = team_color_filter(f["team_a"])
        match["team_b_color"] = team_color_filter(f["team_b"])
        matches.append(match)

    # If no matches today, show next match day
    next_match = None
    if not matches:
        next_match = db.fetch_one(
            "SELECT match_date, COUNT(*) as count FROM fixtures WHERE status = 'SCHEDULED' AND match_date > ? AND league = ? GROUP BY match_date ORDER BY match_date LIMIT 1",
            [today, league]
        )

    return render_template("live.html",
        matches=matches,
        next_match=next_match,
        today=today,
        is_hub=True,
    )


@app.route("/live/<int:match_id>")
@login_required
def live_match(match_id):
    """Live in-play match view."""
    league = _current_league()
    live_row = db.fetch_one(
        "SELECT * FROM live_matches WHERE id = ? AND league = ?",
        [match_id, league],
    )
    if not live_row:
        # Try as fixture_id
        live_row = db.fetch_one(
            "SELECT * FROM live_matches WHERE fixture_id = ? AND league = ?",
            [match_id, league],
        )

    live = None
    if live_row:
        innings = live_row.get("innings", 1) or 1
        score = live_row.get("current_score", 0) or 0
        wickets = live_row.get("current_wickets", 0) or 0
        overs = live_row.get("current_overs", 0) or 0

        live = {
            "match_id": live_row.get("id", match_id),
            "team_a": live_row["team_a"],
            "team_b": live_row["team_b"],
            "venue": live_row.get("venue", ""),
            "team_a_score": f"{score}/{wickets}" if innings == 1 else (f"{live_row.get('target', 0) - 1 if live_row.get('target') else 0}" + ""),
            "team_a_overs": f"{overs}" if innings == 1 else "",
            "team_b_score": f"{score}/{wickets}" if innings == 2 else "Yet to bat",
            "team_b_overs": f"({overs} overs)" if innings == 2 else "",
            "current_rr": f"{live_row.get('current_run_rate', 0):.2f}" if live_row.get("current_run_rate") else "-",
            "required_rr": live_row.get("required_rate"),
            "projected_total": f"{live_row.get('projected_total', 0):.0f}" if live_row.get("projected_total") else None,
            "runs_needed": (live_row.get("target", 0) or 0) - score if innings == 2 and live_row.get("target") else None,
            "team_a_win": (live_row.get("live_win_prob_a") or 0.5) * 100,
            "team_b_win": (live_row.get("live_win_prob_b") or 0.5) * 100,
            "score": f"{score}/{wickets}",
            "wickets": str(wickets),
            "overs": str(overs),
            "last_update": live_row.get("last_updated", "--"),
            "key_moments": [],
        }

        # Parse key_moments JSON
        if live_row.get("key_moments"):
            try:
                km = json.loads(live_row["key_moments"]) if isinstance(live_row["key_moments"], str) else live_row["key_moments"]
                if isinstance(km, list):
                    live["key_moments"] = km
            except (json.JSONDecodeError, TypeError):
                pass

        # Prop tracker
        pred = db.fetch_one(
            "SELECT total_wides_pred, total_noballs_pred, total_sixes_pred, total_fours_pred "
            "FROM predictions WHERE team_a = ? AND team_b = ? AND league = ?",
            [live_row["team_a"], live_row["team_b"], league],
        )
        if pred or live_row.get("prop_wides") is not None:
            live["prop_tracker"] = {
                "wides_actual": live_row.get("prop_wides", 0) or 0,
                "wides_predicted": f"{pred.get('total_wides_pred', 0):.0f}" if pred and pred.get("total_wides_pred") else "-",
                "sixes_actual": live_row.get("prop_sixes", 0) or 0,
                "sixes_predicted": f"{pred.get('total_sixes_pred', 0):.0f}" if pred and pred.get("total_sixes_pred") else "-",
                "fours_actual": live_row.get("prop_fours", 0) or 0,
                "fours_predicted": f"{pred.get('total_fours_pred', 0):.0f}" if pred and pred.get("total_fours_pred") else "-",
                "noballs_actual": live_row.get("prop_noballs", 0) or 0,
                "noballs_predicted": f"{pred.get('total_noballs_pred', 0):.0f}" if pred and pred.get("total_noballs_pred") else "-",
            }

    return render_template("live.html", live=live)


@app.route("/live/recalculate", methods=["POST"])
@login_required
def live_recalculate():
    """Manual recalculation from live page form."""
    match_id = request.form.get("match_id", "")
    flash("Recalculation submitted.", "info")
    return redirect(url_for("live_match", match_id=match_id if match_id else 0))


# ---------------------------------------------------------------------------
# TOURNAMENT
# ---------------------------------------------------------------------------

@app.route("/tournament")
@login_required
def tournament():
    """League standings — points table and schedule."""
    league = _current_league()
    teams_config = _league_teams(league)
    all_teams_list = _league_team_names(league)
    season = _league_season(league)

    # Points table from team_ratings
    standings = []
    for team_name in all_teams_list:
        rating = db.fetch_one("SELECT * FROM team_ratings WHERE team = ? AND league = ?", [team_name, league])
        abbrev = get_abbreviation(team_name)
        team_config = teams_config.get(abbrev, {})

        if rating:
            played = rating.get("matches_played", 0) or 0
            wins = rating.get("wins", 0) or 0
            losses = rating.get("losses", 0) or 0
            nr = rating.get("no_results", 0) or 0
            standings.append({
                "name": team_name,
                "played": played,
                "won": wins,
                "lost": losses,
                "no_result": nr,
                "points": wins * 2 + nr,
                "nrr": rating.get("nrr", 0) or 0,
                "color": team_config.get("color", "#6B7280"),
                "group": team_config.get("group", ""),
            })
        else:
            standings.append({
                "name": team_name,
                "played": 0,
                "won": 0,
                "lost": 0,
                "no_result": 0,
                "points": 0,
                "nrr": 0,
                "color": team_config.get("color", "#6B7280"),
                "group": team_config.get("group", ""),
            })

    # Also try to compute from completed fixtures for more accuracy
    completed = db.fetch_all(
        "SELECT * FROM fixtures WHERE status = 'COMPLETED' AND season = ? AND league = ?",
        [season, league],
    )
    if completed:
        # Rebuild from fixtures
        table = {}
        for team_name in all_teams_list:
            abbrev = get_abbreviation(team_name)
            team_config = teams_config.get(abbrev, {})
            table[team_name] = {
                "name": team_name,
                "played": 0, "won": 0, "lost": 0, "no_result": 0,
                "points": 0, "nrr": 0.0,
                "color": team_config.get("color", "#6B7280"),
                "group": team_config.get("group", ""),
            }

        for match in completed:
            ta = standardise(match.get("team_a", ""))
            tb = standardise(match.get("team_b", ""))
            result_text = match.get("result", "") or ""

            if ta not in table or tb not in table:
                continue

            table[ta]["played"] += 1
            table[tb]["played"] += 1

            # Determine winner from result text or matches table
            match_row = db.fetch_one(
                "SELECT winner FROM matches WHERE team_a = ? AND team_b = ? AND match_date = ? AND league = ?",
                [match["team_a"], match["team_b"], match["match_date"], league],
            )
            winner = standardise(match_row["winner"]) if match_row and match_row.get("winner") else None

            if winner and winner in table:
                loser = tb if winner == ta else ta
                table[winner]["won"] += 1
                table[winner]["points"] += 2
                if loser in table:
                    table[loser]["lost"] += 1
            else:
                table[ta]["no_result"] += 1
                table[tb]["no_result"] += 1
                table[ta]["points"] += 1
                table[tb]["points"] += 1

        # Use NRR from team_ratings
        for team_name in table:
            rating = db.fetch_one("SELECT nrr FROM team_ratings WHERE team = ? AND league = ?", [team_name, league])
            if rating and rating.get("nrr") is not None:
                table[team_name]["nrr"] = rating["nrr"]

        standings = list(table.values())

    # Sort standings
    standings.sort(key=lambda x: (-x["points"], -x["nrr"]))

    # Split into groups
    group_a = sorted(
        [t for t in standings if t.get("group") == "A"],
        key=lambda x: (-x["points"], -x["nrr"]),
    )
    group_b = sorted(
        [t for t in standings if t.get("group") == "B"],
        key=lambda x: (-x["points"], -x["nrr"]),
    )

    # Schedule
    raw_schedule = db.fetch_all("SELECT * FROM fixtures WHERE season = ? AND league = ? ORDER BY match_date ASC", [season, league])
    if not raw_schedule:
        raw_schedule = db.fetch_all("SELECT * FROM fixtures WHERE league = ? ORDER BY match_date ASC", [league])

    today = datetime.utcnow().strftime("%Y-%m-%d")
    found_next = False
    schedule = []
    for s in raw_schedule:
        item = dict(s)
        item["date"] = s.get("match_date", "")
        item["team_a_color"] = _get_team_color(s["team_a"])
        item["team_b_color"] = _get_team_color(s["team_b"])

        if s.get("status") == "COMPLETED":
            item["status"] = "completed"
            item["winner"] = ""
            # Find winner
            match_row = db.fetch_one(
                "SELECT winner FROM matches WHERE team_a = ? AND team_b = ? AND match_date = ? AND league = ?",
                [s["team_a"], s["team_b"], s["match_date"], league],
            )
            if match_row and match_row.get("winner"):
                item["winner"] = match_row["winner"]
            item["result"] = s.get("result", "Completed")
        else:
            item["status"] = "upcoming"
            # Attach prediction
            pred = db.fetch_one(
                "SELECT team_a_win, team_b_win FROM predictions WHERE team_a = ? AND team_b = ? AND match_date = ? AND league = ?",
                [s["team_a"], s["team_b"], s["match_date"], league],
            )
            if pred:
                item["prediction"] = {
                    "team_a_win": (pred["team_a_win"] or 0.5) * 100,
                    "team_b_win": (pred["team_b_win"] or 0.5) * 100,
                }
            else:
                item["prediction"] = None

        # Mark next match
        item["is_next"] = False
        if not found_next and s.get("status") == "SCHEDULED" and s.get("match_date", "") >= today:
            item["is_next"] = True
            found_next = True

        schedule.append(item)

    return render_template(
        "tournament.html",
        standings=standings,
        group_a=group_a if group_a else None,
        group_b=group_b if group_b else None,
        schedule=schedule,
        playoffs=None,
    )


# ---------------------------------------------------------------------------
# SENTIMENT
# ---------------------------------------------------------------------------

@app.route("/sentiment")
@login_required
def sentiment():
    """Sentiment dashboard for all teams."""
    league = _current_league()
    all_teams = _league_team_names(league)
    teams_config = _league_teams(league)
    teams = []
    for team_name in all_teams:
        row = db.fetch_one(
            "SELECT * FROM sentiment WHERE team = ? AND league = ? ORDER BY scored_at DESC LIMIT 1",
            [team_name, league],
        )
        abbrev = get_abbreviation(team_name)
        team_config = teams_config.get(abbrev, {})

        team_data = {
            "name": team_name,
            "color": team_config.get("color", "#6B7280"),
            "sentiment_score": 0,
            "signal": "neutral",
            "trend": "stable",
            "volume": 0,
            "keywords": [],
        }

        if row:
            team_data["sentiment_score"] = row.get("score", 0) or 0
            team_data["signal"] = row.get("signal", "neutral") or "neutral"
            trend_val = row.get("trend", 0) or 0
            team_data["trend"] = "up" if trend_val > 0 else ("down" if trend_val < 0 else "stable")
            team_data["volume"] = row.get("volume", 0) or 0
            if row.get("keywords"):
                try:
                    kws = json.loads(row["keywords"]) if isinstance(row["keywords"], str) else row["keywords"]
                    if isinstance(kws, list):
                        team_data["keywords"] = kws
                except (json.JSONDecodeError, TypeError):
                    pass

        teams.append(team_data)

    return render_template("sentiment.html", teams=teams)


# ---------------------------------------------------------------------------
# PERFORMANCE
# ---------------------------------------------------------------------------

@app.route("/performance")
@login_required
def performance():
    """Model performance metrics."""
    league = _current_league()
    # Summary stats from model_performance table (use evaluated_at, NOT logged_at)
    overall = db.fetch_one(
        "SELECT * FROM model_performance WHERE period = 'overall' AND league = ? ORDER BY evaluated_at DESC LIMIT 1",
        [league]
    )

    total_preds = overall.get("total_predictions", 0) or 0 if overall else 0
    raw_accuracy = (overall.get("accuracy", 0) or 0) if overall else 0
    summary = {
        "accuracy": raw_accuracy * 100,
        "brier": overall.get("brier_score", 0) or 0 if overall else 0,
        "roi": (overall.get("roi", 0) or 0) * 100 if overall else 0,
        "total": total_preds,
        "correct": int(raw_accuracy * total_preds),
    }

    # Per-model breakdown
    perf_rows = db.fetch_all(
        "SELECT * FROM model_performance WHERE period = 'overall' AND league = ? ORDER BY evaluated_at DESC",
        [league]
    )
    # Deduplicate by model_name (keep latest)
    seen_models = set()
    models = []
    for p in perf_rows:
        if p["model_name"] not in seen_models:
            seen_models.add(p["model_name"])
            acc = (p.get("accuracy", 0) or 0) * 100
            models.append({
                "name": p["model_name"],
                "accuracy": acc,
                "brier": p.get("brier_score", 0) or 0,
                "predictions": p.get("total_predictions", 0) or 0,
                "status": "active" if acc >= 50 else "degraded",
            })

    # Recent predictions from model_tracker
    tracker_rows = db.fetch_all(
        "SELECT * FROM model_tracker WHERE league = ? ORDER BY match_date DESC LIMIT 20",
        [league]
    )
    recent_predictions = []
    for t in tracker_rows:
        prob = max(t.get("team_a_prob", 0.5) or 0.5, t.get("team_b_prob", 0.5) or 0.5) * 100
        recent_predictions.append({
            "date": t.get("match_date", ""),
            "match": f"{t['team_a']} vs {t['team_b']}",
            "predicted": t.get("predicted_winner", ""),
            "probability": prob,
            "actual": t.get("actual_winner", ""),
            "correct": bool(t.get("top_pick_correct")),
            "pnl": t.get("top_pick_pnl") or t.get("value_bet_pnl"),
        })

    # Calibration data (empty array if not available)
    calibration = []

    # Per-league model cards
    psl_models = []
    ipl_models = []
    for league_key, league_models_list in [("psl", psl_models), ("ipl", ipl_models)]:
        for model_name in ["batting_bowling", "elo", "xgboost", "sentiment", "player_strength", "ensemble"]:
            perf = db.fetch_one(
                "SELECT * FROM model_performance WHERE model_name = ? AND league = ? AND period = 'overall'",
                [model_name, league_key]
            )
            league_models_list.append({
                "name": model_name,
                "accuracy": perf.get("accuracy", 0) if perf else 0,
                "brier": perf.get("brier_score", 0) if perf else 0,
                "roi": perf.get("roi", 0) if perf else 0,
                "predictions": perf.get("total_predictions", 0) if perf else 0,
            })

    return render_template(
        "performance.html",
        summary=summary,
        models=models,
        recent_predictions=recent_predictions,
        calibration=calibration,
        psl_models=psl_models,
        ipl_models=ipl_models,
    )


# ---------------------------------------------------------------------------
# PORTFOLIO
# ---------------------------------------------------------------------------

@app.route("/portfolio")
@login_required
def portfolio():
    """Betting portfolio management."""
    league = _current_league()
    # Check if specific portfolio requested
    requested_id = request.args.get("id")

    if requested_id:
        active_portfolio = db.fetch_one("SELECT * FROM portfolios WHERE id = ?", [requested_id])
    else:
        active_portfolio = db.fetch_one(
            "SELECT * FROM portfolios WHERE status = 'active' ORDER BY created_at DESC LIMIT 1"
        )

    # Get all portfolios for selector
    all_portfolios = db.fetch_all("SELECT * FROM portfolios ORDER BY created_at DESC")

    portfolio_data = {
        "bankroll": 0, "starting_bankroll": 0, "pnl": 0, "roi": 0,
        "id": None, "name": "No Portfolio", "status": "none",
    }
    portfolio_id = None
    if active_portfolio:
        portfolio_id = active_portfolio["id"]
        bankroll = active_portfolio.get("bankroll", 0) or 0
        starting = active_portfolio.get("starting_bankroll", 0) or 0
        pnl = bankroll - starting
        roi = (pnl / starting * 100) if starting > 0 else 0
        portfolio_data = {
            "bankroll": bankroll, "starting_bankroll": starting,
            "pnl": pnl, "roi": roi,
            "id": portfolio_id,
            "name": active_portfolio.get("name", "Main"),
            "status": active_portfolio.get("status", "active"),
        }

    # Get bets for this portfolio (user_bets table has no username column)
    if portfolio_id:
        raw_bets = db.fetch_all(
            "SELECT * FROM user_bets WHERE portfolio_id = ? ORDER BY created_at DESC",
            [portfolio_id],
        )
    else:
        raw_bets = db.fetch_all(
            "SELECT * FROM user_bets ORDER BY created_at DESC"
        )

    bets = []
    for b in raw_bets:
        bets.append({
            "id": b.get("id"),
            "date": b.get("match_date", ""),
            "match": f"{b.get('team_a', '')} vs {b.get('team_b', '')}",
            "team_a": b.get("team_a", ""),
            "team_b": b.get("team_b", ""),
            "bet_type": b.get("bet_type", ""),
            "selection": b.get("selection", ""),
            "stake": b.get("stake", 0) or 0,
            "odds": b.get("odds", 0) or 0,
            "status": b.get("status", "pending"),
            "pnl": b.get("actual_pnl"),
        })

    # Split bets
    pending_bets = [b for b in bets if b["status"] == "pending"]
    settled_bets = [b for b in bets if b["status"] in ("won", "lost", "void")]

    # Computed stats
    won_count = sum(1 for b in settled_bets if b["status"] == "won")
    lost_count = sum(1 for b in settled_bets if b["status"] == "lost")
    win_rate = (won_count / (won_count + lost_count) * 100) if (won_count + lost_count) > 0 else 0
    total_staked = sum(b["stake"] for b in settled_bets)
    avg_stake = (total_staked / len(settled_bets)) if settled_bets else 0
    avg_odds = (sum(b["odds"] for b in settled_bets) / len(settled_bets)) if settled_bets else 0
    best_win = max((b.get("pnl") or 0 for b in settled_bets), default=0)
    worst_loss = min((b.get("pnl") or 0 for b in settled_bets), default=0)

    # Streak
    bet_streak_type, bet_streak_length = "N", 0
    for b in settled_bets:
        if b["status"] not in ("won", "lost"):
            continue
        is_win = b["status"] == "won"
        if bet_streak_length == 0:
            bet_streak_type = "W" if is_win else "L"
            bet_streak_length = 1
        elif (bet_streak_type == "W" and is_win) or (bet_streak_type == "L" and not is_win):
            bet_streak_length += 1
        else:
            break

    bet_stats = {
        "won": won_count,
        "lost": lost_count,
        "win_rate": round(win_rate, 1),
        "total_staked": round(total_staked, 2),
        "avg_stake": round(avg_stake, 2),
        "avg_odds": round(avg_odds, 2),
        "best_win": round(best_win, 2),
        "worst_loss": round(worst_loss, 2),
        "streak": f"{bet_streak_type}{bet_streak_length}",
    }

    # "All Time" aggregate across all portfolios when no specific one selected
    if not requested_id:
        all_bets_raw = db.fetch_all("SELECT stake, odds, actual_pnl, status FROM user_bets")
        all_settled = [b for b in all_bets_raw if b.get("status") in ("won", "lost", "void")]
        all_time_pnl = sum(b.get("actual_pnl", 0) or 0 for b in all_settled)
        all_time_staked = sum(b.get("stake", 0) or 0 for b in all_settled)
        all_time_roi = (all_time_pnl / all_time_staked * 100) if all_time_staked > 0 else 0
        all_time = {
            "total_bets": len(all_bets_raw),
            "settled": len(all_settled),
            "pnl": round(all_time_pnl, 2),
            "staked": round(all_time_staked, 2),
            "roi": round(all_time_roi, 1),
        }
    else:
        all_time = None

    # Upcoming matches for the bet form
    upcoming_fixtures = db.fetch_all(
        "SELECT id, team_a, team_b, match_date FROM fixtures WHERE status = 'SCHEDULED' AND league = ? ORDER BY match_date ASC LIMIT 20",
        [league]
    )
    upcoming_matches = []
    for f in upcoming_fixtures:
        upcoming_matches.append({
            "id": f["id"],
            "team_a": f["team_a"],
            "team_b": f["team_b"],
            "date": f["match_date"],
        })

    return render_template(
        "portfolio.html",
        portfolio=portfolio_data,
        all_portfolios=all_portfolios,
        bets=bets,
        pending_bets=pending_bets,
        settled_bets=settled_bets,
        bet_stats=bet_stats,
        all_time=all_time,
        upcoming_matches=upcoming_matches,
    )


@app.route("/portfolio/place-bet", methods=["POST"])
@login_required
def place_bet():
    """Place a bet from the portfolio page form."""
    match_id = request.form.get("match_id")
    bet_type = request.form.get("bet_type", "match_winner")
    selection = request.form.get("selection", "")
    stake = float(request.form.get("stake", 0) or 0)
    odds = float(request.form.get("odds", 0) or 0)

    # Look up fixture
    league = _current_league()
    fixture = db.fetch_one("SELECT * FROM fixtures WHERE id = ? AND league = ?", [match_id, league]) if match_id else None

    # Get active portfolio
    active_portfolio = db.fetch_one(
        "SELECT id FROM portfolios WHERE status = 'active' ORDER BY created_at DESC LIMIT 1"
    )
    portfolio_id = active_portfolio["id"] if active_portfolio else None

    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    potential_pnl = stake * (odds - 1) if odds > 0 else 0

    try:
        db.execute(
            """INSERT INTO user_bets
               (portfolio_id, match_date, team_a, team_b,
                bet_type, selection, stake, odds, potential_pnl,
                status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)""",
            [portfolio_id,
             fixture["match_date"] if fixture else "",
             fixture["team_a"] if fixture else "",
             fixture["team_b"] if fixture else "",
             bet_type, selection, stake, odds, potential_pnl, now],
        )
        flash("Bet placed successfully!", "success")
    except Exception as exc:
        flash(f"Error placing bet: {exc}", "danger")

    return redirect(url_for("portfolio"))


@app.route("/portfolio/settle/<int:bet_id>", methods=["POST"])
@login_required
def settle_bet(bet_id):
    """Settle a bet as won, lost, or void."""
    result = request.form.get("result", "")  # won, lost, void
    bet = db.fetch_one("SELECT * FROM user_bets WHERE id = ?", [bet_id])
    if not bet:
        flash("Bet not found.", "danger")
        return redirect(url_for("portfolio"))

    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    stake = bet.get("stake", 0) or 0
    odds = bet.get("odds", 0) or 0

    if result == "won":
        pnl = stake * (odds - 1)
    elif result == "lost":
        pnl = -stake
    else:  # void
        pnl = 0

    db.execute(
        "UPDATE user_bets SET status = ?, actual_pnl = ?, settled_at = ? WHERE id = ?",
        [result, pnl, now, bet_id]
    )

    # Update portfolio bankroll
    portfolio_id = bet.get("portfolio_id")
    if portfolio_id:
        db.execute(
            "UPDATE portfolios SET bankroll = bankroll + ? WHERE id = ?",
            [pnl, portfolio_id]
        )

    flash(f"Bet settled as {result}. P&L: {pnl:+.0f}", "success")
    return redirect(url_for("portfolio"))


@app.route("/portfolio/update-bankroll", methods=["POST"])
@login_required
def update_bankroll():
    """Update portfolio bankroll."""
    portfolio_id = request.form.get("portfolio_id")
    new_bankroll = float(request.form.get("bankroll", 0) or 0)

    db.execute("UPDATE portfolios SET bankroll = ? WHERE id = ?", [new_bankroll, portfolio_id])
    flash("Bankroll updated.", "success")
    return redirect(url_for("portfolio"))


@app.route("/portfolio/close/<int:pid>")
@login_required
def close_portfolio(pid):
    """Close a portfolio."""
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    db.execute("UPDATE portfolios SET status = 'closed', closed_at = ? WHERE id = ?", [now, pid])
    flash("Portfolio closed.", "success")
    return redirect(url_for("portfolio"))


@app.route("/portfolio/edit/<int:pid>", methods=["POST"])
@login_required
def edit_portfolio(pid):
    name = request.form.get("name")
    bankroll = request.form.get("bankroll")
    if name:
        db.execute("UPDATE portfolios SET name = ? WHERE id = ?", [name, pid])
    if bankroll:
        db.execute("UPDATE portfolios SET bankroll = ? WHERE id = ?", [float(bankroll), pid])
    flash("Portfolio updated.", "success")
    return redirect(url_for("portfolio", id=pid))


@app.route("/portfolio/delete/<int:pid>", methods=["POST"])
@login_required
def delete_portfolio(pid):
    db.execute("DELETE FROM user_bets WHERE portfolio_id = ?", [pid])
    db.execute("DELETE FROM portfolios WHERE id = ?", [pid])
    flash("Portfolio deleted.", "success")
    return redirect(url_for("portfolio"))


@app.route("/portfolio/edit-bet/<int:bet_id>", methods=["POST"])
@login_required
def edit_bet(bet_id):
    stake = request.form.get("stake")
    odds = request.form.get("odds")
    selection = request.form.get("selection")
    if stake:
        db.execute("UPDATE user_bets SET stake = ? WHERE id = ? AND status = 'pending'", [float(stake), bet_id])
    if odds:
        db.execute("UPDATE user_bets SET odds = ? WHERE id = ? AND status = 'pending'", [float(odds), bet_id])
    if selection:
        db.execute("UPDATE user_bets SET selection = ? WHERE id = ? AND status = 'pending'", [selection, bet_id])
    flash("Bet updated.", "success")
    return redirect(url_for("portfolio"))


@app.route("/portfolio/delete-bet/<int:bet_id>", methods=["POST"])
@login_required
def delete_bet(bet_id):
    db.execute("DELETE FROM user_bets WHERE id = ? AND status = 'pending'", [bet_id])
    flash("Bet deleted.", "success")
    return redirect(url_for("portfolio"))


# ---------------------------------------------------------------------------
# TRACKER
# ---------------------------------------------------------------------------

@app.route("/tracker")
@login_required
def tracker():
    """Prediction tracker — accuracy, ROI, streaks."""
    from collections import defaultdict

    league = _current_league()

    # Fetch all tracker entries
    all_entries = db.fetch_all(
        "SELECT * FROM model_tracker WHERE league = ? ORDER BY match_date DESC", [league]
    )

    settled = [e for e in all_entries if e["status"] == "settled"]
    pending = [e for e in all_entries if e["status"] == "pending"]

    # Hero metrics
    correct = sum(1 for e in settled if e.get("top_pick_correct") == 1)
    total_settled = len(settled)
    accuracy = (correct / total_settled * 100) if total_settled > 0 else 0
    total_pnl = sum(e.get("top_pick_pnl", 0) or 0 for e in settled)
    total_staked = total_settled * 100
    roi = (total_pnl / total_staked * 100) if total_staked > 0 else 0

    # Streak
    streak_type, streak_length = "N", 0
    for e in sorted(settled, key=lambda x: x["match_date"], reverse=True):
        was_correct = e.get("top_pick_correct") == 1
        if streak_length == 0:
            streak_type = "W" if was_correct else "L"
            streak_length = 1
        elif (streak_type == "W" and was_correct) or (streak_type == "L" and not was_correct):
            streak_length += 1
        else:
            break

    # Value bets strategy
    vb_settled = [e for e in settled if e.get("is_value_bet") == 1]
    vb_correct = sum(1 for e in vb_settled if e.get("value_bet_correct") == 1)
    vb_accuracy = (vb_correct / len(vb_settled) * 100) if vb_settled else 0
    vb_pnl = sum(e.get("value_bet_pnl", 0) or 0 for e in vb_settled)
    vb_staked = len(vb_settled) * 100
    vb_roi = (vb_pnl / vb_staked * 100) if vb_staked > 0 else 0
    vb_avg_edge = (sum(e.get("value_edge", 0) or 0 for e in vb_settled) / len(vb_settled)) if vb_settled else 0

    # Outcome breakdown (team_a vs team_b picks)
    team_a_picks = [e for e in settled if e.get("predicted_winner") == e.get("team_a")]
    team_b_picks = [e for e in settled if e.get("predicted_winner") == e.get("team_b")]
    team_a_correct = sum(1 for e in team_a_picks if e.get("top_pick_correct") == 1)
    team_b_correct = sum(1 for e in team_b_picks if e.get("top_pick_correct") == 1)

    # Weekly report
    weekly = defaultdict(lambda: {"correct": 0, "total": 0, "pnl": 0, "vb_correct": 0, "vb_total": 0})
    for e in settled:
        try:
            d = datetime.strptime(e["match_date"][:10], "%Y-%m-%d")
            week_start = d - timedelta(days=d.weekday())
            week_label = week_start.strftime("Week of %b %d")
        except Exception:
            week_label = "Unknown"
        weekly[week_label]["total"] += 1
        weekly[week_label]["pnl"] += e.get("top_pick_pnl", 0) or 0
        if e.get("top_pick_correct") == 1:
            weekly[week_label]["correct"] += 1
        if e.get("is_value_bet") == 1:
            weekly[week_label]["vb_total"] += 1
            if e.get("value_bet_correct") == 1:
                weekly[week_label]["vb_correct"] += 1

    weekly_report = []
    for label, data in sorted(weekly.items(), reverse=True):
        data["week"] = label
        data["accuracy"] = (data["correct"] / data["total"] * 100) if data["total"] > 0 else 0
        weekly_report.append(data)

    # Best/worst month
    monthly = defaultdict(lambda: {"correct": 0, "total": 0, "pnl": 0})
    for e in settled:
        month = e["match_date"][:7]
        monthly[month]["total"] += 1
        monthly[month]["pnl"] += e.get("top_pick_pnl", 0) or 0
        if e.get("top_pick_correct") == 1:
            monthly[month]["correct"] += 1

    best_month = max(monthly.items(), key=lambda x: x[1]["pnl"], default=(None, {"pnl": 0}))
    worst_month = min(monthly.items(), key=lambda x: x[1]["pnl"], default=(None, {"pnl": 0}))

    # Build match list with details
    matches = []
    for e in all_entries:
        match = dict(e)
        match["pnl_display"] = f"${e.get('top_pick_pnl', 0) or 0:+.0f}" if e["status"] == "settled" else "-"
        match["is_correct"] = e.get("top_pick_correct") == 1
        matches.append(match)

    return render_template("tracker.html",
        hero={"accuracy": round(accuracy, 1), "correct": correct, "total": total_settled,
              "pnl": round(total_pnl, 2), "roi": round(roi, 1), "pending": len(pending),
              "streak": f"{streak_type}{streak_length}"},
        every_game={"matches": total_settled, "correct": correct,
                    "accuracy": round(accuracy, 1), "staked": total_staked,
                    "pnl": round(total_pnl, 2), "roi": round(roi, 1),
                    "streak": f"{streak_type}{streak_length}"},
        value_bets={"matches": len(vb_settled), "correct": vb_correct,
                    "accuracy": round(vb_accuracy, 1), "staked": vb_staked,
                    "pnl": round(vb_pnl, 2), "roi": round(vb_roi, 1),
                    "avg_edge": round(vb_avg_edge, 1)},
        outcome={"team_a": {"correct": team_a_correct, "total": len(team_a_picks),
                            "accuracy": round(team_a_correct/len(team_a_picks)*100, 1) if team_a_picks else 0},
                 "team_b": {"correct": team_b_correct, "total": len(team_b_picks),
                            "accuracy": round(team_b_correct/len(team_b_picks)*100, 1) if team_b_picks else 0}},
        weekly_report=weekly_report,
        best_month={"month": best_month[0], "pnl": round(best_month[1]["pnl"], 2)} if best_month[0] else None,
        worst_month={"month": worst_month[0], "pnl": round(worst_month[1]["pnl"], 2)} if worst_month[0] else None,
        matches=matches,
    )


# ---------------------------------------------------------------------------
# SETTINGS
# ---------------------------------------------------------------------------

@app.route("/settings")
@admin_required
def settings():
    """Admin settings page."""
    league = _current_league()
    raw_users = _load_users()

    # Template expects a list of user objects with id, username, role, created
    users = []
    for idx, (uname, udata) in enumerate(raw_users.items(), start=1):
        users.append({
            "id": uname,  # use username as id for delete
            "username": uname,
            "role": udata.get("role", "viewer"),
            "created": udata.get("created_at", "-"),
        })

    # Current weights
    weights_path = os.path.join(config.CACHE_DIR, "optimized_weights.json")
    if os.path.exists(weights_path):
        with open(weights_path) as fh:
            weights = json.load(fh)
    else:
        weights = config.MODEL_WEIGHTS.copy()

    # Portfolios for management
    raw_portfolios = db.fetch_all("SELECT * FROM portfolios ORDER BY created_at DESC")
    portfolios = []
    for p in raw_portfolios:
        # Count bets and calculate P&L
        bet_rows = db.fetch_all(
            "SELECT stake, actual_pnl, status FROM user_bets WHERE portfolio_id = ?",
            [p["id"]],
        )
        total_bets = len(bet_rows)
        pnl = sum(b.get("actual_pnl", 0) or 0 for b in bet_rows if b.get("status") in ("won", "lost"))

        portfolios.append({
            "name": p.get("name", "Default"),
            "bankroll": p.get("bankroll", 0) or 0,
            "starting_bankroll": p.get("starting_bankroll", 0) or 0,
            "pnl": pnl,
            "total_bets": total_bets,
            "is_active": p.get("status") == "active",
        })

    return render_template(
        "settings.html",
        users=users,
        weights=weights,
        portfolios=portfolios,
    )


@app.route("/settings/add-user", methods=["POST"])
@admin_required
def add_user():
    """Add a new user from settings form."""
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "").strip()
    role = request.form.get("role", "viewer")

    if not username or not password:
        flash("Username and password are required.", "danger")
        return redirect(url_for("settings"))

    users = _load_users()
    if username in users:
        flash("User already exists.", "danger")
        return redirect(url_for("settings"))

    users[username] = {
        "password": _hash_password(password),
        "role": role,
        "email": "",
        "created_at": datetime.utcnow().strftime("%Y-%m-%d"),
    }
    _save_users(users)
    flash(f"User '{username}' created.", "success")
    return redirect(url_for("settings"))


@app.route("/settings/delete-user/<user_id>", methods=["POST"])
@admin_required
def delete_user(user_id):
    """Delete a user."""
    if user_id == "admin":
        flash("Cannot delete admin user.", "danger")
        return redirect(url_for("settings"))

    users = _load_users()
    if user_id in users:
        del users[user_id]
        _save_users(users)
        flash(f"User '{user_id}' deleted.", "success")
    else:
        flash("User not found.", "danger")
    return redirect(url_for("settings"))


@app.route("/settings/change-password", methods=["POST"])
@login_required
def change_password():
    """Change current user's password."""
    current_password = request.form.get("current_password", "").strip()
    new_password = request.form.get("new_password", "").strip()

    if not current_password or not new_password:
        flash("Both current and new passwords are required.", "danger")
        return redirect(url_for("settings"))

    username = session.get("username", "")
    users = _load_users()
    user = users.get(username)
    if not user:
        flash("User not found.", "danger")
        return redirect(url_for("settings"))

    # Verify current password
    if _hash_password(current_password) != user["password"]:
        flash("Current password is incorrect.", "danger")
        return redirect(url_for("settings"))

    users[username]["password"] = _hash_password(new_password)
    _save_users(users)
    flash("Password updated successfully.", "success")
    return redirect(url_for("settings"))


@app.route("/settings/save-weights", methods=["POST"])
@admin_required
def save_weights():
    """Save ensemble model weights from form."""
    weights = {
        "batting_bowling": float(request.form.get("batting_bowling", 0.25)),
        "elo": float(request.form.get("elo", 0.25)),
        "xgboost": float(request.form.get("xgboost", 0.35)),
        "sentiment": float(request.form.get("sentiment", 0.15)),
    }

    weights_path = os.path.join(config.CACHE_DIR, "optimized_weights.json")
    os.makedirs(os.path.dirname(weights_path), exist_ok=True)
    with open(weights_path, "w") as fh:
        json.dump(weights, fh, indent=2)

    flash("Model weights saved.", "success")
    return redirect(url_for("settings"))


@app.route("/settings/auto-optimize", methods=["POST"])
@admin_required
def auto_optimize_weights():
    """Auto-optimize ensemble weights."""
    try:
        from models.ensemble import optimize_weights
        result = optimize_weights()
        if result:
            flash("Weights auto-optimized successfully.", "success")
        else:
            flash("Not enough data to optimize (need 20+ settled predictions).", "warning")
    except Exception as exc:
        flash(f"Optimization error: {exc}", "danger")
    return redirect(url_for("settings"))


@app.route("/settings/create-portfolio", methods=["POST"])
@admin_required
def create_portfolio():
    """Create a new betting portfolio."""
    name = request.form.get("name", "Default").strip()
    starting_bankroll = float(request.form.get("starting_bankroll", 0) or 0)

    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    try:
        db.execute(
            """INSERT INTO portfolios (name, bankroll, starting_bankroll, status, created_at)
               VALUES (?, ?, ?, 'active', ?)""",
            [name, starting_bankroll, starting_bankroll, now],
        )
        flash(f"Portfolio '{name}' created.", "success")
    except Exception as exc:
        flash(f"Error creating portfolio: {exc}", "danger")

    return redirect(url_for("settings"))


# ---------------------------------------------------------------------------
# WATCHDOG
# ---------------------------------------------------------------------------

@app.route("/watchdog")
@login_required
def watchdog():
    """System health monitoring dashboard."""
    league = _current_league()
    # Build checks dict with keys: data_freshness, model_health, data_integrity, api_system
    checks = {
        "data_freshness": [],
        "model_health": [],
        "data_integrity": [],
        "api_system": [],
    }
    last_check_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    # ── Data Freshness checks ──
    freshness_tables = {
        "Fixtures": ("SELECT MAX(updated_at) as latest FROM fixtures WHERE league = ?", "updated_at"),
        "Odds": ("SELECT MAX(fetched_at) as latest FROM odds WHERE league = ?", "fetched_at"),
        "Sentiment": ("SELECT MAX(scored_at) as latest FROM sentiment WHERE league = ?", "scored_at"),
        "Weather": ("SELECT MAX(fetched_at) as latest FROM weather WHERE league = ?", "fetched_at"),
        "Predictions": ("SELECT MAX(updated_at) as latest FROM predictions WHERE league = ?", "updated_at"),
    }
    for table_name, (sql, _col) in freshness_tables.items():
        try:
            row = db.fetch_one(sql, [league])
            latest = row["latest"] if row and row.get("latest") else None
            threshold_hrs = config.WATCHDOG_SETTINGS["data_freshness_hours"].get(table_name.lower(), 24)
            if latest:
                try:
                    age_hrs = (datetime.utcnow() - datetime.fromisoformat(latest.replace("Z", ""))).total_seconds() / 3600
                    status = "ok" if age_hrs < threshold_hrs else "warning"
                    msg = f"Updated {age_hrs:.1f}h ago"
                except Exception:
                    status = "warning"
                    msg = f"Last: {latest}"
            else:
                status = "warning"
                msg = "No data"
            checks["data_freshness"].append({"name": table_name, "status": status, "message": msg})
        except Exception:
            checks["data_freshness"].append({"name": table_name, "status": "error", "message": "Query failed"})

    # ── Model Health checks ──
    try:
        perf_rows = db.fetch_all(
            "SELECT model_name, accuracy, brier_score FROM model_performance WHERE period = 'overall' AND league = ? ORDER BY evaluated_at DESC",
            [league]
        )
        seen = set()
        for p in perf_rows:
            mname = p["model_name"]
            if mname in seen:
                continue
            seen.add(mname)
            acc = (p.get("accuracy", 0) or 0) * 100
            brier = p.get("brier_score", 1) or 1
            if acc >= 60 and brier < 0.25:
                status = "ok"
                msg = f"Accuracy: {acc:.1f}%, Brier: {brier:.3f}"
            elif acc >= 50:
                status = "warning"
                msg = f"Accuracy: {acc:.1f}%, Brier: {brier:.3f}"
            else:
                status = "error"
                msg = f"Accuracy: {acc:.1f}% — below threshold"
            checks["model_health"].append({"name": mname, "status": status, "message": msg})

        if not perf_rows:
            checks["model_health"].append({"name": "Ensemble", "status": "warning", "message": "No performance data"})
    except Exception:
        checks["model_health"].append({"name": "Models", "status": "error", "message": "Could not check"})

    # ── Data Integrity checks ──
    integrity_checks = [
        ("Matches", "SELECT COUNT(*) as cnt FROM matches WHERE league = ?"),
        ("Fixtures", "SELECT COUNT(*) as cnt FROM fixtures WHERE league = ?"),
        ("Team Ratings", "SELECT COUNT(*) as cnt FROM team_ratings WHERE league = ?"),
        ("Venue Stats", "SELECT COUNT(*) as cnt FROM venue_stats WHERE league = ?"),
    ]
    for name, sql in integrity_checks:
        try:
            row = db.fetch_one(sql, [league])
            cnt = row["cnt"] if row else 0
            status = "ok" if cnt > 0 else "warning"
            msg = f"{cnt} records"
            checks["data_integrity"].append({"name": name, "status": status, "message": msg})
        except Exception:
            checks["data_integrity"].append({"name": name, "status": "error", "message": "Query failed"})

    # ── API & System checks ──
    # DB size
    try:
        db_size_mb = os.path.getsize(config.DB_PATH) / (1024 * 1024) if os.path.exists(config.DB_PATH) else 0
        status = "ok" if db_size_mb < config.WATCHDOG_SETTINGS["max_db_size_mb"] else "warning"
        checks["api_system"].append({"name": "Database Size", "status": status, "message": f"{db_size_mb:.1f} MB"})
    except Exception:
        checks["api_system"].append({"name": "Database Size", "status": "error", "message": "Could not check"})

    # Cache size
    try:
        cache_mb = rate_limiter.get_cache_size_mb()
        status = "ok" if cache_mb < config.WATCHDOG_SETTINGS["max_cache_size_mb"] else "warning"
        checks["api_system"].append({"name": "Cache Size", "status": status, "message": f"{cache_mb:.1f} MB"})
    except Exception:
        checks["api_system"].append({"name": "Cache Size", "status": "warning", "message": "N/A"})

    # API usage
    try:
        api_usage = rate_limiter.get_usage_summary()
        for api_name, usage in api_usage.items():
            used = usage.get("used", 0)
            limit = usage.get("limit", 1)
            pct = (used / limit * 100) if limit > 0 else 0
            status = "ok" if pct < 80 else ("warning" if pct < 95 else "error")
            checks["api_system"].append({"name": f"API: {api_name}", "status": status, "message": f"{used}/{limit} calls"})
    except Exception:
        pass

    return render_template("watchdog.html", checks=checks, last_check_time=last_check_time)


@app.route("/watchdog/run-checks", methods=["POST"])
@login_required
def run_health_checks():
    """Trigger health checks and redirect back."""
    flash("Health checks completed.", "info")
    return redirect(url_for("watchdog"))


# ---------------------------------------------------------------------------
# API ENDPOINTS
# ---------------------------------------------------------------------------

@app.route("/api/predict/<team_a>/<team_b>", methods=["POST"])
@login_required
def api_predict(team_a, team_b):
    """Generate prediction on demand."""
    league = _current_league()
    team_a = standardise(team_a.replace("_", " "))
    team_b = standardise(team_b.replace("_", " "))

    try:
        from models import ensemble

        venue = request.json.get("venue") if request.is_json else None
        match_date = request.json.get("match_date") if request.is_json else None

        if not venue or not match_date:
            fix = db.fetch_one(
                "SELECT venue, match_date FROM fixtures WHERE team_a = ? AND team_b = ? "
                "AND status = 'SCHEDULED' AND league = ? ORDER BY match_date ASC LIMIT 1",
                [team_a, team_b, league],
            )
            if fix:
                venue = venue or fix["venue"]
                match_date = match_date or fix["match_date"]

        pred = ensemble.predict(team_a, team_b, venue, match_date)
        if pred:
            fixture_row = db.fetch_one(
                "SELECT id FROM fixtures WHERE team_a = ? AND team_b = ? AND match_date = ? AND league = ?",
                [team_a, team_b, match_date, league],
            )
            fid = fixture_row["id"] if fixture_row else None
            ensemble.save_prediction(pred, team_a, team_b, match_date, venue, fid, league=league)
            return jsonify({"status": "ok", "prediction": pred})

        return jsonify({"status": "error", "message": "Could not generate prediction"}), 500
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 500


@app.route("/api/live/update", methods=["GET", "POST"])
@login_required
def api_live_update():
    """Live match update — supports both GET (auto-poll from JS) and POST (manual)."""
    league = _current_league()
    if request.method == "GET":
        match_id = request.args.get("match_id", "")
        if match_id:
            live_row = db.fetch_one("SELECT * FROM live_matches WHERE id = ? AND league = ?", [match_id, league])
            if live_row:
                return jsonify({
                    "team_a_win": (live_row.get("live_win_prob_a") or 0.5) * 100,
                    "team_b_win": (live_row.get("live_win_prob_b") or 0.5) * 100,
                    "score": f"{live_row.get('current_score', 0)}/{live_row.get('current_wickets', 0)}",
                })
        return jsonify({"team_a_win": 50, "team_b_win": 50})

    # POST — manual update
    data = request.get_json(force=True)
    fixture_id = data.get("fixture_id")
    score = data.get("score", 0)
    wickets = data.get("wickets", 0)
    overs = data.get("overs", 0)
    innings = data.get("innings", 1)
    target = data.get("target")

    if not fixture_id:
        return jsonify({"status": "error", "message": "fixture_id required"}), 400

    try:
        from models.live_predictor import calculate_live_probability

        fixture = db.fetch_one("SELECT * FROM fixtures WHERE id = ? AND league = ?", [fixture_id, league])
        if not fixture:
            return jsonify({"status": "error", "message": "Fixture not found"}), 404

        team_a = fixture["team_a"]
        team_b = fixture["team_b"]
        venue = fixture.get("venue")

        result = calculate_live_probability(team_a, team_b, venue, innings, score, wickets, overs, target)

        existing = db.fetch_one("SELECT id FROM live_matches WHERE fixture_id = ? AND league = ?", [fixture_id, league])
        now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        if existing:
            db.execute(
                """UPDATE live_matches SET innings = ?, current_score = ?, current_wickets = ?,
                   current_overs = ?, target = ?, live_win_prob_a = ?, live_win_prob_b = ?,
                   projected_total = ?, current_run_rate = ?, required_rate = ?,
                   last_updated = ?
                   WHERE fixture_id = ? AND league = ?""",
                [innings, score, wickets, overs, target,
                 result.get("team_a_win", 0.5), result.get("team_b_win", 0.5),
                 result.get("projected_total"), result.get("current_rate"),
                 result.get("required_rate"), now, fixture_id, league],
            )
        else:
            db.execute(
                """INSERT INTO live_matches (fixture_id, team_a, team_b, venue,
                   current_batting, current_score, current_wickets, current_overs,
                   target, live_win_prob_a, live_win_prob_b, projected_total,
                   current_run_rate, required_rate, innings, last_updated, auto_update, league)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)""",
                [fixture_id, team_a, team_b, venue,
                 team_a if innings == 1 else team_b,
                 score, wickets, overs, target,
                 result.get("team_a_win", 0.5), result.get("team_b_win", 0.5),
                 result.get("projected_total"), result.get("current_rate"),
                 result.get("required_rate"), innings, now, league],
            )

        return jsonify({"status": "ok", "probability": result})
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 500


@app.route("/api/live/what-if", methods=["GET"])
@login_required
def api_live_whatif():
    """What-if scenario calculator."""
    match_id = request.args.get("match_id", "")
    scenario = request.args.get("scenario", "")

    if not scenario:
        return jsonify({"error": "No scenario provided"})

    # Default response
    result = {
        "team_a_win": 50,
        "team_b_win": 50,
        "insight": f"Scenario: {scenario.replace('_', ' ').title()}",
    }

    try:
        from models.live_predictor import what_if
        live_row = db.fetch_one("SELECT * FROM live_matches WHERE id = ? AND league = ?", [match_id, _current_league()])
        if live_row:
            calc = what_if(
                team_a=live_row["team_a"],
                team_b=live_row["team_b"],
                venue=live_row.get("venue"),
                innings=live_row.get("innings", 1),
                score=live_row.get("current_score", 0),
                wickets=live_row.get("current_wickets", 0),
                overs=live_row.get("current_overs", 0),
                target=live_row.get("target"),
                scenario=scenario,
            )
            if calc:
                result = calc
    except Exception:
        pass

    return jsonify(result)


@app.route("/api/live/toggle-auto/<int:fixture_id>", methods=["POST"])
@login_required
def api_live_toggle_auto(fixture_id):
    """Toggle auto-update flag for a live match."""
    existing = db.fetch_one("SELECT auto_update FROM live_matches WHERE fixture_id = ? AND league = ?", [fixture_id, _current_league()])
    if not existing:
        return jsonify({"status": "error", "message": "Live match not found"}), 404

    new_val = 0 if existing["auto_update"] else 1
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    db.execute(
        "UPDATE live_matches SET auto_update = ?, last_updated = ? WHERE fixture_id = ? AND league = ?",
        [new_val, now, fixture_id, _current_league()],
    )
    return jsonify({"status": "ok", "auto_update": bool(new_val)})


@app.route("/api/refresh-data", methods=["POST"])
@login_required
def api_refresh_data():
    """Trigger full data refresh — fixtures, odds, weather, sentiment, predictions.
    Called by the Refresh Data button on dashboard."""
    league = _current_league()
    results = {}

    # 1. Fixtures from CricAPI
    try:
        from data.cricket_api import get_psl_fixtures, save_fixtures_to_db
        fixtures = get_psl_fixtures()
        if fixtures:
            save_fixtures_to_db(fixtures)
        results["fixtures"] = f"{len(fixtures)} fetched" if fixtures else "no new fixtures (cached/rate-limited)"
    except Exception as exc:
        results["fixtures"] = f"error: {exc}"

    # 2. Odds from The Odds API
    try:
        from data.odds_api import get_odds, save_odds_to_db
        odds = get_odds()
        if odds:
            save_odds_to_db(odds)
        results["odds"] = f"{len(odds)} fetched" if odds else "no odds available yet"
    except Exception as exc:
        results["odds"] = f"error: {exc}"

    # 3. Weather for upcoming matches
    try:
        from data.weather_api import get_match_weather, save_weather_to_db
        upcoming = db.fetch_all(
            "SELECT DISTINCT venue, match_date, match_time FROM fixtures WHERE status IN ('SCHEDULED','LIVE') AND league = ? ORDER BY match_date LIMIT 5",
            [league]
        )
        weather_count = 0
        for fix in upcoming:
            if fix.get("venue"):
                w = get_match_weather(fix["venue"], fix["match_date"], fix.get("match_time"))
                if w:
                    save_weather_to_db(w)
                    weather_count += 1
        results["weather"] = f"{weather_count} venues updated"
    except Exception as exc:
        results["weather"] = f"error: {exc}"

    # 4. Live scores (if any matches are live)
    try:
        from data.cricket_api import get_live_score
        live_fixtures = db.fetch_all("SELECT * FROM fixtures WHERE status = 'LIVE' AND league = ?", [league])
        for lf in live_fixtures:
            if lf.get("cricapi_id"):
                score = get_live_score(lf["cricapi_id"])
                if score:
                    results["live"] = f"score updated for {lf['team_a']} vs {lf['team_b']}"
        if not live_fixtures:
            results["live"] = "no live matches"
    except Exception as exc:
        results["live"] = f"error: {exc}"

    # 5. Regenerate predictions for upcoming matches
    try:
        from models.ensemble import predict as ens_predict, save_prediction
        upcoming = db.fetch_all(
            "SELECT * FROM fixtures WHERE status IN ('SCHEDULED','LIVE') AND league = ? ORDER BY match_date ASC LIMIT 10",
            [league]
        )
        pred_count = 0
        for fix in upcoming:
            try:
                pred = ens_predict(fix["team_a"], fix["team_b"], fix.get("venue"), fix.get("match_date"), league=league)
                if pred:
                    save_prediction(pred, fix["team_a"], fix["team_b"], fix["match_date"],
                                    fix.get("venue"), fix["id"], league=league)
                    pred_count += 1
            except Exception:
                pass
        results["predictions"] = f"{pred_count} updated"
    except Exception as exc:
        results["predictions"] = f"error: {exc}"

    # Log refresh timestamps for each source
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    for source, detail in results.items():
        status = "ok" if "error" not in str(detail).lower() else "error"
        db.execute(
            """INSERT INTO data_refresh_log (league, source, status, detail, refreshed_at)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(league, source) DO UPDATE SET
               status=excluded.status, detail=excluded.detail, refreshed_at=excluded.refreshed_at""",
            [league, source, status, str(detail), now]
        )

    # Get all refresh timestamps for response
    refresh_log = db.fetch_all(
        "SELECT source, status, detail, refreshed_at FROM data_refresh_log WHERE league = ? ORDER BY source",
        [league]
    )
    results["_last_updated"] = {r["source"]: r["refreshed_at"] for r in refresh_log}

    return jsonify({"status": "ok", "results": results})


@app.route("/api/tracker/generate", methods=["POST"])
@login_required
def api_tracker_generate():
    """Create tracker entries for upcoming matches with predictions."""
    league = _current_league()
    upcoming = db.fetch_all(
        "SELECT f.*, p.team_a_win, p.team_b_win, p.confidence, p.model_details "
        "FROM fixtures f "
        "JOIN predictions p ON f.id = p.fixture_id "
        "WHERE f.status = 'SCHEDULED' AND f.league = ?",
        [league]
    )

    count = 0
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    for fix in upcoming:
        team_a_win = fix.get("team_a_win", 0.5) or 0.5
        team_b_win = fix.get("team_b_win", 0.5) or 0.5
        predicted_winner = fix["team_a"] if team_a_win >= team_b_win else fix["team_b"]
        confidence = fix.get("confidence", 0.5) or 0.5

        vb = db.fetch_one(
            "SELECT * FROM value_bets WHERE team_a = ? AND team_b = ? AND match_date = ? AND status = 'pending' AND league = ? LIMIT 1",
            [fix["team_a"], fix["team_b"], fix["match_date"], league],
        )
        is_value_bet = 1 if vb else 0
        value_bet_type = vb.get("bet_type", "") if vb else None
        value_edge = vb.get("edge_pct", 0) if vb else None
        value_odds = vb.get("best_odds", 0) if vb else None

        try:
            db.execute(
                """INSERT OR IGNORE INTO model_tracker
                   (match_date, team_a, team_b, venue,
                    predicted_winner, team_a_prob, team_b_prob,
                    confidence, is_value_bet, value_bet_type, value_edge, value_odds,
                    status, created_at, league)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)""",
                [fix["match_date"], fix["team_a"], fix["team_b"],
                 fix.get("venue"), predicted_winner, team_a_win, team_b_win,
                 confidence, is_value_bet, value_bet_type, value_edge, value_odds, now, league],
            )
            count += 1
        except Exception:
            pass

    return jsonify({"status": "ok", "generated": count})


@app.route("/api/tracker/settle", methods=["POST"])
@login_required
def api_tracker_settle():
    """Settle completed matches in the tracker."""
    league = _current_league()
    # Join with matches table to get actual winner
    pending = db.fetch_all(
        "SELECT t.*, m.winner as actual FROM model_tracker t "
        "LEFT JOIN matches m ON t.team_a = m.team_a AND t.team_b = m.team_b AND t.match_date = m.match_date AND t.league = m.league "
        "WHERE t.status = 'pending' AND m.winner IS NOT NULL AND t.league = ?",
        [league]
    )

    settled_count = 0
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    for entry in pending:
        actual_winner = standardise(entry.get("actual", "") or "")
        if not actual_winner:
            continue

        predicted_winner = standardise(entry.get("predicted_winner", "") or "")
        correct = 1 if predicted_winner == actual_winner else 0

        # Get actual totals from matches
        match_row = db.fetch_one(
            "SELECT innings1_runs, innings2_runs FROM matches WHERE team_a = ? AND team_b = ? AND match_date = ? AND league = ?",
            [entry["team_a"], entry["team_b"], entry["match_date"], league],
        )
        actual_total_a = match_row.get("innings1_runs") if match_row else None
        actual_total_b = match_row.get("innings2_runs") if match_row else None

        # P&L: top pick uses fair odds from predicted probability
        predicted_prob = max(entry.get("team_a_prob", 0.5) or 0.5, entry.get("team_b_prob", 0.5) or 0.5)
        if correct:
            top_pick_pnl = 100 * (1 / predicted_prob) - 100  # fair odds profit
        else:
            top_pick_pnl = -100.0

        # Value bet P&L uses actual bookmaker odds
        value_bet_correct = None
        value_bet_pnl = None

        if entry.get("is_value_bet") and entry.get("value_odds"):
            vb_type = entry.get("value_bet_type", "")
            if "team_a" in vb_type:
                value_bet_correct = 1 if actual_winner == standardise(entry["team_a"]) else 0
            elif "team_b" in vb_type:
                value_bet_correct = 1 if actual_winner == standardise(entry["team_b"]) else 0
            else:
                value_bet_correct = correct

            value_odds = entry.get("value_odds", 0) or 0
            if value_bet_correct:
                value_bet_pnl = 100 * value_odds - 100  # $100 stake at bookmaker odds
            else:
                value_bet_pnl = -100.0

        db.execute(
            """UPDATE model_tracker SET
               actual_winner = ?, actual_total_a = ?, actual_total_b = ?,
               top_pick_correct = ?, top_pick_pnl = ?,
               value_bet_correct = ?, value_bet_pnl = ?,
               status = 'settled', settled_at = ?
               WHERE id = ? AND league = ?""",
            [actual_winner, actual_total_a, actual_total_b,
             correct, top_pick_pnl, value_bet_correct, value_bet_pnl,
             now, entry["id"], league],
        )
        settled_count += 1

    return jsonify({"status": "ok", "settled": settled_count})


@app.route("/api/bets", methods=["POST"])
@login_required
def api_place_bet():
    """Record a user bet (JSON API)."""
    data = request.get_json(force=True)
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    try:
        potential_pnl = (data.get("stake", 0) or 0) * ((data.get("odds", 0) or 0) - 1)
        bet_id = db.execute(
            """INSERT INTO user_bets
               (portfolio_id, match_date, team_a, team_b,
                bet_type, selection, odds, stake, potential_pnl,
                status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)""",
            [data.get("portfolio_id"), data.get("match_date"),
             data.get("team_a"), data.get("team_b"),
             data.get("bet_type", "match_winner"), data.get("selection"),
             data.get("odds", 0), data.get("stake", 0), potential_pnl, now],
        )
        return jsonify({"status": "ok", "bet_id": bet_id})
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 500


@app.route("/api/bets/<int:bet_id>/settle", methods=["POST"])
@login_required
def api_settle_bet(bet_id):
    """Settle a single bet."""
    data = request.get_json(force=True)
    outcome = data.get("outcome", "")  # "won" or "lost"

    bet = db.fetch_one("SELECT * FROM user_bets WHERE id = ?", [bet_id])
    if not bet:
        return jsonify({"status": "error", "message": "Bet not found"}), 404

    stake = bet.get("stake", 0) or 0
    odds = bet.get("odds", 0) or 0

    if outcome == "won":
        pnl = stake * (odds - 1)
    else:
        pnl = -stake

    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    db.execute(
        """UPDATE user_bets SET status = ?, actual_pnl = ?,
           settled_at = ? WHERE id = ?""",
        [outcome, round(pnl, 2), now, bet_id],
    )
    return jsonify({"status": "ok", "pnl": round(pnl, 2)})


@app.route("/api/portfolios", methods=["GET", "POST"])
@login_required
def api_portfolios():
    """List or create betting portfolios."""
    if request.method == "GET":
        rows = db.fetch_all(
            "SELECT * FROM portfolios ORDER BY created_at DESC"
        )
        return jsonify({"status": "ok", "portfolios": rows})

    data = request.get_json(force=True)
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    bankroll = data.get("bankroll", 1000) or 1000
    pid = db.execute(
        """INSERT INTO portfolios (name, bankroll, starting_bankroll,
           status, created_at)
           VALUES (?, ?, ?, 'active', ?)""",
        [data.get("name", "Default"), bankroll, bankroll, now],
    )
    return jsonify({"status": "ok", "portfolio_id": pid})


@app.route("/api/portfolios/<int:pid>/close", methods=["POST"])
@login_required
def api_close_portfolio(pid):
    """Close a portfolio."""
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    db.execute(
        "UPDATE portfolios SET status = 'closed', closed_at = ? WHERE id = ?",
        [now, pid],
    )
    return jsonify({"status": "ok"})


@app.route("/api/users", methods=["POST"])
@admin_required
def api_add_user():
    """Add a new user (admin only)."""
    data = request.get_json(force=True)
    username = data.get("username", "").strip()
    password = data.get("password", "").strip()
    role = data.get("role", "viewer")

    if not username or not password:
        return jsonify({"status": "error", "message": "Username and password required"}), 400

    users = _load_users()
    if username in users:
        return jsonify({"status": "error", "message": "User already exists"}), 409

    users[username] = {
        "password": _hash_password(password),
        "role": role,
        "email": data.get("email", ""),
        "created_at": datetime.utcnow().strftime("%Y-%m-%d"),
    }
    _save_users(users)
    return jsonify({"status": "ok", "username": username})


@app.route("/api/users/<username>", methods=["DELETE"])
@admin_required
def api_delete_user(username):
    """Remove a user (admin only)."""
    users = _load_users()
    if username not in users:
        return jsonify({"status": "error", "message": "User not found"}), 404
    if username == "admin":
        return jsonify({"status": "error", "message": "Cannot delete admin user"}), 403

    del users[username]
    _save_users(users)
    return jsonify({"status": "ok"})


@app.route("/api/users/<username>/password", methods=["PUT"])
@login_required
def api_change_password(username):
    """Change a user's password."""
    if username != session.get("username") and session.get("role") != "admin":
        return jsonify({"status": "error", "message": "Forbidden"}), 403

    data = request.get_json(force=True)
    new_password = data.get("password", "").strip()
    if not new_password:
        return jsonify({"status": "error", "message": "Password required"}), 400

    users = _load_users()
    if username not in users:
        return jsonify({"status": "error", "message": "User not found"}), 404

    users[username]["password"] = _hash_password(new_password)
    _save_users(users)
    return jsonify({"status": "ok"})


@app.route("/api/bankroll", methods=["PUT"])
@login_required
def api_update_bankroll():
    """Update portfolio bankroll."""
    data = request.get_json(force=True)
    pid = data.get("portfolio_id")
    new_bankroll = data.get("bankroll")

    if pid is None or new_bankroll is None:
        return jsonify({"status": "error", "message": "portfolio_id and bankroll required"}), 400

    db.execute(
        "UPDATE portfolios SET bankroll = ? WHERE id = ?",
        [new_bankroll, pid],
    )
    return jsonify({"status": "ok", "bankroll": new_bankroll})


@app.route("/api/weights", methods=["PUT"])
@admin_required
def api_update_weights():
    """Update ensemble model weights."""
    data = request.get_json(force=True)
    weights = data.get("weights")
    if not weights:
        return jsonify({"status": "error", "message": "weights required"}), 400

    weights_path = os.path.join(config.CACHE_DIR, "optimized_weights.json")
    os.makedirs(os.path.dirname(weights_path), exist_ok=True)
    with open(weights_path, "w") as fh:
        json.dump(weights, fh, indent=2)

    return jsonify({"status": "ok", "weights": weights})


@app.route("/api/weights/optimize", methods=["POST"])
@admin_required
def api_optimize_weights():
    """Auto-optimize model weights from historical data."""
    try:
        from models.ensemble import optimize_weights
        result = optimize_weights()
        if result:
            return jsonify({"status": "ok", "weights": result})
        return jsonify({"status": "error", "message": "Not enough data to optimize (need 20+ settled)"}), 400
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 500


@app.route("/api/export/dashboard", methods=["GET"])
@login_required
def api_export_dashboard():
    """Export all dashboard data as JSON."""
    league = _current_league()
    export = {
        "exported_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
        "league": league,
        "fixtures": db.fetch_all("SELECT * FROM fixtures WHERE league = ? ORDER BY match_date ASC", [league]),
        "predictions": db.fetch_all("SELECT * FROM predictions WHERE league = ? ORDER BY match_date ASC", [league]),
        "odds": db.fetch_all("SELECT * FROM odds WHERE league = ? ORDER BY match_date ASC", [league]),
        "value_bets": db.fetch_all("SELECT * FROM value_bets WHERE league = ? ORDER BY match_date ASC", [league]),
        "model_tracker": db.fetch_all("SELECT * FROM model_tracker WHERE league = ? ORDER BY match_date ASC", [league]),
        "sentiment": db.fetch_all("SELECT * FROM sentiment WHERE league = ? ORDER BY scored_at DESC", [league]),
        "live_matches": db.fetch_all("SELECT * FROM live_matches WHERE league = ? ORDER BY last_updated DESC", [league]),
        "api_usage": rate_limiter.get_usage_summary(),
    }
    return jsonify(export)


# ---------------------------------------------------------------------------
# DIAGNOSTIC ENDPOINT
# ---------------------------------------------------------------------------

@app.route("/api/diag")
def api_diag():
    """Diagnostic endpoint — no login required."""
    import database.db as _db
    league = _current_league()
    fixtures = db.fetch_all("SELECT id, status, match_date, team_a, team_b FROM fixtures WHERE league = ? LIMIT 10", [league])
    preds = db.fetch_all("SELECT match_date, team_a, team_b, team_a_win, team_b_win FROM predictions WHERE league = ? LIMIT 10", [league])
    matches = db.fetch_one("SELECT COUNT(*) as cnt FROM matches WHERE league = ?", [league])
    fix_count = db.fetch_one("SELECT COUNT(*) as cnt FROM fixtures WHERE league = ?", [league])
    pred_count = db.fetch_one("SELECT COUNT(*) as cnt FROM predictions WHERE league = ?", [league])
    ratings = db.fetch_all("SELECT team, elo FROM team_ratings WHERE league = ? ORDER BY elo DESC", [league])
    scheduled = db.fetch_all("SELECT COUNT(*) as cnt FROM fixtures WHERE status = 'SCHEDULED' AND league = ?", [league])
    return jsonify({
        "db_path": _db.DB_PATH,
        "db_exists": os.path.exists(_db.DB_PATH),
        "cwd": os.getcwd(),
        "matches": matches["cnt"] if matches else 0,
        "fixtures": fix_count["cnt"] if fix_count else 0,
        "predictions": pred_count["cnt"] if pred_count else 0,
        "scheduled": scheduled[0]["cnt"] if scheduled else 0,
        "sample_fixtures": [dict(f) for f in fixtures],
        "sample_predictions": [dict(p) for p in preds],
        "team_ratings": [dict(r) for r in ratings],
    })


# ---------------------------------------------------------------------------
# Error Handlers
# ---------------------------------------------------------------------------

@app.errorhandler(404)
def page_not_found(e):
    return render_template("base.html", error="404 — Page not found"), 404


@app.errorhandler(500)
def internal_error(e):
    return render_template("base.html", error="500 — Internal server error"), 500


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    db.init_db()
    app.run(debug=True, port=5000)
