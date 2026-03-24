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
            # Stored value is still plaintext
            if password == stored:
                # Upgrade to hashed
                users[username]["password"] = hashed_input
                _save_users(users)
                stored = hashed_input  # for the check below
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


# ---------------------------------------------------------------------------
# MAIN VIEW ROUTES
# ---------------------------------------------------------------------------

@app.route("/")
@login_required
def dashboard():
    """Command centre — upcoming fixtures, live matches, recent results, value bets."""
    upcoming = db.fetch_all(
        "SELECT * FROM fixtures WHERE status = 'SCHEDULED' ORDER BY match_date ASC LIMIT 7"
    )

    fixture_ids = [f["id"] for f in upcoming] if upcoming else []
    predictions = {}
    for fid in fixture_ids:
        pred = db.fetch_one("SELECT * FROM predictions WHERE fixture_id = ?", [fid])
        if pred:
            predictions[fid] = pred

    live_matches = db.fetch_all(
        "SELECT * FROM live_matches WHERE status = 'LIVE' ORDER BY updated_at DESC"
    )

    recent_results = db.fetch_all(
        "SELECT * FROM fixtures WHERE status = 'COMPLETED' ORDER BY match_date DESC LIMIT 5"
    )

    value_bets = db.fetch_all(
        "SELECT * FROM value_bets WHERE status = 'pending' ORDER BY edge_pct DESC LIMIT 10"
    )

    api_usage = rate_limiter.get_usage_summary()

    return render_template(
        "dashboard.html",
        upcoming=upcoming,
        predictions=predictions,
        live_matches=live_matches,
        recent_results=recent_results,
        value_bets=value_bets,
        api_usage=api_usage,
        now=datetime.utcnow(),
    )


@app.route("/match/<team_a>/<team_b>/<match_date>")
@login_required
def match_detail(team_a, team_b, match_date):
    """Detailed match analysis page."""
    team_a = standardise(team_a.replace("_", " "))
    team_b = standardise(team_b.replace("_", " "))

    # Prediction
    prediction = db.fetch_one(
        "SELECT * FROM predictions WHERE team_a = ? AND team_b = ? AND match_date = ?",
        [team_a, team_b, match_date],
    )
    if not prediction:
        try:
            from models import ensemble
            venue_row = db.fetch_one(
                "SELECT venue FROM fixtures WHERE team_a = ? AND team_b = ? AND match_date = ?",
                [team_a, team_b, match_date],
            )
            venue = venue_row["venue"] if venue_row else None
            pred_data = ensemble.predict(team_a, team_b, venue, match_date)
            if pred_data:
                ensemble.save_prediction(pred_data, team_a, team_b, match_date, venue)
                prediction = db.fetch_one(
                    "SELECT * FROM predictions WHERE team_a = ? AND team_b = ? AND match_date = ?",
                    [team_a, team_b, match_date],
                )
        except Exception:
            prediction = None

    # Odds
    odds = db.fetch_one(
        "SELECT * FROM odds WHERE team_a = ? AND team_b = ? AND match_date = ? ORDER BY updated_at DESC",
        [team_a, team_b, match_date],
    )

    # Head-to-head
    h2h = db.fetch_all(
        "SELECT * FROM head_to_head WHERE (team_a = ? AND team_b = ?) OR (team_a = ? AND team_b = ?) "
        "ORDER BY match_date DESC",
        [team_a, team_b, team_b, team_a],
    )

    # Recent form (last 5 for each team)
    form_a = db.fetch_all(
        "SELECT * FROM fixtures WHERE (team_a = ? OR team_b = ?) AND status = 'COMPLETED' "
        "ORDER BY match_date DESC LIMIT 5",
        [team_a, team_a],
    )
    form_b = db.fetch_all(
        "SELECT * FROM fixtures WHERE (team_a = ? OR team_b = ?) AND status = 'COMPLETED' "
        "ORDER BY match_date DESC LIMIT 5",
        [team_b, team_b],
    )

    # Venue stats
    fixture = db.fetch_one(
        "SELECT venue FROM fixtures WHERE team_a = ? AND team_b = ? AND match_date = ?",
        [team_a, team_b, match_date],
    )
    venue = fixture["venue"] if fixture else None
    venue_stats = None
    if venue:
        venue_stats = db.fetch_one("SELECT * FROM venue_stats WHERE venue = ?", [venue])

    # Weather
    weather = None
    if venue:
        weather = db.fetch_one(
            "SELECT * FROM weather WHERE venue = ? AND match_date = ?",
            [venue, match_date],
        )

    # Value bets for this match
    match_value_bets = db.fetch_all(
        "SELECT * FROM value_bets WHERE team_a = ? AND team_b = ? AND match_date = ?",
        [team_a, team_b, match_date],
    )

    # Calculate value on-the-fly if odds exist and prediction exists
    calculated_value = []
    if prediction and odds:
        try:
            from models.ensemble import calculate_value
            calculated_value = calculate_value(prediction, odds)
        except Exception:
            calculated_value = []

    return render_template(
        "match.html",
        team_a=team_a,
        team_b=team_b,
        match_date=match_date,
        prediction=prediction,
        odds=odds,
        h2h=h2h,
        form_a=form_a,
        form_b=form_b,
        venue=venue,
        venue_stats=venue_stats,
        weather=weather,
        value_bets=match_value_bets,
        calculated_value=calculated_value,
    )


@app.route("/live/<int:fixture_id>")
@login_required
def live_match(fixture_id):
    """Live in-play match view."""
    fixture = db.fetch_one("SELECT * FROM fixtures WHERE id = ?", [fixture_id])
    live_data = db.fetch_one(
        "SELECT * FROM live_matches WHERE fixture_id = ? ORDER BY updated_at DESC",
        [fixture_id],
    )
    return render_template("live.html", fixture=fixture, live=live_data)


@app.route("/tournament")
@login_required
def tournament():
    """PSL standings — points table and schedule."""
    completed = db.fetch_all("SELECT * FROM fixtures WHERE status = 'COMPLETED'")

    # Build points table
    table = {}
    for team in get_all_teams():
        table[team] = {
            "team": team,
            "P": 0, "W": 0, "L": 0, "NR": 0, "Pts": 0,
            "NRR": 0.0,
            "runs_scored": 0, "overs_faced": 0.0,
            "runs_conceded": 0, "overs_bowled": 0.0,
            "group": config.TEAMS.get(get_abbreviation(team), {}).get("group", ""),
        }

    for match in completed:
        ta = standardise(match.get("team_a", ""))
        tb = standardise(match.get("team_b", ""))
        winner = standardise(match.get("winner", ""))

        if ta not in table or tb not in table:
            continue

        table[ta]["P"] += 1
        table[tb]["P"] += 1

        if winner == ta:
            table[ta]["W"] += 1
            table[ta]["Pts"] += 2
            table[tb]["L"] += 1
        elif winner == tb:
            table[tb]["W"] += 1
            table[tb]["Pts"] += 2
            table[ta]["L"] += 1
        else:
            # No result / tie
            table[ta]["NR"] += 1
            table[tb]["NR"] += 1
            table[ta]["Pts"] += 1
            table[tb]["Pts"] += 1

        # NRR components
        for team_key, opp_key in [(ta, tb), (tb, ta)]:
            scored = match.get(f"score_{team_key}", 0) or 0
            ov_faced = match.get(f"overs_{team_key}", 0) or 0
            conceded = match.get(f"score_{opp_key}", 0) or 0
            ov_bowled = match.get(f"overs_{opp_key}", 0) or 0
            table[team_key]["runs_scored"] += scored
            table[team_key]["overs_faced"] += ov_faced
            table[team_key]["runs_conceded"] += conceded
            table[team_key]["overs_bowled"] += ov_bowled

    # Calculate NRR
    for team in table.values():
        rr_for = (team["runs_scored"] / team["overs_faced"]) if team["overs_faced"] > 0 else 0
        rr_against = (team["runs_conceded"] / team["overs_bowled"]) if team["overs_bowled"] > 0 else 0
        team["NRR"] = round(rr_for - rr_against, 3)

    # Split into groups
    group_a = sorted(
        [t for t in table.values() if t["group"] == "A"],
        key=lambda x: (-x["Pts"], -x["NRR"]),
    )
    group_b = sorted(
        [t for t in table.values() if t["group"] == "B"],
        key=lambda x: (-x["Pts"], -x["NRR"]),
    )

    schedule = db.fetch_all("SELECT * FROM fixtures ORDER BY match_date ASC")

    return render_template(
        "tournament.html",
        group_a=group_a,
        group_b=group_b,
        schedule=schedule,
    )


@app.route("/sentiment")
@login_required
def sentiment():
    """Sentiment dashboard for all teams."""
    teams = get_all_teams()
    team_sentiments = {}
    for team in teams:
        row = db.fetch_one(
            "SELECT * FROM sentiment WHERE team = ? ORDER BY fetched_at DESC LIMIT 1",
            [team],
        )
        team_sentiments[team] = row

    return render_template("sentiment.html", team_sentiments=team_sentiments, teams=teams)


@app.route("/performance")
@login_required
def performance():
    """Model performance metrics."""
    perf_data = db.fetch_all(
        "SELECT * FROM model_performance ORDER BY logged_at DESC LIMIT 100"
    )
    tracker_results = db.fetch_all(
        "SELECT * FROM model_tracker WHERE status = 'settled' ORDER BY match_date DESC LIMIT 50"
    )
    return render_template(
        "performance.html",
        performance=perf_data,
        tracker_results=tracker_results,
    )


@app.route("/portfolio")
@login_required
def portfolio():
    """Betting portfolio management."""
    portfolios = db.fetch_all(
        "SELECT * FROM portfolios WHERE status = 'active' ORDER BY created_at DESC"
    )
    user_bets = db.fetch_all(
        "SELECT * FROM user_bets WHERE username = ? ORDER BY created_at DESC",
        [session.get("username", "")],
    )

    # Calculate P&L per portfolio
    for p in portfolios:
        bets = db.fetch_all(
            "SELECT * FROM user_bets WHERE portfolio_id = ?",
            [p["id"]],
        )
        total_staked = sum(b.get("stake", 0) or 0 for b in bets)
        total_return = sum(b.get("payout", 0) or 0 for b in bets if b.get("status") == "settled")
        p["total_staked"] = round(total_staked, 2)
        p["total_return"] = round(total_return, 2)
        p["pnl"] = round(total_return - total_staked, 2)
        p["bets_count"] = len(bets)

    return render_template(
        "portfolio.html",
        portfolios=portfolios,
        user_bets=user_bets,
    )


@app.route("/tracker")
@login_required
def tracker():
    """Prediction tracker — accuracy, ROI, streaks."""
    pending = db.fetch_all(
        "SELECT * FROM model_tracker WHERE status = 'pending' ORDER BY match_date ASC"
    )
    settled = db.fetch_all(
        "SELECT * FROM model_tracker WHERE status = 'settled' ORDER BY match_date DESC"
    )

    # Calculate stats
    total_settled = len(settled)
    correct = sum(1 for s in settled if s.get("correct"))
    accuracy = (correct / total_settled * 100) if total_settled > 0 else 0

    # ROI (from value bets that were tracked)
    total_bet = sum(s.get("stake", 0) or 0 for s in settled)
    total_payout = sum(s.get("payout", 0) or 0 for s in settled)
    roi = ((total_payout - total_bet) / total_bet * 100) if total_bet > 0 else 0

    # Streak (consecutive correct/incorrect)
    streak = 0
    streak_type = ""
    for s in settled:
        if not streak_type:
            streak_type = "W" if s.get("correct") else "L"
            streak = 1
        elif (streak_type == "W" and s.get("correct")) or (streak_type == "L" and not s.get("correct")):
            streak += 1
        else:
            break

    # Top picks accuracy (high confidence)
    top_picks = [s for s in settled if (s.get("confidence") or 0) >= 0.65]
    top_correct = sum(1 for s in top_picks if s.get("correct"))
    top_accuracy = (top_correct / len(top_picks) * 100) if top_picks else 0

    # Value bet accuracy
    vb_settled = [s for s in settled if s.get("is_value_bet")]
    vb_correct = sum(1 for s in vb_settled if s.get("correct"))
    vb_accuracy = (vb_correct / len(vb_settled) * 100) if vb_settled else 0

    stats = {
        "total": total_settled,
        "correct": correct,
        "accuracy": round(accuracy, 1),
        "roi": round(roi, 1),
        "streak": streak,
        "streak_type": streak_type,
        "top_picks_accuracy": round(top_accuracy, 1),
        "top_picks_total": len(top_picks),
        "value_bet_accuracy": round(vb_accuracy, 1),
        "value_bet_total": len(vb_settled),
    }

    return render_template(
        "tracker.html",
        pending=pending,
        settled=settled,
        stats=stats,
    )


@app.route("/settings")
@admin_required
def settings():
    """Admin settings page."""
    users = _load_users()
    # Strip passwords for display
    users_display = {
        u: {"role": d["role"], "email": d.get("email", ""), "created_at": d.get("created_at", "")}
        for u, d in users.items()
    }

    # Current weights
    weights_path = os.path.join(config.CACHE_DIR, "optimized_weights.json")
    if os.path.exists(weights_path):
        with open(weights_path) as fh:
            current_weights = json.load(fh)
    else:
        current_weights = config.MODEL_WEIGHTS.copy()

    portfolios = db.fetch_all("SELECT * FROM portfolios ORDER BY created_at DESC")

    return render_template(
        "settings.html",
        users=users_display,
        current_weights=current_weights,
        portfolios=portfolios,
    )


@app.route("/watchdog")
@login_required
def watchdog():
    """System health monitoring dashboard."""
    checks = []
    try:
        from models.diagnosis import check_model_health
        checks = check_model_health()
    except Exception as exc:
        checks = [{"check": "import_error", "status": "error", "message": str(exc)}]

    # DB size check
    try:
        db_size_mb = os.path.getsize(config.DB_PATH) / (1024 * 1024) if os.path.exists(config.DB_PATH) else 0
        checks.append({
            "check": "database_size",
            "status": "ok" if db_size_mb < config.WATCHDOG_SETTINGS["max_db_size_mb"] else "warning",
            "message": f"Database size: {db_size_mb:.1f} MB",
        })
    except Exception:
        pass

    # Cache size
    try:
        cache_mb = rate_limiter.get_cache_size_mb()
        checks.append({
            "check": "cache_size",
            "status": "ok" if cache_mb < config.WATCHDOG_SETTINGS["max_cache_size_mb"] else "warning",
            "message": f"Cache size: {cache_mb:.1f} MB",
        })
    except Exception:
        pass

    # Data freshness checks
    freshness_tables = {
        "fixtures": "SELECT MAX(updated_at) as latest FROM fixtures",
        "odds": "SELECT MAX(updated_at) as latest FROM odds",
        "sentiment": "SELECT MAX(fetched_at) as latest FROM sentiment",
        "weather": "SELECT MAX(fetched_at) as latest FROM weather",
    }
    for table_name, sql in freshness_tables.items():
        try:
            row = db.fetch_one(sql)
            latest = row["latest"] if row and row.get("latest") else None
            threshold_hrs = config.WATCHDOG_SETTINGS["data_freshness_hours"].get(table_name, 24)
            if latest:
                age_hrs = (datetime.utcnow() - datetime.fromisoformat(latest)).total_seconds() / 3600
                status = "ok" if age_hrs < threshold_hrs else "warning"
                msg = f"{table_name}: last updated {age_hrs:.1f}h ago (threshold {threshold_hrs}h)"
            else:
                status = "warning"
                msg = f"{table_name}: no data found"
            checks.append({"check": f"freshness_{table_name}", "status": status, "message": msg})
        except Exception:
            checks.append({"check": f"freshness_{table_name}", "status": "error",
                           "message": f"Could not check {table_name}"})

    # API usage check
    api_usage = rate_limiter.get_usage_summary()

    return render_template("watchdog.html", checks=checks, api_usage=api_usage)


# ---------------------------------------------------------------------------
# API ENDPOINTS
# ---------------------------------------------------------------------------

@app.route("/api/predict/<team_a>/<team_b>", methods=["POST"])
@login_required
def api_predict(team_a, team_b):
    """Generate prediction on demand."""
    team_a = standardise(team_a.replace("_", " "))
    team_b = standardise(team_b.replace("_", " "))

    try:
        from models import ensemble

        venue = request.json.get("venue") if request.is_json else None
        match_date = request.json.get("match_date") if request.is_json else None

        # Look up fixture for venue/date if not provided
        if not venue or not match_date:
            fix = db.fetch_one(
                "SELECT venue, match_date FROM fixtures WHERE team_a = ? AND team_b = ? "
                "AND status = 'SCHEDULED' ORDER BY match_date ASC LIMIT 1",
                [team_a, team_b],
            )
            if fix:
                venue = venue or fix["venue"]
                match_date = match_date or fix["match_date"]

        pred = ensemble.predict(team_a, team_b, venue, match_date)
        if pred:
            fixture_row = db.fetch_one(
                "SELECT id FROM fixtures WHERE team_a = ? AND team_b = ? AND match_date = ?",
                [team_a, team_b, match_date],
            )
            fid = fixture_row["id"] if fixture_row else None
            ensemble.save_prediction(pred, team_a, team_b, match_date, venue, fid)
            return jsonify({"status": "ok", "prediction": pred})

        return jsonify({"status": "error", "message": "Could not generate prediction"}), 500
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 500


@app.route("/api/live/update", methods=["POST"])
@login_required
def api_live_update():
    """Manual score input for live match."""
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

        fixture = db.fetch_one("SELECT * FROM fixtures WHERE id = ?", [fixture_id])
        if not fixture:
            return jsonify({"status": "error", "message": "Fixture not found"}), 404

        team_a = fixture["team_a"]
        team_b = fixture["team_b"]
        venue = fixture.get("venue")

        result = calculate_live_probability(team_a, team_b, venue, innings, score, wickets, overs, target)

        # Upsert live_matches
        existing = db.fetch_one("SELECT id FROM live_matches WHERE fixture_id = ?", [fixture_id])
        now = db.now_iso()
        if existing:
            db.execute(
                """UPDATE live_matches SET innings = ?, score = ?, wickets = ?, overs = ?,
                   target = ?, team_a_win = ?, team_b_win = ?, projected_total = ?,
                   current_rate = ?, required_rate = ?, momentum = ?, key_insight = ?,
                   status = 'LIVE', updated_at = ?
                   WHERE fixture_id = ?""",
                [innings, score, wickets, overs, target,
                 result["team_a_win"], result["team_b_win"],
                 result.get("projected_total"), result.get("current_rate"),
                 result.get("required_rate"), result.get("momentum"),
                 result.get("key_insight"), now, fixture_id],
            )
        else:
            db.execute(
                """INSERT INTO live_matches (fixture_id, team_a, team_b, venue,
                   innings, score, wickets, overs, target,
                   team_a_win, team_b_win, projected_total,
                   current_rate, required_rate, momentum, key_insight,
                   status, auto_update, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'LIVE', 0, ?, ?)""",
                [fixture_id, team_a, team_b, venue,
                 innings, score, wickets, overs, target,
                 result["team_a_win"], result["team_b_win"],
                 result.get("projected_total"), result.get("current_rate"),
                 result.get("required_rate"), result.get("momentum"),
                 result.get("key_insight"), now, now],
            )

        return jsonify({"status": "ok", "probability": result})
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 500


@app.route("/api/live/toggle-auto/<int:fixture_id>", methods=["POST"])
@login_required
def api_live_toggle_auto(fixture_id):
    """Toggle auto-update flag for a live match."""
    existing = db.fetch_one("SELECT auto_update FROM live_matches WHERE fixture_id = ?", [fixture_id])
    if not existing:
        return jsonify({"status": "error", "message": "Live match not found"}), 404

    new_val = 0 if existing["auto_update"] else 1
    db.execute(
        "UPDATE live_matches SET auto_update = ?, updated_at = ? WHERE fixture_id = ?",
        [new_val, db.now_iso(), fixture_id],
    )
    return jsonify({"status": "ok", "auto_update": bool(new_val)})


@app.route("/api/live/whatif", methods=["POST"])
@login_required
def api_live_whatif():
    """What-if scenario calculator."""
    data = request.get_json(force=True)
    try:
        from models.live_predictor import what_if

        result = what_if(
            team_a=data.get("team_a", ""),
            team_b=data.get("team_b", ""),
            venue=data.get("venue"),
            innings=data.get("innings", 1),
            score=data.get("score", 0),
            wickets=data.get("wickets", 0),
            overs=data.get("overs", 0),
            target=data.get("target"),
            scenario=data.get("scenario", ""),
        )
        if result is None:
            return jsonify({"status": "error", "message": "Invalid scenario"}), 400
        return jsonify({"status": "ok", "result": result})
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 500


@app.route("/api/refresh-data", methods=["POST"])
@admin_required
def api_refresh_data():
    """Trigger data refresh — fixtures, odds, weather, predictions."""
    results = {}
    try:
        from data.cricket_api import fetch_fixtures
        fetch_fixtures()
        results["fixtures"] = "ok"
    except Exception as exc:
        results["fixtures"] = str(exc)

    try:
        from data.odds_api import fetch_odds
        fetch_odds()
        results["odds"] = "ok"
    except Exception as exc:
        results["odds"] = str(exc)

    try:
        from data.weather_api import fetch_weather
        fetch_weather()
        results["weather"] = "ok"
    except Exception as exc:
        results["weather"] = str(exc)

    try:
        from models.ensemble import predict as ens_predict, save_prediction
        upcoming = db.fetch_all(
            "SELECT * FROM fixtures WHERE status = 'SCHEDULED' ORDER BY match_date ASC LIMIT 10"
        )
        pred_count = 0
        for fix in upcoming:
            try:
                pred = ens_predict(fix["team_a"], fix["team_b"], fix.get("venue"), fix.get("match_date"))
                if pred:
                    save_prediction(pred, fix["team_a"], fix["team_b"], fix["match_date"],
                                    fix.get("venue"), fix["id"])
                    pred_count += 1
            except Exception:
                pass
        results["predictions"] = f"{pred_count} generated"
    except Exception as exc:
        results["predictions"] = str(exc)

    return jsonify({"status": "ok", "results": results})


@app.route("/api/tracker/generate", methods=["POST"])
@login_required
def api_tracker_generate():
    """Create tracker entries for upcoming matches with predictions."""
    upcoming = db.fetch_all(
        "SELECT f.*, p.team_a_win, p.team_b_win, p.confidence, p.model_details "
        "FROM fixtures f "
        "JOIN predictions p ON f.id = p.fixture_id "
        "WHERE f.status = 'SCHEDULED'"
    )

    count = 0
    now = db.now_iso()
    for fix in upcoming:
        team_a_win = fix.get("team_a_win", 0.5)
        team_b_win = fix.get("team_b_win", 0.5)
        predicted_winner = fix["team_a"] if team_a_win >= team_b_win else fix["team_b"]
        predicted_prob = max(team_a_win, team_b_win)
        confidence = fix.get("confidence", 0.5)

        # Check for value bet
        vb = db.fetch_one(
            "SELECT * FROM value_bets WHERE team_a = ? AND team_b = ? AND match_date = ? AND status = 'pending' LIMIT 1",
            [fix["team_a"], fix["team_b"], fix["match_date"]],
        )
        is_value_bet = 1 if vb else 0
        edge_pct = vb["edge_pct"] if vb else 0
        stake = vb.get("kelly_stake", 0) if vb else 0
        odds = vb.get("best_odds", 0) if vb else 0

        try:
            db.execute(
                """INSERT OR IGNORE INTO model_tracker
                   (fixture_id, match_date, team_a, team_b, venue,
                    predicted_winner, predicted_prob, confidence,
                    is_value_bet, edge_pct, stake, odds,
                    model_details, status, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)""",
                [fix["id"], fix["match_date"], fix["team_a"], fix["team_b"],
                 fix.get("venue"), predicted_winner, predicted_prob, confidence,
                 is_value_bet, edge_pct, stake, odds,
                 fix.get("model_details", "{}"), now],
            )
            count += 1
        except Exception:
            pass

    return jsonify({"status": "ok", "generated": count})


@app.route("/api/tracker/settle", methods=["POST"])
@login_required
def api_tracker_settle():
    """Settle completed matches in the tracker."""
    pending = db.fetch_all(
        "SELECT t.*, f.winner, f.status as fixture_status "
        "FROM model_tracker t "
        "JOIN fixtures f ON t.fixture_id = f.id "
        "WHERE t.status = 'pending' AND f.status = 'COMPLETED'"
    )

    settled_count = 0
    now = db.now_iso()
    for entry in pending:
        actual_winner = standardise(entry.get("winner", "") or "")
        if not actual_winner:
            continue

        predicted_winner = entry.get("predicted_winner", "")
        correct = 1 if standardise(predicted_winner) == actual_winner else 0

        # P&L calculation
        pnl = 0.0
        if entry.get("is_value_bet") and entry.get("stake", 0) > 0:
            if correct and entry.get("odds", 0) > 0:
                pnl = entry["stake"] * (entry["odds"] - 1)
            else:
                pnl = -entry.get("stake", 0)

        db.execute(
            """UPDATE model_tracker SET
               actual_winner = ?, correct = ?, pnl = ?,
               status = 'settled', settled_at = ?
               WHERE id = ?""",
            [actual_winner, correct, round(pnl, 2), now, entry["id"]],
        )
        settled_count += 1

    return jsonify({"status": "ok", "settled": settled_count})


@app.route("/api/bets", methods=["POST"])
@login_required
def api_place_bet():
    """Record a user bet."""
    data = request.get_json(force=True)
    now = db.now_iso()
    try:
        bet_id = db.execute(
            """INSERT INTO user_bets
               (username, portfolio_id, fixture_id, match_date, team_a, team_b,
                bet_type, selection, odds, stake, model_prob, edge_pct,
                status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)""",
            [session["username"], data.get("portfolio_id"), data.get("fixture_id"),
             data.get("match_date"), data.get("team_a"), data.get("team_b"),
             data.get("bet_type", "match_winner"), data.get("selection"),
             data.get("odds", 0), data.get("stake", 0),
             data.get("model_prob", 0), data.get("edge_pct", 0), now],
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

    if outcome == "won":
        payout = bet["stake"] * bet["odds"]
        pnl = payout - bet["stake"]
    else:
        payout = 0
        pnl = -bet["stake"]

    db.execute(
        """UPDATE user_bets SET status = 'settled', outcome = ?, payout = ?, pnl = ?,
           settled_at = ? WHERE id = ?""",
        [outcome, round(payout, 2), round(pnl, 2), db.now_iso(), bet_id],
    )
    return jsonify({"status": "ok", "pnl": round(pnl, 2), "payout": round(payout, 2)})


@app.route("/api/portfolios", methods=["GET", "POST"])
@login_required
def api_portfolios():
    """List or create betting portfolios."""
    if request.method == "GET":
        rows = db.fetch_all(
            "SELECT * FROM portfolios WHERE username = ? ORDER BY created_at DESC",
            [session["username"]],
        )
        return jsonify({"status": "ok", "portfolios": rows})

    data = request.get_json(force=True)
    now = db.now_iso()
    pid = db.execute(
        """INSERT INTO portfolios (username, name, bankroll, initial_bankroll,
           strategy, status, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, 'active', ?, ?)""",
        [session["username"], data.get("name", "Default"),
         data.get("bankroll", 1000), data.get("bankroll", 1000),
         data.get("strategy", "kelly"), now, now],
    )
    return jsonify({"status": "ok", "portfolio_id": pid})


@app.route("/api/portfolios/<int:pid>/close", methods=["POST"])
@login_required
def api_close_portfolio(pid):
    """Close a portfolio."""
    db.execute(
        "UPDATE portfolios SET status = 'closed', updated_at = ? WHERE id = ?",
        [db.now_iso(), pid],
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
    # Only admin can change other users' passwords
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
        "UPDATE portfolios SET bankroll = ?, updated_at = ? WHERE id = ?",
        [new_bankroll, db.now_iso(), pid],
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
    export = {
        "exported_at": db.now_iso(),
        "fixtures": db.fetch_all("SELECT * FROM fixtures ORDER BY match_date ASC"),
        "predictions": db.fetch_all("SELECT * FROM predictions ORDER BY match_date ASC"),
        "odds": db.fetch_all("SELECT * FROM odds ORDER BY match_date ASC"),
        "value_bets": db.fetch_all("SELECT * FROM value_bets ORDER BY match_date ASC"),
        "model_tracker": db.fetch_all("SELECT * FROM model_tracker ORDER BY match_date ASC"),
        "sentiment": db.fetch_all("SELECT * FROM sentiment ORDER BY fetched_at DESC"),
        "live_matches": db.fetch_all("SELECT * FROM live_matches ORDER BY updated_at DESC"),
        "api_usage": rate_limiter.get_usage_summary(),
    }
    return jsonify(export)


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
