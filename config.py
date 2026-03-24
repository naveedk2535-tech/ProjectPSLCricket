"""
PSL Cricket Prediction & Betting Analytics Engine
Configuration — All settings, API keys, rate limits, teams, venues
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ─── API Keys ───────────────────────────────────────────────────────────────
CRICKET_API_KEY = os.environ.get("CRICKET_API_KEY", "")
ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")
REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT = os.environ.get("REDDIT_USER_AGENT", "PSLCricketAnalytics/1.0")
NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY", "")
SMTP_EMAIL = os.environ.get("SMTP_EMAIL", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
FLASK_SECRET_KEY = os.environ.get("FLASK_SECRET_KEY", "psl_default_secret_change_me")

# ─── Project Paths ──────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "projectpslcricket.db")
CACHE_DIR = os.path.join(BASE_DIR, "data", "cache")
USERS_FILE = os.path.join(BASE_DIR, "users.json")

# ─── PSL 2026 Teams ────────────────────────────────────────────────────────
TEAMS = {
    "ISU": {
        "name": "Islamabad United",
        "short": "ISU",
        "color": "#E4002B",
        "group": "A",
        "is_new": False,
    },
    "KK": {
        "name": "Karachi Kings",
        "short": "KK",
        "color": "#0072CE",
        "group": "A",
        "is_new": False,
    },
    "LQ": {
        "name": "Lahore Qalandars",
        "short": "LQ",
        "color": "#00A651",
        "group": "B",
        "is_new": False,
    },
    "MS": {
        "name": "Multan Sultans",
        "short": "MS",
        "color": "#00843D",
        "group": "B",
        "is_new": False,
    },
    "PZ": {
        "name": "Peshawar Zalmi",
        "short": "PZ",
        "color": "#FFC72C",
        "group": "A",
        "is_new": False,
    },
    "QG": {
        "name": "Quetta Gladiators",
        "short": "QG",
        "color": "#6F2DA8",
        "group": "B",
        "is_new": False,
    },
    "HK": {
        "name": "Hyderabad Kingsmen",
        "short": "HK",
        "color": "#C8102E",
        "group": "A",
        "is_new": True,
    },
    "RP": {
        "name": "Rawalpindi Pindiz",
        "short": "RP",
        "color": "#003DA5",
        "group": "B",
        "is_new": True,
    },
}

TEAM_NAMES = [t["name"] for t in TEAMS.values()]

# ─── PSL 2026 Groups ───────────────────────────────────────────────────────
GROUPS = {
    "A": ["Islamabad United", "Karachi Kings", "Peshawar Zalmi", "Hyderabad Kingsmen"],
    "B": ["Lahore Qalandars", "Multan Sultans", "Quetta Gladiators", "Rawalpindi Pindiz"],
}

# ─── Venues ─────────────────────────────────────────────────────────────────
VENUES = {
    "Gaddafi Stadium": {
        "city": "Lahore",
        "capacity": 27000,
        "lat": 31.5204,
        "lon": 74.3587,
        "pace_friendly": False,
        "spin_friendly": True,
        "avg_first_innings": 170,
        "avg_second_innings": 158,
        "dew_factor": "high",
        "matches_in_psl2026": 15,
    },
    "National Stadium": {
        "city": "Karachi",
        "capacity": 34228,
        "lat": 24.8920,
        "lon": 67.0651,
        "pace_friendly": False,
        "spin_friendly": True,
        "avg_first_innings": 165,
        "avg_second_innings": 155,
        "dew_factor": "high",
        "matches_in_psl2026": 6,
    },
    "Rawalpindi Cricket Stadium": {
        "city": "Rawalpindi",
        "capacity": 15000,
        "lat": 33.5961,
        "lon": 73.0479,
        "pace_friendly": True,
        "spin_friendly": False,
        "avg_first_innings": 175,
        "avg_second_innings": 163,
        "dew_factor": "medium",
        "matches_in_psl2026": 11,
    },
    "Multan Cricket Stadium": {
        "city": "Multan",
        "capacity": 35000,
        "lat": 30.1984,
        "lon": 71.4687,
        "pace_friendly": False,
        "spin_friendly": True,
        "avg_first_innings": 168,
        "avg_second_innings": 160,
        "dew_factor": "high",
        "matches_in_psl2026": 4,
    },
    "Iqbal Stadium": {
        "city": "Faisalabad",
        "capacity": 18000,
        "lat": 31.4187,
        "lon": 73.0791,
        "pace_friendly": True,
        "spin_friendly": False,
        "avg_first_innings": 172,
        "avg_second_innings": 162,
        "dew_factor": "medium",
        "matches_in_psl2026": 7,
    },
    "Arbab Niaz Stadium": {
        "city": "Peshawar",
        "capacity": 16000,
        "lat": 34.0151,
        "lon": 71.5249,
        "pace_friendly": True,
        "spin_friendly": False,
        "avg_first_innings": 170,
        "avg_second_innings": 160,
        "dew_factor": "low",
        "matches_in_psl2026": 1,
    },
}

# ─── Rate Limits ────────────────────────────────────────────────────────────
RATE_LIMITS = {
    "cricket_api": {"calls": 8, "period_seconds": 86400},
    "odds_api": {"calls": 12, "period_seconds": 86400},
    "reddit": {"calls": 6, "period_seconds": 86400},
    "newsapi": {"calls": 6, "period_seconds": 86400},
    "open_meteo": {"calls": 50, "period_seconds": 86400},
    "cricsheet": {"calls": 2, "period_seconds": 86400},
}

# ─── Cache TTLs (seconds) ──────────────────────────────────────────────────
CACHE_TTL = {
    "fixtures": 43200,       # 12 hours
    "odds": 14400,           # 4 hours
    "sentiment": 86400,      # 24 hours
    "weather": 21600,        # 6 hours
    "historical": 172800,    # 48 hours
    "standings": 43200,      # 12 hours
    "live_score": 120,       # 2 minutes
    "player_stats": 86400,   # 24 hours
}

# ─── Model Weights (ensemble blending) ──────────────────────────────────────
MODEL_WEIGHTS = {
    "batting_bowling": 0.15,
    "elo": 0.20,
    "xgboost": 0.50,
    "sentiment": 0.15,
}

# ─── Value Bet Settings ────────────────────────────────────────────────────
VALUE_BET_SETTINGS = {
    "min_edge_percent": 5.0,
    "kelly_fraction": 0.25,
    "max_stake_percent": 5.0,
    "min_odds": 1.20,
    "max_odds": 15.0,
}

# ─── Elo Settings ──────────────────────────────────────────────────────────
ELO_SETTINGS = {
    "k_factor": 32,
    "k_factor_new_team": 48,
    "home_advantage": 30,
    "initial_elo": 1500,
    "new_team_elo": 1450,
    "season_decay": 0.90,
    "new_team_threshold": 5,
}

# ─── XGBoost Settings ──────────────────────────────────────────────────────
XGBOOST_SETTINGS = {
    "n_estimators": 200,
    "max_depth": 6,
    "learning_rate": 0.05,
    "min_child_weight": 3,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "early_stopping_rounds": 20,
    "cv_folds": 5,
}

# ─── Live Match Settings ───────────────────────────────────────────────────
LIVE_SETTINGS = {
    "poll_interval_seconds": 120,
    "max_poll_duration_hours": 8,
}

# ─── Dew Thresholds ────────────────────────────────────────────────────────
DEW_THRESHOLDS = {
    "heavy": {"dew_point_min": 15.0, "humidity_min": 70, "evening_hour": 18},
    "moderate": {"dew_point_min": 12.0, "humidity_min": 60, "evening_hour": 17},
    "batting_second_boost_heavy": 0.12,
    "batting_second_boost_moderate": 0.06,
}

# ─── Cricket API Settings ──────────────────────────────────────────────────
CRICKET_API_BASE = "https://api.cricapi.com/v1"
ODDS_API_BASE = "https://api.the-odds-api.com/v4"
ODDS_API_SPORT = "cricket_psl"
OPEN_METEO_BASE = "https://api.open-meteo.com/v1/forecast"
NEWSAPI_BASE = "https://newsapi.org/v2"
CRICSHEET_PSL_URL = "https://cricsheet.org/downloads/psl_male_csv2.zip"

# ─── Reddit Subreddits ─────────────────────────────────────────────────────
REDDIT_SUBREDDITS = ["Cricket", "PakCricket", "PSL"]

# ─── Season ─────────────────────────────────────────────────────────────────
CURRENT_SEASON = "2026"
PSL_SEASON_START = "2026-03-26"
PSL_SEASON_END = "2026-05-03"

# ─── Backtest Settings ──────────────────────────────────────────────────────
BACKTEST_SETTINGS = {
    "min_matches_to_train": 30,
    "calibration_bins": 10,
    "min_brier_improvement": 0.005,
    "random_brier_2way": 0.25,
}

# ─── Watchdog Thresholds ───────────────────────────────────────────────────
WATCHDOG_SETTINGS = {
    "min_accuracy": 0.55,
    "max_brier": 0.28,
    "max_db_size_mb": 100,
    "max_cache_size_mb": 50,
    "data_freshness_hours": {
        "fixtures": 24,
        "odds": 12,
        "sentiment": 48,
        "weather": 24,
        "historical": 168,
        "ratings": 168,
        "venue_stats": 336,
    },
    "model_max_age_days": 14,
}

# ─── Email Alert Settings ──────────────────────────────────────────────────
ALERT_SETTINGS = {
    "enabled": True,
    "recipients": [],
    "on_critical": True,
    "on_value_bet": False,
}

# ─── PythonAnywhere ────────────────────────────────────────────────────────
PYTHONANYWHERE_USER = "zziai40"
PYTHONANYWHERE_TOKEN = "d3da1f476b873b06d0005e146f08e21e93cca714"
PYTHONANYWHERE_HOST = "zziai40.pythonanywhere.com"
