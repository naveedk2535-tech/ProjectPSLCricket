# Shothai — PSL Cricket Intelligence Engine

## What This Is
A full-stack PSL (Pakistan Super League) T20 cricket prediction and betting analytics engine.
Built with Flask + SQLite + ML ensemble. Deployed on PythonAnywhere (zziai40).

## Architecture
- **Backend:** Python 3.11+ / Flask 3.1.0
- **Database:** SQLite (projectpslcricket.db) with WAL mode
- **Frontend:** Tailwind CSS (CDN) + Alpine.js + Chart.js — no build step
- **ML:** XGBoost, scikit-learn, scipy, pandas, numpy
- **Sentiment:** PRAW (Reddit) + NewsAPI + VADER
- **Deployment:** PythonAnywhere (zziai40)

## Key Patterns (MUST follow)
1. **Every API call goes through data/rate_limiter.py** — check can_call() before calling, record_call() after
2. **Every team name goes through data/team_names.py standardise()** — before ANY database write
3. **Every venue goes through standardise_venue()** — same reason
4. **Upsert pattern** — ON CONFLICT DO UPDATE for all data ingestion (idempotent, safe to re-run)
5. **Cache everything** — API responses saved as JSON in data/cache/, fallback to cache on API failure
6. **Every model exposes same interface:** predict(team_a, team_b, venue, match_date) → {team_a_win, team_b_win, confidence, details}
7. **Tracker uses INSERT OR IGNORE** — prediction snapshots must NOT be overwritten once created
8. **Graceful degradation** — if one model fails, ensemble continues with remaining models

## PSL 2026 Teams (8 teams, 2 groups)
- Group A: Islamabad United (ISU), Karachi Kings (KK), Peshawar Zalmi (PZ), Hyderabad Kingsmen (HK) [NEW]
- Group B: Lahore Qalandars (LQ), Multan Sultans (MS), Quetta Gladiators (QG), Rawalpindi Pindiz (RP) [NEW]

## Models
1. **batting_bowling.py** — Batting/bowling strength model (replaces Poisson for cricket)
2. **elo.py** — Elo rating system (K=32, K=48 for new teams)
3. **xgboost_model.py** — 50+ feature ML classifier
4. **sentiment.py** — Reddit/News sentiment (±5% adjustment)
5. **ensemble.py** — Weighted blend + stacking meta-model
6. **over_under.py** — Total runs + prop bets (wides, no-balls, sixes, fours)
7. **live_predictor.py** — In-play probability calculator
8. **diagnosis.py** — Model self-monitoring

## Data Sources
- CricSheet.org (historical ball-by-ball CSV, free)
- CricAPI (live scores/fixtures, 100/day free)
- The Odds API (bookmaker odds, 500/month free)
- Open-Meteo (weather/dew, free, no key)
- Reddit PRAW + NewsAPI (sentiment)

## Deployment
- PythonAnywhere account: zziai40
- Home: /home/zziai40/ProjectPSLCricket
- WSGI: wsgi.py
- URL: https://zziai40.pythonanywhere.com

## Commands
- `python scheduler.py --task daily` — daily refresh (fixtures, odds, weather, predictions)
- `python scheduler.py --task weekly` — weekly rebuild (historical, ratings, retrain)
- `python scheduler.py --task live` — live match polling (every 2 min)
- `python backtest.py` — run backtesting framework
- `python watchdog.py` — run 28-point health check

## Default Login
- Username: admin
- Password: admin123
