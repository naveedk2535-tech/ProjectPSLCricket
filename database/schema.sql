-- PSL Cricket Prediction Engine — Database Schema
-- All tables for the cricket analytics platform

PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- ─── Historical Match Results ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS matches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    league TEXT DEFAULT 'psl',
    season TEXT NOT NULL,
    match_date TEXT NOT NULL,
    venue TEXT,
    team_a TEXT NOT NULL,
    team_b TEXT NOT NULL,
    toss_winner TEXT,
    toss_decision TEXT,  -- 'bat' or 'field'
    innings1_runs INTEGER,
    innings1_wickets INTEGER,
    innings1_overs REAL,
    innings2_runs INTEGER,
    innings2_wickets INTEGER,
    innings2_overs REAL,
    winner TEXT,
    win_margin INTEGER,
    win_type TEXT,  -- 'runs', 'wickets', 'super_over', 'no_result'
    player_of_match TEXT,
    powerplay_runs_a INTEGER DEFAULT 0,
    powerplay_wickets_a INTEGER DEFAULT 0,
    powerplay_runs_b INTEGER DEFAULT 0,
    powerplay_wickets_b INTEGER DEFAULT 0,
    middle_runs_a INTEGER DEFAULT 0,
    middle_wickets_a INTEGER DEFAULT 0,
    middle_runs_b INTEGER DEFAULT 0,
    middle_wickets_b INTEGER DEFAULT 0,
    death_runs_a INTEGER DEFAULT 0,
    death_wickets_a INTEGER DEFAULT 0,
    death_runs_b INTEGER DEFAULT 0,
    death_wickets_b INTEGER DEFAULT 0,
    total_fours_a INTEGER DEFAULT 0,
    total_sixes_a INTEGER DEFAULT 0,
    total_fours_b INTEGER DEFAULT 0,
    total_sixes_b INTEGER DEFAULT 0,
    total_wides_a INTEGER DEFAULT 0,
    total_noballs_a INTEGER DEFAULT 0,
    total_wides_b INTEGER DEFAULT 0,
    total_noballs_b INTEGER DEFAULT 0,
    total_extras_a INTEGER DEFAULT 0,
    total_extras_b INTEGER DEFAULT 0,
    dot_balls_a INTEGER DEFAULT 0,
    dot_balls_b INTEGER DEFAULT 0,
    UNIQUE(season, match_date, team_a, team_b)
);

-- ─── Upcoming Fixtures ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fixtures (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    league TEXT DEFAULT 'psl',
    season TEXT NOT NULL DEFAULT '2026',
    match_date TEXT NOT NULL,
    match_time TEXT,
    venue TEXT,
    team_a TEXT NOT NULL,
    team_b TEXT NOT NULL,
    match_number INTEGER,
    stage TEXT DEFAULT 'group',  -- 'group', 'qualifier', 'eliminator', 'final'
    group_name TEXT,
    status TEXT DEFAULT 'SCHEDULED',  -- 'SCHEDULED', 'LIVE', 'COMPLETED', 'ABANDONED'
    result TEXT,
    cricapi_id TEXT,
    updated_at TEXT,
    UNIQUE(season, match_date, team_a, team_b)
);

-- ─── Model Predictions ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS predictions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    league TEXT DEFAULT 'psl',
    fixture_id INTEGER,
    match_date TEXT NOT NULL,
    team_a TEXT NOT NULL,
    team_b TEXT NOT NULL,
    venue TEXT,
    team_a_win REAL,
    team_b_win REAL,
    predicted_total_a REAL,
    predicted_total_b REAL,
    over_under_line REAL,
    over_prob REAL,
    under_prob REAL,
    total_wides_pred REAL,
    total_noballs_pred REAL,
    total_sixes_pred REAL,
    total_fours_pred REAL,
    confidence REAL,
    model_details TEXT,  -- JSON blob with per-model breakdown
    toss_advantage TEXT,
    dew_factor REAL,
    venue_bias TEXT,
    created_at TEXT,
    updated_at TEXT,
    UNIQUE(match_date, team_a, team_b)
);

-- ─── Bookmaker Odds ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS odds (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    league TEXT DEFAULT 'psl',
    fixture_id INTEGER,
    match_date TEXT NOT NULL,
    team_a TEXT NOT NULL,
    team_b TEXT NOT NULL,
    team_a_odds REAL,
    team_b_odds REAL,
    over_under_line REAL,
    over_odds REAL,
    under_odds REAL,
    bookmaker TEXT DEFAULT 'best',
    implied_prob_a REAL,
    implied_prob_b REAL,
    margin REAL,
    fetched_at TEXT,
    UNIQUE(match_date, team_a, team_b, bookmaker)
);

-- ─── Value Bets ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS value_bets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    league TEXT DEFAULT 'psl',
    fixture_id INTEGER,
    match_date TEXT NOT NULL,
    team_a TEXT NOT NULL,
    team_b TEXT NOT NULL,
    bet_type TEXT NOT NULL,  -- 'team_a_win', 'team_b_win', 'over', 'under'
    model_prob REAL,
    implied_prob REAL,
    edge_pct REAL,
    kelly_stake REAL,
    best_odds REAL,
    bookmaker TEXT,
    status TEXT DEFAULT 'pending',  -- 'pending', 'won', 'lost', 'void'
    settled_at TEXT,
    pnl REAL,
    created_at TEXT,
    UNIQUE(match_date, team_a, team_b, bet_type)
);

-- ─── Team Ratings & Strengths ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS team_ratings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    league TEXT DEFAULT 'psl',
    team TEXT NOT NULL,
    elo REAL DEFAULT 1500,
    elo_home REAL DEFAULT 1500,
    elo_away REAL DEFAULT 1500,
    batting_avg REAL,
    bowling_avg REAL,
    batting_sr REAL,
    bowling_economy REAL,
    powerplay_run_rate REAL,
    powerplay_wicket_rate REAL,
    middle_run_rate REAL,
    death_overs_economy REAL,
    death_overs_run_rate REAL,
    form_last5 REAL DEFAULT 0,
    form_last10 REAL DEFAULT 0,
    streak_type TEXT DEFAULT 'N',  -- 'W', 'L', 'N'
    streak_length INTEGER DEFAULT 0,
    nrr REAL DEFAULT 0.0,
    matches_played INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    no_results INTEGER DEFAULT 0,
    boundary_pct REAL,
    dot_ball_pct REAL,
    extras_conceded_avg REAL,
    collapse_rate REAL,  -- how often 3+ wickets fall in 3 overs
    updated_at TEXT,
    UNIQUE(team, league)
);

-- ─── Venue Statistics ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS venue_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    league TEXT DEFAULT 'psl',
    venue TEXT NOT NULL,
    city TEXT,
    matches_played INTEGER DEFAULT 0,
    avg_first_innings REAL,
    avg_second_innings REAL,
    chase_win_pct REAL,
    toss_bat_first_pct REAL,
    avg_wides REAL,
    avg_noballs REAL,
    avg_sixes REAL,
    avg_fours REAL,
    avg_extras REAL,
    pace_wicket_pct REAL,
    spin_wicket_pct REAL,
    highest_total INTEGER,
    lowest_total INTEGER,
    avg_powerplay_score REAL,
    avg_death_score REAL,
    day_avg_score REAL,
    night_avg_score REAL,
    dew_impact_score REAL,
    updated_at TEXT,
    UNIQUE(venue, league)
);

-- ─── Player Statistics ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS player_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    league TEXT DEFAULT 'psl',
    name TEXT NOT NULL,
    team TEXT NOT NULL,
    role TEXT,  -- 'batsman', 'bowler', 'all-rounder', 'wicket-keeper'
    batting_avg REAL,
    batting_sr REAL,
    bowling_avg REAL,
    bowling_economy REAL,
    bowling_sr REAL,
    catches INTEGER DEFAULT 0,
    stumpings INTEGER DEFAULT 0,
    matches_played INTEGER DEFAULT 0,
    innings_batted INTEGER DEFAULT 0,
    innings_bowled INTEGER DEFAULT 0,
    runs_scored INTEGER DEFAULT 0,
    wickets_taken INTEGER DEFAULT 0,
    fifties INTEGER DEFAULT 0,
    hundreds INTEGER DEFAULT 0,
    three_wicket_hauls INTEGER DEFAULT 0,
    powerplay_sr REAL,
    death_sr REAL,
    powerplay_economy REAL,
    death_economy REAL,
    dot_ball_pct REAL,
    boundary_pct REAL,
    availability TEXT DEFAULT 'available',
    impact_score REAL,
    updated_at TEXT,
    UNIQUE(name, team, league)
);

-- ─── Head-to-Head Records ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS head_to_head (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    league TEXT DEFAULT 'psl',
    team_a TEXT NOT NULL,
    team_b TEXT NOT NULL,
    matches_played INTEGER DEFAULT 0,
    team_a_wins INTEGER DEFAULT 0,
    team_b_wins INTEGER DEFAULT 0,
    no_results INTEGER DEFAULT 0,
    avg_total_a REAL,
    avg_total_b REAL,
    team_a_bat_first_wins INTEGER DEFAULT 0,
    team_b_bat_first_wins INTEGER DEFAULT 0,
    last_winner TEXT,
    last_match_date TEXT,
    venue_breakdown TEXT,  -- JSON
    updated_at TEXT,
    UNIQUE(team_a, team_b, league)
);

-- ─── Sentiment Scores ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sentiment (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    league TEXT DEFAULT 'psl',
    team TEXT NOT NULL,
    source TEXT NOT NULL,  -- 'reddit', 'news', 'combined'
    score REAL DEFAULT 0.0,
    trend REAL DEFAULT 0.0,
    volume INTEGER DEFAULT 0,
    positive_pct REAL DEFAULT 0.0,
    negative_pct REAL DEFAULT 0.0,
    neutral_pct REAL DEFAULT 0.0,
    keywords TEXT,
    signal TEXT,  -- 'bullish', 'bearish', 'neutral'
    scored_at TEXT NOT NULL,
    UNIQUE(team, source, scored_at)
);

-- ─── Weather Data ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS weather (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    league TEXT DEFAULT 'psl',
    venue TEXT NOT NULL,
    match_date TEXT NOT NULL,
    temperature REAL,
    humidity REAL,
    dew_point REAL,
    wind_speed REAL,
    precipitation REAL,
    cloud_cover REAL,
    heavy_dew INTEGER DEFAULT 0,
    dew_score REAL DEFAULT 0.0,
    weather_summary TEXT,
    fetched_at TEXT,
    UNIQUE(venue, match_date)
);

-- ─── Model Tracker (automated prediction tracking) ─────────────────────────
CREATE TABLE IF NOT EXISTS model_tracker (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    league TEXT DEFAULT 'psl',
    match_date TEXT NOT NULL,
    team_a TEXT NOT NULL,
    team_b TEXT NOT NULL,
    venue TEXT,
    predicted_winner TEXT,
    team_a_prob REAL,
    team_b_prob REAL,
    predicted_total REAL,
    confidence REAL,
    is_value_bet INTEGER DEFAULT 0,
    value_bet_type TEXT,
    value_edge REAL,
    value_odds REAL,
    actual_winner TEXT,
    actual_total_a INTEGER,
    actual_total_b INTEGER,
    top_pick_correct INTEGER,
    top_pick_pnl REAL,
    value_bet_correct INTEGER,
    value_bet_pnl REAL,
    status TEXT DEFAULT 'pending',  -- 'pending', 'settled'
    created_at TEXT,
    settled_at TEXT,
    UNIQUE(match_date, team_a, team_b)
);

-- ─── Model Performance Metrics ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS model_performance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    league TEXT DEFAULT 'psl',
    model_name TEXT NOT NULL,
    period TEXT NOT NULL,  -- '2026-03', 'overall', 'last_10'
    accuracy REAL,
    brier_score REAL,
    log_loss REAL,
    roi REAL,
    total_predictions INTEGER DEFAULT 0,
    correct_predictions INTEGER DEFAULT 0,
    avg_confidence REAL,
    evaluated_at TEXT,
    UNIQUE(model_name, period)
);

-- ─── User Bets ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS user_bets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_id INTEGER,
    match_date TEXT,
    team_a TEXT,
    team_b TEXT,
    bet_type TEXT,  -- 'match_winner', 'over_under', 'total_sixes', etc.
    selection TEXT,  -- specific selection (team name, 'over', 'under', etc.)
    stake REAL,
    odds REAL,
    potential_pnl REAL,
    status TEXT DEFAULT 'pending',  -- 'pending', 'won', 'lost', 'void'
    actual_pnl REAL,
    settled_at TEXT,
    notes TEXT,
    created_at TEXT
);

-- ─── Betting Portfolios ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS portfolios (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    bankroll REAL NOT NULL,
    starting_bankroll REAL NOT NULL,
    status TEXT DEFAULT 'active',  -- 'active', 'closed'
    created_at TEXT,
    closed_at TEXT
);

-- ─── Live Match Tracking ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS live_matches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    league TEXT DEFAULT 'psl',
    fixture_id INTEGER NOT NULL,
    match_date TEXT,
    team_a TEXT NOT NULL,
    team_b TEXT NOT NULL,
    venue TEXT,
    current_batting TEXT,
    current_score INTEGER DEFAULT 0,
    current_wickets INTEGER DEFAULT 0,
    current_overs REAL DEFAULT 0.0,
    current_run_rate REAL DEFAULT 0.0,
    target INTEGER,
    projected_total REAL,
    required_rate REAL,
    innings INTEGER DEFAULT 1,
    live_win_prob_a REAL DEFAULT 0.5,
    live_win_prob_b REAL DEFAULT 0.5,
    last_updated TEXT,
    auto_update INTEGER DEFAULT 1,
    key_moments TEXT,  -- JSON array
    prop_wides INTEGER DEFAULT 0,
    prop_noballs INTEGER DEFAULT 0,
    prop_sixes INTEGER DEFAULT 0,
    prop_fours INTEGER DEFAULT 0,
    UNIQUE(fixture_id)
);

-- ─── API Call Log (Rate Limiting) ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS api_calls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    api_name TEXT NOT NULL,
    endpoint TEXT,
    called_at TEXT NOT NULL,
    response_code INTEGER,
    cached INTEGER DEFAULT 0
);

-- ─── Backtest Results ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS backtest_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    model_name TEXT NOT NULL,
    run_date TEXT NOT NULL,
    start_date TEXT,
    end_date TEXT,
    total_matches INTEGER,
    accuracy REAL,
    brier_score REAL,
    log_loss REAL,
    roi REAL,
    sharpe_ratio REAL,
    max_drawdown REAL,
    calibration_data TEXT,  -- JSON
    per_venue_accuracy TEXT,  -- JSON
    toss_impact TEXT,  -- JSON
    dew_impact TEXT,  -- JSON
    details TEXT,  -- JSON full results
    UNIQUE(model_name, run_date)
);

-- ─── Indexes ───────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_matches_teams ON matches(team_a, team_b);
CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(match_date);
CREATE INDEX IF NOT EXISTS idx_matches_season ON matches(season);
CREATE INDEX IF NOT EXISTS idx_matches_venue ON matches(venue);
CREATE INDEX IF NOT EXISTS idx_matches_winner ON matches(winner);
CREATE INDEX IF NOT EXISTS idx_matches_league ON matches(league);

CREATE INDEX IF NOT EXISTS idx_fixtures_date ON fixtures(match_date);
CREATE INDEX IF NOT EXISTS idx_fixtures_status ON fixtures(status);
CREATE INDEX IF NOT EXISTS idx_fixtures_teams ON fixtures(team_a, team_b);
CREATE INDEX IF NOT EXISTS idx_fixtures_league ON fixtures(league);

CREATE INDEX IF NOT EXISTS idx_predictions_date ON predictions(match_date);
CREATE INDEX IF NOT EXISTS idx_predictions_fixture ON predictions(fixture_id);

CREATE INDEX IF NOT EXISTS idx_odds_date ON odds(match_date);
CREATE INDEX IF NOT EXISTS idx_odds_fixture ON odds(fixture_id);

CREATE INDEX IF NOT EXISTS idx_value_bets_date ON value_bets(match_date);
CREATE INDEX IF NOT EXISTS idx_value_bets_status ON value_bets(status);

CREATE INDEX IF NOT EXISTS idx_sentiment_team ON sentiment(team);
CREATE INDEX IF NOT EXISTS idx_sentiment_date ON sentiment(scored_at);

CREATE INDEX IF NOT EXISTS idx_weather_venue ON weather(venue, match_date);

CREATE INDEX IF NOT EXISTS idx_tracker_date ON model_tracker(match_date);
CREATE INDEX IF NOT EXISTS idx_tracker_status ON model_tracker(status);

CREATE INDEX IF NOT EXISTS idx_api_calls_name ON api_calls(api_name, called_at);

CREATE INDEX IF NOT EXISTS idx_player_team ON player_stats(team);
CREATE INDEX IF NOT EXISTS idx_player_name ON player_stats(name);
CREATE INDEX IF NOT EXISTS idx_player_stats_league ON player_stats(league);

CREATE INDEX IF NOT EXISTS idx_live_fixture ON live_matches(fixture_id);

CREATE INDEX IF NOT EXISTS idx_user_bets_portfolio ON user_bets(portfolio_id);
CREATE INDEX IF NOT EXISTS idx_user_bets_status ON user_bets(status);

CREATE INDEX IF NOT EXISTS idx_team_ratings_league ON team_ratings(league);
