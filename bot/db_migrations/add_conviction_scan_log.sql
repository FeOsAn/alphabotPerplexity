-- v89 — Conviction Long weekly scan log.
-- The canonical schema lives in bot/db.py init_db(); this file documents the
-- migration for reference / manual application.
CREATE TABLE IF NOT EXISTS conviction_scan_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    total_score REAL,
    momentum_score REAL,
    earnings_score REAL,
    analyst_score REAL,
    research_score REAL,
    reasoning TEXT,
    was_selected INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
);
