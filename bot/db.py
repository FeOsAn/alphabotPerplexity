"""
SQLite database for trade logging and strategy performance tracking.
"""
import os
import sqlite3
import json
import logging
from datetime import datetime
from typing import Optional

_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(_BASE_DIR, 'alphabot.db')
logger = logging.getLogger("alphabot.db")


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_connection()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy TEXT NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            qty REAL,
            price REAL,
            pnl REAL,
            metadata TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy TEXT NOT NULL,
            symbol TEXT NOT NULL,
            signal TEXT NOT NULL,
            score REAL,
            metadata TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_value REAL,
            cash REAL,
            equity REAL,
            pnl_today REAL,
            spy_price REAL,
            snapshot_at TEXT DEFAULT (datetime('now'))
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS strategy_perf (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy TEXT NOT NULL,
            date TEXT NOT NULL,
            realized_pnl REAL DEFAULT 0,
            unrealized_pnl REAL DEFAULT 0,
            trade_count INTEGER DEFAULT 0,
            win_count INTEGER DEFAULT 0,
            loss_count INTEGER DEFAULT 0,
            UNIQUE(strategy, date)
        )
    """)

    conn.commit()
    conn.close()
    logger.info("Database initialized")


def log_trade(conn: sqlite3.Connection, strategy: str, symbol: str, side: str,
              qty: float, price: float, pnl: float, metadata: Optional[dict] = None):
    conn.execute("""
        INSERT INTO trades (strategy, symbol, side, qty, price, pnl, metadata)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (strategy, symbol, side, qty, price, pnl, json.dumps(metadata or {})))
    conn.commit()

    # Update strategy performance
    date = datetime.now().strftime("%Y-%m-%d")
    win = 1 if pnl > 0 else 0
    loss = 1 if pnl < 0 else 0
    conn.execute("""
        INSERT INTO strategy_perf (strategy, date, realized_pnl, trade_count, win_count, loss_count)
        VALUES (?, ?, ?, 1, ?, ?)
        ON CONFLICT(strategy, date) DO UPDATE SET
            realized_pnl = realized_pnl + excluded.realized_pnl,
            trade_count = trade_count + 1,
            win_count = win_count + excluded.win_count,
            loss_count = loss_count + excluded.loss_count
    """, (strategy, date, pnl, win, loss))
    conn.commit()


def log_signal(conn: sqlite3.Connection, strategy: str, symbol: str, signal: str,
               score: float, metadata: Optional[dict] = None):
    conn.execute("""
        INSERT INTO signals (strategy, symbol, signal, score, metadata)
        VALUES (?, ?, ?, ?, ?)
    """, (strategy, symbol, signal, score, json.dumps(metadata or {})))
    conn.commit()


def log_snapshot(conn: sqlite3.Connection, portfolio_value: float, cash: float,
                 equity: float, pnl_today: float, spy_price: float):
    conn.execute("""
        INSERT INTO snapshots (portfolio_value, cash, equity, pnl_today, spy_price)
        VALUES (?, ?, ?, ?, ?)
    """, (portfolio_value, cash, equity, pnl_today, spy_price))
    conn.commit()


def get_trades(conn: sqlite3.Connection, strategy: Optional[str] = None,
               limit: int = 100) -> list[dict]:
    if strategy:
        rows = conn.execute(
            "SELECT * FROM trades WHERE strategy=? ORDER BY created_at DESC LIMIT ?",
            (strategy, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM trades ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]


def get_strategy_performance(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("""
        SELECT
            strategy,
            SUM(realized_pnl) as total_pnl,
            SUM(trade_count) as total_trades,
            SUM(win_count) as wins,
            SUM(loss_count) as losses,
            ROUND(CAST(SUM(win_count) AS FLOAT) / NULLIF(SUM(trade_count), 0) * 100, 1) as win_rate
        FROM strategy_perf
        GROUP BY strategy
        ORDER BY total_pnl DESC
    """).fetchall()
    return [dict(r) for r in rows]


def get_daily_pnl(conn: sqlite3.Connection, strategy: Optional[str] = None, days: int = 30) -> list[dict]:
    if strategy:
        rows = conn.execute("""
            SELECT date, SUM(realized_pnl) as pnl, SUM(trade_count) as trades
            FROM strategy_perf
            WHERE strategy=?
            GROUP BY date
            ORDER BY date DESC LIMIT ?
        """, (strategy, days)).fetchall()
    else:
        rows = conn.execute("""
            SELECT date, SUM(realized_pnl) as pnl, SUM(trade_count) as trades
            FROM strategy_perf
            GROUP BY date
            ORDER BY date DESC LIMIT ?
        """, (days,)).fetchall()
    return [dict(r) for r in rows]


def get_snapshots(conn: sqlite3.Connection, limit: int = 90) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM snapshots ORDER BY snapshot_at DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]
