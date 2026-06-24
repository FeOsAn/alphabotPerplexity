"""
SQLite database for trade logging and strategy performance tracking.
"""
import os
import sqlite3
import json
import logging
from datetime import datetime, timezone
from typing import Optional

from utils.clock import now_utc

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

    # Persistent key/value store for restart-fragile state
    # (circuit breaker flag, daily one-shot fire dates, etc.)
    c.execute("""
        CREATE TABLE IF NOT EXISTS bot_state (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)

    # v74 — per-symbol entry record for TP/stop system.
    # Replaces the in-memory _ratchet_stops dict with a durable, side-aware,
    # dollar-priced record. One row per open position; row is deleted on close.
    c.execute("""
        CREATE TABLE IF NOT EXISTS positions_state (
            symbol        TEXT PRIMARY KEY,
            side          TEXT NOT NULL,
            qty           REAL NOT NULL,
            entry_price   REAL NOT NULL,
            entry_atr     REAL NOT NULL,
            initial_stop  REAL NOT NULL,
            tp_target     REAL,
            strategy      TEXT NOT NULL,
            entry_time    TEXT NOT NULL,
            tp_basis      TEXT,
            initial_risk  REAL NOT NULL,
            stop_order_id TEXT,
            updated_at    TEXT DEFAULT (datetime('now'))
        )
    """)

    # v78: add stop_order_id column if not present (existing deployments)
    try:
        conn.execute("ALTER TABLE positions_state ADD COLUMN stop_order_id TEXT")
        conn.commit()
    except Exception:
        pass  # Column already exists

    # v85 — tag each entry with the regime + strategy it was opened under so
    # check_regime_exits() can close positions when the regime flips away from
    # the strategy's compatible set.
    for col_def in ["opening_regime TEXT", "opening_strategy TEXT"]:
        try:
            conn.execute(f"ALTER TABLE positions_state ADD COLUMN {col_def}")
            conn.commit()
        except Exception:
            pass  # Column already exists

    # v79 — per-symbol lifecycle state: TP order IDs, re-eval counter, entry date.
    # Separate from positions_state so lifecycle state survives partial closes.
    c.execute("""
        CREATE TABLE IF NOT EXISTS position_state (
            symbol          TEXT PRIMARY KEY,
            tp1_price       REAL,
            tp2_price       REAL,
            tp1_order_id    TEXT,
            tp2_order_id    TEXT,
            tp1_hit         INTEGER DEFAULT 0,
            reeval_count    INTEGER DEFAULT 0,
            entry_date      TEXT
        )
    """)

    # v79 — ALTER TABLE migrations for position_state (silent if already present)
    for col_def in [
        "tp1_price REAL", "tp2_price REAL",
        "tp1_order_id TEXT", "tp2_order_id TEXT",
        "tp1_hit INTEGER DEFAULT 0",
        "reeval_count INTEGER DEFAULT 0",
        "entry_date TEXT",
    ]:
        col_name = col_def.split()[0]
        try:
            conn.execute(f"ALTER TABLE position_state ADD COLUMN {col_def}")
            conn.commit()
        except Exception:
            pass

    c.execute("""
        CREATE INDEX IF NOT EXISTS positions_state_strategy
            ON positions_state(strategy)
    """)

    # v89 — weekly Conviction Long scan log: one row per ticker per scan.
    c.execute("""
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
        )
    """)

    # v94 — cross-strategy symbol cooldown lock. One row per locked symbol.
    # Set whenever ANY strategy stops out of / takes profit on a symbol; the
    # central entry gate (_entry_blocked) refuses re-entry by ANY strategy
    # until locked_until passes. Replaces the per-symbol bot_state cooldown keys
    # so the lock is durable, global, and queryable.
    c.execute("""
        CREATE TABLE IF NOT EXISTS symbol_cooldown (
            symbol       TEXT PRIMARY KEY,
            locked_until TEXT NOT NULL,
            reason       TEXT,
            created_at   TEXT DEFAULT (datetime('now'))
        )
    """)

    # v75 — per-symbol performance ledger driving the blacklist (FIX 3).
    c.execute("""
        CREATE TABLE IF NOT EXISTS symbol_performance (
            symbol      TEXT PRIMARY KEY,
            trades      INTEGER DEFAULT 0,
            wins        INTEGER DEFAULT 0,
            total_pnl   REAL DEFAULT 0.0,
            avg_pnl     REAL DEFAULT 0.0,
            win_rate    REAL DEFAULT 0.0,
            blacklisted INTEGER DEFAULT 0,
            blacklist_reason TEXT,
            updated_at  TEXT DEFAULT (datetime('now'))
        )
    """)

    # Indexes — multiple callers do `WHERE symbol=? AND side LIKE 'buy%'
    # ORDER BY created_at DESC LIMIT 1`; without an index these are full table
    # scans once the trades table grows past a few thousand rows.
    c.execute("""
        CREATE INDEX IF NOT EXISTS trades_sym_side_dt
        ON trades(symbol, side, created_at DESC)
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS trades_strategy_dt
        ON trades(strategy, created_at DESC)
    """)
    # bot_state primary key already indexes 'key', but make it explicit so
    # future schema migrations don't accidentally drop the implicit index.
    c.execute("""
        CREATE INDEX IF NOT EXISTS bot_state_key
        ON bot_state(key)
    """)

    conn.commit()
    conn.close()
    logger.info("Database initialized")

    # v75 — pre-seed symbol_performance with known poor performers from backtest.
    try:
        from utils.symbol_performance import _seed_known_performers_if_empty
        seed_conn = get_connection()
        try:
            _seed_known_performers_if_empty(seed_conn)
        finally:
            seed_conn.close()
    except Exception as e:
        logger.warning(f"[db] symbol_performance seed failed: {e}")


def log_trade(conn: sqlite3.Connection, strategy: str, symbol: str, side: str,
              qty: float, price: float, pnl: float, metadata: Optional[dict] = None):
    conn.execute("""
        INSERT INTO trades (strategy, symbol, side, qty, price, pnl, metadata)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (strategy, symbol, side, qty, price, pnl, json.dumps(metadata or {})))
    conn.commit()

    # Update strategy performance — UTC to match trades.created_at (SQLite datetime('now') is UTC)
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
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


def get_trades_for_symbol(conn: sqlite3.Connection, symbol: str) -> list[dict]:
    """Return all trade records for a symbol, ordered by created_at desc."""
    try:
        rows = conn.execute(
            "SELECT * FROM trades WHERE symbol=? ORDER BY created_at DESC LIMIT 20",
            (symbol,),
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


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


# ── bot_state helpers ────────────────────────────────────────────────────────
def get_state(conn: sqlite3.Connection, key: str) -> Optional[str]:
    """Read a bot_state value. Returns None if missing."""
    try:
        row = conn.execute("SELECT value FROM bot_state WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None
    except Exception as e:
        logger.warning(f"[db.get_state] {key}: {e}")
        return None


def set_state(conn: sqlite3.Connection, key: str, value: str) -> None:
    """Write a bot_state value. Upserts."""
    try:
        ts = datetime.now(timezone.utc).isoformat()
        conn.execute("""
            INSERT INTO bot_state (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
        """, (key, value, ts))
        conn.commit()
    except Exception as e:
        logger.warning(f"[db.set_state] {key}: {e}")


def del_state(conn: sqlite3.Connection, key: str) -> None:
    """Delete a bot_state key."""
    try:
        conn.execute("DELETE FROM bot_state WHERE key=?", (key,))
        conn.commit()
    except Exception as e:
        logger.warning(f"[db.del_state] {key}: {e}")


# ── v74: positions_state helpers ─────────────────────────────────────────────
def write_position_state(conn: sqlite3.Connection, *, symbol: str, side: str,
                         qty: float, entry_price: float, entry_atr: float,
                         initial_stop: float, tp_target: Optional[float],
                         strategy: str, tp_basis: Optional[str],
                         opening_regime: Optional[str] = None,
                         opening_strategy: Optional[str] = None) -> None:
    """Upsert the per-symbol entry record. Called by record_entry() after every buy."""
    try:
        initial_risk = abs(entry_price - initial_stop)
        entry_time = now_utc().isoformat()
        if opening_strategy is None:
            opening_strategy = strategy
        conn.execute("""
            INSERT INTO positions_state
                (symbol, side, qty, entry_price, entry_atr, initial_stop,
                 tp_target, strategy, entry_time, tp_basis, initial_risk,
                 opening_regime, opening_strategy, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(symbol) DO UPDATE SET
                side=excluded.side, qty=excluded.qty,
                entry_price=excluded.entry_price, entry_atr=excluded.entry_atr,
                initial_stop=excluded.initial_stop, tp_target=excluded.tp_target,
                strategy=excluded.strategy, entry_time=excluded.entry_time,
                tp_basis=excluded.tp_basis, initial_risk=excluded.initial_risk,
                opening_regime=excluded.opening_regime,
                opening_strategy=excluded.opening_strategy,
                updated_at=datetime('now')
        """, (symbol, side, qty, entry_price, entry_atr, initial_stop,
              tp_target, strategy, entry_time, tp_basis, initial_risk,
              opening_regime, opening_strategy))
        conn.commit()
    except Exception as e:
        logger.warning(f"[db.write_position_state] {symbol}: {e}")


def get_position_state(conn: sqlite3.Connection, symbol: str) -> Optional[dict]:
    """Return the positions_state row for a symbol, or None."""
    try:
        row = conn.execute(
            "SELECT * FROM positions_state WHERE symbol=?", (symbol,)
        ).fetchone()
        return dict(row) if row else None
    except Exception as e:
        logger.warning(f"[db.get_position_state] {symbol}: {e}")
        return None


def delete_position_state(conn: sqlite3.Connection, symbol: str) -> None:
    """Delete the positions_state row for a symbol — paired with every close."""
    try:
        conn.execute("DELETE FROM positions_state WHERE symbol=?", (symbol,))
        conn.commit()
    except Exception as e:
        logger.warning(f"[db.delete_position_state] {symbol}: {e}")


# ── v94: cross-strategy symbol cooldown lock ─────────────────────────────────
def set_symbol_cooldown(conn: sqlite3.Connection, symbol: str, hours: float,
                        reason: Optional[str] = None) -> None:
    """Lock a symbol from re-entry by ANY strategy for `hours`. Upserts; never
    shortens an existing, longer lock."""
    try:
        from datetime import timedelta
        until = (now_utc() + timedelta(hours=hours)).isoformat()
        row = conn.execute(
            "SELECT locked_until FROM symbol_cooldown WHERE symbol=?", (symbol,)
        ).fetchone()
        if row and row["locked_until"] and row["locked_until"] >= until:
            return  # existing lock extends at least as far — keep the longer one
        conn.execute("""
            INSERT INTO symbol_cooldown (symbol, locked_until, reason, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                locked_until=excluded.locked_until,
                reason=excluded.reason,
                created_at=excluded.created_at
        """, (symbol, until, reason, now_utc().isoformat()))
        conn.commit()
    except Exception as e:
        logger.warning(f"[db.set_symbol_cooldown] {symbol}: {e}")


def is_symbol_locked(conn: sqlite3.Connection, symbol: str) -> bool:
    """True if symbol is still inside its cross-strategy cooldown window.
    Self-cleans expired rows."""
    try:
        row = conn.execute(
            "SELECT locked_until FROM symbol_cooldown WHERE symbol=?", (symbol,)
        ).fetchone()
        if not row or not row["locked_until"]:
            return False
        try:
            until = datetime.fromisoformat(row["locked_until"])
        except Exception:
            conn.execute("DELETE FROM symbol_cooldown WHERE symbol=?", (symbol,))
            conn.commit()
            return False
        if now_utc() >= until:
            conn.execute("DELETE FROM symbol_cooldown WHERE symbol=?", (symbol,))
            conn.commit()
            return False
        return True
    except Exception as e:
        logger.warning(f"[db.is_symbol_locked] {symbol}: {e}")
        return False


def clear_symbol_cooldown(conn: sqlite3.Connection, symbol: str) -> None:
    """Remove a symbol's cross-strategy lock."""
    try:
        conn.execute("DELETE FROM symbol_cooldown WHERE symbol=?", (symbol,))
        conn.commit()
    except Exception as e:
        logger.warning(f"[db.clear_symbol_cooldown] {symbol}: {e}")


def get_active_symbol_cooldowns(conn: sqlite3.Connection) -> dict[str, str]:
    """Return {symbol: locked_until_iso} for all still-active locks."""
    try:
        now_iso = now_utc().isoformat()
        rows = conn.execute(
            "SELECT symbol, locked_until FROM symbol_cooldown WHERE locked_until > ?",
            (now_iso,),
        ).fetchall()
        return {r["symbol"]: r["locked_until"] for r in rows}
    except Exception as e:
        logger.warning(f"[db.get_active_symbol_cooldowns] {e}")
        return {}
