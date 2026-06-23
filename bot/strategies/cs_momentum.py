"""
Strategy: Cross-Sectional Momentum (CS-MOM)  — v91
---------------------------------------------------
The #1 performer in the v91 deep backtest (+178.7% / Sharpe 1.58 / Sortino 2.19
over 2 years). Classic Jegadeesh-Titman 12-1 cross-sectional momentum over a
~40-name large-cap universe.

Each Monday the universe is ranked by 12-1 momentum and the top 6 (top decile)
are bought at 10% equity each (≈60% deployment, leaving room for the other
strategies). Positions are dropped when they fall out of the top 12 (top-decile
+ buffer) so a fallen-momentum name is not ridden down.

Entry (Monday only, BULL regime only):
  - 12-1 momentum = (close[-1]/close[-252]) - (close[-1]/close[-21])
  - top 6 by momentum, not already held, not blocked by broker safety gates
  - 10% equity, 8% hard stop, 20% take-profit (exchange bracket)

Exit (checked daily):
  - dropped out of the top 12 ranked names → close
  - 8% hard stop (exchange bracket)
  - time stop after 30 trading days
  - BEAR regime → close ALL cs_momentum positions immediately

TP multiples for tp_engine: (1.5, 3.0). Regime compat: ["bull"].
"""

import gc
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import pandas as pd
import yfinance as yf

from broker import AlpacaBroker, tag_symbol
from config import MIN_CASH_RESERVE_PCT
from db import log_trade, log_signal, get_state, set_state, get_position_state

logger = logging.getLogger("alphabot.cs_momentum")
STRATEGY_NAME = "cs_momentum"

CS_MOM_UNIVERSE = [
    "AAPL", "MSFT", "NVDA", "META", "GOOGL", "AMZN", "CRM", "ADBE", "AMD", "AVGO",
    "QCOM", "MU", "JPM", "GS", "MS", "V", "MA", "UNH", "LLY", "ABBV",
    "CAT", "DE", "HON", "GE", "XOM", "CVX", "COP", "COST", "HD", "NKE",
    "SBUX", "NFLX", "TSLA", "TSM", "BLK", "MRK", "WFC", "AXP", "PFE", "MMM",
]
# Deduplicate while preserving order
CS_MOM_UNIVERSE = list(dict.fromkeys(CS_MOM_UNIVERSE))

POSITION_PCT = 0.10        # 10% of equity per position (6 × 10% = 60% deployment)
TOP_N = 6                  # buy the top 6 (top decile of ~40 names)
EXIT_RANK = 12             # close when a holding falls out of the top 12
STOP_PCT = 0.08            # 8% hard stop below entry
TP_PCT = 0.20              # 20% take-profit above entry
TIME_STOP_DAYS = 30        # momentum signals decay — close after 30 trading days
LOOKBACK_DAYS = 280        # daily history to download (needs >= 252 trading rows)

_LAST_DAILY_KEY = "cs_momentum_last_daily_date"     # once-per-day ranking guard
_LAST_ENTRY_KEY = "cs_momentum_last_entry_date"     # once-per-Monday entry guard


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _trading_days_open(entry_time) -> int:
    """Count trading days (Mon-Fri) between entry_time and today UTC."""
    if entry_time is None:
        return 0
    try:
        if isinstance(entry_time, str):
            entry_dt = datetime.fromisoformat(entry_time.replace("Z", "+00:00"))
        else:
            entry_dt = entry_time
        if entry_dt.tzinfo is None:
            entry_dt = entry_dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        days = 0
        current = entry_dt
        while current.date() < now.date():
            current += timedelta(days=1)
            if current.weekday() < 5:
                days += 1
        return days
    except Exception:
        return 0


def _download_closes(symbols: list[str]) -> dict[str, pd.Series]:
    """Download daily closes for the whole universe in ONE yfinance call.

    Returns {symbol: close_series}. Symbols whose data fails or is too short are
    skipped with a warning (never crashes the cycle).
    """
    closes: dict[str, pd.Series] = {}
    try:
        data = yf.download(
            symbols, period=f"{LOOKBACK_DAYS + 60}d", interval="1d",
            progress=False, auto_adjust=True, group_by="ticker", threads=True,
        )
    except Exception as e:
        logger.warning(f"[CS-MOM] bulk download failed: {e}")
        return closes

    if data is None or len(data) == 0:
        logger.warning("[CS-MOM] bulk download returned no data")
        return closes

    multi = isinstance(data.columns, pd.MultiIndex)
    for sym in symbols:
        try:
            if multi:
                if sym not in data.columns.get_level_values(0):
                    logger.warning(f"[CS-MOM] {sym}: missing from download — skipping")
                    continue
                s = data[sym]["Close"].dropna()
            else:
                s = data["Close"].dropna()
            if len(s) >= 252:
                closes[sym] = s
            else:
                logger.warning(f"[CS-MOM] {sym}: only {len(s)} rows (<252) — skipping")
        except Exception as e:
            logger.warning(f"[CS-MOM] {sym}: extract failed ({e}) — skipping")
    return closes


def _rank_universe(symbols: list[str]) -> list[dict]:
    """Compute 12-1 momentum for every symbol and return rows sorted by score desc.

    momentum_score = (close[-1] / close[-252]) - (close[-1] / close[-21])
    """
    closes = _download_closes(symbols)
    rows: list[dict] = []
    for sym, s in closes.items():
        try:
            c_now = float(s.iloc[-1])
            c_252 = float(s.iloc[-252])
            c_21 = float(s.iloc[-21])
            if c_now <= 0 or c_252 <= 0 or c_21 <= 0:
                continue
            score = (c_now / c_252) - (c_now / c_21)
            rows.append({"symbol": sym, "score": score, "price": c_now})
        except Exception as e:
            logger.debug(f"[CS-MOM] {sym}: score failed: {e}")
    rows.sort(key=lambda x: x["score"], reverse=True)
    gc.collect()
    return rows


def _close_position(broker: AlpacaBroker, db_conn, pos: dict, reason: str) -> bool:
    """Cancel resting bracket orders, let them settle, then close at market (v88)."""
    sym = pos["symbol"]
    from strategies.regime_exit import _cancel_open_orders_for_symbol
    n = _cancel_open_orders_for_symbol(broker, sym)
    if n:
        logger.info(f"[CS-MOM] {sym}: cancelled {n} open order(s) before close")
    time.sleep(3)
    result = broker.close_position(sym, STRATEGY_NAME)
    if result is not None:
        logger.info(f"[CS-MOM] CLOSED {sym} — {reason} (pnl={pos['unrealized_pnl_pct']:.1f}%)")
        log_trade(
            db_conn, STRATEGY_NAME, sym, "sell",
            pos["qty"], pos["current_price"], pos["unrealized_pnl"],
            metadata={"reason": reason},
        )
        from utils.cooldown import set_cooldown
        set_cooldown(sym)
        return True
    logger.warning(f"[CS-MOM] {sym}: close failed ({reason}) — will retry next cycle")
    return False


def _check_exits(broker: AlpacaBroker, db_conn, ranked: list[dict]) -> None:
    """Daily exit sweep: rank-drop (out of top 12) + 30-trading-day time stop."""
    top_syms = {r["symbol"] for r in ranked[:EXIT_RANK]}
    ranked_syms = {r["symbol"] for r in ranked}
    positions = [p for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME]

    for pos in positions:
        sym = pos["symbol"]
        state = get_position_state(db_conn, sym)
        entry_time = state.get("entry_time") if state else None

        days_held = _trading_days_open(entry_time)
        if days_held >= TIME_STOP_DAYS:
            _close_position(broker, db_conn, pos, f"time stop ({days_held} trading days)")
            continue

        # Rank-drop: only act when we actually scored the symbol this cycle.
        if sym in ranked_syms and sym not in top_syms:
            _close_position(broker, db_conn, pos, f"dropped out of top {EXIT_RANK}")


def _close_all(broker: AlpacaBroker, db_conn, reason: str) -> int:
    """Close every open cs_momentum position (used on BEAR regime exit)."""
    positions = [p for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME]
    closed = 0
    for pos in positions:
        if _close_position(broker, db_conn, pos, reason):
            closed += 1
    return closed


def run(broker: AlpacaBroker, db_conn):
    """
    CS-MOM dispatch:
      - BEAR regime → close ALL cs_momentum positions immediately, then return.
      - Daily (once/day): re-rank the universe, run rank-drop + time-stop exits.
      - Monday (once/week, BULL only): enter the top 6 names not already held.
    """
    regime = (get_state(db_conn, "current_regime") or "bull").lower()

    # ── BEAR: regime exit — flatten all cs_momentum positions ────────────────
    if regime == "bear":
        n = _close_all(broker, db_conn, "bear regime exit")
        if n:
            logger.info(f"[CS-MOM] BEAR regime — closed {n} position(s)")
        return

    # ── Once-per-day ranking + exit sweep ────────────────────────────────────
    today = _today()
    if get_state(db_conn, _LAST_DAILY_KEY) == today:
        return  # already did the heavy daily work this UTC day

    logger.info("=== CS-MOM: daily ranking + exit sweep ===")
    ranked = _rank_universe(CS_MOM_UNIVERSE)
    if not ranked:
        logger.warning("[CS-MOM] No rankings computed — will retry next cycle")
        return

    logger.info(
        f"[CS-MOM] Ranked {len(ranked)}/{len(CS_MOM_UNIVERSE)} names; top {TOP_N}: "
        + ", ".join(f"{r['symbol']}({r['score']:.3f})" for r in ranked[:TOP_N])
    )

    _check_exits(broker, db_conn, ranked)
    set_state(db_conn, _LAST_DAILY_KEY, today)

    # ── Entries: Monday only, once per week, BULL regime only ────────────────
    if datetime.now(timezone.utc).weekday() != 0:
        return
    if regime != "bull":
        logger.info(f"[CS-MOM] Regime={regime} — no new entries (bull only)")
        return
    if get_state(db_conn, _LAST_ENTRY_KEY) == today:
        return  # already entered this Monday

    from utils.market_hours import is_entry_allowed
    if not is_entry_allowed():
        logger.info("[CS-MOM] Outside safe entry window — entries deferred")
        return

    held = {p["symbol"] for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME}
    targets = [r for r in ranked[:TOP_N] if r["symbol"] not in held]
    if not targets:
        logger.info("[CS-MOM] All top picks already held — nothing to enter")
        set_state(db_conn, _LAST_ENTRY_KEY, today)
        return

    account = broker.get_account()
    equity = account["equity"]
    cash, portfolio_value = broker.get_live_cash()

    for r in targets:
        sym = r["symbol"]

        from utils.cooldown import is_on_cooldown
        if is_on_cooldown(sym):
            logger.debug(f"[CS-MOM] {sym} on cooldown — skipping")
            continue

        from utils.earnings_calendar import has_upcoming_earnings
        if has_upcoming_earnings(sym):
            logger.info(f"[CS-MOM] Skipping {sym} — earnings blackout (within 2 days)")
            continue

        notional = equity * POSITION_PCT
        min_cash = portfolio_value * MIN_CASH_RESERVE_PCT
        if cash - notional < min_cash:
            logger.info(
                f"[CS-MOM] {sym}: insufficient cash "
                f"(available=${cash:.0f}, need=${notional:.0f}, reserve=${min_cash:.0f})"
            )
            continue

        entry_ref = r["price"]
        stop_px = round(entry_ref * (1.0 - STOP_PCT), 2)
        tp_px = round(entry_ref * (1.0 + TP_PCT), 2)
        sig_score = max(0.0, min(1.0, r["score"]))

        log_signal(db_conn, STRATEGY_NAME, sym, "buy", r["score"],
                   {"momentum_score": r["score"], "price": entry_ref})

        logger.info(
            f"[CS-MOM] ENTER {sym} — score={r['score']:.3f}, price=${entry_ref:.2f}, "
            f"notional=${notional:.0f}, stop=${stop_px:.2f}, tp=${tp_px:.2f}"
        )

        result = broker.market_buy(
            sym, notional, STRATEGY_NAME,
            tp_target_override=tp_px,
            stop_override=stop_px,
            signal_score=sig_score,
        )
        if result is None:
            logger.info(f"[CS-MOM] {sym}: entry blocked by broker safety gate")
            continue
        tag_symbol(sym, STRATEGY_NAME)
        log_trade(
            db_conn, STRATEGY_NAME, sym, "buy", 0, entry_ref, 0,
            metadata={"notional": notional, "momentum_score": r["score"],
                      "stop": stop_px, "tp": tp_px},
        )
        cash, portfolio_value = broker.get_live_cash()
        if cash < portfolio_value * MIN_CASH_RESERVE_PCT:
            logger.warning(f"[CS-MOM] Cash floor hit (${cash:,.0f}) — halting entries")
            break

    set_state(db_conn, _LAST_ENTRY_KEY, today)
    active = len([p for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME])
    logger.info(f"[CS-MOM] Entry pass complete — {active} active cs_momentum position(s)")
