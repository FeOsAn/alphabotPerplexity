"""
Strategy 5: Post-Earnings Announcement Drift (PEAD)
-----------------------------------------------------
Academically documented edge: stocks that beat earnings estimates tend to
drift upward for 2-6 weeks after the report. We buy stocks with strong
earnings surprises (actual EPS > estimate) shortly after the announcement
and ride the drift. Exit after 15 trading days or on stop/take-profit.

Signal sources: yFinance earnings history (free, no extra API key needed).
Expected alpha: 3-6% per trade in backtests (Fama, Ball & Brown 1968+).
"""

import logging
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Optional
from broker import AlpacaBroker, tag_symbol
from config import (
    PEAD_WATCHLIST, PEAD_MIN_SURPRISE_PCT, PEAD_MAX_POSITIONS,
    PEAD_HOLD_DAYS, MAX_POSITION_PCT, MAX_TOTAL_EQUITY_POSITIONS,
    STOP_LOSS_PCT, TAKE_PROFIT_PCT, MIN_CASH_RESERVE_PCT
)
from db import log_trade, log_signal

logger = logging.getLogger("alphabot.earnings_drift")
STRATEGY_NAME = "earnings_drift"

# Track when we entered each position (symbol -> entry datetime)
_entry_dates: dict[str, datetime] = {}


def _get_earnings_surprise(symbol: str) -> Optional[dict]:
    """
    Check if this stock recently beat earnings estimates.
    Returns surprise data if fresh beat found, else None.
    """
    try:
        ticker = yf.Ticker(symbol)
        earnings = ticker.earnings_dates

        if earnings is None or earnings.empty:
            return None

        # earnings_dates index is tz-aware (America/New_York) — convert to UTC then strip tz
        earnings.index = pd.to_datetime(earnings.index, utc=True).tz_convert(None)

        # cutoff must also be tz-naive for comparison
        cutoff = datetime.utcnow() - timedelta(days=5)
        recent = earnings[earnings.index >= cutoff]

        if recent.empty:
            return None

        row = recent.iloc[0]
        reported = row.get("Reported EPS")
        estimated = row.get("EPS Estimate")

        if pd.isna(reported) or pd.isna(estimated) or estimated == 0:
            return None

        surprise_pct = ((reported - estimated) / abs(estimated)) * 100

        if surprise_pct >= PEAD_MIN_SURPRISE_PCT:
            return {
                "symbol": symbol,
                "reported_eps": float(reported),
                "estimated_eps": float(estimated),
                "surprise_pct": float(surprise_pct),
                "earnings_date": str(recent.index[0].date()),
            }
    except Exception as e:
        logger.debug(f"[PEAD] Could not fetch earnings for {symbol}: {e}")
    return None


def _holding_too_long(symbol: str) -> bool:
    """Returns True if we've held this position longer than PEAD_HOLD_DAYS."""
    entry = _entry_dates.get(symbol)
    if entry is None:
        return False
    trading_days_held = (datetime.now() - entry).days * (5 / 7)  # approx
    return trading_days_held >= PEAD_HOLD_DAYS


def run(broker: AlpacaBroker, db_conn):
    """Run PEAD strategy — scan for fresh earnings beats and manage positions."""
    logger.info("=== Earnings Drift (PEAD) Strategy: Scanning ===")

    # --- Exit existing positions ---
    all_positions = broker.get_positions()
    pead_positions = [p for p in all_positions if p["strategy"] == STRATEGY_NAME]

    for pos in pead_positions:
        sym = pos["symbol"]

        # Stop loss
        if pos["unrealized_pnl_pct"] <= -STOP_LOSS_PCT * 100:
            logger.info(f"[PEAD] STOP LOSS {sym} @ {pos['unrealized_pnl_pct']:.1f}%")
            broker.close_position(sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, sym, "sell_stop",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])
            _entry_dates.pop(sym, None)
            continue

        # Take profit
        if pos["unrealized_pnl_pct"] >= TAKE_PROFIT_PCT * 100:
            logger.info(f"[PEAD] TAKE PROFIT {sym} @ {pos['unrealized_pnl_pct']:.1f}%")
            broker.close_position(sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, sym, "sell_tp",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])
            _entry_dates.pop(sym, None)
            continue

        # Time-based exit: hold max PEAD_HOLD_DAYS trading days
        if _holding_too_long(sym):
            logger.info(f"[PEAD] TIME EXIT {sym} after {PEAD_HOLD_DAYS} days, PnL: {pos['unrealized_pnl_pct']:.1f}%")
            broker.close_position(sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, sym, "sell_time",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])
            _entry_dates.pop(sym, None)

    # --- Scan for new earnings beats ---
    current_pead_count = len([p for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME])
    if current_pead_count >= PEAD_MAX_POSITIONS:
        logger.info(f"[PEAD] Max positions reached ({PEAD_MAX_POSITIONS})")
        return

    account = broker.get_account()
    portfolio_value = account["portfolio_value"]
    cash = account["cash"]
    current_symbols = {p["symbol"] for p in broker.get_positions()}

    beats = []
    for sym in PEAD_WATCHLIST:
        if sym in current_symbols:
            continue
        surprise = _get_earnings_surprise(sym)
        if surprise:
            beats.append(surprise)
            logger.info(f"[PEAD] Earnings beat found: {sym} +{surprise['surprise_pct']:.1f}% surprise")

    if not beats:
        logger.info("[PEAD] No fresh earnings beats found today")
        logger.info(f"[PEAD] Scan complete — {current_pead_count} active positions")
        return

    # Sort by largest surprise first
    beats.sort(key=lambda x: x["surprise_pct"], reverse=True)

    for beat in beats:
        if current_pead_count >= PEAD_MAX_POSITIONS:
            break

        total_equity = len([p for p in broker.get_positions() if p.get("asset_class", "equity") == "equity"])
        if total_equity >= MAX_TOTAL_EQUITY_POSITIONS:
            break

        notional = portfolio_value * MAX_POSITION_PCT
        min_cash = portfolio_value * MIN_CASH_RESERVE_PCT
        if cash - notional < min_cash:
            logger.info(f"[PEAD] Insufficient cash for {beat['symbol']}")
            continue

        sym = beat["symbol"]
        logger.info(
            f"[PEAD] ENTER {sym} — EPS surprise: +{beat['surprise_pct']:.1f}% "
            f"(reported {beat['reported_eps']:.2f} vs est {beat['estimated_eps']:.2f})"
        )
        log_signal(db_conn, STRATEGY_NAME, sym, "buy", beat["surprise_pct"], beat)
        broker.market_buy(sym, notional, STRATEGY_NAME)
        tag_symbol(sym, STRATEGY_NAME)
        log_trade(db_conn, STRATEGY_NAME, sym, "buy", 0, 0, 0, metadata={
            "notional": notional,
            "surprise_pct": beat["surprise_pct"],
            "earnings_date": beat["earnings_date"],
        })
        _entry_dates[sym] = datetime.now()
        cash -= notional
        current_pead_count += 1

    logger.info(f"[PEAD] Scan complete — {current_pead_count} active positions")
