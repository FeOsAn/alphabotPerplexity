"""
Strategy 1: Cross-Sectional Momentum (12-1 month)
---------------------------------------------------
Academically backed: Jegadeesh & Titman (1993), proven over 150+ years of data.
Buy stocks with the highest 12-month return (skipping last month to avoid reversal).
Rebalance monthly. Equal-weight top-N picks.

Expected alpha: 3-8% annualized over SPY (from quant literature).
"""

import logging
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
from typing import Optional
from broker import AlpacaBroker, tag_symbol
from config import (
    UNIVERSE, MOMENTUM_LOOKBACK, MOMENTUM_SKIP, MOMENTUM_TOP_N,
    MOMENTUM_REBALANCE_DAYS, MAX_POSITION_PCT, MAX_TOTAL_EQUITY_POSITIONS,
    STOP_LOSS_PCT, MIN_CASH_RESERVE_PCT
)
from db import log_trade, log_signal

logger = logging.getLogger("alphabot.momentum")
STRATEGY_NAME = "momentum"

_last_rebalance: Optional[datetime] = None


def should_rebalance() -> bool:
    global _last_rebalance
    if _last_rebalance is None:
        return True
    days_since = (datetime.now() - _last_rebalance).days
    return days_since >= MOMENTUM_REBALANCE_DAYS


def score_universe(broker: AlpacaBroker) -> pd.Series:
    """Compute 12-1 month momentum scores for each symbol in universe."""
    logger.info("Fetching historical data for momentum scoring...")
    def score_universe(broker: AlpacaBroker) -> pd.Series:
    """Compute 12-1 month momentum scores using yFinance (free, full history)."""
    logger.info("Fetching historical data for momentum scoring via yFinance...")
    end = datetime.now()
    start = end - timedelta(days=MOMENTUM_LOOKBACK + 60)

    scores = {}
    try:
        raw = yf.download(
            UNIVERSE, start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"),
            auto_adjust=True, progress=False, threads=True
        )["Close"]
    except Exception as e:
        logger.error(f"yFinance download failed: {e}")
        return pd.Series()

    for sym in UNIVERSE:
        try:
            if sym not in raw.columns:
                continue
            df = raw[sym].dropna()
            if len(df) < MOMENTUM_LOOKBACK:
                continue
            price_now = df.iloc[-MOMENTUM_SKIP - 1]
            price_then = df.iloc[-MOMENTUM_LOOKBACK]
            if price_then > 0:
                scores[sym] = (price_now - price_then) / price_then
        except (IndexError, ZeroDivisionError):
            pass

    return pd.Series(scores).sort_values(ascending=False)

    scores = {}
    for sym, df in bars.items():
        if df is None or len(df) < MOMENTUM_LOOKBACK:
            continue
        df = df.sort_index()
        try:
            price_now = df["close"].iloc[-MOMENTUM_SKIP - 1]  # 1 month ago
            price_then = df["close"].iloc[-MOMENTUM_LOOKBACK]   # 12 months ago
            if price_then > 0:
                scores[sym] = (price_now - price_then) / price_then
        except (IndexError, ZeroDivisionError):
            pass

    return pd.Series(scores).sort_values(ascending=False)


def run(broker: AlpacaBroker, db_conn):
    """Execute momentum strategy rebalance if due."""
    if not should_rebalance():
        # Still check stop losses on existing positions
        _check_stop_losses(broker, db_conn)
        return

    global _last_rebalance
    logger.info("=== Momentum Strategy: Running monthly rebalance ===")

    scores = score_universe(broker)
    if scores.empty:
        logger.warning("No momentum scores computed — skipping")
        return

    top_picks = scores.head(MOMENTUM_TOP_N).index.tolist()
    logger.info(f"Top momentum picks: {top_picks}")

    # Log signals
    for sym in scores.head(20).index:
        log_signal(db_conn, STRATEGY_NAME, sym, "buy" if sym in top_picks else "hold",
                   float(scores[sym]), {"rank": int(scores.index.get_loc(sym)) + 1})

    # Current momentum positions (equity only — skip options)
    all_positions = broker.get_positions()
    equity_positions = [p for p in all_positions if p.get("asset_class", "equity") == "equity"]
    momentum_positions = [p for p in equity_positions if p["strategy"] == STRATEGY_NAME]
    current_symbols = {p["symbol"] for p in momentum_positions}

    account = broker.get_account()
    portfolio_value = account["portfolio_value"]
    cash = account["cash"]
    target_allocation_per_stock = portfolio_value * MAX_POSITION_PCT

    # Exit positions no longer in top picks
    for pos in momentum_positions:
        if pos["symbol"] not in top_picks:
            logger.info(f"Exiting {pos['symbol']} — not in top momentum picks")
            order = broker.close_position(pos["symbol"], STRATEGY_NAME)
            if order:
                log_trade(db_conn, STRATEGY_NAME, pos["symbol"], "sell",
                          pos["qty"], pos["current_price"], pos["unrealized_pnl"])

    # Enter new top picks
    for sym in top_picks:
        if sym not in current_symbols:
            # Check position limits (equity only)
            total_equity = len([p for p in broker.get_positions() if p.get("asset_class", "equity") == "equity"])
            if total_equity >= MAX_TOTAL_EQUITY_POSITIONS:
                logger.info(f"Max equity positions reached ({MAX_TOTAL_EQUITY_POSITIONS}), skipping {sym}")
                break

            # Check cash available
            min_cash = portfolio_value * MIN_CASH_RESERVE_PCT
            if cash - target_allocation_per_stock < min_cash:
                logger.info(f"Insufficient cash for {sym}, skipping")
                break

            notional = min(target_allocation_per_stock, cash * 0.9)
            if notional < 1:
                break

            logger.info(f"Opening momentum position: {sym} ${notional:.0f}")
            order = broker.market_buy(sym, notional, STRATEGY_NAME)
            tag_symbol(sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, sym, "buy", 0, 0, 0,
                      metadata={"notional": notional, "momentum_score": float(scores.get(sym, 0))})
            cash -= notional

    _last_rebalance = datetime.now()
    logger.info("Momentum rebalance complete")


def _check_stop_losses(broker: AlpacaBroker, db_conn):
    """Check and enforce stop losses on existing momentum positions."""
    positions = broker.get_positions()
    for pos in positions:
        if pos["strategy"] != STRATEGY_NAME:
            continue
        if pos["unrealized_pnl_pct"] <= -STOP_LOSS_PCT * 100:
            logger.info(f"STOP LOSS triggered for {pos['symbol']} ({pos['unrealized_pnl_pct']:.1f}%)")
            broker.close_position(pos["symbol"], STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, pos["symbol"], "sell_stop",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])
