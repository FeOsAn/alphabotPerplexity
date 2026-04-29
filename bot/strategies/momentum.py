"""
Strategy 1: Cross-Sectional Momentum (12-1 month)
---------------------------------------------------
Academically backed: Jegadeesh & Titman (1993), proven over 150+ years of data.
Buy stocks with the highest 12-month return (skipping last month to avoid reversal).
Rebalance monthly. Equal-weight top-N picks.

Expected alpha: 3-8% annualized over SPY (from quant literature).
"""

import gc
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
    """Compute momentum scores using yFinance (free, full history)."""
    logger.info("Fetching historical data for momentum scoring via yFinance...")
    end = datetime.now()
    # Request extra buffer — yFinance free tier ~179 calendar days max
    # MOMENTUM_LOOKBACK is in trading days; multiply by ~1.4 for calendar days
    cal_days = int(MOMENTUM_LOOKBACK * 1.5) + 30
    start = end - timedelta(days=cal_days)

    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")
    logger.info(f"Requesting {start_str} → {end_str} ({cal_days} calendar days)")

    scores = {}
    batch_size = 10  # Small batches to stay within Railway 512MB RAM
    all_closes = {}

    for i in range(0, len(UNIVERSE), batch_size):
        batch = UNIVERSE[i:i + batch_size]
        try:
            raw = yf.download(
                batch, start=start_str, end=end_str,
                auto_adjust=True, progress=False, threads=False
            )

            if raw.empty:
                logger.warning(f"Empty data for batch {batch}")
                continue

            logger.info(f"Batch {i//batch_size + 1}: raw shape={raw.shape}, columns_type={type(raw.columns).__name__}")

            # Parse Close column — handle both MultiIndex and flat Index
            if isinstance(raw.columns, pd.MultiIndex):
                level0 = raw.columns.get_level_values(0).unique().tolist()
                level1 = raw.columns.get_level_values(1).unique().tolist()
                logger.info(f"  MultiIndex level0={level0[:5]}, level1={level1[:5]}")

                # Standard yfinance: level0=metric, level1=ticker
                if "Close" in level0:
                    close_batch = raw["Close"]
                elif "close" in level0:
                    close_batch = raw["close"]
                # Inverted: level0=ticker, level1=metric
                elif "Close" in level1:
                    close_batch = raw.xs("Close", axis=1, level=1)
                elif "close" in level1:
                    close_batch = raw.xs("close", axis=1, level=1)
                else:
                    logger.warning(f"  Cannot find Close in MultiIndex — skipping batch")
                    continue
            else:
                # Single ticker — flat DataFrame
                flat_cols = [c.lower() for c in raw.columns]
                if "close" in flat_cols:
                    idx = flat_cols.index("close")
                    close_batch = raw.iloc[:, idx].rename(batch[0]).to_frame()
                else:
                    logger.warning(f"  No close column in flat frame: {raw.columns.tolist()}")
                    continue

            logger.info(f"  close_batch shape={close_batch.shape}, cols={list(close_batch.columns)[:5]}")

            for sym in batch:
                if sym in close_batch.columns:
                    series = close_batch[sym].dropna()
                    if len(series) >= MOMENTUM_LOOKBACK:
                        all_closes[sym] = series
                    else:
                        logger.debug(f"  {sym}: only {len(series)} rows (need {MOMENTUM_LOOKBACK}) — skipping")
                else:
                    logger.debug(f"  {sym} not found in close_batch columns")

        except Exception as e:
            logger.warning(f"Batch download failed for {batch}: {e}", exc_info=True)
            continue
        finally:
            gc.collect()

    logger.info(f"Downloaded usable data for {len(all_closes)} / {len(UNIVERSE)} symbols")

    for sym, series in all_closes.items():
        try:
            # price_now = price MOMENTUM_SKIP days ago (skip last month)
            # price_then = price MOMENTUM_LOOKBACK days ago
            price_now = float(series.iloc[-(MOMENTUM_SKIP + 1)])
            price_then = float(series.iloc[-MOMENTUM_LOOKBACK])
            if price_then > 0:
                scores[sym] = (price_now - price_then) / price_then
                logger.debug(f"  {sym}: score={scores[sym]:.3f} (${price_then:.2f} → ${price_now:.2f})")
        except (IndexError, ZeroDivisionError) as e:
            logger.debug(f"  {sym}: score error — {e}")

    logger.info(f"Momentum scores computed for {len(scores)} symbols")
    if scores:
        top5 = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:5]
        logger.info(f"Top 5 scores: {top5}")
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
        logger.warning("No momentum scores computed — skipping rebalance (will retry next cycle)")
        return

    top_picks = scores.head(MOMENTUM_TOP_N).index.tolist()
    logger.info(f"Top momentum picks: {top_picks}")

    # Log signals
    for sym in scores.head(20).index:
        log_signal(db_conn, STRATEGY_NAME, sym, "buy" if sym in top_picks else "hold",
                   float(scores[sym]), {"rank": int(scores.index.get_loc(sym)) + 1})

    # Current momentum positions (equity only)
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
            total_equity = len([p for p in broker.get_positions() if p.get("asset_class", "equity") == "equity"])
            if total_equity >= MAX_TOTAL_EQUITY_POSITIONS:
                logger.info(f"Max equity positions reached ({MAX_TOTAL_EQUITY_POSITIONS}), skipping {sym}")
                break

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
