"""
Strategy 6: Sector Rotation — Monthly Relative Momentum
---------------------------------------------------------
Rotate into the top 2-3 performing S&P 500 sectors every month based on
3-month relative momentum. Uses sector ETFs as proxies (XLK, XLF, etc.).
Classic Faber (2007) tactical asset allocation — proven to outperform
buy-and-hold over full market cycles by avoiding weak sectors.

No Claude credits needed — purely rule-based and data-driven.
Expected alpha: 2-5% annualized with lower drawdowns than SPY.
"""

import logging
import pandas as pd
from datetime import datetime
from typing import Optional
from broker import AlpacaBroker, tag_symbol
from config import (
    SR_TOP_N, SR_LOOKBACK_DAYS, SR_REBALANCE_DAYS,
    SR_MAX_POSITION_PCT, MAX_TOTAL_EQUITY_POSITIONS,
    STOP_LOSS_PCT, MIN_CASH_RESERVE_PCT
)
from db import log_trade, log_signal

logger = logging.getLogger("alphabot.sector_rotation")
STRATEGY_NAME = "sector_rotation"

# S&P 500 sector ETFs — liquid, low spread, highly representative
SECTOR_ETFS = {
    "XLK":  "Technology",
    "XLF":  "Financials",
    "XLV":  "Health Care",
    "XLY":  "Consumer Discretionary",
    "XLP":  "Consumer Staples",
    "XLE":  "Energy",
    "XLI":  "Industrials",
    "XLB":  "Materials",
    "XLU":  "Utilities",
    "XLRE": "Real Estate",
    "XLC":  "Communication Services",
}

_last_rebalance: Optional[datetime] = None


def _should_rebalance() -> bool:
    global _last_rebalance
    if _last_rebalance is None:
        return True
    return (datetime.now() - _last_rebalance).days >= SR_REBALANCE_DAYS


def _score_sectors(broker: AlpacaBroker) -> pd.Series:
    """Compute 3-month momentum for each sector ETF."""
    symbols = list(SECTOR_ETFS.keys())
    bars = broker.get_bars(symbols, days=SR_LOOKBACK_DAYS + 30)

    scores = {}
    for etf, df in bars.items():
        if df is None or len(df) < SR_LOOKBACK_DAYS:
            continue
        df = df.sort_index()
        try:
            price_now = df["close"].iloc[-1]
            price_then = df["close"].iloc[-SR_LOOKBACK_DAYS]
            if price_then > 0:
                momentum = (price_now - price_then) / price_then
                # Extra filter: price must be above its 50-day MA (trend confirmation)
                ma50 = df["close"].tail(50).mean()
                if price_now > ma50:
                    scores[etf] = momentum
                    logger.debug(f"[SR] {etf} ({SECTOR_ETFS[etf]}): {momentum:.2%} momentum, above MA50")
                else:
                    logger.debug(f"[SR] {etf} ({SECTOR_ETFS[etf]}): {momentum:.2%} momentum, BELOW MA50 — excluded")
        except (IndexError, ZeroDivisionError):
            pass

    return pd.Series(scores).sort_values(ascending=False)


def run(broker: AlpacaBroker, db_conn):
    """Run sector rotation rebalance if due."""
    if not _should_rebalance():
        # Still enforce stop losses between rebalances
        _check_stops(broker, db_conn)
        return

    global _last_rebalance
    logger.info("=== Sector Rotation Strategy: Monthly Rebalance ===")

    scores = _score_sectors(broker)
    if scores.empty:
        logger.warning("[SR] No sector scores computed — skipping")
        return

    top_sectors = scores.head(SR_TOP_N).index.tolist()
    logger.info(f"[SR] Top sectors: {[(s, SECTOR_ETFS[s], f'{scores[s]:.1%}') for s in top_sectors]}")

    # Log all signals
    for etf in scores.index:
        log_signal(db_conn, STRATEGY_NAME, etf, "buy" if etf in top_sectors else "hold",
                   float(scores[etf]), {"sector": SECTOR_ETFS[etf], "momentum": float(scores[etf])})

    account = broker.get_account()
    portfolio_value = account["portfolio_value"]
    cash = account["cash"]
    notional_per_sector = portfolio_value * SR_MAX_POSITION_PCT

    all_positions = broker.get_positions()
    sr_positions = [p for p in all_positions if p["strategy"] == STRATEGY_NAME]
    current_symbols = {p["symbol"] for p in sr_positions}

    # Exit sectors no longer in top N
    for pos in sr_positions:
        if pos["symbol"] not in top_sectors:
            logger.info(f"[SR] EXIT {pos['symbol']} ({SECTOR_ETFS.get(pos['symbol'], '?')}) — rotated out")
            broker.close_position(pos["symbol"], STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, pos["symbol"], "sell",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])
            cash += pos["market_value"]

    # Enter new top sectors
    for etf in top_sectors:
        if etf in current_symbols:
            continue

        total_equity = len([p for p in broker.get_positions() if p.get("asset_class", "equity") == "equity"])
        if total_equity >= MAX_TOTAL_EQUITY_POSITIONS:
            logger.info(f"[SR] Max equity positions reached — skipping {etf}")
            break

        min_cash = portfolio_value * MIN_CASH_RESERVE_PCT
        if cash - notional_per_sector < min_cash:
            logger.info(f"[SR] Insufficient cash for {etf}")
            continue

        notional = min(notional_per_sector, cash * 0.9)
        if notional < 1:
            break

        logger.info(
            f"[SR] ENTER {etf} ({SECTOR_ETFS[etf]}) — "
            f"{scores[etf]:.1%} 3m momentum, ${notional:.0f}"
        )
        broker.market_buy(etf, notional, STRATEGY_NAME)
        tag_symbol(etf, STRATEGY_NAME)
        log_trade(db_conn, STRATEGY_NAME, etf, "buy", 0, 0, 0, metadata={
            "notional": notional,
            "sector": SECTOR_ETFS[etf],
            "momentum": float(scores[etf]),
        })
        cash -= notional

    _last_rebalance = datetime.now()
    logger.info("[SR] Sector rotation rebalance complete")


def _check_stops(broker: AlpacaBroker, db_conn):
    """Enforce stop losses on sector positions between monthly rebalances."""
    for pos in broker.get_positions():
        if pos["strategy"] != STRATEGY_NAME:
            continue
        if pos["unrealized_pnl_pct"] <= -STOP_LOSS_PCT * 100:
            logger.info(f"[SR] STOP LOSS {pos['symbol']} @ {pos['unrealized_pnl_pct']:.1f}%")
            broker.close_position(pos["symbol"], STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, pos["symbol"], "sell_stop",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])
