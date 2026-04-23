"""
Strategy 2: Mean Reversion — RSI + Bollinger Bands + Volume Confirmation
------------------------------------------------------------------------
Buy oversold stocks near lower Bollinger Band when RSI < 32 and volume is elevated.
Exit when price reverts to 20-day moving average or RSI > 65.
Short-term hold: typically 5-15 days.

Works best in range-bound markets; VIX-gated to avoid trending/crisis periods.
"""

import logging
import pandas as pd
import numpy as np
import ta
from broker import AlpacaBroker, tag_symbol
from config import (
    UNIVERSE, MR_RSI_PERIOD, MR_RSI_OVERSOLD, MR_RSI_OVERBOUGHT,
    MR_BB_PERIOD, MR_BB_STD, MR_MAX_POSITIONS, MAX_POSITION_PCT,
    MAX_TOTAL_EQUITY_POSITIONS, STOP_LOSS_PCT, MIN_CASH_RESERVE_PCT
)
from db import log_trade, log_signal

logger = logging.getLogger("alphabot.mean_reversion")
STRATEGY_NAME = "mean_reversion"

# Scan a focused watchlist within the universe for mean reversion setups
MR_WATCHLIST = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "META", "TSLA", "NVDA", "JPM",
    "V", "MA", "BAC", "GS", "MS", "WMT", "HD", "MCD", "COST", "PEP",
    "KO", "XOM", "CVX", "COP", "JNJ", "PG", "ABBV", "LLY", "MRK",
    "NFLX", "ADBE", "CRM", "ORCL", "INTC", "AMD", "AVGO", "TXN",
    "CAT", "DE", "GE", "HON", "UPS", "RTX", "LMT", "SPGI", "BLK",
]


def _compute_signals(df: pd.DataFrame) -> dict:
    """Compute RSI, Bollinger Bands, volume signal for a symbol's daily bars."""
    if df is None or len(df) < max(MR_RSI_PERIOD, MR_BB_PERIOD) + 10:
        return {}

    df = df.sort_index().copy()
    close = df["close"]
    volume = df["volume"]

    # RSI
    rsi = ta.momentum.RSIIndicator(close, window=MR_RSI_PERIOD).rsi()

    # Bollinger Bands
    bb = ta.volatility.BollingerBands(close, window=MR_BB_PERIOD, window_dev=MR_BB_STD)
    bb_lower = bb.bollinger_lband()
    bb_upper = bb.bollinger_hband()
    bb_mid = bb.bollinger_mavg()

    # Volume: is today's volume > 1.2x 20-day average?
    vol_avg = volume.rolling(20).mean()
    vol_elevated = volume.iloc[-1] > vol_avg.iloc[-1] * 1.2

    latest_rsi = rsi.iloc[-1]
    latest_close = close.iloc[-1]
    latest_bb_lower = bb_lower.iloc[-1]
    latest_bb_upper = bb_upper.iloc[-1]
    latest_bb_mid = bb_mid.iloc[-1]

    buy_signal = (
        latest_rsi < MR_RSI_OVERSOLD and
        latest_close <= latest_bb_lower * 1.01 and  # At or just above lower band
        vol_elevated
    )
    sell_signal = (
        latest_rsi > MR_RSI_OVERBOUGHT or
        latest_close >= latest_bb_mid
    )

    return {
        "rsi": float(latest_rsi),
        "close": float(latest_close),
        "bb_lower": float(latest_bb_lower),
        "bb_upper": float(latest_bb_upper),
        "bb_mid": float(latest_bb_mid),
        "vol_elevated": vol_elevated,
        "buy_signal": buy_signal,
        "sell_signal": sell_signal,
    }


def run(broker: AlpacaBroker, db_conn):
    """Run mean reversion scan and execute trades."""
    logger.info("=== Mean Reversion Strategy: Scanning for signals ===")

    bars = broker.get_bars(MR_WATCHLIST, days=60)
    signals = {}
    for sym in MR_WATCHLIST:
        df = bars.get(sym)
        sig = _compute_signals(df)
        if sig:
            signals[sym] = sig

    # --- Exit existing positions where exit signal triggered (equity only) ---
    all_positions = broker.get_positions()
    mr_positions = [p for p in all_positions if p["strategy"] == STRATEGY_NAME and p.get("asset_class", "equity") == "equity"]

    for pos in mr_positions:
        sym = pos["symbol"]
        sig = signals.get(sym, {})

        # Stop loss
        if pos["unrealized_pnl_pct"] <= -STOP_LOSS_PCT * 100:
            logger.info(f"[MR] STOP LOSS {sym} @ {pos['unrealized_pnl_pct']:.1f}%")
            broker.close_position(sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, sym, "sell_stop",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])
            continue

        # Take profit / mean reversion exit
        if sig.get("sell_signal", False) or pos["unrealized_pnl_pct"] >= 12:
            logger.info(f"[MR] EXIT {sym} — RSI: {sig.get('rsi', '?'):.1f}, PnL: {pos['unrealized_pnl_pct']:.1f}%")
            broker.close_position(sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, sym, "sell",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])

    # --- Enter new positions where buy signal triggered ---
    current_mr_count = len([p for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME and p.get("asset_class", "equity") == "equity"])
    if current_mr_count >= MR_MAX_POSITIONS:
        logger.info(f"[MR] Max positions reached ({MR_MAX_POSITIONS})")
        return

    account = broker.get_account()
    portfolio_value = account["portfolio_value"]
    cash = account["cash"]

    buy_candidates = [(sym, sig) for sym, sig in signals.items() if sig.get("buy_signal")]
    # Sort by deepest RSI oversold (most extreme = highest conviction)
    buy_candidates.sort(key=lambda x: x[1]["rsi"])

    current_symbols = {p["symbol"] for p in broker.get_positions()}

    for sym, sig in buy_candidates:
        if sym in current_symbols:
            continue
        if current_mr_count >= MR_MAX_POSITIONS:
            break
        total_equity = len([p for p in broker.get_positions() if p.get("asset_class", "equity") == "equity"])
        if total_equity >= MAX_TOTAL_EQUITY_POSITIONS:
            break

        notional = portfolio_value * MAX_POSITION_PCT
        min_cash = portfolio_value * MIN_CASH_RESERVE_PCT
        if cash - notional < min_cash:
            continue

        logger.info(f"[MR] ENTER {sym} — RSI: {sig['rsi']:.1f}, BB lower: {sig['bb_lower']:.2f}")
        log_signal(db_conn, STRATEGY_NAME, sym, "buy", sig["rsi"],
                   {"rsi": sig["rsi"], "bb_lower": sig["bb_lower"], "vol_elevated": sig["vol_elevated"]})
        order = broker.market_buy(sym, notional, STRATEGY_NAME)
        tag_symbol(sym, STRATEGY_NAME)
        log_trade(db_conn, STRATEGY_NAME, sym, "buy", 0, sig["close"], 0,
                  metadata={"notional": notional, "rsi": sig["rsi"]})
        cash -= notional
        current_mr_count += 1

    logger.info(f"[MR] Scan complete — {current_mr_count} active positions")
