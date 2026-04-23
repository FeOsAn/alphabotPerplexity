"""
Strategy 3: Trend Following — EMA Crossover + Volume + VIX Regime Filter
------------------------------------------------------------------------
Enter long when EMA9 crosses above EMA21, price above both EMAs, volume confirms.
VIX filter: pause entries when VIX > 35 (market fear / crisis regime).
Exit when EMA9 crosses back below EMA21 or stop loss triggered.

Works best in trending bull markets; automatically sits out panic regimes.
"""

import logging
import pandas as pd
import numpy as np
import ta
from broker import AlpacaBroker, tag_symbol
from config import (
    UNIVERSE, TREND_FAST_EMA, TREND_SLOW_EMA, TREND_VIX_MAX,
    TREND_MAX_POSITIONS, MAX_POSITION_PCT, MAX_TOTAL_POSITIONS,
    STOP_LOSS_PCT, TAKE_PROFIT_PCT, MIN_CASH_RESERVE_PCT
)
from db import log_trade, log_signal

logger = logging.getLogger("alphabot.trend_following")
STRATEGY_NAME = "trend_following"

TREND_WATCHLIST = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO",
    "NFLX", "AMD", "CRM", "ADBE", "ORCL", "NOW", "PANW", "MRVL",
    "AMAT", "LRCX", "KLAC", "MU", "ISRG", "VRTX", "REGN", "AMGN",
    "BKNG", "SYK", "BSX", "V", "MA", "SPGI", "CME", "ICE",
    "DE", "CAT", "HON", "GE", "XOM", "COP", "EOG", "SLB",
]


def _get_vix(broker: AlpacaBroker) -> float:
    """Get current VIX level as market regime filter."""
    try:
        bars = broker.get_bars(["VIXY"], days=5)  # VIX proxy ETF
        if "VIXY" in bars and not bars["VIXY"].empty:
            # Use VIXY as VIX proxy (not perfect but good enough for regime filter)
            return float(bars["VIXY"]["close"].iloc[-1])
    except Exception:
        pass
    return 20.0  # Default to neutral if unavailable


def _compute_signals(df: pd.DataFrame) -> dict:
    """EMA crossover with volume confirmation."""
    if df is None or len(df) < TREND_SLOW_EMA + 10:
        return {}

    df = df.sort_index().copy()
    close = df["close"]
    volume = df["volume"]

    ema_fast = ta.trend.EMAIndicator(close, window=TREND_FAST_EMA).ema_indicator()
    ema_slow = ta.trend.EMAIndicator(close, window=TREND_SLOW_EMA).ema_indicator()

    # Volume filter
    vol_avg = volume.rolling(20).mean()
    vol_ok = volume.iloc[-1] > vol_avg.iloc[-1] * 0.8  # At least 80% of avg volume

    # Crossover detection: EMA fast crossed above EMA slow in last 3 days
    recent_cross = False
    for i in range(-3, 0):
        if ema_fast.iloc[i-1] <= ema_slow.iloc[i-1] and ema_fast.iloc[i] > ema_slow.iloc[i]:
            recent_cross = True
            break

    # Price above both EMAs (trend confirmation)
    price_above_trend = close.iloc[-1] > ema_fast.iloc[-1] and close.iloc[-1] > ema_slow.iloc[-1]

    # Exit: EMA fast crossed below EMA slow
    exit_cross = ema_fast.iloc[-1] < ema_slow.iloc[-1]

    # Trend strength: slope of slow EMA over 5 days
    slope = (ema_slow.iloc[-1] - ema_slow.iloc[-6]) / ema_slow.iloc[-6] if len(ema_slow) > 6 else 0

    buy_signal = recent_cross and price_above_trend and vol_ok and slope > 0

    return {
        "close": float(close.iloc[-1]),
        "ema_fast": float(ema_fast.iloc[-1]),
        "ema_slow": float(ema_slow.iloc[-1]),
        "vol_ok": vol_ok,
        "recent_cross": recent_cross,
        "price_above_trend": price_above_trend,
        "exit_cross": exit_cross,
        "slope": float(slope),
        "buy_signal": buy_signal,
    }


def run(broker: AlpacaBroker, db_conn):
    """Run trend following strategy."""
    logger.info("=== Trend Following Strategy: Scanning signals ===")

    # VIX regime filter
    vix = _get_vix(broker)
    if vix > TREND_VIX_MAX:
        logger.info(f"[TF] VIX={vix:.1f} above threshold ({TREND_VIX_MAX}). Regime filter active — no new entries.")
        _check_exits_and_stops(broker, db_conn, {})
        return

    bars = broker.get_bars(TREND_WATCHLIST, days=60)
    signals = {sym: _compute_signals(df) for sym, df in bars.items() if df is not None}

    # --- Exit positions ---
    _check_exits_and_stops(broker, db_conn, signals)

    # --- Enter new positions ---
    current_tf_count = len([p for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME])
    if current_tf_count >= TREND_MAX_POSITIONS:
        logger.info(f"[TF] Max positions reached ({TREND_MAX_POSITIONS})")
        return

    account = broker.get_account()
    portfolio_value = account["portfolio_value"]
    cash = account["cash"]

    buy_candidates = [
        (sym, sig) for sym, sig in signals.items()
        if sig.get("buy_signal") and sig.get("slope", 0) > 0
    ]
    # Sort by strongest trend slope
    buy_candidates.sort(key=lambda x: x[1]["slope"], reverse=True)

    current_symbols = {p["symbol"] for p in broker.get_positions()}

    for sym, sig in buy_candidates:
        if sym in current_symbols:
            continue
        if current_tf_count >= TREND_MAX_POSITIONS:
            break
        if len(broker.get_positions()) >= MAX_TOTAL_POSITIONS:
            break

        notional = portfolio_value * MAX_POSITION_PCT
        min_cash = portfolio_value * MIN_CASH_RESERVE_PCT
        if cash - notional < min_cash:
            continue

        logger.info(f"[TF] ENTER {sym} — EMA cross, slope={sig['slope']:.4f}, VIX={vix:.1f}")
        log_signal(db_conn, STRATEGY_NAME, sym, "buy", sig["slope"],
                   {"ema_fast": sig["ema_fast"], "ema_slow": sig["ema_slow"], "vix": vix})
        order = broker.market_buy(sym, notional, STRATEGY_NAME)
        tag_symbol(sym, STRATEGY_NAME)
        log_trade(db_conn, STRATEGY_NAME, sym, "buy", 0, sig["close"], 0,
                  metadata={"notional": notional, "slope": sig["slope"], "vix": vix})
        cash -= notional
        current_tf_count += 1

    logger.info(f"[TF] Scan complete — {current_tf_count} active positions")


def _check_exits_and_stops(broker: AlpacaBroker, db_conn, signals: dict):
    all_positions = broker.get_positions()
    tf_positions = [p for p in all_positions if p["strategy"] == STRATEGY_NAME]

    for pos in tf_positions:
        sym = pos["symbol"]
        sig = signals.get(sym, {})

        # Stop loss
        if pos["unrealized_pnl_pct"] <= -STOP_LOSS_PCT * 100:
            logger.info(f"[TF] STOP LOSS {sym} @ {pos['unrealized_pnl_pct']:.1f}%")
            broker.close_position(sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, sym, "sell_stop",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])
            continue

        # Take profit
        if pos["unrealized_pnl_pct"] >= TAKE_PROFIT_PCT * 100:
            logger.info(f"[TF] TAKE PROFIT {sym} @ {pos['unrealized_pnl_pct']:.1f}%")
            broker.close_position(sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, sym, "sell_tp",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])
            continue

        # Trend reversal exit
        if sig.get("exit_cross", False):
            logger.info(f"[TF] TREND EXIT {sym} — EMA crossed below")
            broker.close_position(sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, sym, "sell",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])
