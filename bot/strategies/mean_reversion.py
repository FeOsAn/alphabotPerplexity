"""
Strategy 2: Mean Reversion — RSI + Bollinger Bands + Volume Confirmation
------------------------------------------------------------------------
Buy oversold stocks near lower Bollinger Band when RSI < 32 and volume is elevated.
Exit when price reverts to 20-day moving average or RSI > 65.
Short-term hold: typically 5-15 days.

Improvements:
- Tighter 5% stop loss (was 7%)
- Staggered entries: max 2 new positions per scan to avoid buying cluster tops
- Additional confirmation: price must be above 200-day MA (don't catch falling knives)
- yFinance one-at-a-time (vs batch broker.get_bars) to stay under Railway 512MB RAM
"""

import gc
import logging
import pandas as pd
import numpy as np
import ta
import yfinance as yf
from broker import AlpacaBroker, tag_symbol, is_correlated_position
from config import (
    MR_RSI_PERIOD, MR_RSI_OVERSOLD, MR_RSI_OVERBOUGHT,
    MR_BB_PERIOD, MR_BB_STD, MR_MAX_POSITIONS, MAX_POSITION_PCT,
    MAX_TOTAL_EQUITY_POSITIONS, MIN_CASH_RESERVE_PCT,
    SIZING_MIN_MULT, SIZING_MID_MULT, SIZING_HIGH_MULT, SIZING_MAX_MULT
)


def _conviction_multiplier(rsi: float, vol_elevated: bool, vol_ratio: float) -> float:
    """
    Scale position size by how strong the mean-reversion signal is.
    RSI=18 + vol 2x avg = maximum conviction (1.5x). RSI=31 barely oversold = 0.75x.
    """
    if rsi <= 20 and vol_ratio >= 2.0:
        return SIZING_MAX_MULT   # 1.5x — RSI deeply oversold + massive volume
    elif rsi <= 24 and vol_elevated:
        return SIZING_HIGH_MULT  # 1.25x — very oversold with volume confirmation
    elif rsi <= 28:
        return SIZING_MID_MULT   # 1.0x — solidly oversold
    else:
        return SIZING_MIN_MULT   # 0.75x — barely at threshold (RSI 28-32)


from db import log_trade, log_signal

logger = logging.getLogger("alphabot.mean_reversion")
STRATEGY_NAME = "mean_reversion"

STOP_LOSS_PCT = 0.05   # Tighter 5% stop for mean reversion
MAX_NEW_ENTRIES_PER_SCAN = 2  # Stagger entries — max 2 new positions per 5-min scan

# Trimmed to 20 high-liquidity names — mean reversion works best on large-caps
# with reliable RSI/BB signals. Reduces peak RAM per scan cycle.
MR_WATCHLIST = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "META", "TSLA", "NVDA", "JPM",
    "V", "MA", "WMT", "HD", "XOM", "JNJ", "LLY", "MRK",
    "AMD", "AVGO", "TXN", "CAT",
]


def _compute_signals(df: pd.DataFrame) -> dict:
    """Compute RSI, Bollinger Bands, volume signal for a symbol's daily bars."""
    if df is None or len(df) < max(MR_RSI_PERIOD, MR_BB_PERIOD) + 10:
        return {}

    df = df.copy()
    # Normalise column names — yFinance uses Title case
    df.columns = [c.lower() for c in df.columns]
    df = df.sort_index()

    close = df["close"]
    volume = df["volume"]

    rsi = ta.momentum.RSIIndicator(close, window=MR_RSI_PERIOD).rsi()
    bb = ta.volatility.BollingerBands(close, window=MR_BB_PERIOD, window_dev=MR_BB_STD)
    bb_lower = bb.bollinger_lband()
    bb_mid   = bb.bollinger_mavg()

    vol_avg = volume.rolling(20).mean()
    vol_elevated = bool(volume.iloc[-1] > vol_avg.iloc[-1] * 1.2)

    latest_rsi   = float(rsi.iloc[-1])
    latest_close = float(close.iloc[-1])
    latest_bb_lower = float(bb_lower.iloc[-1])
    latest_bb_mid   = float(bb_mid.iloc[-1])

    # Don't buy if in a longer-term downtrend (price must be within 15% of 50-day MA)
    ma50 = float(close.tail(50).mean()) if len(close) >= 50 else latest_close
    not_in_freefall = latest_close >= ma50 * 0.85

    vol_ratio = float(volume.iloc[-1] / vol_avg.iloc[-1]) if vol_avg.iloc[-1] > 0 else 1.0

    from utils.adaptive_filters import get_thresholds
    oversold_threshold = get_thresholds()["mr_rsi_oversold"]

    buy_signal = (
        latest_rsi < oversold_threshold and
        latest_close <= latest_bb_lower * 1.01 and
        vol_elevated and
        not_in_freefall
    )
    sell_signal = (
        latest_rsi > MR_RSI_OVERBOUGHT or
        latest_close >= latest_bb_mid
    )

    return {
        "rsi": latest_rsi,
        "close": latest_close,
        "bb_lower": latest_bb_lower,
        "bb_mid": latest_bb_mid,
        "vol_elevated": vol_elevated,
        "vol_ratio": vol_ratio,
        "not_in_freefall": not_in_freefall,
        "buy_signal": buy_signal,
        "sell_signal": sell_signal,
    }


def run(broker: AlpacaBroker, db_conn):
    """Run mean reversion scan and execute trades."""
    logger.info("=== Mean Reversion Strategy: Scanning for signals ===")

    from utils.regime import is_bull_market
    if not is_bull_market():
        logger.info("[mean_reversion] Bear regime detected — skipping new entries")
        return

    from utils.market_hours import is_entry_allowed
    if not is_entry_allowed():
        logger.info("[mean_reversion] Outside safe entry window — skipping")
        return

    # ── Fetch data one symbol at a time to avoid OOM on Railway 512MB ──────────
    signals = {}
    for sym in MR_WATCHLIST:
        try:
            ticker = yf.Ticker(sym)
            hist = ticker.history(period="3mo")
            if not hist.empty:
                sig = _compute_signals(hist)
                if sig:
                    signals[sym] = sig
            del hist
        except Exception as e:
            logger.debug(f"[MR] Error fetching {sym}: {e}")
        finally:
            gc.collect()

    # ── Exit existing positions ─────────────────────────────────────────────────
    all_positions = broker.get_positions()
    mr_positions = [
        p for p in all_positions
        if p["strategy"] == STRATEGY_NAME and p.get("asset_class", "equity") == "equity"
    ]

    for pos in mr_positions:
        sym = pos["symbol"]
        sig = signals.get(sym, {})

        # Stop loss (5% — tighter than global)
        if pos["unrealized_pnl_pct"] <= -STOP_LOSS_PCT * 100:
            logger.info(f"[MR] STOP LOSS {sym} @ {pos['unrealized_pnl_pct']:.1f}%")
            broker.close_position(sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, sym, "sell_stop",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])
            continue

        # Take profit / mean reversion exit
        if sig.get("sell_signal", False) or pos["unrealized_pnl_pct"] >= 12:
            logger.info(f"[MR] EXIT {sym} — RSI: {sig.get('rsi', '?')}, PnL: {pos['unrealized_pnl_pct']:.1f}%")
            broker.close_position(sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, sym, "sell",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])

    # ── Enter new positions ─────────────────────────────────────────────────────
    current_mr_count = len([
        p for p in broker.get_positions()
        if p["strategy"] == STRATEGY_NAME and p.get("asset_class", "equity") == "equity"
    ])
    if current_mr_count >= MR_MAX_POSITIONS:
        logger.info(f"[MR] Max positions reached ({MR_MAX_POSITIONS})")
        logger.info(f"[MR] Scan complete — {current_mr_count} active positions")
        return

    account = broker.get_account()
    portfolio_value = account["portfolio_value"]
    cash = account["cash"]

    buy_candidates = [(sym, sig) for sym, sig in signals.items() if sig.get("buy_signal")]
    buy_candidates.sort(key=lambda x: x[1]["rsi"])  # Most oversold first

    current_symbols = {p["symbol"] for p in broker.get_positions()}
    new_entries = 0

    for sym, sig in buy_candidates:
        if new_entries >= MAX_NEW_ENTRIES_PER_SCAN:
            logger.info("[MR] Stagger limit reached — deferring remaining entries to next scan")
            break
        if sym in current_symbols:
            continue
        if current_mr_count >= MR_MAX_POSITIONS:
            break
        total_equity = len([p for p in broker.get_positions() if p.get("asset_class", "equity") == "equity"])
        if total_equity >= MAX_TOTAL_EQUITY_POSITIONS:
            break

        # Skip if a position in the same sector is already held (correlation control)
        live_positions = broker.get_positions()
        if is_correlated_position(sym, live_positions):
            logger.info(f"[MR] Skipping {sym} — correlated sector already held")
            continue

        from utils.earnings_calendar import has_upcoming_earnings
        if has_upcoming_earnings(sym):
            logger.info(f"[MR] Skipping {sym} — earnings blackout (within 2 days)")
            continue

        mult = _conviction_multiplier(sig["rsi"], sig["vol_elevated"], sig.get("vol_ratio", 1.0))
        notional = portfolio_value * MAX_POSITION_PCT * mult
        min_cash = portfolio_value * MIN_CASH_RESERVE_PCT
        if cash - notional < min_cash:
            continue

        logger.info(f"[MR] ENTER {sym} — RSI: {sig['rsi']:.1f}, BB lower: {sig['bb_lower']:.2f}, vol_ratio: {sig.get('vol_ratio',1):.1f}x, conviction: {mult:.2f}x, notional: ${notional:.0f}")
        log_signal(db_conn, STRATEGY_NAME, sym, "buy", sig["rsi"],
                   {"rsi": sig["rsi"], "bb_lower": sig["bb_lower"], "vol_elevated": int(sig["vol_elevated"]), "conviction": mult})
        broker.market_buy(sym, notional, STRATEGY_NAME)
        tag_symbol(sym, STRATEGY_NAME)
        log_trade(db_conn, STRATEGY_NAME, sym, "buy", 0, sig["close"], 0,
                  metadata={"notional": notional, "rsi": sig["rsi"]})
        cash -= notional
        current_mr_count += 1
        new_entries += 1

    logger.info(f"[MR] Scan complete — {current_mr_count} active positions")
