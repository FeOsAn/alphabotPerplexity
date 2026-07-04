"""
Strategy 2: Mean Reversion — RSI + Bollinger Bands + Volume Confirmation
------------------------------------------------------------------------
Buy oversold stocks near lower Bollinger Band when RSI < 25 and volume is elevated.
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
import pandas_ta as _pta
import yfinance as yf
from broker import AlpacaBroker, tag_symbol, is_correlated_position
from config import (
    MR_RSI_PERIOD, MR_RSI_OVERSOLD, MR_RSI_OVERBOUGHT,
    MR_BB_PERIOD, MR_BB_STD, MR_MAX_POSITIONS,
    MIN_CASH_RESERVE_PCT, DEFAULT_STRATEGY_ALLOCATION_PCT, MAX_SINGLE_POSITION_PCT,
)


def _conviction_multiplier(rsi: float, vol_elevated: bool, vol_ratio: float) -> float:
    """
    Scale position size by signal strength. v100: rescaled for RSI(2) — the old
    thresholds (20/24/28) were RSI(14)-scale, and with RSI(2)<10 entries every
    trade hit the max-conviction branch (silent 1.25-1.5x oversizing on an
    untested dimension). The validated backtest sized flat, so tiers are modest.
    """
    if rsi <= 2.0 and vol_ratio >= 2.0:
        return 1.25  # pinned RSI(2) + heavy volume — sharpest snaps
    elif rsi <= 5.0:
        return 1.0   # solidly oversold on RSI(2)
    else:
        return 0.85  # barely under the 10 threshold


from db import log_trade, log_signal

logger = logging.getLogger("alphabot.mean_reversion")
STRATEGY_NAME = "mean_reversion"

# ── New-entry switch ──────────────────────────────────────────────────────────
# RE-ENABLED (v100): the OLD RSI(14)<oversold + BB-lower signal was barely +EV
# (+0.04%/trade, PF 1.03) — worst vs baseline — so it was disabled. The signal is
# now Connors RSI(2)<10 in an uptrend (see _compute_signals): +0.35%/trade, 67%
# win, PF 1.35, robust across all sub-periods (backtests/rsi2_validate.py). As a
# counter-trend sleeve it also diversifies the (correlated) momentum book.
ENABLE_NEW_ENTRIES = True

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
    """Connors RSI(2) mean reversion (v100).

    backtests/rsi2_validate.py: RSI(2)<10 in an uptrend (close>MA200) with a quick
    snap-back exit (close>MA5) is +0.35%/trade, 67% win, PF 1.35, robust across
    2015-18/19-22/23-26 — vs the old RSI(14)<oversold + BB-lower version at
    +0.04%/trade (PF 1.03). Needs >=200 bars for the MA200 trend filter.
    """
    if df is None or len(df) < 210:
        return {}

    df = df.copy()
    # Normalise column names — yFinance uses Title case
    df.columns = [c.lower() for c in df.columns]
    df = df.sort_index()

    close = df["close"]
    volume = df["volume"]

    rsi2 = _pta.rsi(close, length=2)
    _bb = _pta.bbands(close, length=MR_BB_PERIOD, std=float(MR_BB_STD))
    bb_lower = _bb[[c for c in _bb.columns if c.startswith('BBL_')][0]]
    bb_mid   = _bb[[c for c in _bb.columns if c.startswith('BBM_')][0]]
    bb_upper = _bb[[c for c in _bb.columns if c.startswith('BBU_')][0]]

    ma200 = float(close.tail(200).mean())
    ma5   = float(close.tail(5).mean())

    vol_avg = volume.rolling(20).mean()
    vol_last = volume.iloc[-2] if len(volume) >= 2 else volume.iloc[-1]
    vol_avg_last = vol_avg.iloc[-2] if len(vol_avg) >= 2 else vol_avg.iloc[-1]
    vol_elevated = bool(vol_last > vol_avg_last * 1.2)

    latest_rsi   = float(rsi2.iloc[-1])          # RSI(2), not RSI(14)
    latest_close = float(close.iloc[-1])
    latest_bb_lower = float(bb_lower.iloc[-1])
    latest_bb_mid   = float(bb_mid.iloc[-1])
    latest_bb_upper = float(bb_upper.iloc[-1])
    bb_width = (latest_bb_upper - latest_bb_lower) / latest_bb_mid if latest_bb_mid > 0 else 0.0

    # Uptrend filter: only buy oversold dips ABOVE the 200-day MA.
    not_in_freefall = latest_close > ma200

    vol_ratio = float(vol_last / vol_avg_last) if vol_avg_last and vol_avg_last > 0 else 1.0
    oversold_threshold = 10.0

    # Entry: RSI(2) < 10 in an uptrend. Exit: quick snap back above the 5-day MA
    # (or RSI(2) overbought). Hard 5% stop is enforced in run().
    buy_signal = (latest_rsi < oversold_threshold and latest_close > ma200)
    sell_signal = (latest_close > ma5 or latest_rsi > 70.0)

    return {
        "rsi": latest_rsi,
        "close": latest_close,
        "bb_lower": latest_bb_lower,
        "bb_mid": latest_bb_mid,
        "bb_width": bb_width,
        "vol_elevated": vol_elevated,
        "vol_ratio": vol_ratio,
        "not_in_freefall": not_in_freefall,
        "buy_signal": buy_signal,
        "sell_signal": sell_signal,
        "oversold_threshold": oversold_threshold,
    }


def run(broker: AlpacaBroker, db_conn):
    """Run mean reversion scan and execute trades."""
    logger.info("=== Mean Reversion Strategy: Scanning for signals ===")

    # v83: mean reversion runs in ALL regimes (1.5× in chop, 1.2× in bear)
    # Only hard-skip if regime_weight is explicitly 0.0
    try:
        from utils.regime_weights import get_multiplier as _rm
        if _rm("mean_reversion") == 0.0:
            logger.info("[mean_reversion] Regime weight 0.0 — skipping")
            return
    except Exception:
        pass  # always run mean reversion unless explicitly blocked

    from utils.market_hours import is_entry_allowed
    if not is_entry_allowed():
        logger.info("[mean_reversion] Outside safe entry window — skipping")
        return

    # ── Fetch data one symbol at a time to avoid OOM on Railway 512MB ──────────
    signals = {}
    for sym in MR_WATCHLIST:
        try:
            ticker = yf.Ticker(sym)
            hist = ticker.history(period="12mo")  # v100: need >=200 bars for MA200 filter
            if not hist.empty:
                sig = _compute_signals(hist)
                if sig:
                    signals[sym] = sig
            del hist
        except Exception as e:
            logger.debug(f"[MR] Error fetching {sym}: {e}")
        finally:
            pass

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
            from utils.cooldown import set_cooldown
            set_cooldown(sym)
            continue

        # Take profit / mean reversion exit
        if sig.get("sell_signal", False) or pos["unrealized_pnl_pct"] >= 12:
            logger.info(f"[MR] EXIT {sym} — RSI: {sig.get('rsi', '?')}, PnL: {pos['unrealized_pnl_pct']:.1f}%")
            broker.close_position(sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, sym, "sell",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])
            from utils.cooldown import set_cooldown
            set_cooldown(sym)
            logger.info(f"[MR] {sym} cooldown set after exit")

    # Backtest-driven kill switch (see ENABLE_NEW_ENTRIES note). Exits above still
    # run every cycle; only new entries are gated off.
    if not ENABLE_NEW_ENTRIES:
        logger.info("[MR] New entries disabled (backtest: no edge vs baseline) — exits only")
        return

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

    # Hoist all broker.get_positions() calls out of the loop (QW4) — n+1 query before
    cached_positions = broker.get_positions()
    current_symbols = {p["symbol"] for p in cached_positions}
    new_entries = 0

    for sym, sig in buy_candidates:
        if new_entries >= MAX_NEW_ENTRIES_PER_SCAN:
            logger.info("[MR] Stagger limit reached — deferring remaining entries to next scan")
            break
        if sym in current_symbols:
            continue
        from utils.cooldown import is_on_cooldown
        if is_on_cooldown(sym):
            logger.debug(f"[STRATEGY] {sym} on cooldown — skipping")
            continue
        if current_mr_count >= MR_MAX_POSITIONS:
            break

        # Skip if a position in the same sector is already held (correlation control)
        if is_correlated_position(sym, cached_positions):
            logger.info(f"[MR] Skipping {sym} — correlated sector already held")
            continue

        from utils.earnings_calendar import has_upcoming_earnings
        if has_upcoming_earnings(sym):
            logger.info(f"[MR] Skipping {sym} — earnings blackout (within 2 days)")
            continue

        # v100: bb_width gate removed — it was tuned for the old BB-lower entry and
        # is an untested extra filter on the validated RSI(2) signal (the backtest
        # that proved +0.35%/trade had no such gate). Keeping live == backtested.

        mult = _conviction_multiplier(sig["rsi"], sig["vol_elevated"], sig.get("vol_ratio", 1.0))
        size_pct = min(DEFAULT_STRATEGY_ALLOCATION_PCT * mult, MAX_SINGLE_POSITION_PCT)
        notional = portfolio_value * size_pct
        min_cash = portfolio_value * MIN_CASH_RESERVE_PCT
        if cash - notional < min_cash:
            continue

        if sig["rsi"] < MR_RSI_OVERSOLD:
            logger.info(f"[MR] {sym}: RSI={sig['rsi']:.1f} — EXTREME OVERSOLD signal (institutional-grade reversal, <{sig.get('oversold_threshold', MR_RSI_OVERSOLD)} adaptive threshold)")
        logger.info(f"[MR] ENTER {sym} — RSI: {sig['rsi']:.1f}, BB lower: {sig['bb_lower']:.2f}, vol_ratio: {sig.get('vol_ratio',1):.1f}x, conviction: {mult:.2f}x, notional: ${notional:.0f}")
        log_signal(db_conn, STRATEGY_NAME, sym, "buy", sig["rsi"],
                   {"rsi": sig["rsi"], "bb_lower": sig["bb_lower"], "vol_elevated": int(sig["vol_elevated"]), "conviction": mult})
        broker.market_buy(sym, notional, STRATEGY_NAME)
        tag_symbol(sym, STRATEGY_NAME)
        log_trade(db_conn, STRATEGY_NAME, sym, "buy", 0, sig["close"], 0,
                  metadata={"notional": notional, "rsi": sig["rsi"]})
        cash, portfolio_value = broker.get_live_cash()
        if cash < 0:
            logger.critical(f"[{STRATEGY_NAME}] Cash went negative (${cash:,.0f}) — halting entries")
            from utils.notify import emergency as _notify_emergency
            _notify_emergency("🚨 Cash went negative", f"[mean_reversion] cash ${cash:,.0f} — halting entries", key="negative_cash_mean_reversion")
            break
        if cash < portfolio_value * MIN_CASH_RESERVE_PCT:
            logger.warning(f"[{STRATEGY_NAME}] Cash floor hit (${cash:,.0f}) — halting entries")
            break
        current_mr_count += 1
        new_entries += 1

    logger.info(f"[MR] Scan complete — {current_mr_count} active positions")
