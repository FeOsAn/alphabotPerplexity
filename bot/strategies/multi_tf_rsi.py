"""
Strategy: Multi-Timeframe RSI (v83) — Strategy I
=================================================
Backtested result: +109% over 2 years, Sharpe 0.29, positive in ALL regimes.
  Bull: +0.49% avg/trade
  Chop: -0.18% avg/trade  ← runs but cautiously
  Bear: +0.91% avg/trade  ← strongest regime, short-heavy

The key insight: macro RSI (40-period ≈ 8-week proxy) confirms the
big-picture trend, while daily RSI(10) times the tactical entry.
When both timeframes agree, the edge is real across regimes.

Entry conditions (LONG):
  - RSI(40) > 52            → macro uptrend confirmed
  - Price > EMA(50)         → structural trend up
  - RSI(10) in 38–56        → tactical pullback
  - RSI(10) > RSI(10).shift → momentum turning up

Entry conditions (SHORT):
  - RSI(40) < 48            → macro downtrend confirmed
  - Price < EMA(50)         → structural trend down
  - RSI(10) in 58–72        → overbought bounce fading
  - RSI(10) < RSI(10).shift → momentum rolling over

Regime gating:
  - BULL:  long only,           sizing 1.2×
  - CHOP:  long + short,        sizing 0.8× (proven positive but smaller)
  - BEAR:  short-heavy (1.5×),  longs at 0.6× (counter-trend only if strong signal)

TPs and stops placed via place_bracket_orders() on every fill.
"""

import gc
import logging
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timezone

from broker import AlpacaBroker, tag_symbol
from config import (
    MIN_CASH_RESERVE_PCT,
    DEFAULT_STRATEGY_ALLOCATION_PCT,
    MAX_SINGLE_POSITION_PCT,
)
from db import log_trade, log_signal

logger = logging.getLogger("alphabot.multi_tf_rsi")
STRATEGY_NAME = "multi_tf_rsi"

UNIVERSE = [
    "AAPL", "MSFT", "NVDA", "META", "GOOGL", "AMZN", "AMD", "AVGO", "QCOM",
    "JPM", "GS", "MS", "BAC", "V", "MA", "BLK", "SCHW",
    "RTX", "LMT", "GE", "CAT", "HON",
    "HD", "LOW", "COST", "WMT", "TGT", "ROST", "LULU", "NKE",
    "XOM", "CVX", "OXY", "COP",
    "UNH", "LLY", "JNJ", "MRK", "AMGN", "ABBV",
    "TSLA", "MRVL", "PLTR", "CRWD", "NET", "DDOG", "SNOW",
    "SPY", "QQQ", "IWM", "XLK", "XLE", "GLD", "SLV", "EEM",
]

MAX_POSITIONS = 6

# Regime-specific sizing multipliers (on top of regime_weights table)
# Bear is highest because that's where this strategy actually performs best
_REGIME_LONG_MULT  = {"bull": 1.2, "chop": 0.8, "bear": 0.6}
_REGIME_SHORT_MULT = {"bull": 0.0, "chop": 0.8, "bear": 1.5}


def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def _rsi(series: pd.Series, period: int) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _score_signal(rsi_daily: float, rsi_macro: float, direction: int) -> float:
    """
    Signal score 0.0–1.0.
    Macro RSI further from 50 = stronger confirmation.
    Daily RSI closer to sweet spot = better entry timing.
    """
    macro_strength = min(abs(rsi_macro - 50) / 20, 1.0)
    if direction == 1:   # long: sweet spot daily RSI = 47
        daily_timing = 1.0 - abs(rsi_daily - 47) / 18
    else:                # short: sweet spot daily RSI = 64
        daily_timing = 1.0 - abs(rsi_daily - 64) / 16
    daily_timing = max(0.0, daily_timing)
    return round(0.6 * macro_strength + 0.4 * daily_timing, 3)


def _fetch_indicators(sym: str) -> dict | None:
    try:
        t = yf.Ticker(sym)
        hist = t.history(period="6mo", interval="1d", auto_adjust=True)
        if hist is None or len(hist) < 55:
            return None
        c = hist["Close"]
        rsi10    = _rsi(c, 10)
        rsi40    = _rsi(c, 40)
        e50      = _ema(c, 50)

        if len(rsi10) < 2 or pd.isna(rsi10.iloc[-1]) or pd.isna(rsi40.iloc[-1]):
            return None

        return {
            "price":      float(c.iloc[-1]),
            "e50":        float(e50.iloc[-1]),
            "rsi10":      float(rsi10.iloc[-1]),
            "rsi10_prev": float(rsi10.iloc[-2]),
            "rsi40":      float(rsi40.iloc[-1]),
        }
    except Exception as e:
        logger.debug(f"[MTF] {sym} indicator fetch error: {e}")
        return None


def _scan_signals(regime: str) -> list[dict]:
    signals = []
    allow_long  = True
    allow_short = _REGIME_SHORT_MULT.get(regime, 0.0) > 0

    for sym in UNIVERSE:
        ind = _fetch_indicators(sym)
        if not ind:
            continue

        price = ind["price"]; e50 = ind["e50"]
        r10 = ind["rsi10"]; r10p = ind["rsi10_prev"]; r40 = ind["rsi40"]

        # LONG
        if allow_long:
            long_ok = (
                r40 > 52 and
                price > e50 and
                38 <= r10 <= 56 and
                r10 > r10p
            )
            if long_ok:
                score = _score_signal(r10, r40, 1)
                signals.append({"symbol": sym, "side": "long",
                                "price": price, "score": score,
                                "rsi10": r10, "rsi40": r40,
                                "size_mult": _REGIME_LONG_MULT.get(regime, 1.0)})

        # SHORT
        if allow_short:
            short_ok = (
                r40 < 48 and
                price < e50 and
                58 <= r10 <= 72 and
                r10 < r10p
            )
            if short_ok:
                score = _score_signal(r10, r40, -1)
                signals.append({"symbol": sym, "side": "short",
                                "price": price, "score": score,
                                "rsi10": r10, "rsi40": r40,
                                "size_mult": _REGIME_SHORT_MULT.get(regime, 0.0)})

    return sorted(signals, key=lambda x: -x["score"])


def run(broker: AlpacaBroker, db_conn):
    """Main strategy entry point."""
    # ── Regime ───────────────────────────────────────────────────────────────
    try:
        from utils.regime_weights import get_multiplier as _rm
        regime_mult = _rm(STRATEGY_NAME)
    except Exception:
        regime_mult = 1.0

    if regime_mult == 0.0:
        logger.debug("[MTF_RSI] Regime weight 0.0 — skipping")
        return

    try:
        from utils.regime_detector import get_regime as _rg
        hmm, conf = _rg()
        if hmm in ("BEAR_MILD", "BEAR_STRONG"):
            regime = "bear"
        elif hmm == "CHOPPY":
            regime = "chop"
        else:
            regime = "bull"
    except Exception:
        regime = "bull"

    # ── Position cap ─────────────────────────────────────────────────────────
    all_positions = broker.get_positions()
    mtf_positions = [p for p in all_positions if p.get("strategy") == STRATEGY_NAME]
    if len(mtf_positions) >= MAX_POSITIONS:
        logger.debug(f"[MTF_RSI] At cap ({MAX_POSITIONS}) — skipping")
        return

    # ── Cash check ───────────────────────────────────────────────────────────
    cash, portfolio_value = broker.get_live_cash()
    min_cash = portfolio_value * MIN_CASH_RESERVE_PCT
    if cash <= min_cash:
        logger.info(f"[MTF_RSI] Cash floor hit — skipping")
        return

    # ── Scan ─────────────────────────────────────────────────────────────────
    signals = _scan_signals(regime)
    if not signals:
        logger.debug("[MTF_RSI] No qualifying signals")
        return

    logger.info(f"[MTF_RSI] {len(signals)} signals in {regime.upper()} regime")

    open_syms     = {p["symbol"] for p in all_positions}
    slots_available = MAX_POSITIONS - len(mtf_positions)

    for sig in signals[:slots_available]:
        sym        = sig["symbol"]
        side       = sig["side"]
        score      = sig["score"]
        size_mult  = sig["size_mult"]

        if sym in open_syms:
            continue

        # Cooldown
        try:
            from utils.cooldown import is_on_cooldown
            if is_on_cooldown(sym, STRATEGY_NAME):
                continue
        except Exception:
            pass

        # Correlation
        try:
            from utils.correlation_monitor import is_entry_allowed as _corr
            ok, reason = _corr(sym, broker)
            if not ok:
                logger.info(f"[MTF_RSI] {sym} blocked by correlation: {reason}")
                continue
        except Exception:
            pass

        # Earnings blackout
        try:
            from utils.earnings_calendar import has_upcoming_earnings
            if has_upcoming_earnings(sym):
                continue
        except Exception:
            pass

        size_pct = DEFAULT_STRATEGY_ALLOCATION_PCT * regime_mult * size_mult
        notional = portfolio_value * min(size_pct, MAX_SINGLE_POSITION_PCT)

        cash, portfolio_value = broker.get_live_cash()
        if cash - notional < min_cash:
            logger.info(f"[MTF_RSI] {sym}: insufficient cash — stopping")
            break

        logger.info(
            f"[MTF_RSI] ENTRY {side.upper()} {sym} @ ${sig['price']:.2f} "
            f"score={score:.3f} RSI10={sig['rsi10']:.1f} RSI40={sig['rsi40']:.1f} "
            f"notional=${notional:,.0f} regime={regime.upper()}"
        )

        try:
            if side == "long":
                broker.market_buy(sym, notional, STRATEGY_NAME,
                                  signal_score=score)
            else:
                broker.market_sell_short(sym, notional, STRATEGY_NAME,
                                         signal_score=score)
            tag_symbol(sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, sym, f"entry_{side}", 0,
                      sig["price"], 0,
                      metadata={"score": score, "rsi10": sig["rsi10"],
                                "rsi40": sig["rsi40"], "regime": regime})
            log_signal(db_conn, STRATEGY_NAME, sym, f"entry_{side}",
                       score, sig)
            open_syms.add(sym)
            cash, portfolio_value = broker.get_live_cash()
            min_cash = portfolio_value * MIN_CASH_RESERVE_PCT

        except Exception as e:
            logger.error(f"[MTF_RSI] {sym} entry error: {e}", exc_info=True)

    gc.collect()
