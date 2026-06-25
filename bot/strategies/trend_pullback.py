"""
Strategy: Trend Pullback (v83) — Strategy B+G combined
=======================================================
Backtested result: Sharpe 3.13, win rate 59%, +45.7% in bull month.
With ADX gate: regime-robust, avoids choppy bleed.

Entry conditions (LONG):
  - Price above MA(50)          → confirmed uptrend (v90: was EMA20; MA50
                                   pullbacks are where institutions re-enter)
  - +DI > -DI                   → bullish directional bias
  - ADX(14) > 20                → trend is STRONG, not drifting
  - RSI(10) in 38–55            → pullback within trend (not breakdown)
  - RSI(10) > RSI(10).shift(1)  → momentum turning back up

Entry conditions (SHORT — bear regime only):
  - Price below MA(50)
  - -DI > +DI
  - ADX(14) > 20
  - RSI(10) in 55–70
  - RSI(10) < RSI(10).shift(1)  → momentum rolling over

Hold: time-stop at 20 trading days (v90: was 15).

Regime gating:
  - BULL: long only, sizing 1.5×
  - BEAR: both long (on counter-trend bounces) and short, sizing 0.8×
  - CHOP: SKIP — backtested -0.42% avg per trade in choppy markets

TPs and stops placed via place_bracket_orders() — same as all other strategies.
"""

import gc
import logging
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timezone, timedelta

from broker import AlpacaBroker, tag_symbol
from config import (
    MIN_CASH_RESERVE_PCT,
    DEFAULT_STRATEGY_ALLOCATION_PCT,
    MAX_SINGLE_POSITION_PCT,
)
from db import log_trade, log_signal, get_state, set_state

logger = logging.getLogger("alphabot.trend_pullback")
STRATEGY_NAME = "trend_pullback"

# Universe — broad liquid equities, same as momentum
UNIVERSE = [
    "AAPL", "MSFT", "NVDA", "META", "GOOGL", "AMZN", "AMD", "AVGO", "QCOM",
    "JPM", "GS", "MS", "BAC", "V", "MA",
    "RTX", "LMT", "GE", "CAT", "HON",
    "HD", "LOW", "COST", "WMT", "TGT", "ROST", "LULU", "NKE",
    "XOM", "CVX", "OXY", "COP", "SLB",
    "UNH", "LLY", "JNJ", "MRK", "AMGN", "ABBV",
    "TSLA", "MRVL", "PLTR", "CRWD", "NET", "DDOG", "SNOW",
    "SPY", "QQQ", "IWM", "XLK", "XLE", "GLD", "SLV",
]

# Position sizing by regime
REGIME_SIZE = {
    "bull": 1.5,
    "bear": 0.8,
    "chop": 0.0,   # blocked
}
BASE_ALLOC_PCT = DEFAULT_STRATEGY_ALLOCATION_PCT   # 5% of portfolio per position
MAX_POSITIONS  = 5    # max concurrent trend_pullback positions
MA_TREND_SPAN  = 50   # v90: trend/pullback reference MA (was 20)
MAX_HOLD_DAYS  = 20   # v90: time-stop at 20 trading days (was 15)


def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def _rsi(series: pd.Series, period: int = 10) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _adx(high: pd.Series, low: pd.Series, close: pd.Series,
         period: int = 14) -> tuple:
    """Returns (adx, +DI, -DI)."""
    up   = high - high.shift(1)
    dn   = low.shift(1) - low
    pdm  = up.where((up > dn) & (up > 0), 0.0)
    ndm  = dn.where((dn > up) & (dn > 0), 0.0)
    tr   = pd.concat(
        [high - low, (high - close.shift()).abs(), (low - close.shift()).abs()],
        axis=1
    ).max(axis=1)
    atr  = tr.ewm(span=period, adjust=False).mean()
    pdi  = 100 * pdm.ewm(span=period, adjust=False).mean() / atr
    ndi  = 100 * ndm.ewm(span=period, adjust=False).mean() / atr
    dx   = 100 * (pdi - ndi).abs() / (pdi + ndi).replace(0, np.nan)
    adx  = dx.ewm(span=period, adjust=False).mean()
    return adx, pdi, ndi


def _score_signal(rsi_val: float, adx_val: float, direction: int) -> float:
    """
    Signal conviction score 0.0–1.0.
    Higher ADX = stronger trend = higher conviction.
    RSI deep in pullback zone (42-50 for longs) = better entry timing.
    """
    adx_score = min((adx_val - 20) / 30, 1.0) if adx_val > 20 else 0.0
    if direction == 1:   # long
        rsi_score = 1.0 - abs(rsi_val - 46) / 16   # peaks at RSI=46
    else:                # short
        rsi_score = 1.0 - abs(rsi_val - 62) / 16   # peaks at RSI=62
    rsi_score = max(0.0, rsi_score)
    return round(0.5 * adx_score + 0.5 * rsi_score, 3)


def _fetch_indicators(sym: str) -> dict | None:
    """Fetch OHLCV and compute indicators. Returns None on failure."""
    try:
        t = yf.Ticker(sym)
        # v90: 6mo window so the 50-period trend MA has enough warmup bars.
        hist = t.history(period="6mo", interval="1d", auto_adjust=True)
        if hist is None or len(hist) < MA_TREND_SPAN + 10:
            return None
        c = hist["Close"]; h = hist["High"]; l = hist["Low"]
        rsi     = _rsi(c, 10)
        ma_trend = _ema(c, MA_TREND_SPAN)   # v90: MA50 (was EMA20)
        adx, pdi, ndi = _adx(h, l, c, 14)

        # Need at least 2 bars of RSI to check direction
        if len(rsi) < 2 or pd.isna(rsi.iloc[-1]) or pd.isna(adx.iloc[-1]):
            return None

        return {
            "price":    float(c.iloc[-1]),
            "ma_trend": float(ma_trend.iloc[-1]),
            "rsi":      float(rsi.iloc[-1]),
            "rsi_prev": float(rsi.iloc[-2]),
            "adx":      float(adx.iloc[-1]),
            "pdi":      float(pdi.iloc[-1]),
            "ndi":      float(ndi.iloc[-1]),
        }
    except Exception as e:
        logger.debug(f"[TP] {sym} indicator fetch error: {e}")
        return None


def _scan_signals(regime: str) -> list[dict]:
    """Scan universe and return qualifying signals for current regime."""
    signals = []
    for sym in UNIVERSE:
        ind = _fetch_indicators(sym)
        if not ind:
            continue

        price = ind["price"]; ma_trend = ind["ma_trend"]
        rsi = ind["rsi"]; rsi_prev = ind["rsi_prev"]
        adx = ind["adx"]; pdi = ind["pdi"]; ndi = ind["ndi"]

        # Shared gate: strong trend
        if adx < 20:
            continue

        # LONG signal
        long_ok = (
            price > ma_trend and
            pdi > ndi and
            35 <= rsi <= 65 and   # v98 — widened pullback band (was 38–55)
            rsi > rsi_prev
        )

        # SHORT signal — only in bear regime
        short_ok = (
            regime == "bear" and
            price < ma_trend and
            ndi > pdi and
            55 <= rsi <= 70 and
            rsi < rsi_prev
        )

        if long_ok:
            score = _score_signal(rsi, adx, 1)
            signals.append({"symbol": sym, "side": "long",
                            "price": price, "score": score,
                            "rsi": rsi, "adx": adx})
        elif short_ok:
            score = _score_signal(rsi, adx, -1)
            signals.append({"symbol": sym, "side": "short",
                            "price": price, "score": score,
                            "rsi": rsi, "adx": adx})

    # Sort by score descending
    return sorted(signals, key=lambda x: -x["score"])


def _trading_days_open(entry_time) -> int:
    """Count trading days (Mon–Fri) between entry_time and today UTC."""
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


def _check_time_stops(broker: AlpacaBroker, db_conn) -> None:
    """v90: close any trend_pullback position held >= MAX_HOLD_DAYS trading days.

    Runs every cycle regardless of regime. Cancels resting bracket orders first
    (they reserve the shares), mirroring fifty_two_wh.py's time-stop pattern.
    """
    from db import get_position_state
    for pos in broker.get_positions():
        if pos.get("strategy") != STRATEGY_NAME:
            continue
        sym = pos["symbol"]
        state = get_position_state(db_conn, sym)
        entry_time = state.get("entry_time") if state else None
        days_held = _trading_days_open(entry_time)
        if days_held < MAX_HOLD_DAYS:
            continue
        logger.info(
            f"[TrendPullback] TIME STOP {sym} — held {days_held} trading days "
            f"(>= {MAX_HOLD_DAYS}), pnl={pos.get('unrealized_pnl_pct', 0):.1f}%"
        )
        try:
            from strategies.regime_exit import _cancel_open_orders_for_symbol
            _cancel_open_orders_for_symbol(broker, sym)
        except Exception as e:
            logger.debug(f"[TrendPullback] {sym}: order cancel before time-stop failed: {e}")
        result = broker.close_position(sym, STRATEGY_NAME)
        if result is not None:
            log_trade(db_conn, STRATEGY_NAME, sym, "sell_time_stop",
                      pos["qty"], pos.get("current_price", 0), pos.get("unrealized_pnl", 0))
            try:
                from utils.cooldown import set_cooldown
                set_cooldown(sym)
            except Exception:
                pass
        else:
            logger.warning(f"[TrendPullback] {sym}: time-stop close failed — will retry next cycle")


def run(broker: AlpacaBroker, db_conn):
    """Main strategy entry point — called every bot cycle."""
    # ── Time-stop exits (run every cycle, independent of regime) ───────────────
    _check_time_stops(broker, db_conn)

    # ── Regime gate ──────────────────────────────────────────────────────────
    try:
        from utils.regime_weights import get_multiplier as _rm
        regime_mult = _rm(STRATEGY_NAME)
    except Exception:
        regime_mult = 1.0

    if regime_mult == 0.0:
        logger.debug("[TrendPullback] Regime weight 0.0 — skipping entries")
        return

    # Determine current regime for signal filtering
    try:
        from utils.regime_detector import get_regime as _rg
        hmm, conf = _rg()
        if hmm in ("BEAR_MILD", "BEAR_STRONG"):
            regime = "bear"
        elif hmm == "CHOPPY":
            return   # blocked in chop
        else:
            regime = "bull"
    except Exception:
        regime = "bull"

    # ── Position cap ─────────────────────────────────────────────────────────
    all_positions = broker.get_positions()
    tp_positions = [p for p in all_positions if p.get("strategy") == STRATEGY_NAME]
    if len(tp_positions) >= MAX_POSITIONS:
        logger.debug(f"[TrendPullback] At position cap ({MAX_POSITIONS}) — skipping scan")
        return

    # ── Cash check ───────────────────────────────────────────────────────────
    cash, portfolio_value = broker.get_live_cash()
    min_cash = portfolio_value * MIN_CASH_RESERVE_PCT
    if cash <= min_cash:
        logger.info(f"[TrendPullback] Cash floor hit (${cash:,.0f}) — skipping")
        return

    # ── Scan for signals ─────────────────────────────────────────────────────
    signals = _scan_signals(regime)
    if not signals:
        logger.debug("[TrendPullback] No qualifying signals")
        return

    logger.info(f"[TrendPullback] {len(signals)} signals in {regime.upper()} regime")

    open_syms = {p["symbol"] for p in all_positions}
    slots_available = MAX_POSITIONS - len(tp_positions)

    for sig in signals[:slots_available]:
        sym   = sig["symbol"]
        side  = sig["side"]
        score = sig["score"]

        if sym in open_syms:
            logger.debug(f"[TrendPullback] {sym} already in portfolio — skipping")
            continue

        # Cooldown check
        try:
            from utils.cooldown import is_on_cooldown
            if is_on_cooldown(sym, STRATEGY_NAME):
                logger.debug(f"[TrendPullback] {sym} on cooldown")
                continue
        except Exception:
            pass

        # Correlation check
        try:
            from utils.correlation_monitor import is_entry_allowed as _corr_ok
            allowed, reason = _corr_ok(sym, broker)
            if not allowed:
                logger.info(f"[TrendPullback] {sym} blocked by correlation: {reason}")
                continue
        except Exception:
            pass

        # Earnings blackout
        try:
            from utils.earnings_calendar import has_upcoming_earnings
            if has_upcoming_earnings(sym):
                logger.info(f"[TrendPullback] {sym} — earnings blackout, skipping")
                continue
        except Exception:
            pass

        # Sizing
        size_pct = BASE_ALLOC_PCT * regime_mult
        notional = portfolio_value * min(size_pct, MAX_SINGLE_POSITION_PCT)

        # Refresh cash
        cash, portfolio_value = broker.get_live_cash()
        if cash - notional < min_cash:
            logger.info(f"[TrendPullback] {sym}: insufficient cash — stopping")
            break

        logger.info(
            f"[TrendPullback] ENTRY {side.upper()} {sym} "
            f"@ ${sig['price']:.2f} score={score:.3f} "
            f"RSI={sig['rsi']:.1f} ADX={sig['adx']:.1f} "
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
                      metadata={"score": score, "rsi": sig["rsi"],
                                "adx": sig["adx"], "regime": regime})
            log_signal(db_conn, STRATEGY_NAME, sym, f"entry_{side}",
                       score, sig)
            open_syms.add(sym)

            # Refresh cash after trade
            cash, portfolio_value = broker.get_live_cash()
            min_cash = portfolio_value * MIN_CASH_RESERVE_PCT

        except Exception as e:
            logger.error(f"[TrendPullback] {sym} entry error: {e}", exc_info=True)

    gc.collect()
