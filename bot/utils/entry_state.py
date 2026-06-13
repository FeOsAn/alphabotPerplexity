"""
entry_state.py — v74

Per-strategy initial-stop + take-profit calculation, plus the `record_entry()`
wrapper that every successful buy/short calls. Writes a row to
`positions_state` so trade_management can drive dollar-based, side-aware
stops/TPs off durable state instead of in-memory percentages.
"""
import logging
import time
from typing import Optional

import pandas as pd

logger = logging.getLogger("alphabot.entry_state")


_STRATEGY_ATR_TIMEFRAME: dict[str, str] = {
    "vwap_reclaim":        "15Min",
    "breakout":            "15Min",
    "event_driven":        "15Min",
    "mean_reversion":      "15Min",
    "options_flow":        "1Hour",
    "ts_momentum":         "1Hour",
    "squeeze_screener":    "1Hour",
    "spy_dip":             "1Hour",
    "short_hedge":         "1Hour",
    "momentum":            "1Day",
    "sector_rotation":     "1Day",
    "trend_following":     "1Day",
    "ai_research":         "1Day",
    "insider_buying":      "1Day",
    "earnings_prediction": "1Day",
    "pairs_trading":       "1Day",
    "default":             "1Day",
}


# (symbol, timeframe) -> (atr_value, fetched_at_timestamp)
_atr_tf_cache: dict[tuple[str, str], tuple[float, float]] = {}
_ATR_CACHE_TTL = 300  # seconds


# (stop_atr_mult, tp_basis) per strategy. Stop is expressed as a multiple of
# ATR(14); the side determines whether it sits below (long) or above (short) entry.
_STRATEGY_RISK: dict[str, tuple[float, str]] = {
    "momentum":            (2.0, "R-multiple"),
    "breakout":            (1.5, "measured-move"),
    "earnings_prediction": (2.0, "R-multiple"),
    "event_driven":        (2.0, "R-multiple"),
    "pairs_trading":       (2.0, "zscore-to-zero"),
    "sector_rotation":     (2.5, "none"),
    "ts_momentum":         (2.0, "R-multiple"),
    "vwap_reclaim":        (1.5, "R-multiple"),
    "options_flow":        (2.0, "R-multiple"),
    "squeeze_screener":    (2.0, "R-multiple"),
    "insider_buying":      (2.5, "R-multiple"),
    "trend_following":     (2.5, "none"),
    "ai_research":         (2.0, "analyst-or-R"),
    "mean_reversion":      (1.5, "reversion"),
    "short_hedge":         (1.5, "R-multiple"),
    "spy_dip":             (1.5, "R-multiple"),
    "earnings_drift":      (2.0, "R-multiple"),
    "earnings_nlp":        (2.0, "R-multiple"),
    "gap_scanner":         (2.0, "R-multiple"),
    "vix_reversal":        (2.0, "R-multiple"),
    "default":             (2.0, "R-multiple"),
}


def _strategy_risk_profile(strategy: str) -> tuple[float, str]:
    return _STRATEGY_RISK.get(strategy, _STRATEGY_RISK["default"])


# ── ATR(14) — local fallback to avoid circular import on trade_management ────
_atr_cache: dict[str, tuple[float, float]] = {}
_ATR_TTL = 600  # 10 min — match trade_management's TTL


def _local_atr(symbol: str) -> float:
    """ATR(14) from last ~1mo of daily OHLC. 0.0 on any failure."""
    now_ts = time.time()
    cached = _atr_cache.get(symbol)
    if cached is not None and (now_ts - cached[1]) < _ATR_TTL:
        return cached[0]
    atr_val = 0.0
    try:
        from utils import yf_cache
        hist = yf_cache.get_history(symbol, period="1mo", interval="1d")
        if hist is not None and not hist.empty and len(hist) >= 15:
            high = hist["High"].dropna()
            low = hist["Low"].dropna()
            close = hist["Close"].dropna()
            prev_close = close.shift(1)
            tr = pd.concat([
                high - low,
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ], axis=1).max(axis=1).dropna()
            if len(tr) >= 14:
                atr_val = float(tr.tail(14).mean())
    except Exception as e:
        logger.debug(f"[entry_state ATR] {symbol}: {e}")
    _atr_cache[symbol] = (atr_val, now_ts)
    return atr_val


def _get_atr(symbol: str) -> float:
    """Prefer trade_management's cached ATR; fall back to local fetch."""
    try:
        from strategies.trade_management import _get_current_atr
        v = _get_current_atr(symbol)
        if v and v > 0:
            return float(v)
    except Exception:
        pass
    return _local_atr(symbol)


def _get_atr_for_timeframe(symbol: str, timeframe: str, entry_price: float) -> float:
    """Fetch ATR(14) using the appropriate bar timeframe. Cached for 5 min."""
    import requests
    import pandas_ta as pta

    cache_key = (symbol, timeframe)
    cached = _atr_tf_cache.get(cache_key)
    if cached and (time.time() - cached[1]) < _ATR_CACHE_TTL:
        return cached[0]

    fallback_pct = {"15Min": 0.005, "1Hour": 0.008, "1Day": 0.02}
    fallback = entry_price * fallback_pct.get(timeframe, 0.02)

    try:
        url = f"https://data.alpaca.markets/v2/stocks/{symbol}/bars"
        params = {
            "timeframe": timeframe,
            "limit": 20,
            "feed": "iex",
            "adjustment": "raw",
        }
        headers = {
            "APCA-API-KEY-ID": "PKADXUOZZJ4XBCAQRB4KORUDEG",
            "APCA-API-SECRET-KEY": "J91d12qXkkceyp51y7f4YyzVfKN1LbWupuPjP99WKJdR",
        }
        resp = requests.get(url, params=params, headers=headers, timeout=5)
        resp.raise_for_status()
        bars = resp.json().get("bars", [])
        if len(bars) < 5:
            return fallback
        df = pd.DataFrame(bars)
        df = df.rename(columns={"h": "high", "l": "low", "c": "close"})
        atr_series = pta.atr(df["high"], df["low"], df["close"], length=min(14, len(df) - 1))
        if atr_series is None or atr_series.dropna().empty:
            return fallback
        atr_val = float(atr_series.dropna().iloc[-1])
        if atr_val <= 0:
            return fallback
        _atr_tf_cache[cache_key] = (atr_val, time.time())
        return atr_val
    except Exception as e:
        logger.debug(f"[EntryState] ATR fetch failed for {symbol}/{timeframe}: {e}")
        return fallback


def _compute_tp_target(symbol: str, side: str, entry: float, initial_stop: float,
                       strategy: str, atr: float) -> Optional[float]:
    """Return the TP $-price, or None for strategies that don't use a fixed TP."""
    R = abs(entry - initial_stop)
    if R <= 0:
        return None

    s = strategy

    # No fixed TP — pure trailing
    if s in ("sector_rotation", "trend_following"):
        return None

    def _r_target(mult: float) -> float:
        return entry + (mult * R if side == "long" else -mult * R)

    def _cap(pct: float) -> float:
        return entry * ((1.0 + pct) if side == "long" else (1.0 - pct))

    def _bounded(tp_r: float, cap: float) -> float:
        if side == "long":
            return min(tp_r, cap)
        return max(tp_r, cap)

    if s == "momentum":
        return _bounded(_r_target(2.0), _cap(0.15))

    if s == "breakout":
        return _bounded(_r_target(2.0), _cap(0.12))

    if s in ("earnings_prediction", "insider_buying"):
        return _r_target(3.0)

    if s == "ai_research":
        return _r_target(2.0)

    if s in ("vwap_reclaim", "short_hedge", "spy_dip"):
        return _r_target(1.5)

    if s == "mean_reversion":
        return entry + (atr if side == "long" else -atr)

    if s == "pairs_trading":
        return entry + (atr if side == "long" else -atr)

    return _r_target(2.0)


def record_entry(conn, broker, *, symbol: str, side: str, qty: float,
                 entry_price: float, strategy: str,
                 tp_target_override: Optional[float] = None,
                 stop_override: Optional[float] = None) -> None:
    """
    Persist the per-symbol entry record after a successful buy/short.

    Every strategy's entry path goes through here (via broker.market_buy /
    submit_order). Strategies that compute their own TP/stop can pass
    `tp_target_override` and/or `stop_override`.
    """
    if not symbol or not strategy or entry_price <= 0 or qty <= 0:
        logger.debug(
            f"[record_entry] skip — invalid args sym={symbol} strat={strategy} "
            f"px={entry_price} qty={qty}"
        )
        return
    if side not in ("long", "short"):
        logger.debug(f"[record_entry] skip {symbol}: bad side={side!r}")
        return

    try:
        timeframe = _STRATEGY_ATR_TIMEFRAME.get(strategy, "1Day")
        atr = _get_atr_for_timeframe(symbol, timeframe, entry_price)
        logger.debug(f"[EntryState] {symbol} ({strategy}): ATR={atr:.4f} timeframe={timeframe}")
        if atr <= 0:
            atr = entry_price * 0.02  # 2% proxy
        stop_mult, tp_basis = _strategy_risk_profile(strategy)

        if stop_override is not None and stop_override > 0:
            initial_stop = float(stop_override)
        elif side == "long":
            initial_stop = entry_price - stop_mult * atr
        else:
            initial_stop = entry_price + stop_mult * atr

        if tp_target_override is not None and tp_target_override > 0:
            tp_target = float(tp_target_override)
            tp_basis = "override"
        else:
            tp_target = _compute_tp_target(
                symbol, side, entry_price, initial_stop, strategy, atr
            )

        # v85 — record the regime this position was opened under. main.py
        # persists the active 3-tier regime ("bull"/"chop"/"bear") into bot_state
        # under "current_regime" after every regime evaluation.
        opening_regime = None
        try:
            from db import get_state
            opening_regime = get_state(conn, "current_regime")
        except Exception:
            opening_regime = None

        from db import write_position_state
        write_position_state(
            conn,
            symbol=symbol, side=side, qty=qty,
            entry_price=entry_price, entry_atr=atr,
            initial_stop=initial_stop, tp_target=tp_target,
            strategy=strategy, tp_basis=tp_basis,
            opening_regime=opening_regime, opening_strategy=strategy,
        )
        tp_txt = f"${tp_target:.2f}" if tp_target is not None else "n/a"
        logger.info(
            f"[record_entry] {symbol} ({strategy}/{side}) entry=${entry_price:.2f} "
            f"atr=${atr:.2f} stop=${initial_stop:.2f} tp={tp_txt} basis={tp_basis}"
        )
    except Exception as e:
        logger.warning(f"[record_entry] {symbol}: {e}")
