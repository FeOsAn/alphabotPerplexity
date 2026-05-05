"""
Strategy 3: Trend Following — EMA Crossover + Volume + VIX Regime Filter
------------------------------------------------------------------------
Enter long when EMA9 crosses above EMA21, price above both EMAs, volume confirms.
VIX filter: pause entries when VIX > 35 (market fear / crisis regime).
Exit when EMA9 crosses back below EMA21 or stop loss triggered.

Improvements:
- Tighter 5% stop loss (was 7%)
- Staggered entries: max 2 new positions per scan
- Additional filter: SPY must be above its 20-day MA before entering
- yFinance one-at-a-time (vs batch broker.get_bars) to stay under Railway 512MB RAM
"""

import gc
import logging
import pandas as pd
import numpy as np
import yfinance as yf
import ta
from broker import AlpacaBroker, tag_symbol, is_correlated_position
from config import (
    TREND_FAST_EMA, TREND_SLOW_EMA, TREND_VIX_MAX,
    TREND_MAX_POSITIONS, MAX_POSITION_PCT, MAX_TOTAL_POSITIONS,
    TAKE_PROFIT_PCT, MIN_CASH_RESERVE_PCT,
    SIZING_MIN_MULT, SIZING_MID_MULT, SIZING_HIGH_MULT, SIZING_MAX_MULT
)


def _conviction_multiplier(slope: float, recent_cross: bool) -> float:
    """
    Scale position size by trend strength.
    Steep slope + fresh cross = max conviction (1.5x).
    Weak slope barely crossing = reduce size (0.75x).
    """
    if slope >= 0.03 and recent_cross:
        return SIZING_MAX_MULT   # 1.5x — strong trend + fresh cross
    elif slope >= 0.015:
        return SIZING_HIGH_MULT  # 1.25x — solid momentum
    elif slope >= 0.005:
        return SIZING_MID_MULT   # 1.0x — moderate trend
    else:
        return SIZING_MIN_MULT   # 0.75x — weak slope, marginal signal


from db import log_trade, log_signal

logger = logging.getLogger("alphabot.trend_following")
STRATEGY_NAME = "trend_following"

STOP_LOSS_PCT = 0.05   # Tighter 5% stop for trend following
MAX_NEW_ENTRIES_PER_SCAN = 2  # Stagger entries
ADX_MIN = 25.0   # Only enter when ADX(14) confirms a strong trend (filters whipsaws)

# Trimmed to 20 names — trend following needs strong momentum stocks with clean
# EMA crossovers. These are the most trend-friendly large-caps. Reduces RAM per scan.
TREND_WATCHLIST = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "TSLA", "AVGO",
    "AMD", "CRM", "NOW", "PANW", "KLAC",
    "ISRG", "VRTX", "V", "MA",
    "CAT", "HON", "XOM", "COP",
]


def _get_vix() -> float:
    """Get current VIXY level as market regime filter (via yFinance, not IEX)."""
    try:
        ticker = yf.Ticker("VIXY")
        hist = ticker.history(period="5d")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception:
        pass
    return 20.0


def _spy_above_ma() -> bool:
    """Check if SPY is above its 20-day MA — basic bull confirmation."""
    try:
        spy = yf.Ticker("SPY")
        hist = spy.history(period="1mo")
        if len(hist) >= 20:
            price = hist["Close"].iloc[-1]
            ma20 = hist["Close"].tail(20).mean()
            return bool(price > ma20)
    except Exception:
        pass
    return True  # Default to True if unavailable


def _compute_adx(highs: pd.Series, lows: pd.Series, closes: pd.Series, period: int = 14) -> float:
    """Manual ADX(14) — fallback path when ta.trend.ADXIndicator misbehaves."""
    tr1 = highs - lows
    tr2 = (highs - closes.shift(1)).abs()
    tr3 = (lows - closes.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    up_move = highs - highs.shift(1)
    down_move = lows.shift(1) - lows
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
    atr = tr.ewm(span=period, adjust=False).mean()
    plus_di = 100 * plus_dm.ewm(span=period, adjust=False).mean() / atr
    minus_di = 100 * minus_dm.ewm(span=period, adjust=False).mean() / atr
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    adx = dx.ewm(span=period, adjust=False).mean()
    val = adx.iloc[-1]
    return float(val) if pd.notna(val) else 0.0


def _compute_signals(df: pd.DataFrame) -> dict:
    """EMA crossover with volume + ADX(14) trend strength confirmation."""
    if df is None or len(df) < TREND_SLOW_EMA + 10:
        return {}

    df = df.copy()
    # Normalise column names — yFinance uses Title case
    df.columns = [c.lower() for c in df.columns]
    df = df.sort_index()

    high   = df["high"]
    low    = df["low"]
    close  = df["close"]
    volume = df["volume"]

    ema_fast = ta.trend.EMAIndicator(close, window=TREND_FAST_EMA).ema_indicator()
    ema_slow = ta.trend.EMAIndicator(close, window=TREND_SLOW_EMA).ema_indicator()

    # ADX(14) trend strength — primary whipsaw filter
    try:
        adx_series = ta.trend.ADXIndicator(high=high, low=low, close=close, window=14).adx()
        adx_val = float(adx_series.iloc[-1])
        if pd.isna(adx_val):
            adx_val = _compute_adx(high, low, close, period=14)
    except Exception:
        adx_val = _compute_adx(high, low, close, period=14)

    vol_avg = volume.rolling(20).mean()
    vol_ok  = bool(volume.iloc[-1] > vol_avg.iloc[-1] * 0.8)

    # Crossover detection: EMA fast crossed above EMA slow in last 3 days
    recent_cross = False
    for i in range(-3, 0):
        if ema_fast.iloc[i-1] <= ema_slow.iloc[i-1] and ema_fast.iloc[i] > ema_slow.iloc[i]:
            recent_cross = True
            break

    price_above_trend = bool(
        close.iloc[-1] > ema_fast.iloc[-1] and close.iloc[-1] > ema_slow.iloc[-1]
    )
    exit_cross = bool(ema_fast.iloc[-1] < ema_slow.iloc[-1])
    slope = float(
        (ema_slow.iloc[-1] - ema_slow.iloc[-6]) / ema_slow.iloc[-6]
        if len(ema_slow) > 6 else 0
    )

    buy_signal = recent_cross and price_above_trend and vol_ok and slope > 0

    return {
        "close": float(close.iloc[-1]),
        "ema_fast": float(ema_fast.iloc[-1]),
        "ema_slow": float(ema_slow.iloc[-1]),
        "vol_ok": vol_ok,
        "recent_cross": recent_cross,
        "price_above_trend": price_above_trend,
        "exit_cross": exit_cross,
        "slope": slope,
        "adx": adx_val,
        "buy_signal": buy_signal,
    }


def run(broker: AlpacaBroker, db_conn):
    """Run trend following strategy."""
    logger.info("=== Trend Following Strategy: Scanning signals ===")

    from utils.regime import is_bull_market
    if not is_bull_market():
        logger.info("[trend_following] Bear regime detected — skipping new entries")
        return

    from utils.market_hours import is_entry_allowed
    if not is_entry_allowed():
        logger.info("[trend_following] Outside safe entry window — skipping")
        return

    vix = _get_vix()
    if vix > TREND_VIX_MAX:
        logger.info(f"[TF] VIX={vix:.1f} above threshold ({TREND_VIX_MAX}) — exits only")
        _check_exits_and_stops(broker, db_conn, {})
        return

    # ── Fetch data one symbol at a time to avoid OOM on Railway 512MB ──────────
    signals = {}
    for sym in TREND_WATCHLIST:
        try:
            ticker = yf.Ticker(sym)
            hist = ticker.history(period="3mo")
            if not hist.empty:
                sig = _compute_signals(hist)
                if sig:
                    signals[sym] = sig
            del hist
        except Exception as e:
            logger.debug(f"[TF] Error fetching {sym}: {e}")
        finally:
            gc.collect()

    _check_exits_and_stops(broker, db_conn, signals)

    current_tf_count = len([p for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME])
    if current_tf_count >= TREND_MAX_POSITIONS:
        logger.info(f"[TF] Max positions reached ({TREND_MAX_POSITIONS})")
        logger.info(f"[TF] Scan complete — {current_tf_count} active positions")
        return

    # Extra safety: only enter new positions if SPY is above its 20-day MA
    if not _spy_above_ma():
        logger.info("[TF] SPY below 20-day MA — skipping new entries")
        logger.info(f"[TF] Scan complete — {current_tf_count} active positions")
        return

    account = broker.get_account()
    portfolio_value = account["portfolio_value"]
    cash = account["cash"]

    buy_candidates = [
        (sym, sig) for sym, sig in signals.items()
        if sig.get("buy_signal") and sig.get("slope", 0) > 0
    ]
    buy_candidates.sort(key=lambda x: x[1]["slope"], reverse=True)

    current_symbols = {p["symbol"] for p in broker.get_positions()}
    new_entries = 0

    for sym, sig in buy_candidates:
        if new_entries >= MAX_NEW_ENTRIES_PER_SCAN:
            logger.info("[TF] Stagger limit reached — deferring remaining entries")
            break
        if sym in current_symbols:
            continue
        if current_tf_count >= TREND_MAX_POSITIONS:
            break
        if len(broker.get_positions()) >= MAX_TOTAL_POSITIONS:
            break

        # ADX(14) gate — reject weak/whipsawing trends (adaptive by regime)
        from utils.adaptive_filters import get_thresholds
        adx_min = get_thresholds()["tf_adx_min"]
        adx = sig.get("adx", 0.0)
        if adx < adx_min:
            logger.info(f"[TF] Skipping {sym} — ADX {adx:.1f} < {adx_min:.0f} (weak trend)")
            continue

        # Correlation control — don't pile on the same sector
        live_positions = broker.get_positions()
        if is_correlated_position(sym, live_positions):
            logger.info(f"[TF] Skipping {sym} — correlated sector already held")
            continue

        from utils.earnings_calendar import has_upcoming_earnings
        if has_upcoming_earnings(sym):
            logger.info(f"[TF] Skipping {sym} — earnings blackout (within 2 days)")
            continue

        mult = _conviction_multiplier(sig["slope"], sig.get("recent_cross", False))
        notional = portfolio_value * MAX_POSITION_PCT * mult
        min_cash = portfolio_value * MIN_CASH_RESERVE_PCT
        if cash - notional < min_cash:
            continue

        logger.info(f"[TF] ENTER {sym} — slope={sig['slope']:.4f}, ADX={adx:.1f}, conviction={mult:.2f}x, notional=${notional:.0f}, VIX={vix:.1f}")
        log_signal(db_conn, STRATEGY_NAME, sym, "buy", sig["slope"],
                   {"ema_fast": sig["ema_fast"], "ema_slow": sig["ema_slow"], "vix": vix, "adx": adx, "conviction": mult})
        broker.market_buy(sym, notional, STRATEGY_NAME)
        tag_symbol(sym, STRATEGY_NAME)
        log_trade(db_conn, STRATEGY_NAME, sym, "buy", 0, sig["close"], 0,
                  metadata={"notional": notional, "slope": sig["slope"], "vix": vix, "adx": adx})
        cash -= notional
        current_tf_count += 1
        new_entries += 1

    logger.info(f"[TF] Scan complete — {current_tf_count} active positions")


def _check_exits_and_stops(broker: AlpacaBroker, db_conn, signals: dict):
    all_positions = broker.get_positions()
    tf_positions = [p for p in all_positions if p["strategy"] == STRATEGY_NAME]

    for pos in tf_positions:
        sym = pos["symbol"]
        sig = signals.get(sym, {})

        # Stop loss (5% — tighter)
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
