"""
Strategy: 52-Week High Breakout with Volume Confirmation
---------------------------------------------------------
Buys stocks making new 52-week highs backed by strong volume.
Classic breakout theory: institutional buyers push price through resistance —
the breakout IS the signal. Works best in bull markets near all-time highs.

Entry requires ALL five conditions:
  1. Price within 5% of 52-week high (>= 95% of high)
  2. Volume >= 1.5x 20-day average (institutional accumulation confirmation)
  3. RSI(14) between 55 and 78 (trending, not exhausted)
  4. Price above MA50 (uptrend structure)
  5. 20-day slope >= +2% (confirmed uptrend, not a dead-cat bounce)

Exits:
  - 5% trailing stop (tight — breakouts fail fast if they're going to fail)
  - Price falls more than 8% below 52-week high (breakout failed)

Max 4 concurrent positions. One symbol at a time for RAM safety.
"""

import gc
import logging
import pandas as pd
import yfinance as yf
import ta
from datetime import datetime
from typing import Optional
from broker import AlpacaBroker, tag_symbol
from config import (
    MAX_POSITION_PCT, MIN_CASH_RESERVE_PCT, MAX_TOTAL_EQUITY_POSITIONS,
    SIZING_MIN_MULT, SIZING_MID_MULT, SIZING_HIGH_MULT, SIZING_MAX_MULT,
)
from db import log_trade, log_signal

logger = logging.getLogger("alphabot.breakout")
STRATEGY_NAME = "breakout"

BREAKOUT_MAX_POSITIONS = 4
STOP_LOSS_PCT = 0.05            # 5% trailing stop — tight for fast-fail detection
BREAKOUT_FAIL_PCT = 0.08        # Exit if price falls >8% below 52w high

BREAKOUT_UNIVERSE = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META",
    "TSLA", "AVGO", "AMD", "NFLX", "GOOGL",
    "CRM", "NOW", "PANW", "ISRG", "V",
    "MA", "SPGI", "CME", "CAT", "TXN",
]


def _confirm_4h_breakout(symbol: str, breakout_level: float) -> bool:
    """
    Confirm breakout on 4-hour chart.
    Returns True if the most recent 4H close (approximated as average of last 4 hourly bars)
    is above the breakout level. Fail-open on data errors.
    """
    try:
        ticker = yf.Ticker(symbol)
        hist_4h = ticker.history(period="5d", interval="1h")
        gc.collect()
        if hist_4h.empty:
            logger.debug(f"[BRK] No 4H data for {symbol} — skipping MTF check")
            return True
        recent = hist_4h["Close"].iloc[-4:]
        if len(recent) < 4:
            return True
        last_4h_close = float(recent.mean())
        confirmed = last_4h_close > breakout_level
        if not confirmed:
            logger.info(
                f"[Breakout] {symbol} daily breakout above ${breakout_level:.2f} "
                f"NOT confirmed on 4H (4H avg=${last_4h_close:.2f})"
            )
        return confirmed
    except Exception as e:
        logger.debug(f"[BRK] 4H confirmation error for {symbol}: {e}")
        return True


def _conviction_multiplier(vol_ratio: float, pct_from_high: float) -> float:
    """
    Scale position size by breakout quality.
    Strongest: massive volume surge AND price essentially at the 52w high.
    """
    if vol_ratio >= 2.5 and pct_from_high >= 0.99:
        return SIZING_MAX_MULT   # 1.5x — textbook breakout
    elif vol_ratio >= 2.0:
        return SIZING_HIGH_MULT  # 1.25x — strong volume
    else:
        return SIZING_MID_MULT   # 1.0x — standard volume (>= 1.5x minimum already enforced)


def _compute_signals(sym: str) -> Optional[dict]:
    """
    Fetch 1 year + 3 months of history for a single symbol (RAM-safe).
    Compute all breakout entry conditions and return a signals dict.
    Returns None on data failure.
    """
    try:
        ticker = yf.Ticker(sym)
        # 15mo gives us a full 252-day 52w high window with buffer
        hist = ticker.history(period="15mo")

        if hist is None or hist.empty or len(hist) < 252:
            logger.debug(
                f"[BRK] {sym}: insufficient data "
                f"({len(hist) if hist is not None else 0} rows, need 252)"
            )
            return None

        hist = hist.sort_index()
        close = hist["Close"].dropna()
        volume = hist["Volume"].dropna()

        if len(close) < 252:
            return None

        price_now = float(close.iloc[-1])

        # 52-week high over the last 252 trading days
        high_52w = float(close.tail(252).max())

        # % of 52w high
        pct_from_high = price_now / high_52w if high_52w > 0 else 0.0

        # MA50
        ma50 = float(close.tail(50).mean()) if len(close) >= 50 else None
        above_ma50 = bool(ma50 is not None and price_now > ma50)

        # RSI(14)
        rsi_series = ta.momentum.RSIIndicator(close, window=14).rsi()
        rsi = float(rsi_series.iloc[-1]) if not rsi_series.empty else 50.0

        # Volume ratio: last day vs 20-day average
        vol_avg_20 = float(volume.tail(20).mean()) if len(volume) >= 20 else float(volume.mean())
        vol_last = float(volume.iloc[-1])
        vol_ratio = vol_last / vol_avg_20 if vol_avg_20 > 0 else 0.0

        # 20-day slope: (price_now - price_20d_ago) / price_20d_ago
        price_20d = float(close.iloc[-21]) if len(close) >= 21 else float(close.iloc[0])
        slope_20d = (price_now - price_20d) / price_20d if price_20d > 0 else 0.0

        # Evaluate entry conditions (adaptive — driven by regime)
        from utils.adaptive_filters import get_thresholds
        t = get_thresholds()
        cond_near_high = pct_from_high >= t["breakout_proximity"]
        cond_volume = vol_ratio >= t["breakout_vol_min"]
        cond_rsi = 55 <= rsi <= t["breakout_rsi_max"]
        cond_ma50 = above_ma50
        cond_slope = slope_20d >= 0.02

        buy_signal = cond_near_high and cond_volume and cond_rsi and cond_ma50 and cond_slope

        return {
            "symbol": sym,
            "price": price_now,
            "high_52w": high_52w,
            "pct_from_high": pct_from_high,
            "vol_ratio": vol_ratio,
            "rsi": rsi,
            "above_ma50": above_ma50,
            "slope_20d": slope_20d,
            "buy_signal": buy_signal,
            "cond_near_high": cond_near_high,
            "cond_volume": cond_volume,
            "cond_rsi": cond_rsi,
            "cond_ma50": cond_ma50,
            "cond_slope": cond_slope,
        }

    except Exception as e:
        logger.debug(f"[BRK] Error computing signals for {sym}: {e}")
        return None
    finally:
        gc.collect()


def _check_exits(broker: AlpacaBroker, db_conn, signals: dict):
    """
    Enforce stop-loss and breakout-failure exits on all breakout positions.

    Exit conditions (either triggers exit):
    1. Unrealized P&L <= -5% (trailing stop)
    2. Price has fallen more than 8% below the 52-week high (breakout failed)
    """
    positions = broker.get_positions()
    for pos in positions:
        if pos["strategy"] != STRATEGY_NAME:
            continue

        sym = pos["symbol"]
        loss_pct = pos["unrealized_pnl_pct"]

        # Condition 1: trailing stop
        if loss_pct <= -STOP_LOSS_PCT * 100:
            logger.info(
                f"[BRK] STOP LOSS {sym} @ {loss_pct:.1f}% "
                f"(threshold: -{STOP_LOSS_PCT * 100:.0f}%)"
            )
            broker.close_position(sym, STRATEGY_NAME)
            log_trade(
                db_conn, STRATEGY_NAME, sym, "sell_stop",
                pos["qty"], pos["current_price"], pos["unrealized_pnl"],
            )
            from utils.cooldown import set_cooldown
            set_cooldown(sym)
            continue

        # Condition 2: breakout failure — price fell away from 52w high
        sig = signals.get(sym)
        if sig is not None:
            pct_from_high = sig.get("pct_from_high", 1.0)
            # Fail threshold: price < (1 - BREAKOUT_FAIL_PCT) of 52w high
            if pct_from_high < (1.0 - BREAKOUT_FAIL_PCT):
                logger.info(
                    f"[BRK] BREAKOUT FAILED {sym} — price now {pct_from_high:.1%} of 52w high "
                    f"(threshold: {1 - BREAKOUT_FAIL_PCT:.0%}), pnl={loss_pct:.1f}%"
                )
                broker.close_position(sym, STRATEGY_NAME)
                log_trade(
                    db_conn, STRATEGY_NAME, sym, "sell_breakout_fail",
                    pos["qty"], pos["current_price"], pos["unrealized_pnl"],
                )


def run(broker: AlpacaBroker, db_conn):
    """
    Run the breakout strategy every scan cycle.

    1. Fetch signals for all universe symbols (one at a time — RAM safe)
    2. Check exits on existing positions (stop loss + breakout failure)
    3. Enter new breakouts if capacity allows
    """
    logger.info("=== Breakout Strategy: Scanning for 52-week high breakouts ===")

    from utils.regime import is_bull_market
    if not is_bull_market():
        logger.info("[breakout] Bear regime detected — skipping new entries")
        return

    from utils.market_hours import is_entry_allowed
    if not is_entry_allowed():
        logger.info("[breakout] Outside safe entry window — skipping")
        return

    # ── Scan universe one symbol at a time ──────────────────────────────────
    signals: dict[str, dict] = {}
    for sym in BREAKOUT_UNIVERSE:
        sig = _compute_signals(sym)
        if sig is not None:
            signals[sym] = sig
        # gc.collect() already called in finally block inside _compute_signals

    logger.info(f"[BRK] Scanned {len(signals)}/{len(BREAKOUT_UNIVERSE)} symbols")

    # ── Check exits and stops first ──────────────────────────────────────────
    _check_exits(broker, db_conn, signals)

    # ── Count active breakout positions ─────────────────────────────────────
    all_positions = broker.get_positions()
    brk_positions = [p for p in all_positions if p["strategy"] == STRATEGY_NAME]
    brk_count = len(brk_positions)
    current_symbols = {p["symbol"] for p in brk_positions}

    if brk_count >= BREAKOUT_MAX_POSITIONS:
        logger.info(f"[BRK] Max positions ({BREAKOUT_MAX_POSITIONS}) reached — exits only this cycle")
        return

    # ── Find buy candidates ──────────────────────────────────────────────────
    candidates = [
        sig for sig in signals.values()
        if sig.get("buy_signal") and sig["symbol"] not in current_symbols
    ]

    if not candidates:
        logger.info("[BRK] No breakout candidates found this cycle")
        return

    # Sort by volume ratio (strongest institutional confirmation first),
    # then by proximity to 52w high as tiebreaker
    candidates.sort(key=lambda x: (x["vol_ratio"], x["pct_from_high"]), reverse=True)

    logger.info(
        f"[BRK] {len(candidates)} breakout candidate(s): "
        + ", ".join(
            f"{s['symbol']}(vol={s['vol_ratio']:.1f}x, {s['pct_from_high']:.1%})"
            for s in candidates[:5]
        )
    )

    # Log all candidate signals
    for sig in candidates:
        log_signal(
            db_conn, STRATEGY_NAME, sig["symbol"], "buy",
            sig["vol_ratio"],
            {
                "pct_from_high": sig["pct_from_high"],
                "high_52w": sig["high_52w"],
                "rsi": sig["rsi"],
                "slope_20d": sig["slope_20d"],
                "vol_ratio": sig["vol_ratio"],
            },
        )

    account = broker.get_account()
    portfolio_value = account["portfolio_value"]
    cash = account["cash"]

    # ── Enter positions ──────────────────────────────────────────────────────
    for sig in candidates:
        sym = sig["symbol"]

        if brk_count >= BREAKOUT_MAX_POSITIONS:
            break

        from utils.cooldown import is_on_cooldown
        if is_on_cooldown(sym):
            logger.debug(f"[STRATEGY] {sym} on cooldown — skipping")
            continue

        # Portfolio equity cap
        equity_count = len([
            p for p in broker.get_positions()
            if p.get("asset_class", "equity") == "equity"
        ])
        if equity_count >= MAX_TOTAL_EQUITY_POSITIONS:
            logger.info(f"[BRK] Max equity positions ({MAX_TOTAL_EQUITY_POSITIONS}) — stopping entries")
            break

        from utils.earnings_calendar import has_upcoming_earnings
        if has_upcoming_earnings(sym):
            logger.info(f"[BRK] Skipping {sym} — earnings blackout (within 2 days)")
            continue

        # Multi-timeframe confirmation: only enter if 4H also above the breakout level
        breakout_level = sig["high_52w"]
        if not _confirm_4h_breakout(sym, breakout_level):
            logger.info(f"[Breakout] {sym} — skipping, 4H confirmation failed")
            continue

        mult = _conviction_multiplier(sig["vol_ratio"], sig["pct_from_high"])
        from utils.position_sizer import get_position_size_pct
        size_pct = get_position_size_pct(sym, fallback_pct=MAX_POSITION_PCT)
        notional = portfolio_value * size_pct * mult
        min_cash = portfolio_value * MIN_CASH_RESERVE_PCT

        if cash - notional < min_cash:
            logger.info(
                f"[BRK] {sym}: insufficient cash (available=${cash:.0f}, "
                f"need=${notional:.0f}, reserve=${min_cash:.0f})"
            )
            continue

        logger.info(
            f"[BRK] ENTER {sym} — "
            f"price=${sig['price']:.2f} ({sig['pct_from_high']:.1%} of 52w high=${sig['high_52w']:.2f}), "
            f"vol={sig['vol_ratio']:.1f}x avg, rsi={sig['rsi']:.1f}, "
            f"slope_20d={sig['slope_20d']:.2%}, conviction={mult:.2f}x, notional=${notional:.0f}"
        )

        broker.market_buy(sym, notional, STRATEGY_NAME)
        tag_symbol(sym, STRATEGY_NAME)
        log_trade(
            db_conn, STRATEGY_NAME, sym, "buy", 0, sig["price"], 0,
            metadata={
                "notional": notional,
                "pct_from_high": sig["pct_from_high"],
                "high_52w": sig["high_52w"],
                "vol_ratio": sig["vol_ratio"],
                "rsi": sig["rsi"],
                "slope_20d": sig["slope_20d"],
                "conviction": mult,
            },
        )
        cash -= notional
        brk_count += 1

    logger.info(f"[BRK] Scan complete — {brk_count} active breakout positions")
