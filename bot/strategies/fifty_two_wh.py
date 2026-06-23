"""
Strategy: 52-Week High Breakout with Volume Confirmation (52WH-Vol)
-------------------------------------------------------------------
Edge: When price breaks a 52-week CLOSING high with confirmed volume
(>= 1.2x the 20-day average), investor anchoring at the prior high creates
persistent upward momentum (George & Hwang 2004, Journal of Finance).

Distinct from `breakout` (which trades proximity to the 52w high + RSI + slope):
52WH-Vol requires an ACTUAL new-closing-high breach (today's close exceeds the
prior 252-day closing high) plus a strict volume + trend + volatility gate.

Entry requires ALL five conditions:
  1. Today's close > prior 252-day closing high (shift(1) — yesterday's high,
     so today's close is the breakout itself)
  2. Volume >= 1.2x the 20-day average volume (institutional confirmation)
  3. Close >= 3% above the 50-day SMA (trend structure)
  4. ATR(14) >= 0.4% of price (minimum volatility filter — skip dead names)
  5. Regime: BULL or CHOP only (never BEAR)

Exits:
  - Hard stop: 5% below entry (placed as bracket stop)
  - Take profit: 10% above entry (placed as bracket TP)
  - Trailing ratchet:
      after +7% gain  -> move stop to breakeven (entry price)
      after +15% gain -> move stop to entry + 7%
  - Time stop: 20 trading days (close at market if still open)

TP multiples for tp_engine: (1.0, 2.0) — single TP at +10%, stop at -5%.

Sizing: notional-based, identical mechanics to `breakout` — conviction-scaled
DEFAULT_STRATEGY_ALLOCATION_PCT, regime-multiplied, capped at MAX_SINGLE_POSITION_PCT.
One symbol at a time for RAM safety on Railway 512MB.
"""

import gc
import logging
import pandas as pd
import yfinance as yf
import pandas_ta as _pta
from datetime import datetime, timezone, timedelta
from typing import Optional
from broker import AlpacaBroker, tag_symbol
from config import (
    MIN_CASH_RESERVE_PCT, DEFAULT_STRATEGY_ALLOCATION_PCT, MAX_SINGLE_POSITION_PCT,
)
from db import log_trade, log_signal, get_state, set_state, get_position_state
from utils.clock import today_utc

logger = logging.getLogger("alphabot.fifty_two_wh")
STRATEGY_NAME = "52wh_vol"

FIFTY_TWO_WH_MAX_POSITIONS = 4
STOP_LOSS_PCT = 0.05            # 5% hard stop below entry
TAKE_PROFIT_PCT = 0.10         # 10% take profit above entry
VOL_MULT_MIN = 1.2            # volume >= 1.2x 20-day average
MA50_BUFFER = 0.03            # close >= 3% above 50-day SMA
ATR_MIN_PCT = 0.004          # ATR(14) >= 0.4% of price
TRAIL_BE_GAIN = 0.07         # +7% -> ratchet stop to breakeven
TRAIL_LOCK_GAIN = 0.15       # +15% -> ratchet stop to entry + 7%
TRAIL_LOCK_LEVEL = 0.07      # locked stop sits 7% above entry
TIME_STOP_DAYS = 20          # close after 20 trading days

# Strategy-owned universe (mirrors breakout.py's pattern of a per-strategy list).
FIFTY_TWO_WH_UNIVERSE = [
    "SPY", "QQQ", "AAPL", "MSFT", "NVDA", "AMD", "TSLA", "JPM", "BAC", "GS",
    "XLE", "XLF", "XLK", "GLD", "SLV", "EEM", "MRVL", "DDOG", "PANW", "RTX",
    "LOW", "ROST", "HD", "CVX", "XOM", "NKE",
]
# Deduplicate while preserving order
FIFTY_TWO_WH_UNIVERSE = list(dict.fromkeys(FIFTY_TWO_WH_UNIVERSE))


# ── Per-day re-entry guard (mirrors breakout.py) ─────────────────────────────
# A symbol bought today by 52wh_vol cannot be re-bought by 52wh_vol the same day,
# even if the position is closed. Persisted to DB so a restart doesn't wipe it.
_traded_today: set[str] = set()
_traded_today_date: str = ""
_TRADED_DB_KEY = "52wh_vol_traded_today"
_TRADED_DATE_DB_KEY = "52wh_vol_traded_today_date"


def _load_traded_today(db_conn) -> None:
    """Reload _traded_today from DB if stored date != today UTC (auto-reset at midnight UTC)."""
    global _traded_today, _traded_today_date
    today = today_utc()
    if _traded_today_date == today:
        return
    try:
        stored_date = get_state(db_conn, _TRADED_DATE_DB_KEY)
        if stored_date == today:
            raw = get_state(db_conn, _TRADED_DB_KEY) or ""
            _traded_today = {s for s in raw.split(",") if s}
        else:
            _traded_today = set()
        _traded_today_date = today
    except Exception as e:
        logger.debug(f"[52WH] _load_traded_today failed: {e}")
        _traded_today = set()
        _traded_today_date = today


def _mark_traded_today(db_conn, sym: str) -> None:
    """Add symbol to _traded_today and persist."""
    global _traded_today, _traded_today_date
    today = today_utc()
    if _traded_today_date != today:
        _traded_today = set()
        _traded_today_date = today
    _traded_today.add(sym)
    try:
        set_state(db_conn, _TRADED_DATE_DB_KEY, today)
        set_state(db_conn, _TRADED_DB_KEY, ",".join(sorted(_traded_today)))
    except Exception as e:
        logger.debug(f"[52WH] _mark_traded_today persist failed: {e}")


def _ratchet_stage(db_conn, sym: str) -> int:
    """Current trailing-ratchet stage for a symbol (0=none, 1=breakeven, 2=locked)."""
    try:
        raw = get_state(db_conn, f"52wh_vol_ratchet:{sym}")
        return int(raw) if raw else 0
    except Exception:
        return 0


def _set_ratchet_stage(db_conn, sym: str, stage: int) -> None:
    try:
        set_state(db_conn, f"52wh_vol_ratchet:{sym}", str(stage))
    except Exception as e:
        logger.debug(f"[52WH] _set_ratchet_stage persist failed for {sym}: {e}")


def _clear_ratchet_stage(db_conn, sym: str) -> None:
    try:
        set_state(db_conn, f"52wh_vol_ratchet:{sym}", "0")
    except Exception:
        pass


def _trading_days_open(entry_time) -> int:
    """Count trading days (Mon-Fri) between entry_time and today UTC."""
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


def _compute_signals(sym: str) -> Optional[dict]:
    """
    Fetch 15 months of daily history for a single symbol (RAM-safe).
    Evaluate all five 52WH-Vol entry conditions and return a signals dict.
    Returns None on data failure.
    """
    try:
        ticker = yf.Ticker(sym)
        hist = ticker.history(period="15mo")

        if hist is None or hist.empty or len(hist) < 252:
            logger.debug(
                f"[52WH] {sym}: insufficient data "
                f"({len(hist) if hist is not None else 0} rows, need 252)"
            )
            return None

        hist = hist.sort_index()
        close = hist["Close"].dropna()
        volume = hist["Volume"].dropna()

        if len(close) < 252:
            return None

        price_now = float(close.iloc[-1])

        # 1. Prior 252-day closing high via shift(1) — exclude today so that
        #    today's close is the breakout that breaches yesterday's high.
        high_prev_252 = float(close.iloc[:-1].tail(252).max())
        cond_breakout = price_now > high_prev_252

        # 2. Volume confirmation. Use the previous COMPLETED day (iloc[-2]); during
        #    market hours iloc[-1] is a partial bar that always reads ~0.1x average.
        vol_avg_20 = float(volume.tail(21).iloc[:-1].mean()) if len(volume) >= 21 else float(volume.mean())
        vol_last = float(volume.iloc[-2]) if len(volume) >= 2 else float(volume.iloc[-1])
        vol_ratio = vol_last / vol_avg_20 if vol_avg_20 > 0 else 0.0
        cond_volume = vol_ratio >= VOL_MULT_MIN

        # 3. Close >= 3% above the 50-day SMA.
        ma50 = float(close.tail(50).mean()) if len(close) >= 50 else None
        cond_ma50 = bool(ma50 is not None and ma50 > 0 and price_now >= ma50 * (1.0 + MA50_BUFFER))

        # 4. ATR(14) >= 0.4% of price (minimum volatility filter).
        try:
            atr_series = _pta.atr(hist["High"], hist["Low"], hist["Close"], length=14)
            atr = float(atr_series.dropna().iloc[-1]) if atr_series is not None and not atr_series.dropna().empty else 0.0
        except Exception:
            atr = 0.0
        atr_pct = atr / price_now if price_now > 0 else 0.0
        cond_atr = atr_pct >= ATR_MIN_PCT

        buy_signal = cond_breakout and cond_volume and cond_ma50 and cond_atr

        return {
            "symbol": sym,
            "price": price_now,
            "high_prev_252": high_prev_252,
            "vol_ratio": vol_ratio,
            "ma50": ma50,
            "atr": atr,
            "atr_pct": atr_pct,
            "buy_signal": buy_signal,
            "cond_breakout": cond_breakout,
            "cond_volume": cond_volume,
            "cond_ma50": cond_ma50,
            "cond_atr": cond_atr,
        }

    except Exception as e:
        logger.debug(f"[52WH] Error computing signals for {sym}: {e}")
        return None
    finally:
        gc.collect()


def _conviction_multiplier(vol_ratio: float) -> float:
    """Scale position size by volume conviction (same spirit as breakout.py)."""
    if vol_ratio >= 2.5:
        return 1.5
    elif vol_ratio >= 1.8:
        return 1.25
    else:
        return 1.0


def _check_exits(broker: AlpacaBroker, db_conn):
    """
    Manage open 52wh_vol positions:
      - Trailing ratchet: +7% -> stop to breakeven, +15% -> stop to entry+7%
      - Time stop: close any position held > 20 trading days
    Entry time and price are read from the positions_state DB row.
    """
    positions = broker.get_positions()
    for pos in positions:
        if pos["strategy"] != STRATEGY_NAME:
            continue

        sym = pos["symbol"]
        state = get_position_state(db_conn, sym)

        entry_price = None
        entry_time = None
        if state:
            entry_price = state.get("entry_price")
            entry_time = state.get("entry_time")
        if not entry_price or entry_price <= 0:
            entry_price = pos.get("avg_entry")

        current_price = pos.get("current_price", 0.0)
        gain_pct = (current_price / entry_price - 1.0) if entry_price and entry_price > 0 else 0.0

        # ── Time stop ──────────────────────────────────────────────────────────
        days_held = _trading_days_open(entry_time)
        if days_held >= TIME_STOP_DAYS:
            logger.info(
                f"[52WH] TIME STOP {sym} — held {days_held} trading days "
                f"(>= {TIME_STOP_DAYS}), pnl={pos['unrealized_pnl_pct']:.1f}%"
            )
            broker.close_position(sym, STRATEGY_NAME)
            log_trade(
                db_conn, STRATEGY_NAME, sym, "sell_time_stop",
                pos["qty"], current_price, pos["unrealized_pnl"],
            )
            from utils.cooldown import set_cooldown
            set_cooldown(sym)
            _clear_ratchet_stage(db_conn, sym)
            continue

        # ── Trailing ratchet ─────────────────────────────────────────────────────
        if not entry_price or entry_price <= 0:
            continue
        stage = _ratchet_stage(db_conn, sym)

        if gain_pct >= TRAIL_LOCK_GAIN and stage < 2:
            new_stop = round(entry_price * (1.0 + TRAIL_LOCK_LEVEL), 2)
            logger.info(
                f"[52WH] RATCHET {sym} — +{gain_pct:.1%} >= {TRAIL_LOCK_GAIN:.0%}, "
                f"locking stop to entry+{TRAIL_LOCK_LEVEL:.0%} (${new_stop:.2f})"
            )
            from strategies.trade_management import update_exchange_stop
            update_exchange_stop(broker, sym, new_stop)
            _set_ratchet_stage(db_conn, sym, 2)
        elif gain_pct >= TRAIL_BE_GAIN and stage < 1:
            new_stop = round(entry_price, 2)
            logger.info(
                f"[52WH] RATCHET {sym} — +{gain_pct:.1%} >= {TRAIL_BE_GAIN:.0%}, "
                f"moving stop to breakeven (${new_stop:.2f})"
            )
            from strategies.trade_management import update_exchange_stop
            update_exchange_stop(broker, sym, new_stop)
            _set_ratchet_stage(db_conn, sym, 1)


def run(broker: AlpacaBroker, db_conn):
    """
    Run the 52WH-Vol strategy every scan cycle.

    1. Manage exits on existing positions (time stop + trailing ratchet)
    2. Scan the universe one symbol at a time (RAM safe)
    3. Enter confirmed 52-week-high breakouts if capacity allows
    """
    logger.info("=== 52WH-Vol Strategy: Scanning for confirmed 52-week high breakouts ===")

    # Manage existing positions every cycle (independent of regime gating).
    _check_exits(broker, db_conn)

    # Regime gate — block new entries in chop AND bear (mirrors breakout.py).
    try:
        from utils.regime_weights import get_multiplier as _rm
        if _rm(STRATEGY_NAME) == 0.0:
            logger.info("[52wh_vol] Regime weight 0.0 (bear) — skipping new entries")
            return
    except Exception:
        from utils.regime import is_bull_market
        if not is_bull_market():
            logger.info("[52wh_vol] Bear regime detected — skipping new entries")
            return

    from utils.market_hours import is_entry_allowed
    if not is_entry_allowed():
        logger.info("[52wh_vol] Outside safe entry window — skipping")
        return

    _load_traded_today(db_conn)

    # ── Scan universe one symbol at a time ───────────────────────────────────
    signals: dict[str, dict] = {}
    for sym in FIFTY_TWO_WH_UNIVERSE:
        sig = _compute_signals(sym)
        if sig is not None:
            signals[sym] = sig

    logger.info(f"[52WH] Scanned {len(signals)}/{len(FIFTY_TWO_WH_UNIVERSE)} symbols")

    # ── Count active 52wh_vol positions ──────────────────────────────────────
    all_positions = broker.get_positions()
    wh_positions = [p for p in all_positions if p["strategy"] == STRATEGY_NAME]
    wh_count = len(wh_positions)
    current_symbols = {p["symbol"] for p in wh_positions}

    if wh_count >= FIFTY_TWO_WH_MAX_POSITIONS:
        logger.info(f"[52WH] Max positions ({FIFTY_TWO_WH_MAX_POSITIONS}) reached — exits only this cycle")
        return

    # ── Find buy candidates ──────────────────────────────────────────────────
    candidates = [
        sig for sig in signals.values()
        if sig.get("buy_signal") and sig["symbol"] not in current_symbols
    ]

    if not candidates:
        logger.info("[52WH] No confirmed breakout candidates found this cycle")
        return

    # Strongest volume confirmation first.
    candidates.sort(key=lambda x: x["vol_ratio"], reverse=True)

    logger.info(
        f"[52WH] {len(candidates)} breakout candidate(s): "
        + ", ".join(
            f"{s['symbol']}(vol={s['vol_ratio']:.1f}x, atr={s['atr_pct']:.2%})"
            for s in candidates[:5]
        )
    )

    for sig in candidates:
        log_signal(
            db_conn, STRATEGY_NAME, sig["symbol"], "buy",
            sig["vol_ratio"],
            {
                "high_prev_252": sig["high_prev_252"],
                "vol_ratio": sig["vol_ratio"],
                "ma50": sig["ma50"],
                "atr_pct": sig["atr_pct"],
            },
        )

    account = broker.get_account()
    portfolio_value = account["portfolio_value"]
    cash = account["cash"]

    # ── Enter positions ──────────────────────────────────────────────────────
    for sig in candidates:
        sym = sig["symbol"]

        if wh_count >= FIFTY_TWO_WH_MAX_POSITIONS:
            break

        from utils.cooldown import is_on_cooldown
        if is_on_cooldown(sym):
            logger.debug(f"[STRATEGY] {sym} on cooldown — skipping")
            continue

        if sym in _traded_today:
            logger.info(f"[52WH] {sym} already traded today by 52wh_vol — skipping (daily re-entry guard)")
            continue

        from utils.earnings_calendar import has_upcoming_earnings
        if has_upcoming_earnings(sym):
            logger.info(f"[52WH] Skipping {sym} — earnings blackout (within 2 days)")
            continue

        # Regime-aware sizing
        try:
            from utils.regime_weights import get_multiplier as _regime_mult
            regime_mult = _regime_mult(STRATEGY_NAME)
        except Exception:
            regime_mult = 1.0
        if regime_mult == 0.0:
            logger.info(f"[52WH] Regime weight 0.0 for 52wh_vol — skipping {sym}")
            continue

        mult = _conviction_multiplier(sig["vol_ratio"])
        size_pct = min(DEFAULT_STRATEGY_ALLOCATION_PCT * mult, MAX_SINGLE_POSITION_PCT)
        notional = portfolio_value * size_pct * regime_mult
        min_cash = portfolio_value * MIN_CASH_RESERVE_PCT

        # Hard cap: total exposure per symbol cannot exceed MAX_SINGLE_POSITION_PCT
        existing_mv = sum(
            float(p["market_value"]) for p in broker.get_positions()
            if p["symbol"] == sym
        )
        max_notional = portfolio_value * MAX_SINGLE_POSITION_PCT
        if existing_mv >= max_notional:
            logger.info(f"[52WH] {sym}: already at position cap ({existing_mv/portfolio_value:.1%}) — skipping")
            continue
        notional = min(notional, max_notional - existing_mv)

        rotated_in = False
        if cash - notional < min_cash:
            from utils.capital_rotator import find_rotation_candidate, execute_rotation
            rotation_candidate = find_rotation_candidate(
                new_symbol=sym,
                new_score=0.30,
                new_notional=notional,
                current_positions=broker.get_positions(),
                broker=broker,
                db_conn=db_conn,
            )
            if rotation_candidate:
                rotated = execute_rotation(
                    sell_symbol=rotation_candidate,
                    buy_symbol=sym,
                    buy_notional=notional,
                    buy_score=0.30,
                    broker=broker,
                    db_conn=db_conn,
                    strategy_name=STRATEGY_NAME,
                )
                if not rotated:
                    continue
                rotated_in = True
                cash, portfolio_value = broker.get_live_cash()
                min_cash = portfolio_value * MIN_CASH_RESERVE_PCT
                if cash - notional < min_cash:
                    logger.info(
                        f"[52WH] {sym}: still insufficient after rotation "
                        f"(cash=${cash:.0f}, need=${notional:.0f}) — skipping"
                    )
                    continue
            else:
                logger.info(
                    f"[52WH] {sym}: insufficient cash (available=${cash:.0f}, "
                    f"need=${notional:.0f}, reserve=${min_cash:.0f})"
                )
                continue

        # 5% hard stop / 10% take profit anchored to the live entry price.
        entry_ref = sig["price"]
        stop_px = round(entry_ref * (1.0 - STOP_LOSS_PCT), 2)
        tp_px = round(entry_ref * (1.0 + TAKE_PROFIT_PCT), 2)

        logger.info(
            f"[52WH] ENTER {sym} — price=${sig['price']:.2f} "
            f"(breaks prior 252d high=${sig['high_prev_252']:.2f}), "
            f"vol={sig['vol_ratio']:.1f}x avg, atr={sig['atr_pct']:.2%}, "
            f"conviction={mult:.2f}x, notional=${notional:.0f}, "
            f"stop=${stop_px:.2f}, tp=${tp_px:.2f}"
        )

        _buy_result = broker.market_buy(
            sym, notional, STRATEGY_NAME,
            tp_target_override=tp_px,
            stop_override=stop_px,
        )
        tag_symbol(sym, STRATEGY_NAME)
        _mark_traded_today(db_conn, sym)
        _clear_ratchet_stage(db_conn, sym)
        if _buy_result is not None and rotated_in:
            from utils.capital_rotator import mark_rotation_in
            mark_rotation_in(sym)
        log_trade(
            db_conn, STRATEGY_NAME, sym, "buy", 0, sig["price"], 0,
            metadata={
                "notional": notional,
                "high_prev_252": sig["high_prev_252"],
                "vol_ratio": sig["vol_ratio"],
                "ma50": sig["ma50"],
                "atr_pct": sig["atr_pct"],
                "conviction": mult,
                "stop": stop_px,
                "tp": tp_px,
            },
        )
        cash, portfolio_value = broker.get_live_cash()
        if cash < portfolio_value * MIN_CASH_RESERVE_PCT:
            logger.warning(f"[{STRATEGY_NAME}] Cash floor hit (${cash:,.0f}) — halting entries")
            break
        wh_count += 1

    logger.info(f"[52WH] Scan complete — {wh_count} active 52wh_vol positions")
