"""
Shared Trade Management Utilities
-----------------------------------
Trailing stops and partial profit taking — used by all strategies.

TRAILING STOP:
  Tracks each position's peak price since entry. Stop loss moves up with
  the price but never down. E.g. if a stock goes +10% then drops 5% from
  the peak, we exit at +5% instead of waiting for the original -5% stop.

  On restart, restore_trade_management_state() rebuilds _peak_prices,
  _ratchet_stops, and _partial_taken from the trades table and current
  prices so trailing-stop integrity isn't lost across redeploys.

PARTIAL PROFIT TAKING:
  At +PARTIAL_TAKE_PCT, sell half the position and let the rest run.
  Tracked per-symbol to avoid double-selling.

DUST CLEANUP:
  After a partial take the remaining stub may be too small to matter.
  Any position below MIN_POSITION_VALUE ($500) where a partial take has
  already fired gets closed entirely rather than left as dead weight.
"""

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple
import pandas as pd
from broker import AlpacaBroker
from db import log_trade, get_position_state, delete_position_state, write_position_state
from utils.cooldown import set_cooldown
from utils.clock import now_utc, now_london
from config import (
    OVERNIGHT_LOSS_THRESHOLD,
    OVERNIGHT_EXIT_WINDOW_START,
    OVERNIGHT_EXIT_WINDOW_END,
    TRAIL_ACTIVATE_PCT,
    TRAIL_DISTANCE_PCT,
    TRAIL_TIGHTEN_PCT,
    TRAIL_TIGHTEN_AT_PCT,
)
from utils.symbol_performance import update_symbol_performance, check_and_update_blacklist

logger = logging.getLogger("alphabot.trade_management")


# ETFs and commodity funds don't have earnings calendars — skip to avoid 404 spam
_ETF_PREFIXES = {"XL", "XL"}
_NO_EARNINGS_SYMBOLS: set = {
    "DBC", "EEM", "GLD", "SLV", "XLE", "XLK", "XLV", "XLF", "XLI", "XLB",
    "XLU", "XLP", "XLY", "XLC", "XLRE", "SPY", "QQQ", "IWM", "DIA", "VXX",
    "VIXY", "USO", "UNG", "IAU", "SIVR", "AGG", "TLT", "HYG", "LQD",
}

def _is_post_earnings_window(symbol: str, days: int = 2) -> bool:
    """Returns True if earnings occurred within the last `days` trading days."""
    if symbol in _NO_EARNINGS_SYMBOLS:  # v77-fix: ETFs have no earnings calendar
        return False
    try:
        import yfinance as yf, gc
        ticker = yf.Ticker(symbol)
        cal = ticker.calendar
        if cal is None or cal.empty:
            return False
        if "Earnings Date" in cal.columns:
            dates = cal["Earnings Date"].dropna()
        elif hasattr(cal, "index") and "Earnings Date" in cal.index:
            dates = [cal.loc["Earnings Date"]]
        else:
            return False
        now = now_utc().date()
        for d in dates:
            try:
                earn_date = d.date() if hasattr(d, "date") else d
                if now - timedelta(days=days) <= earn_date <= now:
                    return True
            except Exception:
                continue
        return False
    except Exception:
        return False

# ── Configuration ─────────────────────────────────────────────────────────────
TRAILING_STOP_PCT   = 0.05   # Trail 5% below peak — same as our hard stop floor
PARTIAL_TAKE_PCT    = 0.08   # Take 50% profit at +8%
PARTIAL_TAKE_RATIO  = 0.50   # Sell 50% of position
MIN_POSITION_VALUE   = 500    # Close stub if below $500 after partial take

# Trailing take profit — replaces the old hard 25% exit.
# Instead of capping upside at 25%, we ride the winner until momentum
# genuinely exhausts. Exit triggers when BOTH:
#   1. Position is up >= TRAIL_TAKE_ACTIVATE_PCT (40% — was 25%)
#   2. Price pulls back >= TRAIL_TAKE_DRAWDOWN_PCT from the peak (8%)
TRAIL_TAKE_ACTIVATE_PCT  = 0.40  # Start protecting profit once up 40%
TRAIL_TAKE_DRAWDOWN_PCT  = 0.08  # Exit if pulls back 8% from peak after activation

# Per-strategy base stop loss (fraction). Used as the floor before ratchet kicks in.
# Strategy-specific values intentionally override config.STOP_LOSS_PCT — see M20.
_STRATEGY_BASE_STOP = {
    "momentum":        0.06,
    "breakout":        0.05,
    "mean_reversion":  0.05,
    "trend_following": 0.05,
    "ai_research":     0.08,
    "gap_scanner":     0.06,
    "spy_dip":         0.04,
    "vix_reversal":    0.03,
}


def _base_stop_for(strategy: str) -> float:
    return _STRATEGY_BASE_STOP.get(strategy, TRAILING_STOP_PCT)

# ── In-memory state ────────────────────────────────────────────────────────────
_peak_prices: dict[str, float] = {}        # symbol -> highest price seen
_partial_taken: set[str] = set()           # symbols where partial exit already fired
_ratchet_stops: dict[str, float] = {}      # legacy: symbol -> locked stop pct (kept for back-compat readers)
_ratchet_stops_dollar: dict[str, float] = {}  # v74: symbol -> locked stop $-price
_state_restored: bool = False              # gate so restore runs only once


# ── ATR-based ratchet logic ───────────────────────────────────────────────────
# Gain tiers: once a position reaches this gain, the stop is placed
# (gain - ATR_RATCHET_MULT * ATR / entry_price) from entry, but never below
# the LOCK_FLOOR_PCT for that tier. This means:
#   - Volatile stocks (high ATR) get more breathing room automatically
#   - Calm stocks (low ATR) get tighter stops
#   - You never give back more than ~1 ATR of profit once in a tier

_ATR_RATCHET_TIERS = [
    # (min_gain_pct, lock_floor_pct, atr_mult)
    # Once gain >= min_gain, stop = max(lock_floor, gain - atr_mult * ATR/entry)
    # lock_floor ensures we always lock in at least this much gain
    (0.25, 0.18, 1.0),  # >= +25%: floor +18%, give 1 ATR room
    (0.18, 0.12, 1.0),  # >= +18%: floor +12%, give 1 ATR room
    (0.12, 0.07, 1.2),  # >= +12%: floor +7%,  give 1.2 ATR room
    (0.08, 0.04, 1.5),  # >= +8%:  floor +4%,  give 1.5 ATR room
    (0.05, 0.02, 1.5),  # >= +5%:  floor +2%,  give 1.5 ATR room
    (0.03, 0.00, 2.0),  # >= +3%:  floor breakeven, give 2 ATR room
]


def _atr_ratchet_stop_pct(
    symbol: str,
    current_pnl_pct: float,
    entry_price: float,
    current_price: float,
    base_stop_pct: float,
) -> float:
    """
    Returns the effective stop as a fraction from entry (e.g. 0.05 = stop at entry*1.05).
    Uses ATR to give each stock appropriate breathing room.
    Never loosens an existing stop.
    """
    # Find applicable tier
    applicable = None
    for min_gain, lock_floor, atr_mult in _ATR_RATCHET_TIERS:
        if current_pnl_pct >= min_gain:
            applicable = (min_gain, lock_floor, atr_mult)
            break

    if applicable is None:
        # Below lowest tier — use existing ratchet or base stop
        existing = _ratchet_stops.get(symbol)
        return existing if existing is not None else -base_stop_pct

    _, lock_floor, atr_mult = applicable

    # Compute ATR-based stop price
    atr = _get_current_atr(symbol)
    if atr > 0 and entry_price > 0:
        atr_room_pct = (atr * atr_mult) / entry_price
        atr_stop_pct = current_pnl_pct - atr_room_pct
    else:
        atr_stop_pct = lock_floor  # fallback if ATR unavailable

    # Take the higher of lock_floor and ATR stop (never go below the floor)
    new_stop_pct = max(lock_floor, atr_stop_pct)

    # Never loosen existing stop
    existing = _ratchet_stops.get(symbol, -base_stop_pct)
    if new_stop_pct > existing:
        _ratchet_stops[symbol] = new_stop_pct
        atr_str = f"ATR=${atr:.2f}" if atr > 0 else "ATR=n/a"
        logger.info(
            f"[Ratchet] {symbol} P&L={current_pnl_pct:+.1%} {atr_str} → "
            f"stop locked at {new_stop_pct:+.2%} (floor={lock_floor:+.0%})"
        )

    return _ratchet_stops.get(symbol, -base_stop_pct)


# Keep backward-compatible wrapper used by restore_trade_management_state
def _ratchet_for_pnl(current_pnl_pct: float):
    """Return a simple lock_pct for the tier (used during state restore)."""
    for min_gain, lock_floor, _ in _ATR_RATCHET_TIERS:
        if current_pnl_pct >= min_gain:
            return lock_floor
    return None


def _get_ratchet_stop(symbol: str, current_pnl_pct: float, base_stop_pct: float,
                      entry_price: float = 0.0, current_price: float = 0.0) -> float:
    """Wrapper — use ATR ratchet if prices available, else fall back to floor-only."""
    if entry_price > 0 and current_price > 0:
        return _atr_ratchet_stop_pct(
            symbol, current_pnl_pct, entry_price, current_price, base_stop_pct
        )
    # Fallback: floor-only (no ATR data)
    for min_gain, lock_floor, _ in _ATR_RATCHET_TIERS:
        if current_pnl_pct >= min_gain:
            existing = _ratchet_stops.get(symbol, -base_stop_pct)
            new_stop = max(lock_floor, existing)
            if new_stop > existing:
                _ratchet_stops[symbol] = new_stop
            return _ratchet_stops.get(symbol, -base_stop_pct)
    existing = _ratchet_stops.get(symbol)
    return existing if existing is not None else -base_stop_pct


def update_peak(symbol: str, current_price: float) -> float:
    """Update and return the peak price for a symbol."""
    current_peak = _peak_prices.get(symbol, current_price)
    new_peak = max(current_peak, current_price)
    _peak_prices[symbol] = new_peak
    return new_peak


def clear_symbol(symbol: str):
    """Remove tracking state when a position is fully closed."""
    _peak_prices.pop(symbol, None)
    _partial_taken.discard(symbol)
    _ratchet_stops.pop(symbol, None)
    _ratchet_stops_dollar.pop(symbol, None)
    _post_earnings_checked.discard(symbol)  # allow re-review if re-entered


def _record_close_perf(db_conn, symbol: str, realized_pnl) -> None:
    """v75 FIX 3 — update symbol_performance + blacklist after every close."""
    try:
        pnl = float(realized_pnl or 0.0)
    except Exception:
        pnl = 0.0
    try:
        update_symbol_performance(db_conn, symbol, pnl)
        check_and_update_blacklist(db_conn, symbol)
    except Exception as e:
        logger.debug(f"[SymPerf] post-close update failed for {symbol}: {e}")


# ── v74: dollar-based ratchet ────────────────────────────────────────────────
def _ratchet_stop_price(sym: str, ps: dict, current_price: float) -> float:
    """Return the current effective $-stop. Monotone tightening; never loosens.

    Multiplier tiers (favourable move expressed in R-multiples):
      r_mult >= 1.5  →  1.0 × ATR
      r_mult >= 1.0  →  1.5 × ATR
      otherwise      →  2.0 × ATR (initial)
    """
    entry = float(ps["entry_price"])
    atr   = float(ps["entry_atr"])
    init  = float(ps["initial_stop"])
    R     = float(ps["initial_risk"])
    side  = ps["side"]

    favourable_move = (current_price - entry) if side == "long" else (entry - current_price)
    r_mult = (favourable_move / R) if R > 0 else 0.0

    if r_mult >= 1.5:
        mult = 1.0
    elif r_mult >= 1.0:
        mult = 1.5
    else:
        mult = 2.0

    if side == "long":
        candidate = current_price - mult * atr
        candidate = max(candidate, init)  # never loosen below initial
        existing = _ratchet_stops_dollar.get(sym, init)
        new_stop = max(candidate, existing)  # never loosen
    else:
        candidate = current_price + mult * atr
        candidate = min(candidate, init)
        existing = _ratchet_stops_dollar.get(sym, init)
        new_stop = min(candidate, existing)

    _ratchet_stops_dollar[sym] = new_stop
    return new_stop


# ── Exchange-side stop order management ──────────────────────────────────────

def place_exchange_stop(broker, symbol: str, entry_price: float, qty: float, strategy: str = "unknown") -> Optional[str]:
    """
    Place a hard stop-loss order on Alpaca immediately after entry.
    Returns the Alpaca order ID of the stop, or None on failure.
    Stored in positions_state so it can be cancelled/replaced when ratchet moves up.
    """
    try:
        from alpaca.trading.requests import StopOrderRequest
        from alpaca.trading.enums import OrderSide, TimeInForce

        base_stop_pct = _base_stop_for(strategy)
        stop_price = round(entry_price * (1 - base_stop_pct), 2)

        # For short positions qty will be negative — flip direction
        is_short = qty < 0
        side = OrderSide.BUY if is_short else OrderSide.SELL
        abs_qty = abs(qty)

        req = StopOrderRequest(
            symbol=symbol,
            qty=abs_qty,
            side=side,
            stop_price=stop_price,
            time_in_force=TimeInForce.GTC,  # GTC so it survives overnight
        )
        result = broker.trading.submit_order(req)
        order_id = str(result.id) if result else None

        # Persist the stop order ID so we can replace it when the ratchet moves
        try:
            from db import get_connection as _gc
            _conn = _gc()
            try:
                _conn.execute(
                    "UPDATE positions_state SET stop_order_id=? WHERE symbol=?",
                    (order_id, symbol)
                )
                _conn.commit()
            finally:
                _conn.close()
        except Exception as _e:
            logger.debug(f"[TM] Could not persist stop_order_id for {symbol}: {_e}")

        logger.info(
            f"[TM] Exchange stop placed: {symbol} stop=${stop_price:.2f} "
            f"({base_stop_pct:.0%} below entry ${entry_price:.2f}) | order={order_id}"
        )
        return order_id

    except Exception as e:
        logger.warning(f"[TM] place_exchange_stop failed for {symbol}: {e}")
        return None


def update_exchange_stop(broker, symbol: str, new_stop_price: float) -> Optional[str]:
    """
    Replace the existing exchange stop order with a new (higher) stop price.
    Called by the ratchet logic when the stop moves up.
    Returns the new order ID or None on failure.
    """
    try:
        from alpaca.trading.requests import StopOrderRequest
        from alpaca.trading.enums import OrderSide, TimeInForce

        # Get the current stop order ID and position details from DB
        from db import get_connection as _gc
        _conn = _gc()
        try:
            row = _conn.execute(
                "SELECT stop_order_id, qty, side FROM positions_state WHERE symbol=?",
                (symbol,)
            ).fetchone()
        finally:
            _conn.close()

        if not row:
            logger.debug(f"[TM] update_exchange_stop: no positions_state row for {symbol}")
            return None

        old_order_id, qty, side = row["stop_order_id"], row["qty"], row["side"]

        # Cancel old stop if exists
        if old_order_id:
            try:
                broker.trading.cancel_order_by_id(old_order_id)
                logger.debug(f"[TM] Cancelled old stop {old_order_id} for {symbol}")
            except Exception as _ce:
                logger.debug(f"[TM] Could not cancel old stop for {symbol}: {_ce}")

        # Place new stop
        is_short = (side == "short") if side else (qty and float(qty) < 0)
        order_side = OrderSide.BUY if is_short else OrderSide.SELL
        abs_qty = abs(float(qty)) if qty else 0
        if abs_qty <= 0:
            return None

        req = StopOrderRequest(
            symbol=symbol,
            qty=abs_qty,
            side=order_side,
            stop_price=round(new_stop_price, 2),
            time_in_force=TimeInForce.GTC,
        )
        result = broker.trading.submit_order(req)
        new_order_id = str(result.id) if result else None

        # Update DB
        from db import get_connection as _gc2
        _conn2 = _gc2()
        try:
            _conn2.execute(
                "UPDATE positions_state SET stop_order_id=? WHERE symbol=?",
                (new_order_id, symbol)
            )
            _conn2.commit()
        finally:
            _conn2.close()

        logger.info(
            f"[TM] Exchange stop ratcheted: {symbol} → new stop=${new_stop_price:.2f} | order={new_order_id}"
        )
        return new_order_id

    except Exception as e:
        logger.warning(f"[TM] update_exchange_stop failed for {symbol}: {e}")
        return None


def place_exchange_tp(broker, symbol: str, tp1_price: float, tp2_price: float,
                      qty: float, strategy: str = "unknown") -> Tuple[Optional[str], Optional[str]]:
    """
    Place TP1 (50% qty) and TP2 (remaining 50%) as GTC limit orders on Alpaca.
    TP1 triggers partial take; after fill, bot places TP2 and moves stop to breakeven.

    v79: called immediately after entry, alongside place_exchange_stop.
    """
    try:
        from alpaca.trading.requests import LimitOrderRequest
        from alpaca.trading.enums import OrderSide, TimeInForce

        is_short = qty < 0
        abs_qty = abs(qty)
        tp1_qty = round(abs_qty * 0.5, 6)
        tp2_qty = round(abs_qty - tp1_qty, 6)
        side = OrderSide.BUY if is_short else OrderSide.SELL

        tp1_order_id = None
        tp2_order_id = None

        # Place TP1
        if tp1_qty > 0:
            req1 = LimitOrderRequest(
                symbol=symbol, qty=tp1_qty, side=side,
                limit_price=round(tp1_price, 2),
                time_in_force=TimeInForce.GTC,
            )
            r1 = broker.trading.submit_order(req1)
            tp1_order_id = str(r1.id) if r1 else None
            logger.info(f"[TM] TP1 placed: {symbol} limit=${tp1_price:.2f} qty={tp1_qty} | order={tp1_order_id}")

        # Place TP2
        if tp2_qty > 0:
            req2 = LimitOrderRequest(
                symbol=symbol, qty=tp2_qty, side=side,
                limit_price=round(tp2_price, 2),
                time_in_force=TimeInForce.GTC,
            )
            r2 = broker.trading.submit_order(req2)
            tp2_order_id = str(r2.id) if r2 else None
            logger.info(f"[TM] TP2 placed: {symbol} limit=${tp2_price:.2f} qty={tp2_qty} | order={tp2_order_id}")

        # Persist TP order IDs and entry date to position_state
        try:
            from db import get_connection as _gc
            _conn = _gc()
            try:
                from datetime import datetime, timezone as _tz
                entry_ts = datetime.now(_tz.utc).isoformat()
                # Upsert position_state row
                _conn.execute(
                    """
                    INSERT INTO position_state (symbol, tp1_price, tp2_price, tp1_order_id, tp2_order_id, entry_date)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol) DO UPDATE SET
                        tp1_price=excluded.tp1_price,
                        tp2_price=excluded.tp2_price,
                        tp1_order_id=excluded.tp1_order_id,
                        tp2_order_id=excluded.tp2_order_id,
                        entry_date=COALESCE(entry_date, excluded.entry_date)
                    """,
                    (symbol, tp1_price, tp2_price, tp1_order_id, tp2_order_id, entry_ts)
                )
                _conn.commit()
            finally:
                _conn.close()
        except Exception as _e:
            logger.debug(f"[TM] Could not persist TP order IDs for {symbol}: {_e}")

        return tp1_order_id, tp2_order_id

    except Exception as e:
        logger.warning(f"[TM] place_exchange_tp failed for {symbol}: {e}")
        return None, None


def _seed_legacy_position_state(conn, broker: AlpacaBroker, pos: dict) -> None:
    """Seed a positions_state row for a pre-v74 orphan position.

    Anchors stop/TP at CURRENT price (not entry) — anchoring at entry would
    either produce a stop already breached or require an amnesty period.
    """
    sym = pos["symbol"]
    strategy = pos.get("strategy", "unknown")
    try:
        qty_sgn = float(pos.get("qty") or 0)
    except Exception:
        qty_sgn = 0.0
    side = "long" if qty_sgn > 0 else "short"
    qty = abs(qty_sgn)
    current = float(pos.get("current_price") or 0)
    avg_entry = float(pos.get("avg_entry") or 0)
    if current <= 0:
        return
    entry_price = avg_entry if avg_entry > 0 else current

    atr = _get_current_atr(sym)
    if atr <= 0:
        atr = current * 0.02

    stop_mult = 2.0
    if side == "long":
        initial_stop = current - stop_mult * atr
        tp_target = current + 3.0 * atr
    else:
        initial_stop = current + stop_mult * atr
        tp_target = current - 3.0 * atr

    try:
        write_position_state(
            conn,
            symbol=sym, side=side, qty=qty,
            entry_price=entry_price, entry_atr=atr,
            initial_stop=initial_stop, tp_target=tp_target,
            strategy=strategy, tp_basis="legacy-recovery",
        )
        _ratchet_stops_dollar[sym] = initial_stop
        logger.warning(
            f"[TM] Seeded legacy state for {sym} ({strategy}/{side}): "
            f"stop=${initial_stop:.2f} tp=${tp_target:.2f} atr=${atr:.2f} "
            f"(anchor=current, not entry)"
        )
    except Exception as e:
        logger.error(f"[TM] Legacy seed failed for {sym}: {e}")


def restore_trade_management_state(broker: AlpacaBroker, db_conn):
    """
    Rebuild _peak_prices / _ratchet_stops / _partial_taken from the trades table
    so trailing-stop integrity is preserved across restarts.

    Strategy per open position:
      1. Find the most recent BUY in the trades table for that symbol → entry_price.
      2. Peak  = max(current_price, entry_price)  (safe starting point)
      3. Ratchet = highest tier whose threshold is met by current unrealized gain
      4. partial_taken = True if any SELL trade exists for the symbol after entry
    """
    global _state_restored
    if _state_restored:
        return
    _state_restored = True
    try:
        positions = broker.get_positions()
    except Exception as e:
        logger.warning(f"[TM Restore] Could not fetch positions: {e}")
        return

    restored_count = 0
    for pos in positions:
        sym = pos["symbol"]
        try:
            current_price = float(pos.get("current_price") or 0)
            avg_entry = float(pos.get("avg_entry") or 0)

            # v74 — prefer positions_state row (dollar-priced, side-aware).
            ps = get_position_state(db_conn, sym)
            if ps is not None:
                try:
                    init_stop = float(ps["initial_stop"])
                    _ratchet_stops_dollar[sym] = init_stop
                    entry_price = float(ps["entry_price"]) or avg_entry or current_price
                    peak = max(current_price, entry_price) if current_price > 0 else entry_price
                    _peak_prices[sym] = peak
                except Exception as e:
                    logger.debug(f"[TM Restore] positions_state hydrate failed {sym}: {e}")
            else:
                # Pre-v74 orphan — fall through to legacy percentage seed.
                # 1. Entry price — prefer the most recent BUY in the DB
                entry_price = None
                try:
                    row = db_conn.execute(
                        "SELECT price, created_at FROM trades "
                        "WHERE symbol=? AND side IN ('buy','buy_pyramid') "
                        "ORDER BY created_at DESC LIMIT 1",
                        (sym,),
                    ).fetchone()
                    if row and row["price"] and float(row["price"]) > 0:
                        entry_price = float(row["price"])
                except Exception as e:
                    logger.debug(f"[TM Restore] DB entry lookup failed {sym}: {e}")

                if not entry_price or entry_price <= 0:
                    entry_price = avg_entry if avg_entry > 0 else current_price
                if entry_price <= 0:
                    continue

                # 2. Peak
                peak = max(current_price, entry_price) if current_price > 0 else entry_price
                _peak_prices[sym] = peak

                # 3. Legacy percentage ratchet — only seed the old dict so the
                # legacy fallback ratchet path (run only for positions without
                # a positions_state row) has a starting tier.
                try:
                    pnl_pct = float(pos.get("unrealized_pnl_pct") or 0.0)
                    if current_price > 0 and entry_price > 0:
                        _atr_ratchet_stop_pct(sym, pnl_pct, entry_price, current_price, base_stop_pct=0.07)
                    else:
                        ratchet = _ratchet_for_pnl(pnl_pct)
                        if ratchet is not None:
                            _ratchet_stops[sym] = ratchet
                except Exception:
                    pass

            # 4. Partial-taken — if any SELL trade exists since entry
            try:
                sell_row = db_conn.execute(
                    "SELECT 1 FROM trades "
                    "WHERE symbol=? AND side LIKE 'sell%' LIMIT 1",
                    (sym,),
                ).fetchone()
                if sell_row:
                    _partial_taken.add(sym)
            except Exception:
                pass

            restored_count += 1
        except Exception as e:
            logger.debug(f"[TM Restore] {sym}: {e}")

    logger.info(
        f"[TM Restore] Hydrated {restored_count} position(s) | "
        f"peaks={len(_peak_prices)} ratchets$={len(_ratchet_stops_dollar)} "
        f"legacy_ratchets={len(_ratchet_stops)} partial={len(_partial_taken)} | "
        f"v74 dollar-stops wired on startup"
    )


def check_trailing_stop(pos: dict) -> bool:
    """
    Returns True if trailing stop has been hit.
    Trailing stop = peak_price * (1 - stop_pct)
    Post-earnings (within 2 days), stop is widened 1.5× to give the position
    room to digest the earnings move before being stopped out.
    """
    sym = pos["symbol"]
    current_price = pos["current_price"]
    peak = update_peak(sym, current_price)
    stop_pct = TRAILING_STOP_PCT
    if _is_post_earnings_window(sym):
        stop_pct = TRAILING_STOP_PCT * 1.5
        logger.info(f"[TM] {sym} post-earnings — stop widened to {stop_pct:.1%}")
    trail_level = peak * (1 - stop_pct)
    hit = current_price <= trail_level

    if hit:
        drawdown_from_peak = (peak - current_price) / peak * 100
        logger.info(
            f"[TRAIL] {sym}: price=${current_price:.2f} peak=${peak:.2f} "
            f"trail_level=${trail_level:.2f} drawdown={drawdown_from_peak:.1f}% — STOP HIT"
        )
    else:
        drawdown_from_peak = (peak - current_price) / peak * 100
        if drawdown_from_peak > 1:  # only log if meaningful drawdown
            logger.debug(
                f"[TRAIL] {sym}: price=${current_price:.2f} peak=${peak:.2f} "
                f"({drawdown_from_peak:.1f}% off peak, trail at ${trail_level:.2f})"
            )
    return hit


def check_partial_take(pos: dict, broker: AlpacaBroker, db_conn, strategy: str) -> bool:
    """
    If position is up PARTIAL_TAKE_PCT and we haven't taken partial profits yet,
    sell half. Returns True if partial exit was executed.
    """
    sym = pos["symbol"]

    # Already taken partial on this position
    if sym in _partial_taken:
        return False

    pnl_pct = pos["unrealized_pnl_pct"]
    if pnl_pct < PARTIAL_TAKE_PCT * 100:
        return False

    qty = pos["qty"]
    sell_qty = round(qty * PARTIAL_TAKE_RATIO, 6)

    if sell_qty <= 0:
        return False

    logger.info(
        f"[PARTIAL] {sym} ({strategy}): up {pnl_pct:.1f}% — "
        f"selling {PARTIAL_TAKE_RATIO*100:.0f}% ({sell_qty:.4f} shares), letting rest run with ratchet stop"
    )

    try:
        broker.market_sell(sym, sell_qty, strategy)
        # Log as partial sell — PnL is proportional
        partial_pnl = pos["unrealized_pnl"] * PARTIAL_TAKE_RATIO
        log_trade(db_conn, strategy, sym, "sell_partial",
                  sell_qty, pos["current_price"], partial_pnl,
                  metadata={
                      "pnl_pct_at_partial": pnl_pct,
                      "remaining_qty": qty - sell_qty,
                      "partial_ratio": PARTIAL_TAKE_RATIO,
                  })
        _partial_taken.add(sym)
        # Reset peak tracking to current price for the remaining half
        _peak_prices[sym] = pos["current_price"]
        return True
    except Exception as e:
        logger.error(f"[PARTIAL] Failed partial exit for {sym}: {e}")
        return False


def run_global_trade_management(broker: AlpacaBroker, db_conn):
    """
    Run trailing stops + partial profit taking across ALL positions.
    Called once per cycle from main.py before individual strategies run.
    """
    positions = broker.get_positions()
    logger.info(f"[TRADE MGT] Checking {len(positions)} position(s) — trailing stops + partial takes")

    for pos in positions:
        sym = pos["symbol"]
        pnl_pct = pos["unrealized_pnl_pct"]
        strategy = pos.get("strategy", "unknown")
        current_price = pos["current_price"]
        market_value = float(pos.get("market_value", 0))
        peak = _peak_prices.get(sym, current_price)

        logger.info(
            f"  {sym} ({strategy}): P&L={pnl_pct:+.1f}% | "
            f"price=${current_price:.2f} | value=${market_value:.0f} | peak=${peak:.2f}"
        )

        # ── v74: TP check FIRST — fastest possible exit on TP hit ────────
        try:
            ps_tp = get_position_state(db_conn, sym)
        except Exception:
            ps_tp = None
        if ps_tp and ps_tp.get("tp_target") is not None:
            try:
                tp = float(ps_tp["tp_target"])
                side_tp = ps_tp["side"]
                tp_hit = (current_price >= tp) if side_tp == "long" else (current_price <= tp)
                if tp_hit:
                    logger.info(
                        f"[TP] {sym} ({strategy}): hit TP target ${tp:.2f} "
                        f"(side={side_tp}, price=${current_price:.2f}, basis={ps_tp.get('tp_basis')})"
                    )
                    try:
                        broker.close_position(sym, strategy)
                        log_trade(
                            db_conn, strategy, sym, "sell_take_profit",
                            pos["qty"], current_price, pos["unrealized_pnl"],
                            metadata={
                                "reason": "take_profit",
                                "tp_target": tp,
                                "tp_basis": ps_tp.get("tp_basis"),
                                "entry_price": ps_tp.get("entry_price"),
                                "side": side_tp,
                            },
                        )
                        clear_symbol(sym)
                        delete_position_state(db_conn, sym)
                        set_cooldown(sym)
                        _record_close_perf(db_conn, sym, pos.get("unrealized_pnl"))
                        logger.info(f"[TradeManagement] {sym} cooldown set after sell_take_profit")
                    except Exception as e:
                        logger.error(f"[TP] Failed close for {sym}: {e}")
                    continue
            except Exception as e:
                logger.debug(f"[TP] {sym} TP check error: {e}")

        # 0a. Dead money timeout — momentum/breakout/drift positions flat ±2.5% for >=14 days
        # NEVER fires on structural strategies (sector_rotation, pairs_trading, short_hedge,
        # spy_dip, vix_reversal) — those are intentionally held flat as diversification.
        DEAD_MONEY_DAYS = 14
        DEAD_MONEY_MIN_PCT = -2.5
        DEAD_MONEY_MAX_PCT = 2.5
        _DEAD_MONEY_EXEMPT = {
            "sector_rotation", "pairs_trading", "short_hedge",
            "spy_dip", "vix_reversal", "mean_reversion",
        }
        try:
            qty_signed = float(pos.get("qty", 0))
            asset_class = pos.get("asset_class", "equity")
            if (
                qty_signed > 0
                and asset_class == "equity"
                and strategy not in _DEAD_MONEY_EXEMPT
                and DEAD_MONEY_MIN_PCT <= pnl_pct <= DEAD_MONEY_MAX_PCT
            ):
                entry_dt = None
                if db_conn is not None:
                    try:
                        cur = db_conn.cursor()
                        cur.execute(
                            "SELECT created_at FROM trades "
                            "WHERE symbol=? AND side IN ('buy','buy_pyramid') "
                            "ORDER BY created_at DESC LIMIT 1",
                            (sym,),
                        )
                        row = cur.fetchone()
                        if row:
                            raw = row[0] if not hasattr(row, "keys") else row["created_at"]
                            try:
                                entry_dt = datetime.fromisoformat(raw)
                            except Exception:
                                entry_dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
                    except Exception as e:
                        logger.debug(f"[TradeMgt] Dead money DB lookup failed for {sym}: {e}")
                if entry_dt is not None:
                    if entry_dt.tzinfo is None:
                        entry_dt = entry_dt.replace(tzinfo=timezone.utc)
                    age_days = (now_utc() - entry_dt).days
                    if age_days >= DEAD_MONEY_DAYS:
                        logger.info(
                            f"[TRADE MGT] DEAD MONEY EXIT {sym} ({strategy}): held {age_days}d "
                            f"at {pnl_pct:+.2f}% (within ±{DEAD_MONEY_MAX_PCT}%) — "
                            f"redeploying capital"
                        )
                        try:
                            broker.close_position(sym, strategy)
                            log_trade(db_conn, strategy, sym, "sell_dead_money",
                                      pos["qty"], current_price, pos["unrealized_pnl"],
                                      metadata={
                                          "reason": "dead_money_timeout",
                                          "age_days": age_days,
                                          "pnl_pct": pnl_pct,
                                      })
                            clear_symbol(sym)
                            delete_position_state(db_conn, sym)
                            set_cooldown(sym)
                            _record_close_perf(db_conn, sym, pos.get("unrealized_pnl"))
                            logger.info(f"[TradeManagement] {sym} cooldown set after sell_dead_money")
                            continue
                        except Exception as e:
                            logger.error(f"[TRADE MGT] Dead money close failed for {sym}: {e}")
        except Exception as e:
            logger.debug(f"[TradeMgt] Dead money check error for {sym}: {e}")

        # 0b. Dust cleanup
        if sym in _partial_taken and market_value < MIN_POSITION_VALUE:
            logger.info(
                f"[TRADE MGT] DUST CLOSE {sym}: stub=${market_value:.0f} "
                f"< ${MIN_POSITION_VALUE} after partial take — closing"
            )
            try:
                broker.close_position(sym, strategy)
                log_trade(db_conn, strategy, sym, "sell_dust_close",
                          pos["qty"], pos["current_price"], pos["unrealized_pnl"],
                          metadata={
                              "reason": "dust_close",
                              "market_value": market_value,
                              "min_position_value": MIN_POSITION_VALUE,
                              "pnl_pct": pnl_pct,
                          })
                clear_symbol(sym)
                delete_position_state(db_conn, sym)
                set_cooldown(sym)
                _record_close_perf(db_conn, sym, pos.get("unrealized_pnl"))
                logger.info(f"[TradeManagement] {sym} cooldown set after sell_dust_close")
            except Exception as e:
                logger.error(f"[TRADE MGT] Failed dust close for {sym}: {e}")
            continue

        # 1. Trailing take profit — ride winners until momentum exhausts.
        #    Long-only: the peak-from-max logic is structurally wrong for shorts;
        #    shorts use the dollar-based ratchet (which tightens as price falls).
        qty_signed_tt = float(pos.get("qty") or 0)
        if qty_signed_tt > 0 and pnl_pct >= TRAIL_TAKE_ACTIVATE_PCT * 100:
            peak_price = _peak_prices.get(sym, current_price)
            drawdown_from_peak = (peak_price - current_price) / peak_price if peak_price > 0 else 0
            if drawdown_from_peak >= TRAIL_TAKE_DRAWDOWN_PCT:
                logger.info(
                    f"[TRADE MGT] TRAILING TAKE PROFIT {sym}: "
                    f"P&L={pnl_pct:+.1f}% peak=${peak_price:.2f} "
                    f"drawdown={drawdown_from_peak:.1%} >= {TRAIL_TAKE_DRAWDOWN_PCT:.0%} — closing"
                )
                try:
                    broker.close_position(sym, strategy)
                    log_trade(db_conn, strategy, sym, "sell_trail_take",
                              pos["qty"], current_price, pos["unrealized_pnl"],
                              metadata={
                                  "reason": "trailing_take_profit",
                                  "pnl_pct": pnl_pct,
                                  "peak_price": peak_price,
                                  "drawdown_from_peak_pct": round(drawdown_from_peak * 100, 2),
                              })
                    clear_symbol(sym)
                    delete_position_state(db_conn, sym)
                    set_cooldown(sym)
                    _record_close_perf(db_conn, sym, pos.get("unrealized_pnl"))
                    logger.info(f"[TradeManagement] {sym} cooldown set after sell_trail_take")
                    try:
                        pass  # [ntfy silenced — logged only]
                    except Exception:
                        pass
                except Exception as e:
                    logger.error(f"[TRADE MGT] Failed trailing take profit for {sym}: {e}")
                continue
            else:
                logger.debug(
                    f"[TRADE MGT] {sym} trailing take active: "
                    f"P&L={pnl_pct:+.1f}% drawdown={drawdown_from_peak:.1%} — still riding"
                )

        # 1b. OFI early exit — sustained sell pressure on a profitable long
        try:
            from utils.ofi_monitor import is_sell_pressure
            qty_signed = float(pos.get("qty", 0))
            if qty_signed > 0 and is_sell_pressure(sym):
                if pnl_pct > 2.0:  # only OFI-exit if up >2%
                    logger.info(
                        f"[TradeManagement] OFI sell pressure on {sym} "
                        f"({pnl_pct:+.2f}% profit) — early exit"
                    )
                    try:
                        broker.close_position(sym, strategy)
                        log_trade(db_conn, strategy, sym, "sell_ofi",
                                  pos["qty"], current_price, pos["unrealized_pnl"],
                                  metadata={"reason": "ofi_sell_pressure", "pnl_pct": pnl_pct})
                        clear_symbol(sym)
                        delete_position_state(db_conn, sym)
                        set_cooldown(sym)
                        _record_close_perf(db_conn, sym, pos.get("unrealized_pnl"))
                        logger.info(f"[TradeManagement] {sym} cooldown set after sell_ofi")
                    except Exception as e:
                        logger.error(f"[TradeManagement] OFI close failed for {sym}: {e}")
                    continue
        except Exception as e:
            logger.debug(f"[TradeManagement] OFI check error {sym}: {e}")

        # 2. Check partial profit taking first (before stop checks)
        if check_partial_take(pos, broker, db_conn, strategy):
            continue

        # 2b. Check trailing stop — long-only (trail logic uses max-peak which is
        # structurally wrong for shorts; shorts are protected by the dollar-based
        # ratchet path below).
        qty_signed_tr = float(pos.get("qty") or 0)
        if qty_signed_tr > 0 and check_trailing_stop(pos):
            logger.info(f"[TRADE MGT] TRAILING STOP — closing {sym} ({strategy})")
            try:
                broker.close_position(sym, strategy)
                log_trade(db_conn, strategy, sym, "sell_trail_stop",
                          pos["qty"], pos["current_price"], pos["unrealized_pnl"],
                          metadata={"reason": "trailing_stop", "peak_price": peak})
                clear_symbol(sym)
                delete_position_state(db_conn, sym)
                set_cooldown(sym)
                _record_close_perf(db_conn, sym, pos.get("unrealized_pnl"))
                logger.info(f"[TradeManagement] {sym} cooldown set after sell_trail_stop")
            except Exception as e:
                logger.error(f"[TRADE MGT] Failed to close {sym}: {e}")
            continue

        # 2c. v75 FIX 2 — trailing stop for ACTIVE WINNERS.
        # Activates only once gain ≥ TRAIL_ACTIVATE_PCT (1%). Side-aware:
        #   long  → peak = highest price; trail = peak * (1 - dist)
        #   short → peak = lowest  price; trail = peak * (1 + dist)
        # Tightens to TRAIL_TIGHTEN_PCT once gain ≥ TRAIL_TIGHTEN_AT_PCT (3%).
        # Stored in _ratchet_stops_dollar; never loosens. This sits ABOVE the
        # v74 initial-stop logic — initial stop still fires for losers; this
        # is purely the profit-protection layer.
        try:
            ps_trail = get_position_state(db_conn, sym)
        except Exception:
            ps_trail = None
        try:
            qty_signed_trail = float(pos.get("qty") or 0)
        except Exception:
            qty_signed_trail = 0.0
        side_trail = (
            ps_trail["side"] if ps_trail else
            ("long" if qty_signed_trail > 0 else "short")
        )
        # pnl_pct in this loop is in PERCENT (e.g. 1.23 for +1.23%); convert to fraction.
        pnl_frac = (pnl_pct / 100.0) if pnl_pct is not None else 0.0
        try:
            entry_trail = (
                float(ps_trail["entry_price"]) if ps_trail and ps_trail.get("entry_price") else
                (current_price / (1 + pnl_frac) if (1 + pnl_frac) != 0 else 0.0)
            )
        except Exception:
            entry_trail = 0.0

        if entry_trail > 0 and current_price > 0 and side_trail in ("long", "short"):
            if side_trail == "long":
                prev_peak = _peak_prices.get(sym, current_price)
                new_peak = max(prev_peak, current_price)
                _peak_prices[sym] = new_peak
                gain_pct_trail = (current_price - entry_trail) / entry_trail
            else:  # short — peak is the LOWEST price seen (most favourable)
                prev_peak = _peak_prices.get(sym, current_price)
                new_peak = min(prev_peak, current_price)
                _peak_prices[sym] = new_peak
                gain_pct_trail = (entry_trail - current_price) / entry_trail

            if gain_pct_trail >= TRAIL_ACTIVATE_PCT:
                trail_dist = (
                    TRAIL_TIGHTEN_PCT if gain_pct_trail >= TRAIL_TIGHTEN_AT_PCT
                    else TRAIL_DISTANCE_PCT
                )
                if side_trail == "long":
                    trail_stop = new_peak * (1 - trail_dist)
                    existing = _ratchet_stops_dollar.get(sym, 0.0)
                    new_stop = max(trail_stop, existing)  # never loosen for longs
                    _ratchet_stops_dollar[sym] = new_stop
                    # v78: sync ratchet to exchange-side stop order
                    if new_stop > existing:
                        try:
                            update_exchange_stop(broker, sym, new_stop)
                        except Exception as _e:
                            logger.debug(f"[TM] exchange stop update failed for {sym}: {_e}")
                    fired = current_price <= new_stop
                else:  # short
                    trail_stop = new_peak * (1 + trail_dist)
                    existing = _ratchet_stops_dollar.get(sym, float("inf"))
                    new_stop = min(trail_stop, existing)  # tighten down for shorts
                    _ratchet_stops_dollar[sym] = new_stop
                    # v78: sync ratchet to exchange-side stop order
                    if new_stop < existing:
                        try:
                            update_exchange_stop(broker, sym, new_stop)
                        except Exception as _e:
                            logger.debug(f"[TM] exchange stop update failed for {sym}: {_e}")
                    fired = current_price >= new_stop

                if fired:
                    side_label = "" if side_trail == "long" else " (short)"
                    peak_label = "peak" if side_trail == "long" else "peak_low"
                    logger.info(
                        f"[TrailStop] {sym}{side_label}: trail fired. "
                        f"{peak_label}=${new_peak:.2f} trail=${new_stop:.2f} "
                        f"price=${current_price:.2f} gain={gain_pct_trail:.2%}"
                    )
                    try:
                        broker.close_position(sym, "trade_management")
                        log_trade(
                            db_conn, strategy, sym, "sell_trail_winner",
                            pos["qty"], current_price, pos.get("unrealized_pnl"),
                            metadata={
                                "reason": (
                                    "trail_winner" if side_trail == "long"
                                    else "trail_winner_short"
                                ),
                                ("peak" if side_trail == "long" else "peak_low"): new_peak,
                                "trail_stop": new_stop,
                                "gain_pct": gain_pct_trail,
                                "side": side_trail,
                            },
                        )
                        clear_symbol(sym)
                        set_cooldown(sym)
                        delete_position_state(db_conn, sym)
                        _record_close_perf(db_conn, sym, pos.get("unrealized_pnl"))
                    except Exception as e:
                        logger.error(f"[TrailStop] close failed for {sym}: {e}")
                    continue

        # 3. v74 — dollar-based ratchet stop driven by positions_state.
        #    The position is checked against a $-price, side-aware. Tightens
        #    monotonically with favourable move. For pre-v74 orphans without
        #    a positions_state row, we seed one and skip this cycle (next
        #    cycle uses the fresh state).
        ps = None
        try:
            ps = get_position_state(db_conn, sym)
        except Exception as e:
            logger.debug(f"[TM] get_position_state failed for {sym}: {e}")

        if ps is None:
            try:
                _seed_legacy_position_state(db_conn, broker, pos)
            except Exception as e:
                logger.error(f"[TM] Legacy seed error for {sym}: {e}")
            # Skip stop logic this cycle to avoid acting on freshly-seeded state.
            continue

        try:
            _prev_stop = _ratchet_stops_dollar.get(sym)
            effective_stop = _ratchet_stop_price(sym, ps, current_price)
            side_ps = ps["side"]
            stop_hit = (current_price <= effective_stop) if side_ps == "long" else (current_price >= effective_stop)
            # v78: sync ratchet to exchange-side stop order when stop moves
            _moved = (
                _prev_stop is None or
                (side_ps == "long" and effective_stop > _prev_stop) or
                (side_ps == "short" and effective_stop < _prev_stop)
            )
            if _moved and not stop_hit:
                try:
                    update_exchange_stop(broker, sym, effective_stop)
                except Exception as _e:
                    logger.debug(f"[TM] exchange stop update failed for {sym}: {_e}")
        except Exception as e:
            logger.error(f"[TM] ratchet-stop calc failed for {sym}: {e}")
            stop_hit = False
            effective_stop = 0.0
            side_ps = "long"

        if stop_hit:
            initial_stop_v = float(ps["initial_stop"])
            is_ratchet = (effective_stop != initial_stop_v)
            reason = "ratchet_stop" if is_ratchet else "initial_stop"
            action = "sell_ratchet_stop" if is_ratchet else "sell_stop"
            logger.info(
                f"[TM] {sym} {'RATCHET STOP' if is_ratchet else 'INITIAL STOP'} "
                f"at ${effective_stop:.2f} (side={side_ps}, price=${current_price:.2f}, "
                f"P&L={pnl_pct:+.1f}%)"
            )
            try:
                broker.close_position(sym, strategy)
                log_trade(db_conn, strategy, sym, action,
                          pos["qty"], pos["current_price"], pos["unrealized_pnl"],
                          metadata={
                              "reason": reason,
                              "effective_stop": effective_stop,
                              "initial_stop": initial_stop_v,
                              "side": side_ps,
                              "pnl_pct_at_stop": pnl_pct,
                          })
                clear_symbol(sym)
                delete_position_state(db_conn, sym)
                set_cooldown(sym)
                _record_close_perf(db_conn, sym, pos.get("unrealized_pnl"))
                logger.info(f"[TradeManagement] {sym} cooldown set after {action}")
            except Exception as e:
                logger.error(f"[TRADE MGT] Failed to close {sym}: {e}")
            continue

        # 3b. Sector ETF cap — trim long ETF positions exceeding 5% of equity.
        SECTOR_ETFS = {
            "XLK", "XLE", "XLRE", "XLF", "XLV", "XLI", "XLY",
            "XLP", "XLB", "XLU", "XLC", "GDX", "IAU", "TLT", "HYG",
        }
        ETF_MAX_PCT = 0.05  # 5% of portfolio
        try:
            qty_signed = float(pos.get("qty", 0))
            if qty_signed > 0 and sym in SECTOR_ETFS:
                account = broker.get_account()
                equity = float(account["equity"])
                position_value = abs(market_value)
                max_value = equity * ETF_MAX_PCT
                if equity > 0 and position_value > max_value * 1.1:  # 10% buffer
                    excess_value = position_value - max_value
                    if current_price > 0:
                        trim_qty = int(excess_value / current_price)
                        if trim_qty >= 1:
                            logger.info(
                                f"[TRADE MGT] ETF CAP TRIM {sym}: ${position_value:.0f} "
                                f"(>{ETF_MAX_PCT:.0%} of ${equity:.0f}) — selling {trim_qty} shares"
                            )
                            try:
                                broker.market_sell(sym, trim_qty, strategy)
                                log_trade(db_conn, strategy, sym, "sell_etf_cap",
                                          trim_qty, current_price, 0.0,
                                          metadata={
                                              "reason": "etf_cap_trim",
                                              "position_value": position_value,
                                              "max_value": max_value,
                                              "trim_qty": trim_qty,
                                          })
                                set_cooldown(sym)
                                logger.info(f"[TradeManagement] {sym} cooldown set after sell_etf_cap")
                                continue
                            except Exception as e:
                                logger.error(f"[TRADE MGT] ETF cap trim failed for {sym}: {e}")
        except Exception as e:
            logger.debug(f"[TradeMgt] ETF cap check error for {sym}: {e}")

        # 4. Hard floor stop — catches any position down 7%+ regardless
        if pnl_pct <= -7.0:
            logger.info(f"[TRADE MGT] HARD FLOOR STOP {sym} @ {pnl_pct:.1f}% — closing")
            try:
                broker.close_position(sym, strategy)
                log_trade(db_conn, strategy, sym, "sell_stop",
                          pos["qty"], pos["current_price"], pos["unrealized_pnl"],
                          metadata={"reason": "hard_floor_stop"})
                clear_symbol(sym)
                delete_position_state(db_conn, sym)
                set_cooldown(sym)
                _record_close_perf(db_conn, sym, pos.get("unrealized_pnl"))
                logger.info(f"[TradeManagement] {sym} cooldown set after hard_floor_stop")
            except Exception as e:
                logger.error(f"[TRADE MGT] Failed to close {sym}: {e}")


# ── v44: Earnings-aware autonomous stop tightening ───────────────────────────
# RSI(14) helper — cached 10 minutes so repeated cycles don't re-fetch.
_rsi_cache: dict = {}   # symbol -> (rsi_value, cached_at_epoch)
_atr_cache: dict = {}   # symbol -> (atr_value, cached_at_epoch)
_RSI_TTL = 600          # 10 minutes

# Symbols whose post-earnings review has already fired (don't re-act).
_post_earnings_checked: set = set()


def _get_current_rsi(symbol: str) -> float:
    """Compute RSI(14) from the last ~20 days of daily closes via yf_cache.

    Returns a float in [0, 100], or 50.0 if data is unavailable.
    Cached per-symbol for 10 minutes.
    """
    now_ts = time.time()
    cached = _rsi_cache.get(symbol)
    if cached is not None and (now_ts - cached[1]) < _RSI_TTL:
        return cached[0]

    rsi_val = 50.0
    try:
        from utils import yf_cache
        hist = yf_cache.get_history(symbol, period="1mo", interval="1d")
        if hist is not None and not hist.empty and len(hist) >= 15:
            closes = hist["Close"].dropna()
            if len(closes) >= 15:
                deltas = closes.diff().dropna()
                period = 14
                recent = deltas.tail(period)
                gains = recent.clip(lower=0).sum() / period
                losses = (-recent.clip(upper=0)).sum() / period
                if losses == 0:
                    rsi_val = 100.0
                else:
                    rs = gains / losses
                    rsi_val = 100.0 - (100.0 / (1.0 + rs))
                rsi_val = float(rsi_val)
    except Exception as e:
        logger.debug(f"[RSI] {symbol}: compute failed — {e}")

    _rsi_cache[symbol] = (rsi_val, now_ts)
    return rsi_val


def _get_current_atr(symbol: str) -> float:
    """Compute ATR(14) from last 20 days of daily OHLC. Returns 0.0 on failure."""
    now_ts = time.time()
    cached = _atr_cache.get(symbol)
    if cached is not None and (now_ts - cached[1]) < _RSI_TTL:  # reuse same 10-min TTL
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
        logger.debug(f"[ATR] {symbol}: compute failed — {e}")
    _atr_cache[symbol] = (atr_val, now_ts)
    return atr_val


def _resolve_entry_price(symbol: str, pos: dict, db_conn) -> float:
    """Best-effort entry price: prefer most-recent BUY in trades table."""
    try:
        if db_conn is not None:
            row = db_conn.execute(
                "SELECT price FROM trades "
                "WHERE symbol=? AND side IN ('buy','buy_pyramid') "
                "ORDER BY created_at DESC LIMIT 1",
                (symbol,),
            ).fetchone()
            if row and row["price"] and float(row["price"]) > 0:
                return float(row["price"])
    except Exception:
        pass
    try:
        return float(pos.get("avg_entry") or 0)
    except Exception:
        return 0.0


def _atr_tighten(symbol: str, entry_price: float, current_price: float,
                 min_gain_pct: float, atr_mult: float) -> tuple[bool, float]:
    """
    Compute ATR-aware stop and tighten _ratchet_stops if it improves on the current lock.

    Stop = max(
        entry * (1 + min_gain_pct),      # never give back more than min_gain
        current - atr * atr_mult          # breathing room based on volatility
    )

    Never loosens an existing stop. Returns (tightened: bool, new_stop_price: float).
    """
    atr = _get_current_atr(symbol)
    if atr <= 0:
        # Fallback: use min_gain_pct only
        new_stop = entry_price * (1.0 + min_gain_pct)
    else:
        gain_floor = entry_price * (1.0 + min_gain_pct)
        atr_stop   = current_price - (atr * atr_mult)
        new_stop   = max(gain_floor, atr_stop)

    # Convert to a gain-fraction relative to entry for storage in _ratchet_stops
    if entry_price <= 0:
        return False, 0.0
    new_lock = (new_stop / entry_price) - 1.0

    current_lock = _ratchet_stops.get(symbol)
    if current_lock is None or new_lock > current_lock:
        _ratchet_stops[symbol] = new_lock
        return True, new_stop
    return False, entry_price * (1.0 + current_lock)


def apply_earnings_stop_tightening(positions, broker: AlpacaBroker, db_conn):
    """Autonomously tighten stops on positions based on earnings proximity + RSI + gain.

    Goal: ride winners hard, protect aggressively when earnings + extreme RSI raise risk.
    Never loosens an existing stop.
    """
    try:
        from utils.earnings_calendar import get_next_earnings_date
    except Exception as e:
        logger.warning(f"[EarningsTighten] earnings_calendar import failed: {e}")
        return

    today = now_utc().date()

    for pos in positions:
        sym = pos["symbol"]
        try:
            qty_signed = float(pos.get("qty", 0))
            if qty_signed <= 0:
                continue   # longs only
            try:
                gain_pct = float(pos.get("unrealized_pnl_pct") or 0.0)
            except Exception:
                continue

            entry_price = _resolve_entry_price(sym, pos, db_conn)
            if entry_price <= 0:
                continue

            current_price = float(pos.get("current_price") or pos.get("price") or 0.0)
            if current_price <= 0:
                continue

            earn_date = None
            try:
                earn_date = get_next_earnings_date(sym)
            except Exception as e:
                logger.debug(f"[EarningsTighten] {sym} earnings lookup failed: {e}")
            days_to_earnings = (earn_date - today).days if earn_date else None

            rsi = _get_current_rsi(sym)

            tightened = False
            new_stop = 0.0
            tier = ""

            if days_to_earnings is not None and days_to_earnings <= 1:
                if rsi > 80 and gain_pct > 15:
                    tightened, new_stop = _atr_tighten(sym, entry_price, current_price, 0.12, 1.5)
                    tier = "earnings≤1d / RSI>80 / +15%"
                elif rsi > 70 and gain_pct > 10:
                    tightened, new_stop = _atr_tighten(sym, entry_price, current_price, 0.07, 1.5)
                    tier = "earnings≤1d / RSI>70 / +10%"
                elif gain_pct > 5:
                    tightened, new_stop = _atr_tighten(sym, entry_price, current_price, 0.03, 2.0)
                    tier = "earnings≤1d / +5%"
                else:
                    tightened, new_stop = _atr_tighten(sym, entry_price, current_price, -0.03, 2.0)
                    tier = "earnings≤1d / floor -3%"
            elif days_to_earnings is not None and days_to_earnings <= 3:
                if rsi > 85 and gain_pct > 20:
                    tightened, new_stop = _atr_tighten(sym, entry_price, current_price, 0.10, 2.0)
                    tier = "earnings≤3d / RSI>85 / +20%"
                elif rsi > 75 and gain_pct > 10:
                    tightened, new_stop = _atr_tighten(sym, entry_price, current_price, 0.06, 2.0)
                    tier = "earnings≤3d / RSI>75 / +10%"
            elif days_to_earnings is not None and days_to_earnings <= 7:
                if rsi > 90:
                    tightened, new_stop = _atr_tighten(sym, entry_price, current_price, 0.08, 2.0)
                    tier = "earnings≤7d / RSI>90"
            else:
                # Feature 2: extreme RSI tightening even without imminent earnings
                if rsi > 90 and gain_pct > 20:
                    tightened, new_stop = _atr_tighten(sym, entry_price, current_price, 0.12, 2.0)
                    tier = "no-earnings / RSI>90 / +20%"
                elif rsi > 85 and gain_pct > 15:
                    tightened, new_stop = _atr_tighten(sym, entry_price, current_price, 0.08, 2.0)
                    tier = "no-earnings / RSI>85 / +15%"

            if tightened:
                days_txt = (
                    f"{days_to_earnings} day(s)" if days_to_earnings is not None else "n/a"
                )
                atr_val = _get_current_atr(sym)
                logger.info(
                    f"[EarningsTighten] {sym} → stop ${new_stop:.2f} "
                    f"(ATR=${atr_val:.2f}) | gain={gain_pct:+.1f}% RSI={rsi:.0f} "
                    f"earnings_in={days_txt} | tier={tier}"
                )
                try:
                    pass  # [ntfy silenced — logged only]
                except Exception:
                    pass
        except Exception as e:
            logger.debug(f"[EarningsTighten] {sym}: {e}")


def check_post_earnings_action(positions, broker: AlpacaBroker, db_conn):
    """Post-earnings review for the 0-2 day window after earnings.

    - DOWN >5% since pre-earnings close → close (gap-down protection)
    - UP >10% → upgrade ratchet one tier (treat gain as +5% higher)
    - Flat (±3%) → no change
    Each symbol acted-on at most once (tracked in _post_earnings_checked).
    """
    today = now_utc().date()

    for pos in positions:
        sym = pos["symbol"]
        if sym in _post_earnings_checked:
            continue
        try:
            qty_signed = float(pos.get("qty", 0))
            if qty_signed <= 0:
                continue
            strategy = pos.get("strategy", "unknown")
            current_price = float(pos.get("current_price") or 0)
            if current_price <= 0:
                continue

            # Find most-recent earnings date in the past 0-2 days for this symbol.
            earn_date = None
            if sym not in _NO_EARNINGS_SYMBOLS:  # v77-fix: skip ETF calendar fetch
                try:
                    import yfinance as yf, gc as _gc
                    ticker = yf.Ticker(sym)
                    cal = ticker.calendar
                    raw_dates = []
                    if cal is None:
                        pass
                    elif isinstance(cal, dict):
                        ed = cal.get("Earnings Date", [])
                        if ed:
                            raw_dates = ed if isinstance(ed, list) else [ed]
                    elif hasattr(cal, "empty") and not cal.empty:
                        if hasattr(cal, "columns") and "Earnings Date" in getattr(cal, "columns", []):
                            raw_dates = cal["Earnings Date"].dropna().tolist()
                        elif hasattr(cal, "index") and "Earnings Date" in getattr(cal, "index", []):
                            val = cal.loc["Earnings Date"]
                            raw_dates = val.tolist() if hasattr(val, "tolist") else [val]
                    for d in raw_dates:
                        try:
                            dd = d.date() if hasattr(d, "date") else d
                            if (today - timedelta(days=2)) <= dd <= today:
                                earn_date = dd
                                break
                        except Exception:
                            continue
                except Exception as e:
                    logger.debug(f"[PostEarnings] {sym} calendar fetch failed: {e}")

            if earn_date is None:
                continue

            # Pre-earnings close — last trading day strictly before earn_date.
            pre_close = None
            try:
                from utils import yf_cache
                hist = yf_cache.get_history(sym, period="1mo", interval="1d")
                if hist is not None and not hist.empty:
                    closes = hist["Close"].dropna()
                    pre_rows = []
                    for ts, val in closes.items():
                        try:
                            d = ts.date() if hasattr(ts, "date") else ts
                            if d < earn_date:
                                pre_rows.append((d, float(val)))
                        except Exception:
                            continue
                    if pre_rows:
                        pre_rows.sort()
                        pre_close = pre_rows[-1][1]
            except Exception as e:
                logger.debug(f"[PostEarnings] {sym} pre-close lookup failed: {e}")

            if not pre_close or pre_close <= 0:
                continue

            move_pct = (current_price - pre_close) / pre_close * 100.0
            logger.info(
                f"[PostEarnings] {sym} earnings {earn_date} | pre=${pre_close:.2f} → "
                f"now=${current_price:.2f} ({move_pct:+.1f}%)"
            )

            if move_pct < -5.0:
                logger.info(f"[PostEarnings] {sym} gapped down {move_pct:+.1f}% — closing")
                try:
                    broker.close_position(sym, strategy)
                    log_trade(db_conn, strategy, sym, "sell_post_earnings_gap",
                              pos["qty"], current_price, pos.get("unrealized_pnl", 0.0),
                              metadata={
                                  "reason": "post_earnings_gap_down",
                                  "pre_earnings_close": pre_close,
                                  "move_pct": move_pct,
                              })
                    clear_symbol(sym)
                    delete_position_state(db_conn, sym)
                    _record_close_perf(db_conn, sym, pos.get("unrealized_pnl"))
                    try:
                        pass  # [ntfy silenced — logged only]
                    except Exception:
                        pass
                except Exception as e:
                    logger.error(f"[PostEarnings] Close failed for {sym}: {e}")
                _post_earnings_checked.add(sym)
                continue

            if move_pct > 10.0:
                try:
                    boosted = float(pos.get("unrealized_pnl_pct") or 0.0) + 5.0
                    new_ratchet = _ratchet_for_pnl(boosted)
                    if new_ratchet is not None:
                        current = _ratchet_stops.get(sym, -1.0)
                        if new_ratchet > current:
                            _ratchet_stops[sym] = new_ratchet
                            logger.info(
                                f"[PostEarnings] {sym} upgraded ratchet to +{new_ratchet:.0%} "
                                f"(post-earnings beat: {move_pct:+.1f}%)"
                            )
                            try:
                                pass  # [ntfy silenced — logged only]
                            except Exception:
                                pass
                except Exception as e:
                    logger.debug(f"[PostEarnings] {sym} ratchet upgrade failed: {e}")
                _post_earnings_checked.add(sym)
                continue

            if -3.0 <= move_pct <= 3.0:
                logger.info(f"[PostEarnings] {sym} flat ({move_pct:+.1f}%) — no change")
                _post_earnings_checked.add(sym)
        except Exception as e:
            logger.debug(f"[PostEarnings] {sym}: {e}")

    # Suspicious-gain watchlist — flag any position with >20% 1w gain for manual review
    for pos in positions:
        sym = pos["symbol"]
        if sym in _post_earnings_checked:
            continue
        try:
            pnl_1w = float(pos.get("unrealized_pnl_pct") or 0.0)  # approximate with unrealized
            # Also check 1w return via yf
            from utils import yf_cache
            hist = yf_cache.get_history(sym, period="5d", interval="1d")
            if hist is not None and not hist.empty:
                closes = hist["Close"].dropna()
                if len(closes) >= 2:
                    w_ret = (closes.iloc[-1] - closes.iloc[0]) / closes.iloc[0]
                    if w_ret > 0.20:
                        logger.warning(f"[PostEarnings] {sym} flagged: +{w_ret:.1%} 1w — manual review needed")
                        try:
                            pass  # [ntfy silenced — logged only]
                        except Exception:
                            pass
                        _post_earnings_checked.add(sym)  # only notify once
        except Exception:
            pass


def _in_overnight_exit_window() -> bool:
    """True if current London (BST/GMT) time is within OVERNIGHT_EXIT_WINDOW_*."""
    try:
        now = now_london()
        start_h, start_m = (int(x) for x in OVERNIGHT_EXIT_WINDOW_START.split(":"))
        end_h, end_m = (int(x) for x in OVERNIGHT_EXIT_WINDOW_END.split(":"))
        cur_minutes = now.hour * 60 + now.minute
        return (start_h * 60 + start_m) <= cur_minutes <= (end_h * 60 + end_m)
    except Exception:
        return False


def check_overnight_exit(broker: AlpacaBroker, db_conn):
    """
    Run between 20:15–20:29 BST (15–29 min before NYSE close at 20:30 BST in summer).
    Close any position with unrealized P&L pct < OVERNIGHT_LOSS_THRESHOLD.

    Rationale (v75 / alpha_attribution): overnight losers (QCOM −$1,414, MPC −$818,
    MPWR −$427, NOW −$460) accounted for 33%+ of total gross loss. Any position
    losing into the close gets flat — winners can ride overnight.
    """
    if not _in_overnight_exit_window():
        return
    try:
        positions = broker.get_positions()
    except Exception as e:
        logger.warning(f"[OvernightExit] get_positions failed: {e}")
        return

    logger.info(
        f"[OvernightExit] Window active — scanning {len(positions)} position(s) "
        f"for pnl_pct < {OVERNIGHT_LOSS_THRESHOLD:.2%}"
    )

    for pos in positions:
        sym = pos.get("symbol")
        if not sym:
            continue
        try:
            # Alpaca's unrealized_plpc is a fraction (e.g. -0.0123 for -1.23%).
            # broker.get_positions() multiplies by 100, so we must convert back.
            pnl_pct_pct = float(pos.get("unrealized_pnl_pct") or 0.0)
            pnl_pct = pnl_pct_pct / 100.0
        except Exception:
            continue

        if pnl_pct >= OVERNIGHT_LOSS_THRESHOLD:
            continue

        strategy = pos.get("strategy", "unknown")
        current_price = float(pos.get("current_price") or 0.0)
        qty = pos.get("qty")
        unrealized_pnl = pos.get("unrealized_pnl", 0.0)
        logger.info(
            f"[OvernightExit] Closing {sym} before overnight: "
            f"pnl={pnl_pct:.2%} < threshold={OVERNIGHT_LOSS_THRESHOLD:.2%}"
        )
        try:
            broker.close_position(sym, "trade_management")
            log_trade(
                db_conn, strategy, sym, "sell_overnight_exit",
                qty, current_price, unrealized_pnl,
                metadata={
                    "reason": "overnight_loss_exit",
                    "pnl_pct": pnl_pct,
                    "threshold": OVERNIGHT_LOSS_THRESHOLD,
                },
            )
            set_cooldown(sym)
            try:
                delete_position_state(db_conn, sym)
            except Exception as e:
                logger.debug(f"[OvernightExit] delete_position_state {sym}: {e}")
            clear_symbol(sym)
            try:
                update_symbol_performance(db_conn, sym, float(unrealized_pnl or 0.0))
                check_and_update_blacklist(db_conn, sym)
            except Exception as e:
                logger.debug(f"[OvernightExit] symbol_performance {sym}: {e}")
        except Exception as e:
            logger.error(f"[OvernightExit] close failed for {sym}: {e}")


def run_trade_management(broker: AlpacaBroker, db_conn):
    """v44 entry point — earnings tightening + post-earnings review BEFORE normal ratchet logic."""
    try:
        positions = broker.get_positions()
    except Exception as e:
        logger.error(f"[TradeMgt] get_positions failed: {e}")
        return
    try:
        apply_earnings_stop_tightening(positions, broker, db_conn)
    except Exception as e:
        logger.error(f"[TradeMgt] earnings tightening failed: {e}", exc_info=True)
    try:
        check_post_earnings_action(positions, broker, db_conn)
    except Exception as e:
        logger.error(f"[TradeMgt] post-earnings review failed: {e}", exc_info=True)
    # Then normal global trade management (trailing stops + partial takes).
    run_global_trade_management(broker, db_conn)
