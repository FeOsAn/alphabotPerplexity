"""
Shared Trade Management Utilities
-----------------------------------
Trailing stops and partial profit taking — used by all strategies.

TRAILING STOP:
  Tracks each position's peak price since entry. Stop loss moves up with
  the price but never down. E.g. if a stock goes +10% then drops 5% from
  the peak, we exit at +5% instead of waiting for the original -5% stop.

  Peak tracking is in-memory (resets on restart). On restart, we
  conservatively use current price as the initial peak — this means
  the trailing stop starts fresh from the current level, which is safe
  (worst case we give back a bit more than intended on the first cycle
  after restart, but we never stay in a losing position indefinitely).

PARTIAL PROFIT TAKING:
  At +PARTIAL_TAKE_PCT, sell half the position and let the rest run.
  This locks in real gains while keeping upside exposure.
  The remaining half stays open with a ratchet trailing stop that locks
  in profit as the position climbs — we ride the winner, not cap it.
  Tracked per-symbol to avoid double-selling.

DUST CLEANUP:
  After a partial take the remaining stub may be too small to matter.
  Any position below MIN_POSITION_VALUE ($500) where a partial take has
  already fired gets closed entirely rather than left as dead weight.
"""

import logging
from datetime import datetime
from broker import AlpacaBroker
from db import log_trade

logger = logging.getLogger("alphabot.trade_management")


def _is_post_earnings_window(symbol: str, days: int = 2) -> bool:
    """Returns True if earnings occurred within the last `days` trading days."""
    try:
        import yfinance as yf, gc
        ticker = yf.Ticker(symbol)
        cal = ticker.calendar
        gc.collect()
        if cal is None or cal.empty:
            return False
        if "Earnings Date" in cal.columns:
            dates = cal["Earnings Date"].dropna()
        elif hasattr(cal, "index") and "Earnings Date" in cal.index:
            dates = [cal.loc["Earnings Date"]]
        else:
            return False
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc).date()
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
MIN_POSITION_VALUE  = 500    # Close entire position if stub is below $500 after partial take

# Per-strategy base stop loss (fraction). Used as the floor before ratchet kicks in.
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
_ratchet_stops: dict[str, float] = {}      # symbol -> locked stop pct (e.g. 0.05 = +5%)


def _get_ratchet_stop(symbol: str, current_pnl_pct: float, base_stop_pct: float) -> float:
    """
    Returns the effective stop loss % (as a fraction; negative = below entry).
    Ratchets up as position gains. Never moves the stop down.
    current_pnl_pct: e.g. 8.5 (percent)
    base_stop_pct: e.g. 0.05 (fraction)
    Returns: effective stop as fraction, e.g. 0.0 means stop at breakeven
    """
    RATCHET_LEVELS = [
        (25.0, 0.20),   # pnl >= 25% -> stop at +20%
        (20.0, 0.15),   # pnl >= 20% -> stop at +15%
        (15.0, 0.10),   # pnl >= 15% -> stop at +10%
        (10.0, 0.05),   # pnl >= 10% -> stop at +5%
        (5.0,  0.00),   # pnl >= 5%  -> stop at breakeven
    ]

    new_ratchet = None
    for threshold_pct, lock_pct in RATCHET_LEVELS:
        if current_pnl_pct >= threshold_pct:
            new_ratchet = lock_pct
            break

    if new_ratchet is None:
        # Below +5%, use normal stop loss (negative) unless already ratcheted up
        current_ratchet = _ratchet_stops.get(symbol)
        if current_ratchet is not None:
            return current_ratchet
        return -base_stop_pct

    # Only ratchet UP, never down
    current_ratchet = _ratchet_stops.get(symbol, -base_stop_pct)
    if new_ratchet > current_ratchet:
        _ratchet_stops[symbol] = new_ratchet
        logger.info(
            f"[Ratchet] {symbol} P&L={current_pnl_pct:+.1f}% → "
            f"stop ratcheted to {new_ratchet:+.0%}"
        )

    return _ratchet_stops.get(symbol, -base_stop_pct)


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
    The remaining half stays open — the ratchet trailing stop will ride it up
    and lock in profit as the stock climbs. We don't cap upside here.
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
        # (trailing stop now trails from here, not the original peak)
        _peak_prices[sym] = pos["current_price"]
        return True
    except Exception as e:
        logger.error(f"[PARTIAL] Failed partial exit for {sym}: {e}")
        return False


def run_global_trade_management(broker: AlpacaBroker, db_conn):
    """
    Run trailing stops + partial profit taking across ALL positions.
    Called once per cycle from main.py before individual strategies run.
    Replaces the old fixed-stop global enforcer.
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

        # 0. Dust cleanup — if a partial take already fired and the remaining
        #    stub is below MIN_POSITION_VALUE, close the whole thing.
        #    No point holding $50 with a trailing stop — it's irrelevant money.
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
            except Exception as e:
                logger.error(f"[TRADE MGT] Failed dust close for {sym}: {e}")
            continue

        # 1. Check partial profit taking first (before stop checks)
        if check_partial_take(pos, broker, db_conn, strategy):
            continue  # partial exit done, re-check next cycle with updated qty

        # 2. Check trailing stop
        if check_trailing_stop(pos):
            logger.info(f"[TRADE MGT] TRAILING STOP — closing {sym} ({strategy})")
            try:
                broker.close_position(sym, strategy)
                log_trade(db_conn, strategy, sym, "sell_trail_stop",
                          pos["qty"], pos["current_price"], pos["unrealized_pnl"],
                          metadata={"reason": "trailing_stop", "peak_price": peak})
                clear_symbol(sym)
                from utils.cooldown import set_cooldown
                set_cooldown(sym)
            except Exception as e:
                logger.error(f"[TRADE MGT] Failed to close {sym}: {e}")
            continue

        # 3. Ratchet stop — locks in profit as position gains, never moves down
        base_stop_pct = _base_stop_for(strategy)
        effective_stop = _get_ratchet_stop(sym, pnl_pct, base_stop_pct)
        if pnl_pct / 100 < effective_stop:
            if effective_stop >= 0:
                logger.info(
                    f"[TM] {sym} RATCHET STOP at {effective_stop:+.0%} "
                    f"(P&L={pnl_pct:+.1f}%)"
                )
                reason = "ratchet_stop"
                action = "sell_ratchet_stop"
            else:
                logger.info(
                    f"[TM] {sym} STOP LOSS at {pnl_pct:+.1f}% "
                    f"(stop={effective_stop:.0%})"
                )
                reason = "base_stop"
                action = "sell_stop"
            try:
                broker.close_position(sym, strategy)
                log_trade(db_conn, strategy, sym, action,
                          pos["qty"], pos["current_price"], pos["unrealized_pnl"],
                          metadata={
                              "reason": reason,
                              "effective_stop_pct": effective_stop,
                              "base_stop_pct": base_stop_pct,
                              "pnl_pct_at_stop": pnl_pct,
                          })
                clear_symbol(sym)
                if reason == "base_stop":
                    from utils.cooldown import set_cooldown
                    set_cooldown(sym)
            except Exception as e:
                logger.error(f"[TRADE MGT] Failed to close {sym}: {e}")
            continue

        # 4. Hard floor stop — catches any position down 7%+ regardless
        # (safety net in case trailing stop tracking reset after deploy)
        if pnl_pct <= -7.0:
            logger.info(f"[TRADE MGT] HARD FLOOR STOP {sym} @ {pnl_pct:.1f}% — closing")
            try:
                broker.close_position(sym, strategy)
                log_trade(db_conn, strategy, sym, "sell_stop",
                          pos["qty"], pos["current_price"], pos["unrealized_pnl"],
                          metadata={"reason": "hard_floor_stop"})
                clear_symbol(sym)
                from utils.cooldown import set_cooldown
                set_cooldown(sym)
            except Exception as e:
                logger.error(f"[TRADE MGT] Failed to close {sym}: {e}")
