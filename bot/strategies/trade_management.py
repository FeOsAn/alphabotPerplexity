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
  Tracked per-symbol to avoid double-selling.
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

# ── In-memory state ────────────────────────────────────────────────────────────
_peak_prices: dict[str, float] = {}        # symbol -> highest price seen
_partial_taken: set[str] = set()           # symbols where partial exit already fired


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
        f"selling {PARTIAL_TAKE_RATIO*100:.0f}% ({sell_qty:.4f} shares), letting rest run"
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
        peak = _peak_prices.get(sym, current_price)

        logger.info(
            f"  {sym} ({strategy}): P&L={pnl_pct:+.1f}% | "
            f"price=${current_price:.2f} | peak=${peak:.2f}"
        )

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
            except Exception as e:
                logger.error(f"[TRADE MGT] Failed to close {sym}: {e}")
            continue

        # 3. Hard floor stop — catches any position down 7%+ regardless
        # (safety net in case trailing stop tracking reset after deploy)
        if pnl_pct <= -7.0:
            logger.info(f"[TRADE MGT] HARD FLOOR STOP {sym} @ {pnl_pct:.1f}% — closing")
            try:
                broker.close_position(sym, strategy)
                log_trade(db_conn, strategy, sym, "sell_stop",
                          pos["qty"], pos["current_price"], pos["unrealized_pnl"],
                          metadata={"reason": "hard_floor_stop"})
                clear_symbol(sym)
            except Exception as e:
                logger.error(f"[TRADE MGT] Failed to close {sym}: {e}")
