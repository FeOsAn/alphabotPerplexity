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
import pandas as pd
from broker import AlpacaBroker
from db import log_trade
from utils import notify

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
_ratchet_stops: dict[str, float] = {}      # symbol -> locked stop pct (e.g. 0.05 = +5%)
_state_restored: bool = False              # gate so restore runs only once


# ── Ratchet logic ─────────────────────────────────────────────────────────────
_RATCHET_LEVELS = [
    (25.0, 0.20),   # pnl >= 25% -> stop at +20%
    (20.0, 0.15),   # pnl >= 20% -> stop at +15%
    (15.0, 0.10),   # pnl >= 15% -> stop at +10%
    (10.0, 0.05),   # pnl >= 10% -> stop at +5%
    (5.0,  0.00),   # pnl >= 5%  -> stop at breakeven
]


def _ratchet_for_pnl(current_pnl_pct: float):
    """Return the highest ratchet lock_pct applicable for current_pnl_pct, or None."""
    for threshold_pct, lock_pct in _RATCHET_LEVELS:
        if current_pnl_pct >= threshold_pct:
            return lock_pct
    return None


def _get_ratchet_stop(symbol: str, current_pnl_pct: float, base_stop_pct: float) -> float:
    """
    Returns the effective stop loss % (as a fraction; negative = below entry).
    Ratchets up as position gains. Never moves the stop down.
    """
    new_ratchet = _ratchet_for_pnl(current_pnl_pct)
    if new_ratchet is None:
        current_ratchet = _ratchet_stops.get(symbol)
        if current_ratchet is not None:
            return current_ratchet
        return -base_stop_pct

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

            # 3. Ratchet — based on current unrealized gain
            try:
                pnl_pct = float(pos.get("unrealized_pnl_pct") or 0.0)
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
        f"peaks={len(_peak_prices)} ratchets={len(_ratchet_stops)} partial={len(_partial_taken)}"
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

        # 0a. Dead money timeout — any equity position flat ±1.5% for >=5 trading days
        # exits regardless of strategy tag. Runs FIRST so trailing stops, ratchets,
        # and partial-take logic don't keep stale positions alive indefinitely.
        DEAD_MONEY_DAYS = 5
        DEAD_MONEY_MIN_PCT = -1.5
        DEAD_MONEY_MAX_PCT = 1.5
        try:
            qty_signed = float(pos.get("qty", 0))
            asset_class = pos.get("asset_class", "equity")
            if (
                qty_signed > 0
                and asset_class == "equity"
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
                    age_days = (datetime.now(timezone.utc) - entry_dt).days
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
            except Exception as e:
                logger.error(f"[TRADE MGT] Failed dust close for {sym}: {e}")
            continue

        # 1. Trailing take profit — ride winners until momentum exhausts.
        if pnl_pct >= TRAIL_TAKE_ACTIVATE_PCT * 100:
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
                    try:
                        notify.send(
                            title=f"📈 Trailing Take-Profit: {sym}",
                            body=f"{sym} exited at +{pnl_pct/100:.1%} after {drawdown_from_peak:.1%} pullback from peak.",
                            priority="default",
                            tags="chart_with_upwards_trend",
                        )
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
                    except Exception as e:
                        logger.error(f"[TradeManagement] OFI close failed for {sym}: {e}")
                    continue
        except Exception as e:
            logger.debug(f"[TradeManagement] OFI check error {sym}: {e}")

        # 2. Check partial profit taking first (before stop checks)
        if check_partial_take(pos, broker, db_conn, strategy):
            continue

        # 2b. Check trailing stop
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
                from utils.cooldown import set_cooldown
                set_cooldown(sym)
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

    today = datetime.now(timezone.utc).date()

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
                    notify.send(
                        title=f"🔒 Stop tightened: {sym}",
                        body=(
                            f"🔒 {sym}: stop → ${new_stop:.2f} "
                            f"(ATR=${atr_val:.2f}, {days_txt} to earnings, "
                            f"RSI={rsi:.0f}, gain={gain_pct:+.1f}%)"
                        ),
                        priority="default",
                        tags="lock",
                    )
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
    today = datetime.now(timezone.utc).date()

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
            try:
                import yfinance as yf, gc as _gc
                ticker = yf.Ticker(sym)
                cal = ticker.calendar
                _gc.collect()
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
                    try:
                        notify.send(
                            title=f"🛡️ Post-earnings gap-down: {sym}",
                            body=f"{sym} {move_pct:+.1f}% since earnings — closed.",
                            priority="high",
                            tags="shield",
                        )
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
                                notify.send(
                                    title=f"📈 Post-earnings beat: {sym}",
                                    body=(
                                        f"{sym} {move_pct:+.1f}% post-earnings — "
                                        f"ratchet upgraded to +{new_ratchet * 100:.0f}%."
                                    ),
                                    priority="default",
                                    tags="chart_with_upwards_trend",
                                )
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
