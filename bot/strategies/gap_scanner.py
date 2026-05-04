"""
Strategy 9: Pre-Market Gap Scanner
-------------------------------------
Stocks gapping up 3%+ pre-market on heavy volume after earnings beats
are the highest-probability PEAD setups — you're entering before the
institutional drift starts, not after it's already priced in.

Signal logic:
  1. Fetch pre-market price via yFinance (available 4am-9:30am ET)
  2. Gap = (pre-market price - yesterday close) / yesterday close
  3. Only buy if gap >= 3% AND gap <= 15% (>15% is too extended, fades fast)
  4. Volume confirmation: pre-market volume must be above average
  5. Price must be above 20-day MA (uptrend context)

This runs ONCE per day in the pre-market window (8:00–9:25 AM ET),
before regular market open. Checked inline on each 5-min cycle.

Hold: 3-5 days (quick momentum play).
Exit: 6% stop, 12% take profit, or 5 trading days.
"""

import gc
import logging
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import pytz
from typing import Optional
from broker import AlpacaBroker, tag_symbol
from config import MAX_POSITION_PCT, MIN_CASH_RESERVE_PCT, MAX_TOTAL_EQUITY_POSITIONS
from db import log_trade, log_signal

logger = logging.getLogger("alphabot.gap_scanner")
STRATEGY_NAME = "gap_scanner"
EASTERN = pytz.timezone("America/New_York")

GAP_MIN_PCT      = 0.03   # Minimum 3% gap up
GAP_MAX_PCT      = 0.15   # Maximum 15% (too extended = mean reverts fast)
STOP_LOSS_PCT    = 0.06   # 6% stop — slightly wider, gaps can be volatile
TAKE_PROFIT_PCT  = 0.12   # 12% take profit
MAX_HOLD_DAYS    = 5
MAX_GAP_POSITIONS = 3     # Max concurrent gap plays

# High-volume, earnings-sensitive stocks most likely to gap meaningfully
GAP_WATCHLIST = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA",
    "NFLX", "AMD", "CRM", "ADBE", "ORCL", "NOW", "PANW", "SNOW",
    "JPM", "GS", "MS", "V", "MA",
    "LLY", "ABBV", "AMGN", "VRTX", "ISRG",
    "CAT", "GE", "HON", "DE", "UPS",
    "COIN", "SHOP", "UBER", "ABNB", "PLTR",
]

_entry_dates: dict[str, datetime] = {}
_scanned_today: str = ""  # date string — only scan once per day

GAP_PROTECT_DROP_PCT = 0.04   # 4% adverse pre-market move triggers close


def check_overnight_gaps(broker, db_conn):
    """
    Pre-market gap protection (runs 8:00–9:25 AM ET).

    For every open position across ALL strategies, fetch the current pre-market
    price via yFinance.  If a position is down more than GAP_PROTECT_DROP_PCT
    (4%) from its average entry price, close it immediately before the regular
    session opens.
    """
    now_et = datetime.now(EASTERN)
    from datetime import time as dtime
    if not (dtime(8, 0) <= now_et.time() <= dtime(9, 25)):
        return  # Only runs in pre-market window

    all_positions = broker.get_positions()
    if not all_positions:
        return

    logger.info(f"[GAP PROTECT] Checking {len(all_positions)} open position(s) for overnight gap risk")

    for pos in all_positions:
        sym = pos["symbol"]
        avg_entry = pos.get("avg_entry", 0)
        if avg_entry <= 0:
            continue

        try:
            ticker = yf.Ticker(sym)
            # Try pre-market price first, fall back to last available price
            pre_price = (
                ticker.info.get("preMarketPrice")
                or ticker.fast_info.get("last_price")
            )
            if pre_price is None:
                logger.debug(f"[GAP PROTECT] {sym}: no pre-market price available")
                continue

            pre_price = float(pre_price)
            if pre_price <= 0:
                continue

            drop_pct = (pre_price - avg_entry) / avg_entry  # negative = drop

            if drop_pct <= -GAP_PROTECT_DROP_PCT:
                pct_display = drop_pct * 100  # e.g. -5.2
                logger.warning(
                    f"[GAP PROTECT] {sym} down {pct_display:.1f}% pre-market — closing before open"
                )
                broker.close_position(sym, "gap_protect")
                log_trade(
                    db_conn,
                    pos.get("strategy", "unknown"),
                    sym,
                    "sell_gap_protect",
                    pos["qty"],
                    pre_price,
                    (pre_price - avg_entry) * pos["qty"],
                )
            else:
                logger.debug(
                    f"[GAP PROTECT] {sym}: pre-market {drop_pct*100:+.1f}% — OK"
                )

        except Exception as e:
            logger.debug(f"[GAP PROTECT] Could not check {sym}: {e}")


def _is_premarket_window() -> bool:
    """Returns True if we're in the pre-market scanning window (8:00–9:25 AM ET)."""
    now_et = datetime.now(EASTERN)
    from datetime import time as dtime
    return dtime(8, 0) <= now_et.time() <= dtime(9, 25)


def _get_gap_signals() -> list[dict]:
    """
    Scan watchlist for pre-market gap-ups.
    Uses yFinance pre-market data where available.
    """
    signals = []

    for sym in GAP_WATCHLIST:
        try:
            ticker = yf.Ticker(sym)

            # Get recent history for context
            hist = ticker.history(period="1mo")
            if len(hist) < 20:
                continue

            yesterday_close = float(hist["Close"].iloc[-1])
            ma20 = float(hist["Close"].tail(20).mean())

            # Pre-market price via fast_info
            try:
                pre_price = ticker.fast_info.get("preMarketPrice") or ticker.fast_info.get("lastPrice")
                if pre_price is None:
                    continue
                pre_price = float(pre_price)
            except Exception:
                continue

            if pre_price <= 0 or yesterday_close <= 0:
                continue

            gap_pct = (pre_price - yesterday_close) / yesterday_close

            if not (GAP_MIN_PCT <= gap_pct <= GAP_MAX_PCT):
                continue

            # Price must be above 20-day MA (uptrend context)
            if pre_price < ma20 * 0.98:
                logger.debug(f"[GAP] {sym}: gap={gap_pct:.1%} but below MA20 — skipping")
                continue

            signals.append({
                "symbol": sym,
                "gap_pct": gap_pct,
                "pre_price": pre_price,
                "yesterday_close": yesterday_close,
                "ma20": ma20,
            })
            logger.info(
                f"[GAP] {sym}: gap={gap_pct*100:+.1f}% "
                f"(${yesterday_close:.2f} → ${pre_price:.2f}) | MA20=${ma20:.2f}"
            )

        except Exception as e:
            logger.debug(f"[GAP] Error for {sym}: {e}")
            continue
        finally:
            gc.collect()

    # Sort by gap size — biggest legitimate gaps first
    signals.sort(key=lambda x: x["gap_pct"], reverse=True)
    return signals


def _holding_too_long(symbol: str) -> bool:
    entry = _entry_dates.get(symbol)
    if entry is None:
        return False
    approx_trading_days = (datetime.now() - entry).days * (5 / 7)
    return approx_trading_days >= MAX_HOLD_DAYS


def run(broker: AlpacaBroker, db_conn):
    """Run gap scanner — pre-market only, then manage existing positions during market hours."""
    global _scanned_today
    logger.info("=== Gap Scanner Strategy: Running ===")

    from utils.regime import is_bull_market
    if not is_bull_market():
        logger.info("[gap_scanner] Bear regime detected — skipping new entries")
        return

    from utils.market_hours import is_entry_allowed
    if not is_entry_allowed():
        logger.info("[gap_scanner] Outside safe entry window — skipping")
        return

    # ── 0. Pre-market gap protection (must run before any other logic) ───────
    check_overnight_gaps(broker, db_conn)

    account = broker.get_account()
    portfolio_value = account["portfolio_value"]
    cash = account["cash"]
    min_cash = portfolio_value * MIN_CASH_RESERVE_PCT

    all_positions = broker.get_positions()
    gap_positions = [p for p in all_positions if p["strategy"] == STRATEGY_NAME]
    current_symbols = {p["symbol"] for p in all_positions}

    # ── 1. Exit checks (runs during market hours too) ─────────────────────────
    for pos in gap_positions:
        sym = pos["symbol"]
        pnl_pct = pos["unrealized_pnl_pct"]

        if pnl_pct <= -STOP_LOSS_PCT * 100:
            logger.info(f"[GAP] STOP LOSS {sym} @ {pnl_pct:.1f}%")
            broker.close_position(sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, sym, "sell_stop",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])
            _entry_dates.pop(sym, None)
            current_symbols.discard(sym)
            continue

        if pnl_pct >= TAKE_PROFIT_PCT * 100:
            logger.info(f"[GAP] TAKE PROFIT {sym} @ {pnl_pct:.1f}%")
            broker.close_position(sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, sym, "sell_tp",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])
            _entry_dates.pop(sym, None)
            current_symbols.discard(sym)
            continue

        if _holding_too_long(sym):
            logger.info(f"[GAP] TIME EXIT {sym} after {MAX_HOLD_DAYS} days, PnL={pnl_pct:.1f}%")
            broker.close_position(sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, sym, "sell_time",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])
            _entry_dates.pop(sym, None)
            current_symbols.discard(sym)

    # ── 2. Pre-market scan — once per day only ────────────────────────────────
    today_str = datetime.now(EASTERN).strftime("%Y-%m-%d")

    if not _is_premarket_window():
        logger.info(f"[GAP] Outside pre-market window (8:00–9:25 ET) — exits managed, no new scans")
        return

    if _scanned_today == today_str:
        logger.info(f"[GAP] Already scanned today ({today_str}) — skipping")
        return

    current_gap_count = len([p for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME])
    if current_gap_count >= MAX_GAP_POSITIONS:
        logger.info(f"[GAP] Max positions ({MAX_GAP_POSITIONS}) already open")
        _scanned_today = today_str
        return

    logger.info(f"[GAP] Pre-market scan starting — {today_str}")
    signals = _get_gap_signals()

    if not signals:
        logger.info("[GAP] No qualifying gap-ups found today")
        _scanned_today = today_str
        return

    logger.info(f"[GAP] Found {len(signals)} qualifying gap(s): {[s['symbol'] for s in signals]}")

    for sig in signals:
        if current_gap_count >= MAX_GAP_POSITIONS:
            break

        sym = sig["symbol"]
        if sym in current_symbols:
            continue

        total_equity = len([p for p in broker.get_positions() if p.get("asset_class", "equity") == "equity"])
        if total_equity >= MAX_TOTAL_EQUITY_POSITIONS:
            break

        notional = portfolio_value * MAX_POSITION_PCT
        if cash - notional < min_cash:
            logger.info(f"[GAP] Insufficient cash for {sym}")
            continue

        logger.info(
            f"[GAP] ENTER {sym} — gap={sig['gap_pct']*100:+.1f}%, "
            f"pre=${sig['pre_price']:.2f}, notional=${notional:.0f}"
        )
        log_signal(db_conn, STRATEGY_NAME, sym, "buy", sig["gap_pct"], {
            "gap_pct": round(sig["gap_pct"] * 100, 2),
            "pre_price": sig["pre_price"],
            "yesterday_close": sig["yesterday_close"],
        })
        broker.market_buy(sym, notional, STRATEGY_NAME)
        tag_symbol(sym, STRATEGY_NAME)
        log_trade(db_conn, STRATEGY_NAME, sym, "buy", 0, sig["pre_price"], 0,
                  metadata={"notional": notional, "gap_pct": sig["gap_pct"]})
        _entry_dates[sym] = datetime.now()
        cash -= notional
        current_gap_count += 1

    _scanned_today = today_str
    logger.info(f"[GAP] Pre-market scan complete — {current_gap_count} active positions")
