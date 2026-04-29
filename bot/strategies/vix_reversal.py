"""
Strategy 8: VIX Spike Reversal
--------------------------------
When the VIX spikes sharply (fear event), the market almost always bounces
within 3-5 days. This is one of the highest-probability short-term setups
in all of finance — documented extensively (Whaley 2000, CBOE research).

Entry logic (all must be true):
  1. VIX (via VIXY ETF proxy) has spiked 20%+ above its 10-day average
     OR VIX is above 28 in absolute terms (elevated fear regime)
  2. SPY has dropped 2%+ today (confirms fear is real, not noise)
  3. SPY is still above its 200-day MA (not a structural bear market — 
     just a fear spike within a longer uptrend)
  4. Not already in a VIX reversal position

Allocation: 15% of portfolio into SPY on the spike day.
Exit logic:
  - Take profit: +5% (fear bounces are fast, take it)
  - Hard stop: -3% (if the fear is justified and keeps going, cut quick)
  - Time exit: close after 5 trading days regardless (these are short holds)
"""

import logging
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
from broker import AlpacaBroker, tag_symbol
from config import MIN_CASH_RESERVE_PCT
from db import log_trade, log_signal

logger = logging.getLogger("alphabot.vix_reversal")
STRATEGY_NAME = "vix_reversal"

STOP_LOSS_PCT    = 0.03   # 3% stop — tight, these should bounce fast
TAKE_PROFIT_PCT  = 0.05   # 5% take profit — fear bounces are quick
ALLOCATION_PCT   = 0.15   # 15% of portfolio — high conviction setup
MAX_HOLD_DAYS    = 5      # Exit after 5 trading days if neither stop nor TP hit
VIX_SPIKE_RATIO  = 1.25   # VIX must be 25% above its 10-day average
VIXY_ABS_MIN     = 35.0   # OR VIXY above $35 in absolute terms (= real VIX ~85, genuine panic)
SPY_DROP_MIN     = 0.02   # SPY must be down 2%+ on the day

# Track entry dates for time-based exits
_entry_dates: dict[str, datetime] = {}


def _get_vix_signal() -> dict:
    """
    Fetch VIX signal using VIXY (VIX short-term futures ETF) as proxy.
    Also checks SPY for the day's move and 200-day MA.
    """
    result = {
        "vixy_price": 0.0,
        "vixy_avg10": 0.0,
        "spike_ratio": 0.0,
        "spy_price": 0.0,
        "spy_change_today": 0.0,
        "spy_above_ma200": False,
        "vix_spiked": False,
        "spy_dropped": False,
        "buy_signal": False,
    }

    try:
        # VIXY as VIX proxy
        vixy = yf.Ticker("VIXY")
        vixy_hist = vixy.history(period="1mo")
        if len(vixy_hist) < 10:
            logger.warning("[VIX] Not enough VIXY data")
            return result

        vixy_price = float(vixy_hist["Close"].iloc[-1])
        vixy_avg10 = float(vixy_hist["Close"].tail(10).mean())
        spike_ratio = vixy_price / vixy_avg10 if vixy_avg10 > 0 else 1.0

        result["vixy_price"] = vixy_price
        result["vixy_avg10"] = vixy_avg10
        result["spike_ratio"] = spike_ratio

        # VIX spiked if ratio >= threshold OR VIXY is at an extreme absolute level
        # VIXY normal range: $25-38. Above $35 = genuine panic event.
        vix_spiked = spike_ratio >= VIX_SPIKE_RATIO or vixy_price >= VIXY_ABS_MIN
        result["vix_spiked"] = vix_spiked

        # SPY day move and 200MA
        spy = yf.Ticker("SPY")
        spy_hist = spy.history(period="1y")
        if len(spy_hist) < 200:
            return result

        spy_price = float(spy_hist["Close"].iloc[-1])
        spy_prev = float(spy_hist["Close"].iloc[-2])
        spy_change = (spy_price - spy_prev) / spy_prev
        spy_ma200 = float(spy_hist["Close"].tail(200).mean())
        spy_above_ma200 = spy_price > spy_ma200

        result["spy_price"] = spy_price
        result["spy_change_today"] = spy_change
        result["spy_above_ma200"] = spy_above_ma200
        result["spy_dropped"] = spy_change <= -SPY_DROP_MIN

        result["buy_signal"] = vix_spiked and result["spy_dropped"] and spy_above_ma200

        logger.info(
            f"[VIX] VIXY=${vixy_price:.2f} (avg10=${vixy_avg10:.2f}, ratio={spike_ratio:.2f}) | "
            f"SPY day={spy_change*100:+.1f}% | above MA200={spy_above_ma200} | "
            f"BUY={'YES' if result['buy_signal'] else 'no'}"
        )

    except Exception as e:
        logger.warning(f"[VIX] Signal error: {e}", exc_info=True)

    return result


def _holding_too_long(symbol: str) -> bool:
    entry = _entry_dates.get(symbol)
    if entry is None:
        return False
    approx_trading_days = (datetime.now() - entry).days * (5 / 7)
    return approx_trading_days >= MAX_HOLD_DAYS


def run(broker: AlpacaBroker, db_conn):
    """Run VIX spike reversal strategy."""
    logger.info("=== VIX Spike Reversal Strategy: Checking ===")

    account = broker.get_account()
    portfolio_value = account["portfolio_value"]
    cash = account["cash"]
    min_cash = portfolio_value * MIN_CASH_RESERVE_PCT

    all_positions = broker.get_positions()
    vix_positions = [p for p in all_positions if p["strategy"] == STRATEGY_NAME]
    current_symbols = {p["symbol"] for p in all_positions}

    # ── 1. Exit checks ────────────────────────────────────────────────────────
    for pos in vix_positions:
        sym = pos["symbol"]
        pnl_pct = pos["unrealized_pnl_pct"]

        if pnl_pct <= -STOP_LOSS_PCT * 100:
            logger.info(f"[VIX] STOP LOSS {sym} @ {pnl_pct:.1f}%")
            broker.close_position(sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, sym, "sell_stop",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])
            _entry_dates.pop(sym, None)
            current_symbols.discard(sym)
            continue

        if pnl_pct >= TAKE_PROFIT_PCT * 100:
            logger.info(f"[VIX] TAKE PROFIT {sym} @ {pnl_pct:.1f}% — fear bounce complete")
            broker.close_position(sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, sym, "sell_tp",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])
            _entry_dates.pop(sym, None)
            current_symbols.discard(sym)
            continue

        if _holding_too_long(sym):
            logger.info(f"[VIX] TIME EXIT {sym} after {MAX_HOLD_DAYS} trading days, PnL={pnl_pct:.1f}%")
            broker.close_position(sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, sym, "sell_time",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])
            _entry_dates.pop(sym, None)
            current_symbols.discard(sym)

    # ── 2. Entry — only if not already holding a VIX reversal position ────────
    if "SPY" in {p["symbol"] for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME}:
        logger.info("[VIX] Already in a VIX reversal position — skipping new entry")
        return

    sig = _get_vix_signal()
    if not sig["buy_signal"]:
        return

    notional = portfolio_value * ALLOCATION_PCT
    if cash - notional < min_cash:
        logger.info(f"[VIX] Insufficient cash — need ${notional:.0f}, have ${cash:.0f}")
        return

    logger.info(
        f"[VIX] SPIKE DETECTED — BUYING SPY ${notional:.0f} | "
        f"VIXY ratio={sig['spike_ratio']:.2f} | SPY day={sig['spy_change_today']*100:+.1f}%"
    )
    log_signal(db_conn, STRATEGY_NAME, "SPY", "buy", sig["spike_ratio"], {
        "vixy_price": sig["vixy_price"],
        "vixy_avg10": sig["vixy_avg10"],
        "spike_ratio": round(sig["spike_ratio"], 3),
        "spy_change_today": round(sig["spy_change_today"] * 100, 2),
        "spy_above_ma200": sig["spy_above_ma200"],
    })
    broker.market_buy("SPY", notional, STRATEGY_NAME)
    tag_symbol("SPY", STRATEGY_NAME)
    log_trade(db_conn, STRATEGY_NAME, "SPY", "buy", 0, sig["spy_price"], 0,
              metadata={"notional": notional, "vixy_spike_ratio": sig["spike_ratio"]})
    _entry_dates["SPY"] = datetime.now()

    logger.info("[VIX] VIX reversal position opened")
