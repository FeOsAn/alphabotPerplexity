"""
Strategy 7: SPY/QQQ Dip-in-Uptrend
-------------------------------------
Deploys idle cash into broad market ETFs but ONLY on confirmed pullbacks
within an established uptrend. Avoids chasing tops.

Entry logic (all must be true):
  1. Bull regime confirmed — SPY above its 50-day MA (medium-term trend intact)
  2. SPY has pulled back 2–6% from its 20-day rolling high (buying the dip, not the top)
  3. RSI(14) is between 35–55 — oversold relative to recent trend, not in freefall
  4. Not already holding a SPY or QQQ position

Allocation: up to 20% of portfolio split between SPY and QQQ (10% each).
This ensures we always have meaningful market exposure in bull regimes
without momentum strategy's RAM requirements.

Exit logic:
  - Hard stop: 4% loss (ETFs move slower than single stocks)
  - Take profit: 8% gain
  - Regime exit: if SPY drops below 50-day MA, exit both positions
"""

import logging
import yfinance as yf
import pandas as pd
from broker import AlpacaBroker, tag_symbol
from config import MIN_CASH_RESERVE_PCT
from db import log_trade, log_signal

logger = logging.getLogger("alphabot.spy_dip")
STRATEGY_NAME = "spy_dip"

STOP_LOSS_PCT   = 0.04   # 4% stop — ETFs are less volatile than single stocks
TAKE_PROFIT_PCT = 0.08   # 8% take profit
ALLOCATION_PCT  = 0.10   # 10% per ETF (SPY + QQQ = 20% total)
WATCHLIST       = ["SPY", "QQQ"]

# Dip parameters
DIP_MIN_PCT    = 0.02    # Must be at least 2% off 20-day high
DIP_MAX_PCT    = 0.08    # Don't buy if down more than 8% (potential breakdown)
RSI_MIN        = 35      # Not in freefall
RSI_MAX        = 55      # Not already overbought/recovered


def _get_signals() -> dict:
    """Fetch SPY and QQQ data and compute entry/exit signals."""
    signals = {}
    for sym in WATCHLIST:
        try:
            ticker = yf.Ticker(sym)
            hist = ticker.history(period="6mo")
            if len(hist) < 55:
                logger.warning(f"[SPY Dip] Not enough data for {sym}")
                continue

            close = hist["Close"]
            price = float(close.iloc[-1])

            # Trend filter: price vs 50-day MA
            ma50 = float(close.tail(50).mean())
            above_ma50 = price > ma50

            # Dip filter: how far off the 20-day rolling high?
            high_20d = float(close.tail(20).max())
            pct_off_high = (high_20d - price) / high_20d  # positive = below high

            # RSI(14)
            delta = close.diff()
            gain = delta.clip(lower=0).rolling(14).mean()
            loss = (-delta.clip(upper=0)).rolling(14).mean()
            rs = gain / loss.replace(0, 1e-10)
            rsi = float((100 - 100 / (1 + rs)).iloc[-1])

            # MA20 slope — uptrend must be rising, not just flat
            ma20_now  = float(close.tail(20).mean())
            ma20_prev = float(close.tail(25).head(20).mean())  # 5 days ago
            ma20_slope = (ma20_now - ma20_prev) / ma20_prev

            buy_signal = (
                above_ma50 and
                DIP_MIN_PCT <= pct_off_high <= DIP_MAX_PCT and
                RSI_MIN <= rsi <= RSI_MAX and
                ma20_slope > 0  # trend must be rising
            )

            sell_signal_regime = not above_ma50  # exit if regime flips bear

            signals[sym] = {
                "price": price,
                "ma50": ma50,
                "above_ma50": above_ma50,
                "high_20d": high_20d,
                "pct_off_high": pct_off_high,
                "rsi": rsi,
                "ma20_slope": ma20_slope,
                "buy_signal": buy_signal,
                "sell_signal_regime": sell_signal_regime,
            }

            logger.info(
                f"[SPY Dip] {sym}: ${price:.2f} | MA50=${ma50:.2f} ({'above' if above_ma50 else 'BELOW'}) | "
                f"off_high={pct_off_high*100:.1f}% | RSI={rsi:.1f} | slope={ma20_slope*100:.2f}% | "
                f"BUY={'YES' if buy_signal else 'no'}"
            )

        except Exception as e:
            logger.warning(f"[SPY Dip] Signal error for {sym}: {e}")

    return signals


def run(broker: AlpacaBroker, db_conn):
    """Run SPY/QQQ dip-in-uptrend strategy."""
    logger.info("=== SPY Dip Strategy: Checking signals ===")

    signals = _get_signals()
    if not signals:
        logger.warning("[SPY Dip] No signals computed — skipping")
        return

    account = broker.get_account()
    portfolio_value = account["portfolio_value"]
    cash = account["cash"]
    min_cash = portfolio_value * MIN_CASH_RESERVE_PCT

    all_positions = broker.get_positions()
    current_symbols = {p["symbol"] for p in all_positions}
    spy_dip_positions = [p for p in all_positions if p["strategy"] == STRATEGY_NAME]

    # ── 1. Exit checks ────────────────────────────────────────────────────────
    for pos in spy_dip_positions:
        sym = pos["symbol"]
        pnl_pct = pos["unrealized_pnl_pct"]
        sig = signals.get(sym, {})

        # Stop loss
        if pnl_pct <= -STOP_LOSS_PCT * 100:
            logger.info(f"[SPY Dip] STOP LOSS {sym} @ {pnl_pct:.1f}%")
            broker.close_position(sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, sym, "sell_stop",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])
            current_symbols.discard(sym)
            continue

        # Take profit
        if pnl_pct >= TAKE_PROFIT_PCT * 100:
            logger.info(f"[SPY Dip] TAKE PROFIT {sym} @ {pnl_pct:.1f}%")
            broker.close_position(sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, sym, "sell_tp",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])
            current_symbols.discard(sym)
            continue

        # Regime flip — SPY broke below 50MA, exit all ETF positions
        if sig.get("sell_signal_regime", False):
            logger.info(f"[SPY Dip] REGIME EXIT {sym} — SPY broke below 50MA")
            broker.close_position(sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, sym, "sell_regime",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])
            current_symbols.discard(sym)

    # ── 2. Entry ──────────────────────────────────────────────────────────────
    for sym, sig in signals.items():
        if sym in current_symbols:
            continue
        if not sig.get("buy_signal"):
            continue

        notional = portfolio_value * ALLOCATION_PCT
        if cash - notional < min_cash:
            logger.info(f"[SPY Dip] Insufficient cash for {sym} — need ${notional:.0f}, have ${cash:.0f}")
            continue

        logger.info(
            f"[SPY Dip] ENTER {sym} — ${notional:.0f} | "
            f"price=${sig['price']:.2f} | {sig['pct_off_high']*100:.1f}% off high | "
            f"RSI={sig['rsi']:.1f} | MA50 confirmed"
        )
        log_signal(db_conn, STRATEGY_NAME, sym, "buy", sig["rsi"], {
            "price": sig["price"],
            "pct_off_high": round(sig["pct_off_high"] * 100, 2),
            "rsi": round(sig["rsi"], 1),
            "ma50": round(sig["ma50"], 2),
            "ma20_slope": round(sig["ma20_slope"] * 100, 3),
        })
        broker.market_buy(sym, notional, STRATEGY_NAME)
        tag_symbol(sym, STRATEGY_NAME)
        log_trade(db_conn, STRATEGY_NAME, sym, "buy", 0, sig["price"], 0,
                  metadata={"notional": notional, "pct_off_high": sig["pct_off_high"]})
        cash -= notional

    logger.info("[SPY Dip] Scan complete")
