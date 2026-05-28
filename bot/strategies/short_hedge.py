"""
Short Hedge Strategy — inverse ETF positions in bear market regimes.
Uses leveraged inverse ETFs (no margin/borrow needed, works on paper trading).
Only activates in BEAR_MILD or BEAR_STRONG regime.
Max hold: 5 days (leveraged ETF decay).
"""
import logging
import gc
from datetime import datetime, timezone, timedelta
from typing import Optional
import yfinance as yf
import pandas_ta as _pta

from broker import AlpacaBroker, tag_symbol
from config import MIN_CASH_RESERVE_PCT
from db import log_trade, log_signal, get_connection

logger = logging.getLogger("alphabot.short_hedge")
STRATEGY_NAME = "short_hedge"

# Inverse ETF universe — each represents a bear position on a major index/sector
INVERSE_ETFS = {
    "SDS":   {"description": "2x inverse S&P500",     "beta": 2.0},
    "SQQQ":  {"description": "3x inverse QQQ/tech",   "beta": 3.0},
    "SPXS":  {"description": "3x inverse S&P500",     "beta": 3.0},
    "SOXS":  {"description": "3x inverse semis",      "beta": 3.0},
    "UVXY":  {"description": "1.5x long volatility",  "beta": 1.5},
}

MAX_SHORT_POSITIONS = 2       # max concurrent inverse ETF positions
STOP_LOSS_PCT = 0.08          # 8% stop (tight — leveraged decay accelerates losses)
TAKE_PROFIT_PCT = 0.15        # 15% take profit (leveraged moves fast)
MAX_HOLD_DAYS = 5             # force-close after 5 days regardless (decay protection)
MAX_POSITION_PCT = 0.05       # 5% of portfolio per inverse ETF position

# Track entry times so max-hold-days check works (broker.get_positions() doesn't include entry_time)
_entry_times: dict[str, datetime] = {}  # symbol -> entry datetime (UTC)


def _get_signals(symbol: str) -> Optional[dict]:
    """Compute entry signals for a single inverse ETF."""
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="3mo", interval="1d")
        if hist.empty or len(hist) < 30:
            return None

        close = hist["Close"]
        volume = hist["Volume"]
        price = float(close.iloc[-1])

        # RSI
        rsi = float(_pta.rsi(close, length=14).iloc[-1])

        # 5-day slope (is the inverse ETF itself trending up = market trending down)
        price_5d = float(close.iloc[-6]) if len(close) >= 6 else float(close.iloc[0])
        slope_5d = (price - price_5d) / price_5d if price_5d > 0 else 0.0

        # Volume confirmation
        vol_last = float(volume.iloc[-1])
        vol_avg = float(volume.tail(20).mean()) if len(volume) >= 20 else vol_last
        vol_ratio = vol_last / vol_avg if vol_avg > 0 else 1.0

        # MA20
        ma20 = float(close.tail(20).mean()) if len(close) >= 20 else price
        above_ma20 = price > ma20

        # ADX (trend strength)
        _adx_df = _pta.adx(hist["High"], hist["Low"], close, length=14)
        adx = float(_adx_df['ADX_14'].iloc[-1]) if _adx_df is not None and 'ADX_14' in _adx_df.columns else 0.0

        # Entry: inverse ETF is rising (market falling), above MA20, ADX > 18, volume elevated
        buy_signal = (
            above_ma20 and
            slope_5d > 0.01 and      # rising at least 1% over 5d
            adx > 18 and             # real trend, not noise
            vol_ratio >= 1.1 and     # some volume confirmation
            rsi < 75                 # not already overbought
        )

        return {
            "symbol": symbol,
            "price": price,
            "rsi": rsi,
            "slope_5d": slope_5d,
            "vol_ratio": vol_ratio,
            "above_ma20": above_ma20,
            "adx": adx,
            "buy_signal": buy_signal,
        }
    except Exception as e:
        logger.warning(f"[SHORT] Signal error for {symbol}: {e}")
        return None


def _check_exits(broker: AlpacaBroker, db_conn) -> None:
    """Check stops, take profits, and max hold duration on all short hedge positions."""
    positions = broker.get_positions()
    for pos in positions:
        if pos["strategy"] != STRATEGY_NAME:
            continue
        symbol = pos["symbol"]
        pnl_pct = pos["unrealized_pnl_pct"]
        # Max hold duration — look up from _entry_times dict (broker positions don't carry entry_time)
        entry_time = _entry_times.get(symbol)
        if entry_time:
            try:
                age_days = (datetime.now(timezone.utc) - entry_time).days
                if age_days >= MAX_HOLD_DAYS:
                    logger.info(f"[SHORT] {symbol} — max hold {MAX_HOLD_DAYS}d reached, closing")
                    broker.close_position(symbol, STRATEGY_NAME)
                    log_trade(db_conn, STRATEGY_NAME, symbol, "sell", pos["qty"],
                              pos["current_price"], pos.get("unrealized_pnl", 0.0),
                              metadata={"reason": "max_hold_days"})
                    _entry_times.pop(symbol, None)
                    continue
            except Exception:
                pass

        # Stop loss
        if pnl_pct <= -(STOP_LOSS_PCT * 100):
            logger.info(f"[SHORT] {symbol} STOP LOSS at {pnl_pct:.1f}%")
            broker.close_position(symbol, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, symbol, "sell", pos["qty"],
                      pos["current_price"], pos.get("unrealized_pnl", 0.0),
                      metadata={"reason": "stop_loss"})
            _entry_times.pop(symbol, None)
            continue

        # Take profit
        if pnl_pct >= (TAKE_PROFIT_PCT * 100):
            logger.info(f"[SHORT] {symbol} TAKE PROFIT at +{pnl_pct:.1f}%")
            broker.close_position(symbol, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, symbol, "sell", pos["qty"],
                      pos["current_price"], pos.get("unrealized_pnl", 0.0),
                      metadata={"reason": "take_profit"})
            _entry_times.pop(symbol, None)
            continue

        logger.info(f"[SHORT] {symbol}: P&L={pnl_pct:+.1f}% | holding")


def run(broker: AlpacaBroker, db_conn) -> None:
    """Main entry point — called every cycle from main.py."""
    from utils.adaptive_filters import get_regime, get_thresholds
    from strategies.trade_management import clear_symbol
    regime = get_regime()

    # Force-close all short positions if we somehow still hold them in a bull regime
    if regime not in ("BEAR_MILD", "BEAR_STRONG"):
        all_positions = broker.get_positions()
        short_positions = [p for p in all_positions if p["strategy"] == STRATEGY_NAME]
        if short_positions:
            logger.warning(f"[SHORT] Regime flipped to {regime} — force-closing {len(short_positions)} short position(s)")
            for pos in short_positions:
                sym = pos["symbol"]
                qty = float(pos.get("qty", 0))
                price = float(pos.get("current_price", 0))
                pnl = float(pos.get("unrealized_pnl", 0.0))
                try:
                    broker.close_position(sym, STRATEGY_NAME)
                    log_trade(db_conn, STRATEGY_NAME, sym, "sell_regime_flip",
                              qty, price, pnl,
                              metadata={"reason": "regime_flip", "new_regime": regime})
                    clear_symbol(sym)
                    _entry_times.pop(sym, None)
                except Exception as e:
                    logger.error(f"[SHORT] Force-close failed for {sym}: {e}")
        else:
            logger.info(f"[SHORT] Regime={regime} — short hedge inactive (bull market)")
        return

    logger.info(f"=== Short Hedge Strategy: Active (regime={regime}) ===")

    # Check exits first
    _check_exits(broker, db_conn)

    # Count current short hedge positions
    all_positions = broker.get_positions()
    short_positions = [p for p in all_positions if p["strategy"] == STRATEGY_NAME]
    short_count = len(short_positions)
    held_symbols = {p["symbol"] for p in short_positions}

    max_positions = 1 if regime == "BEAR_MILD" else MAX_SHORT_POSITIONS
    if short_count >= max_positions:
        logger.info(f"[SHORT] At max positions ({short_count}/{max_positions}) for regime={regime}")
        return

    # Check cash
    account = broker.get_account()
    portfolio_value = float(account["portfolio_value"])
    cash = float(account["cash"])
    min_cash = portfolio_value * MIN_CASH_RESERVE_PCT
    if cash <= min_cash:
        logger.info(f"[SHORT] Insufficient cash (${cash:,.0f} <= reserve ${min_cash:,.0f})")
        return

    # Scan inverse ETFs for entry signals
    candidates = []
    for symbol in INVERSE_ETFS:
        if symbol in held_symbols:
            continue
        sig = _get_signals(symbol)
        if sig and sig["buy_signal"]:
            candidates.append(sig)

    if not candidates:
        logger.info("[SHORT] No inverse ETF entry signals this cycle")
        return

    # Sort by slope (strongest bear momentum first)
    candidates.sort(key=lambda x: x["slope_5d"], reverse=True)
    slots = max_positions - short_count

    for sig in candidates[:slots]:
        symbol = sig["symbol"]
        notional = portfolio_value * MAX_POSITION_PCT

        try:
            broker.market_buy(symbol, notional, STRATEGY_NAME)
            tag_symbol(symbol, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, symbol, "buy", 0, sig["price"], 0,
                      metadata={"regime": regime, "notional": notional,
                                "slope_5d": sig["slope_5d"], "adx": sig["adx"]})
            logger.info(
                f"[SHORT] BUY ${notional:.0f} {symbol} @ ~${sig['price']:.2f} | "
                f"regime={regime} slope5d={sig['slope_5d']:+.1%} ADX={sig['adx']:.1f}"
            )
            _entry_times[symbol] = datetime.now(timezone.utc)
        except Exception as e:
            logger.error(f"[SHORT] Order failed for {symbol}: {e}")
