"""
Pairs Trading Strategy — statistical arbitrage via cointegration.
Trades sector duopolies (XOM/CVX, JPM/BAC, KO/PEP, HD/LOW).
Market-neutral: one long + one short per pair.
Entry: z-score of spread > ±2.0
Exit: z-score reverts to ±0.5 OR stop at z > ±3.5
"""
import logging
import gc
from datetime import datetime, timezone
from typing import Optional
import numpy as np
import pandas as pd
import yfinance as yf

from broker import AlpacaBroker, tag_symbol
from config import MIN_CASH_RESERVE_PCT
from db import log_trade, log_signal

logger = logging.getLogger("alphabot.pairs_trading")
STRATEGY_NAME = "pairs_trading"

# Validated cointegrated pairs (same-sector duopolies)
PAIRS = [
    ("XOM", "CVX"),   # Energy majors
    ("JPM", "BAC"),   # Large banks
    ("KO",  "PEP"),   # Consumer staples duopoly
    ("HD",  "LOW"),   # Home improvement duopoly
    ("GS",  "MS"),    # Investment banks
]

Z_ENTRY = 2.0      # Enter when z-score exceeds this
Z_EXIT = 0.5       # Exit when z-score reverts to this
Z_STOP = 3.5       # Stop-loss: spread diverging further (z exceeded this)
LOOKBACK_DAYS = 63 # ~3 months for hedge ratio + z-score
ZSCORE_WINDOW = 30 # Rolling window for z-score normalisation
MAX_PAIRS_ACTIVE = 3  # Max concurrent pair trades
POSITION_PCT = 0.04   # 4% of portfolio per LEG (so 8% total per pair)
MAX_HOLD_DAYS = 20    # Force-close after 20 days if spread hasn't reverted


# ── Pair state tracking ───────────────────────────────────────────────────────
# {pair_key: {"long": symbol, "short": symbol, "entry_zscore": float, "entry_date": datetime}}
_active_pairs: dict[str, dict] = {}


def _pair_key(s1: str, s2: str) -> str:
    return f"{s1}/{s2}"


def _compute_pair_signals(s1: str, s2: str) -> Optional[dict]:
    """Fetch history and compute z-score for a pair. Returns None on failure."""
    try:
        data = yf.download([s1, s2], period="6mo", interval="1d",
                           auto_adjust=True, progress=False)
        gc.collect()

        if data.empty:
            return None

        if isinstance(data.columns, pd.MultiIndex):
            close = data["Close"]
        else:
            close = data[["Close"]]

        close = close[[s1, s2]].dropna()
        if len(close) < LOOKBACK_DAYS:
            return None

        recent = close.tail(LOOKBACK_DAYS)
        s1_prices = recent[s1].values
        s2_prices = recent[s2].values

        # OLS hedge ratio: S1 = beta * S2 + alpha
        beta = float(np.cov(s1_prices, s2_prices)[0, 1] / np.var(s2_prices))
        alpha = float(np.mean(s1_prices) - beta * np.mean(s2_prices))

        spread = close[s1] - (beta * close[s2] + alpha)

        rolling_mean = spread.rolling(ZSCORE_WINDOW).mean()
        rolling_std = spread.rolling(ZSCORE_WINDOW).std()
        zscore = (spread - rolling_mean) / (rolling_std + 1e-10)

        current_z = float(zscore.iloc[-1])
        current_s1 = float(close[s1].iloc[-1])
        current_s2 = float(close[s2].iloc[-1])

        return {
            "s1": s1, "s2": s2,
            "zscore": current_z,
            "beta": beta,
            "s1_price": current_s1,
            "s2_price": current_s2,
            "spread_mean": float(rolling_mean.iloc[-1]),
            "spread_std": float(rolling_std.iloc[-1]),
        }

    except Exception as e:
        logger.warning(f"[PAIRS] Signal error {s1}/{s2}: {e}")
        return None


def _check_exits(broker: AlpacaBroker, db_conn) -> None:
    """Check all active pairs for exit conditions."""
    positions = broker.get_positions()
    pos_map = {p["symbol"]: p for p in positions}

    for key, state in list(_active_pairs.items()):
        s1, s2 = key.split("/")

        age_days = (datetime.now(timezone.utc) - state["entry_date"]).days
        if age_days >= MAX_HOLD_DAYS:
            logger.info(f"[PAIRS] {key} max hold {MAX_HOLD_DAYS}d reached — force closing")
            _close_pair(broker, db_conn, key, state, pos_map, reason="max_hold")
            continue

        signals = _compute_pair_signals(s1, s2)
        if signals is None:
            continue

        z = signals["zscore"]

        if abs(z) <= Z_EXIT:
            logger.info(f"[PAIRS] {key} z={z:.2f} reverted to exit zone — closing")
            _close_pair(broker, db_conn, key, state, pos_map, reason="reversion")
            continue

        if abs(z) >= Z_STOP:
            logger.warning(f"[PAIRS] {key} z={z:.2f} exceeded stop {Z_STOP} — stop loss")
            _close_pair(broker, db_conn, key, state, pos_map, reason="stop_loss")
            continue

        logger.info(f"[PAIRS] {key} holding | z={z:.2f} | age={age_days}d")


def _close_pair(broker: AlpacaBroker, db_conn, key: str, state: dict,
                pos_map: dict, reason: str) -> None:
    """Close both legs of a pair."""
    for sym, side in [(state["long"], "sell"), (state["short"], "buy_to_cover")]:
        if sym in pos_map:
            try:
                broker.close_position(sym, STRATEGY_NAME)
                log_trade(db_conn, STRATEGY_NAME, sym, side,
                         float(pos_map[sym]["qty"]),
                         float(pos_map[sym]["current_price"]),
                         float(pos_map[sym]["unrealized_pnl"]),
                         {"reason": reason, "pair": key})
                logger.info(f"[PAIRS] Closed {side} {sym} — {reason}")
            except Exception as e:
                logger.error(f"[PAIRS] Failed to close {sym}: {e}")
    _active_pairs.pop(key, None)


def run(broker: AlpacaBroker, db_conn) -> None:
    """Main entry — called every cycle."""
    logger.info("=== Pairs Trading Strategy: Scanning ===")

    try:
        _check_exits(broker, db_conn)
    except Exception as e:
        logger.error(f"[PAIRS] Exit check failed: {e}", exc_info=True)

    if len(_active_pairs) >= MAX_PAIRS_ACTIVE:
        logger.info(f"[PAIRS] At max pairs ({MAX_PAIRS_ACTIVE}) — exits only")
        return

    try:
        account = broker.get_account()
    except Exception as e:
        logger.error(f"[PAIRS] Could not fetch account: {e}")
        return

    portfolio_value = float(account["portfolio_value"])
    cash = float(account["cash"])
    min_cash = portfolio_value * MIN_CASH_RESERVE_PCT
    if cash <= min_cash + (portfolio_value * POSITION_PCT * 2):
        logger.info(f"[PAIRS] Insufficient cash for new pair")
        return

    active_keys = set(_active_pairs.keys())
    try:
        positions = broker.get_positions()
    except Exception:
        positions = []
    held_symbols = {p["symbol"] for p in positions if p.get("strategy") == STRATEGY_NAME}

    for s1, s2 in PAIRS:
        key = _pair_key(s1, s2)
        if key in active_keys:
            continue
        if s1 in held_symbols or s2 in held_symbols:
            continue

        signals = _compute_pair_signals(s1, s2)
        if signals is None:
            continue

        z = signals["zscore"]
        try:
            log_signal(db_conn, STRATEGY_NAME, key, "scan", z,
                      {"zscore": z, "beta": signals["beta"]})
        except Exception:
            pass

        long_sym = short_sym = None

        if z > Z_ENTRY:
            # S1 overpriced vs S2 → Short S1, Long S2
            short_sym, long_sym = s1, s2
        elif z < -Z_ENTRY:
            # S1 underpriced vs S2 → Long S1, Short S2
            long_sym, short_sym = s1, s2

        if long_sym is None:
            logger.debug(f"[PAIRS] {key} z={z:.2f} — no signal")
            continue

        notional = portfolio_value * POSITION_PCT
        long_price = signals["s1_price"] if long_sym == s1 else signals["s2_price"]
        short_price = signals["s2_price"] if short_sym == s2 else signals["s1_price"]
        short_qty = max(1, int(notional / short_price))

        logger.info(
            f"[PAIRS] {key} z={z:.2f} → LONG {long_sym} (~${notional:.0f}), "
            f"SHORT {short_sym} ({short_qty} sh)"
        )

        try:
            # Long leg — notional buy
            broker.market_buy(long_sym, notional, STRATEGY_NAME)
            tag_symbol(long_sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, long_sym, "buy",
                     notional / long_price, long_price, 0,
                     {"pair": key, "zscore": z, "leg": "long"})

            # Short leg — qty-based sell (Alpaca paper supports shorting)
            broker.market_sell(short_sym, short_qty, STRATEGY_NAME)
            tag_symbol(short_sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, short_sym, "sell_short",
                     short_qty, short_price, 0,
                     {"pair": key, "zscore": z, "leg": "short"})

            _active_pairs[key] = {
                "long": long_sym,
                "short": short_sym,
                "entry_zscore": z,
                "entry_date": datetime.now(timezone.utc),
                "beta": signals["beta"],
            }
            logger.info(f"[PAIRS] Opened {key} — LONG {long_sym}, SHORT {short_sym}")

        except Exception as e:
            logger.error(f"[PAIRS] Failed to open {key}: {e}", exc_info=True)
            # Try to close any partial fills
            try:
                broker.close_position(long_sym, STRATEGY_NAME)
            except Exception:
                pass
            try:
                broker.close_position(short_sym, STRATEGY_NAME)
            except Exception:
                pass
