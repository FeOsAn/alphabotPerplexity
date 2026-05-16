"""
Strategy: Cross-Sectional Momentum (3-month minus 1-month)
-----------------------------------------------------------
Academically backed short-term momentum: 3-month return minus last 1-month
return prevents chasing exhausted moves. Rebalances weekly. Holds top 5 picks.

Improvements over the original (now disabled) OOM version:
- Downloads one symbol at a time via yfinance Ticker.history() — never batches
- gc.collect() after every fetch to stay under Railway 512MB RAM
- Entry filters: above MA50, RSI < 75, volume >= 0.8x average
- Conviction-based position sizing (0.75x–1.5x)
- 6% trailing stop (looser than other strategies — momentum needs room to breathe)
"""

import gc
import logging
import pandas as pd
import yfinance as yf
import ta
from datetime import datetime
from typing import Optional
from broker import AlpacaBroker, tag_symbol
from config import (
    MAX_POSITION_PCT, MIN_CASH_RESERVE_PCT, MAX_TOTAL_EQUITY_POSITIONS,
    SIZING_MIN_MULT, SIZING_MID_MULT, SIZING_HIGH_MULT, SIZING_MAX_MULT,
)
from db import log_trade, log_signal

logger = logging.getLogger("alphabot.momentum")
STRATEGY_NAME = "momentum"

MOMENTUM_TOP_N = 6             # top 6 picks per rebalance
MOMENTUM_REBALANCE_DAYS = 5    # was 7 — rebalance weekly (every 5 trading days)
STOP_LOSS_PCT = 0.06           # 6% trailing — looser for momentum

MOMENTUM_UNIVERSE = [
    # Mega-cap
    "AAPL", "MSFT", "NVDA", "GOOGL", "META", "AMZN", "TSLA", "AVGO", "ORCL", "ADBE",
    # Semis
    "AMD", "QCOM", "MU", "TXN", "AMAT", "LRCX", "KLAC", "MRVL", "MCHP", "ADI",
    "NXPI", "MPWR", "ON", "WOLF", "ACLS",
    # Cloud/SaaS
    "CRM", "NOW", "SNOW", "DDOG", "PANW", "CRWD", "ZS", "NET", "FTNT", "MDB",
    "HUBS", "WDAY", "TEAM", "VEEV", "GTLB",
    # Financials
    "JPM", "GS", "MS", "BAC", "V", "MA", "BLK", "SCHW", "AXP", "COF",
    # Healthcare
    "LLY", "JNJ", "MRK", "AMGN", "ABBV", "GILD", "BMY", "VRTX", "REGN", "MRNA",
    # Consumer
    "NFLX", "SBUX", "NKE", "HD", "MCD", "COST", "LOW", "LULU", "TJX", "ROST",
    # Industrials
    "CAT", "HON", "GE", "BA", "RTX", "LMT", "DE", "EMR", "ETN", "PH",
    # Energy
    "XOM", "CVX", "OXY", "SLB", "COP", "EOG", "DVN", "MPC", "VLO", "PSX",
    # Tech hardware
    "AAPL", "HPQ", "DELL", "STX", "WDC", "NTAP",
    # Media/Telecom
    "DIS", "CMCSA", "T", "VZ", "TMUS", "NFLX",
    # High-vol momentum names
    "UBER", "ABNB", "COIN", "PLTR", "RBLX", "SNAP", "RDDT", "HOOD",
    "RIVN", "GM", "F",
    # ETFs for regime signals
    "SPY", "QQQ", "IWM", "XLF", "XLE", "XLK", "XLV", "XLI", "GLD", "TLT",
]
# Deduplicate while preserving order
MOMENTUM_UNIVERSE = list(dict.fromkeys(MOMENTUM_UNIVERSE))

_last_rebalance: Optional[datetime] = None


def _should_rebalance() -> bool:
    global _last_rebalance
    if _last_rebalance is None:
        return True
    return (datetime.now() - _last_rebalance).days >= MOMENTUM_REBALANCE_DAYS


def _conviction_multiplier(score: float, rsi: float = 50, vol_ratio: float = 1.0) -> float:
    """
    Scale position size by signal quality across three dimensions:
      - score: straight 3m return (higher = stronger trend)
      - rsi: momentum confirmation (mid-range RSI = healthy trend)
      - vol_ratio: institutional participation

    Thresholds use the widened 0.5x–2.0x range from config.
    """
    # Base score tier
    if score >= 0.50:      base = SIZING_MAX_MULT   # 2.0x — +50% in 3m, exceptional
    elif score >= 0.25:    base = SIZING_HIGH_MULT  # 1.5x — +25% in 3m, strong
    elif score >= 0.10:    base = SIZING_MID_MULT   # 1.0x — +10% in 3m, solid
    elif score >= 0.03:    base = 0.75              # modest but positive
    else:                  base = SIZING_MIN_MULT   # 0.5x — barely positive

    # Boost for healthy RSI (50–70 = ideal momentum zone, not exhausted)
    rsi_boost = 0.25 if 50 <= rsi <= 72 else 0.0

    # Boost for above-average volume (institutional conviction)
    vol_boost = 0.25 if vol_ratio >= 1.2 else 0.0

    mult = min(SIZING_MAX_MULT, base + rsi_boost + vol_boost)
    return mult


def _compute_score(sym: str) -> Optional[dict]:
    """
    Fetch 6 months of daily history for a single symbol (one at a time — RAM safe)
    and compute:
      - Momentum score = 3-month return minus 1-month return
      - RSI(14)
      - MA50 check
      - Volume ratio (last day vs 20-day average)

    Returns a dict with the score and filter flags, or None on failure.
    """
    try:
        ticker = yf.Ticker(sym)
        hist = ticker.history(period="6mo")

        if hist is None or hist.empty or len(hist) < 65:
            logger.debug(f"[MOM] {sym}: insufficient data ({len(hist) if hist is not None else 0} rows)")
            return None

        hist = hist.sort_index()
        close = hist["Close"].dropna()
        volume = hist["Volume"].dropna()

        if len(close) < 65:
            return None

        price_now = float(close.iloc[-1])
        price_21d = float(close.iloc[-22]) if len(close) >= 22 else float(close.iloc[0])
        price_63d = float(close.iloc[-64]) if len(close) >= 64 else float(close.iloc[0])

        # Momentum score: straight 3-month return.
        # The classic "3m minus 1m" formula was penalising stocks in strong
        # rally conditions (1m > 3m everywhere during the tariff-relief rip).
        # Straight 3m captures trend without inverting in fast markets.
        ret_3m = (price_now - price_63d) / price_63d if price_63d > 0 else 0.0
        ret_1m = (price_now - price_21d) / price_21d if price_21d > 0 else 0.0
        score = ret_3m  # was ret_3m - ret_1m

        # Acceleration: 1m return should be positive (stock trending up recently)
        price_42d = float(close.iloc[-43]) if len(close) >= 43 else price_21d
        ret_1m_prior = (price_21d - price_42d) / price_42d if price_42d > 0 else 0.0

        # MA50
        ma50 = float(close.tail(50).mean()) if len(close) >= 50 else None
        above_ma50 = bool(ma50 is not None and price_now > ma50)

        # RSI(14)
        rsi_series = ta.momentum.RSIIndicator(close, window=14).rsi()
        rsi = float(rsi_series.iloc[-1]) if not rsi_series.empty else 50.0

        # Volume ratio — use the PREVIOUS completed day (iloc[-2]), not today's
        # partial bar. During market hours iloc[-1] is incomplete and always
        # looks like 0.1x average, which kills every entry signal.
        vol_avg_20 = float(volume.tail(21).iloc[:-1].mean()) if len(volume) >= 21 else float(volume.mean())
        vol_last = float(volume.iloc[-2]) if len(volume) >= 2 else float(volume.iloc[-1])
        vol_ratio = vol_last / vol_avg_20 if vol_avg_20 > 0 else 0.0

        return {
            "symbol": sym,
            "price": price_now,
            "score": score,
            "ret_3m": ret_3m,
            "ret_1m": ret_1m,
            "ret_1m_prior": ret_1m_prior,
            "rsi": rsi,
            "above_ma50": above_ma50,
            "vol_ratio": vol_ratio,
        }

    except Exception as e:
        logger.debug(f"[MOM] Error computing score for {sym}: {e}")
        return None
    finally:
        gc.collect()


def _passes_entry_filters(sig: dict) -> bool:
    """All four entry filters must pass."""
    from utils.adaptive_filters import get_thresholds
    t = get_thresholds()
    if not sig.get("above_ma50", False):
        logger.debug(f"[MOM] {sig['symbol']}: filtered — below MA50 (price={sig['price']:.2f})")
        return False
    if sig.get("rsi", 100) >= t["momentum_rsi_max"]:
        logger.debug(f"[MOM] {sig['symbol']}: filtered — RSI={sig['rsi']:.1f} >= {t['momentum_rsi_max']} (overbought)")
        return False
    # Volume: require prev day >= 0.8x average (not 1.1x — that was too tight)
    if sig.get("vol_ratio", 0) < 0.8:
        logger.debug(f"[MOM] {sig['symbol']}: filtered — vol_ratio={sig['vol_ratio']:.2f} < 0.8x")
        return False
    # Score: straight 3m return must be positive
    if sig.get("score", 0) < t["momentum_score_min"]:
        logger.debug(f"[MOM] {sig['symbol']}: filtered — score={sig['score']:.4f} < {t['momentum_score_min']}")
        return False
    # 1m return must be positive — stock is trending up in the last month
    if sig.get("ret_1m", 0) <= 0:
        logger.debug(f"[MOM] {sig['symbol']}: filtered — 1m return negative ({sig['ret_1m']:.2%})")
        return False
    return True


def _check_stops(broker: AlpacaBroker, db_conn):
    """Enforce 6% trailing stop on all momentum positions."""
    positions = broker.get_positions()
    for pos in positions:
        if pos["strategy"] != STRATEGY_NAME:
            continue
        loss_pct = pos["unrealized_pnl_pct"]
        if loss_pct <= -STOP_LOSS_PCT * 100:
            logger.info(
                f"[MOM] STOP LOSS {pos['symbol']} @ {loss_pct:.1f}% "
                f"(threshold: -{STOP_LOSS_PCT * 100:.0f}%)"
            )
            broker.close_position(pos["symbol"], STRATEGY_NAME)
            log_trade(
                db_conn, STRATEGY_NAME, pos["symbol"], "sell_stop",
                pos["qty"], pos["current_price"], pos["unrealized_pnl"],
            )
            from utils.cooldown import set_cooldown
            set_cooldown(pos["symbol"])


def run(broker: AlpacaBroker, db_conn):
    """
    Run the momentum strategy.

    Between rebalances: only enforce stop losses.
    On rebalance (every 7 days): score all universe symbols one-at-a-time,
    pick top 5 by score (with entry filters), exit dropped names, enter new ones.
    """
    global _last_rebalance

    if not _should_rebalance():
        _check_stops(broker, db_conn)
        return

    from utils.regime import is_bull_market
    if not is_bull_market():
        logger.info("[momentum] Bear regime detected — skipping new entries")
        return

    from utils.market_hours import is_entry_allowed
    if not is_entry_allowed():
        logger.info("[momentum] Outside safe entry window — skipping")
        return

    logger.info("=== Momentum Strategy: Weekly Rebalance ===")

    # ── Score universe one symbol at a time (RAM-safe on Railway 512MB) ─────
    raw_scores = []
    for sym in MOMENTUM_UNIVERSE:
        sig = _compute_score(sym)
        if sig is not None:
            raw_scores.append(sig)
        # gc.collect() is already called inside _compute_score's finally block

    if not raw_scores:
        logger.warning("[MOM] No scores computed — skipping rebalance (will retry next cycle)")
        return

    logger.info(f"[MOM] Scored {len(raw_scores)}/{len(MOMENTUM_UNIVERSE)} symbols")

    # Log all signals (before filter — full picture)
    for sig in sorted(raw_scores, key=lambda x: x["score"], reverse=True):
        log_signal(
            db_conn, STRATEGY_NAME, sig["symbol"],
            "candidate",
            sig["score"],
            {
                "ret_3m": sig["ret_3m"],
                "ret_1m": sig["ret_1m"],
                "rsi": sig["rsi"],
                "above_ma50": sig["above_ma50"],
                "vol_ratio": sig["vol_ratio"],
            },
        )

    # Apply entry filters and pick top N by score
    filtered = [s for s in raw_scores if _passes_entry_filters(s)]
    filtered.sort(key=lambda x: x["score"], reverse=True)
    top_picks_data = filtered[:MOMENTUM_TOP_N]
    top_picks = [s["symbol"] for s in top_picks_data]

    logger.info(
        f"[MOM] Top {MOMENTUM_TOP_N} picks after filters: "
        + ", ".join(f"{s['symbol']}({s['score']:.3f})" for s in top_picks_data)
    )

    # Log buy signals for top picks
    for sig in top_picks_data:
        log_signal(
            db_conn, STRATEGY_NAME, sig["symbol"], "buy",
            sig["score"],
            {"rsi": sig["rsi"], "vol_ratio": sig["vol_ratio"]},
        )

    # ── Current momentum positions ───────────────────────────────────────────
    all_positions = broker.get_positions()
    mom_positions = [p for p in all_positions if p["strategy"] == STRATEGY_NAME]
    current_symbols = {p["symbol"] for p in mom_positions}

    account = broker.get_account()
    portfolio_value = account["portfolio_value"]
    cash = account["cash"]

    # ── Exit positions no longer in top picks ────────────────────────────────
    # Anti-churn guard: only replace an existing position if a new candidate
    # (not currently held) scores meaningfully better. Threshold is 15% relative
    # improvement, or 25% if the existing position is currently profitable.
    score_map = {s["symbol"]: s["score"] for s in raw_scores}
    held_syms = {p["symbol"] for p in mom_positions}
    candidate_new_picks = [s for s in top_picks_data if s["symbol"] not in held_syms]
    best_new_score = max((s["score"] for s in candidate_new_picks), default=None)
    best_new_symbol = (
        max(candidate_new_picks, key=lambda x: x["score"])["symbol"]
        if candidate_new_picks else None
    )

    for pos in mom_positions:
        if pos["symbol"] in top_picks:
            continue

        current_score = score_map.get(pos["symbol"])
        is_profitable = pos["unrealized_pnl_pct"] > 0
        required_mult = 1.25 if is_profitable else 1.15
        required_pct  = 25 if is_profitable else 15

        if (
            current_score is not None
            and best_new_score is not None
            and best_new_score < current_score * required_mult
        ):
            logger.info(
                f"[MOM] Keeping {pos['symbol']} (score {current_score:.4f}) — "
                f"new pick {best_new_symbol} (score {best_new_score:.4f}) "
                f"not {required_pct}% better"
            )
            continue

        logger.info(
            f"[MOM] EXIT {pos['symbol']} — rotated out of top {MOMENTUM_TOP_N} "
            f"(pnl={pos['unrealized_pnl_pct']:.1f}%)"
        )
        order = broker.close_position(pos["symbol"], STRATEGY_NAME)
        if order:
            log_trade(
                db_conn, STRATEGY_NAME, pos["symbol"], "sell",
                pos["qty"], pos["current_price"], pos["unrealized_pnl"],
            )
            cash += pos["market_value"]

    # Refresh positions after exits
    all_positions = broker.get_positions()
    current_symbols = {p["symbol"] for p in all_positions if p["strategy"] == STRATEGY_NAME}

    # ── Enter new top picks ───────────────────────────────────────────────────
    for sig in top_picks_data:
        sym = sig["symbol"]
        if sym in current_symbols:
            logger.debug(f"[MOM] {sym}: already held — skipping entry")
            continue

        from utils.cooldown import is_on_cooldown
        if is_on_cooldown(sym):
            logger.debug(f"[STRATEGY] {sym} on cooldown — skipping")
            continue

        # Portfolio-level guards
        equity_count = len([
            p for p in broker.get_positions()
            if p.get("asset_class", "equity") == "equity"
        ])
        if equity_count >= MAX_TOTAL_EQUITY_POSITIONS:
            logger.info(f"[MOM] Max equity positions ({MAX_TOTAL_EQUITY_POSITIONS}) — stopping entries")
            break

        from utils.earnings_calendar import has_upcoming_earnings
        if has_upcoming_earnings(sym):
            logger.info(f"[MOM] Skipping {sym} — earnings blackout (within 2 days)")
            continue

        mult = _conviction_multiplier(sig["score"], sig.get("rsi", 50), sig.get("vol_ratio", 1.0))
        from utils.position_sizer import get_position_size_pct
        size_pct = get_position_size_pct(sym, fallback_pct=MAX_POSITION_PCT)
        notional = portfolio_value * size_pct * mult
        min_cash = portfolio_value * MIN_CASH_RESERVE_PCT

        if cash - notional < min_cash:
            logger.info(
                f"[MOM] {sym}: insufficient cash (available=${cash:.0f}, "
                f"need=${notional:.0f}, reserve=${min_cash:.0f}) — skipping"
            )
            continue

        logger.info(
            f"[MOM] ENTER {sym} — score={sig['score']:.4f} "
            f"(3m={sig['ret_3m']:.2%}, 1m={sig['ret_1m']:.2%}), "
            f"rsi={sig['rsi']:.1f}, vol_ratio={sig['vol_ratio']:.2f}x, "
            f"conviction={mult:.2f}x, notional=${notional:.0f}"
        )

        broker.market_buy(sym, notional, STRATEGY_NAME)
        tag_symbol(sym, STRATEGY_NAME)
        log_trade(
            db_conn, STRATEGY_NAME, sym, "buy", 0, sig["price"], 0,
            metadata={
                "notional": notional,
                "momentum_score": sig["score"],
                "ret_3m": sig["ret_3m"],
                "ret_1m": sig["ret_1m"],
                "rsi": sig["rsi"],
                "conviction": mult,
            },
        )
        cash -= notional

    _last_rebalance = datetime.now()

    active = len([p for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME])
    logger.info(f"[MOM] Rebalance complete — {active} active positions, next in {MOMENTUM_REBALANCE_DAYS} days")
