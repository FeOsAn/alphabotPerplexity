"""
Strategy: Cross-Sectional Momentum (3-month minus 1-month)
-----------------------------------------------------------
Academically backed short-term momentum: 3-month return minus last 1-month
return prevents chasing exhausted moves. Rebalances weekly. Processes the
entire filtered universe — no fixed slot count.

Sizing is purely conviction-driven (see config.CONVICTION_TIER_*):
  score tier base + RSI sweet-spot bonus + volume bonus, capped at 20%.
"""

import gc
import logging
import pandas as pd
import yfinance as yf
import pandas_ta as _pta
from utils.clock import now_utc as _now_utc
from datetime import datetime, timezone
from typing import Optional
from broker import AlpacaBroker, tag_symbol
from config import (
    MIN_CASH_RESERVE_PCT,
    CONVICTION_TIER_MAX, CONVICTION_TIER_HIGH, CONVICTION_TIER_MID,
    CONVICTION_TIER_LOW, CONVICTION_TIER_MIN,
    CONVICTION_RSI_BONUS, CONVICTION_VOL_BONUS, MAX_SINGLE_POSITION_PCT,
    CATALYST_SIZING_BOOST, CATALYST_MIN_SCORE, CATALYST_EARNINGS_DAYS,
    MAX_CATALYST_POSITION_PCT,
)
from db import log_trade, log_signal, get_state, set_state

logger = logging.getLogger("alphabot.momentum")
STRATEGY_NAME = "momentum"

MOMENTUM_REBALANCE_DAYS = 5    # rebalance weekly (every 5 trading days)
STOP_LOSS_PCT = 0.06           # 6% trailing — looser for momentum
MIN_VOL_RATIO = 1.5   # v76: volume surge required — 1.5-2.0x bucket best by backtest
MIN_MA20_FILTER = True  # v77: dual MA filter — price must be above both MA20 and MA50

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


def _should_rebalance(db_conn=None) -> bool:
    global _last_rebalance
    if _last_rebalance is None and db_conn is not None:
        ts_str = get_state(db_conn, "momentum_last_rebalance")
        if ts_str:
            try:
                _last_rebalance = datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc) if datetime.fromisoformat(ts_str).tzinfo is None else datetime.fromisoformat(ts_str)
            except Exception:
                pass
    if _last_rebalance is None:
        return True
    return (_now_utc() - _last_rebalance).days >= MOMENTUM_REBALANCE_DAYS


def _has_earnings_catalyst(sym: str) -> tuple[bool, int]:
    """v75 FIX 4 — True if `sym` has earnings within CATALYST_EARNINGS_DAYS.

    Wraps yfinance ticker.calendar in try/except — earnings data is best-effort.
    Returns (has_catalyst, days_to_earnings). days_to_earnings is -1 when unknown.
    """
    try:
        ticker = yf.Ticker(sym)
        cal = ticker.calendar
        if cal is None:
            return False, -1
        raw_dates = []
        if isinstance(cal, dict):
            ed = cal.get("Earnings Date", [])
            if ed:
                raw_dates = ed if isinstance(ed, list) else [ed]
        elif hasattr(cal, "empty") and not cal.empty:
            if hasattr(cal, "columns") and "Earnings Date" in getattr(cal, "columns", []):
                raw_dates = cal["Earnings Date"].dropna().tolist()
            elif hasattr(cal, "index") and "Earnings Date" in getattr(cal, "index", []):
                val = cal.loc["Earnings Date"]
                raw_dates = val.tolist() if hasattr(val, "tolist") else [val]
        if not raw_dates:
            return False, -1
        from utils.clock import now_utc as _nu
        now_ts = pd.Timestamp(_nu()).tz_convert(None)
        for d in raw_dates:
            try:
                ts = pd.Timestamp(d)
                if ts.tzinfo is not None:
                    ts = ts.tz_convert(None)
                days_to = (ts - now_ts).days
                if 0 < days_to <= CATALYST_EARNINGS_DAYS:
                    return True, days_to
            except Exception:
                continue
        return False, -1
    except Exception:
        return False, -1


def _apply_catalyst_boost(sym: str, base_alloc: float, momentum_score: float) -> float:
    """v75 FIX 4 — boost allocation when both an earnings catalyst exists AND
    momentum_score >= CATALYST_MIN_SCORE. Capped at MAX_CATALYST_POSITION_PCT,
    which equals MAX_SINGLE_POSITION_PCT (15%). Boost never breaches the hard cap.
    """
    if momentum_score < CATALYST_MIN_SCORE:
        return base_alloc
    has_catalyst, days_to = _has_earnings_catalyst(sym)
    if not has_catalyst:
        return base_alloc
    boosted = base_alloc * CATALYST_SIZING_BOOST
    boosted = min(boosted, MAX_CATALYST_POSITION_PCT)
    logger.info(
        f"[Momentum] {sym}: catalyst sizing boost applied → {boosted:.1%} "
        f"(earnings in {days_to}d, score={momentum_score:.3f})"
    )
    return boosted


def _conviction_allocation_pct(score: float, rsi: float = 50, vol_ratio: float = 1.0) -> float:
    """
    Returns the fraction of portfolio value to allocate to this position.
    Pure conviction-based: score tier + RSI sweet-spot bonus + volume bonus.
    Capped at MAX_SINGLE_POSITION_PCT (20%).
    """
    if score >= 0.50:   base = CONVICTION_TIER_MAX
    elif score >= 0.25: base = CONVICTION_TIER_HIGH
    elif score >= 0.10: base = CONVICTION_TIER_MID
    elif score >= 0.03: base = CONVICTION_TIER_LOW
    else:               base = CONVICTION_TIER_MIN

    rsi_bonus = CONVICTION_RSI_BONUS if 50 <= rsi <= 72 else 0.0
    vol_bonus = CONVICTION_VOL_BONUS if vol_ratio >= 1.2 else 0.0

    return min(base + rsi_bonus + vol_bonus, MAX_SINGLE_POSITION_PCT)


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

        ret_3m = (price_now - price_63d) / price_63d if price_63d > 0 else 0.0
        ret_1m = (price_now - price_21d) / price_21d if price_21d > 0 else 0.0
        # Jegadeesh-Titman 12-1 momentum (scaled to 3-1): skip the most recent month
        # to avoid chasing exhausted moves / short-term mean reversion.
        score = ret_3m - ret_1m

        price_42d = float(close.iloc[-43]) if len(close) >= 43 else price_21d
        ret_1m_prior = (price_21d - price_42d) / price_42d if price_42d > 0 else 0.0

        ma50 = float(close.tail(50).mean()) if len(close) >= 50 else None
        above_ma50 = bool(ma50 is not None and price_now > ma50)
        ma20 = float(close.rolling(20).mean().iloc[-1]) if len(close) >= 20 else None
        above_ma20 = bool(ma20 is not None and price_now > ma20)

        rsi_series = _pta.rsi(close, length=14)
        rsi = float(rsi_series.iloc[-1]) if not rsi_series.empty else 50.0

        # Volume ratio — use the PREVIOUS completed day (iloc[-2]), not today's
        # partial bar.
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
            "ma20": ma20,
            "above_ma20": above_ma20,
            "vol_ratio": vol_ratio,
        }

    except Exception as e:
        logger.debug(f"[MOM] Error computing score for {sym}: {e}")
        return None
    finally:
        pass


def _passes_entry_filters(sig: dict) -> bool:
    """All entry filters must pass."""
    from utils.adaptive_filters import get_thresholds
    t = get_thresholds()
    if not sig.get("above_ma50", False):
        logger.debug(f"[MOM] {sig['symbol']}: filtered — below MA50 (price={sig['price']:.2f})")
        return False
    if sig.get("price", 0) < sig.get("ma20", 0):
        logger.debug(f"[MOM] {sig['symbol']}: filtered — price below MA20 (short-term trend not aligned)")
        return False
    if sig.get("rsi", 100) >= t["momentum_rsi_max"]:
        logger.debug(f"[MOM] {sig['symbol']}: filtered — RSI={sig['rsi']:.1f} >= {t['momentum_rsi_max']} (overbought)")
        return False
    if sig.get("vol_ratio", 0) < MIN_VOL_RATIO:
        logger.debug(f"[MOM] {sig['symbol']}: filtered — vol_ratio={sig['vol_ratio']:.2f} < {MIN_VOL_RATIO}x (volume surge required)")
        return False
    if sig.get("score", 0) < t["momentum_score_min"]:
        logger.debug(f"[MOM] {sig['symbol']}: filtered — score={sig['score']:.4f} < {t['momentum_score_min']}")
        return False
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
    On rebalance: score all universe symbols, process every filtered candidate,
    size by conviction. Exit positions that drop below the median score of the
    filtered universe or fail any entry filter.
    """
    global _last_rebalance

    if not _should_rebalance(db_conn):
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

    if not raw_scores:
        logger.warning("[MOM] No scores computed — skipping rebalance (will retry next cycle)")
        return

    logger.info(f"[MOM] Scored {len(raw_scores)}/{len(MOMENTUM_UNIVERSE)} symbols")

    # Log all signals (full picture)
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

    # Apply entry filters — process every passing candidate (no slot cap)
    filtered = [s for s in raw_scores if _passes_entry_filters(s)]
    filtered.sort(key=lambda x: x["score"], reverse=True)
    if not filtered:
        logger.info("[MOM] No candidates passed entry filters")
        _last_rebalance = _now_utc()
        try:
            set_state(db_conn, "momentum_last_rebalance", _last_rebalance.isoformat())
        except Exception:
            pass
        return

    scores_sorted = sorted([s["score"] for s in filtered])
    n = len(scores_sorted)
    median_score = scores_sorted[n // 2] if n % 2 == 1 else (
        (scores_sorted[n // 2 - 1] + scores_sorted[n // 2]) / 2
    )

    top_picks_data = filtered
    top_picks = [s["symbol"] for s in top_picks_data]

    logger.info(
        f"[MOM] {len(top_picks_data)} candidates passed filters (median score={median_score:.4f}): "
        + ", ".join(f"{s['symbol']}({s['score']:.3f})" for s in top_picks_data[:15])
        + (" ..." if len(top_picks_data) > 15 else "")
    )

    for sig in top_picks_data:
        log_signal(
            db_conn, STRATEGY_NAME, sig["symbol"], "buy",
            sig["score"],
            {"rsi": sig["rsi"], "vol_ratio": sig["vol_ratio"]},
        )

    # ── Current momentum positions ───────────────────────────────────────────
    all_positions = broker.get_positions()
    mom_positions = [p for p in all_positions if p["strategy"] == STRATEGY_NAME]

    account = broker.get_account()
    portfolio_value = account["portfolio_value"]
    cash = account["cash"]

    # ── Exit positions whose score has fallen below the median or that fail filters ──
    score_map = {s["symbol"]: s["score"] for s in raw_scores}
    sig_map = {s["symbol"]: s for s in raw_scores}
    held_syms = {p["symbol"] for p in mom_positions}

    for pos in mom_positions:
        sym = pos["symbol"]
        current_score = score_map.get(sym)
        held_sig = sig_map.get(sym)

        # If the symbol couldn't be scored (data outage), keep it.
        if current_score is None or held_sig is None:
            continue

        still_passes = _passes_entry_filters(held_sig)
        above_median = current_score >= median_score

        if still_passes and above_median:
            continue

        reason = []
        if not still_passes:
            reason.append("filters failed")
        if not above_median:
            reason.append(f"score {current_score:.4f} below median {median_score:.4f}")

        logger.info(
            f"[MOM] EXIT {sym} — {', '.join(reason)} (pnl={pos['unrealized_pnl_pct']:.1f}%)"
        )
        order = broker.close_position(sym, STRATEGY_NAME)
        if order:
            log_trade(
                db_conn, STRATEGY_NAME, sym, "sell",
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

        from utils.earnings_calendar import has_upcoming_earnings
        if has_upcoming_earnings(sym):
            logger.info(f"[MOM] Skipping {sym} — earnings blackout (within 2 days)")
            continue

        # Correlation monitor — block highly correlated entries
        try:
            from utils.correlation_monitor import is_entry_allowed as _corr_ok
            allowed, reason = _corr_ok(sym, broker)
            if not allowed:
                logger.info(f"[MOM] {sym} blocked by correlation monitor: {reason}")
                continue
        except Exception as _e:
            logger.debug(f"[MOM] Correlation check error for {sym}: {_e}")

        # Regime-aware sizing
        try:
            from utils.regime_weights import get_multiplier as _regime_mult
            regime_mult = _regime_mult("momentum")
        except Exception:
            regime_mult = 1.0
        if regime_mult == 0.0:
            logger.info(f"[MOM] Regime weight 0.0 for momentum — skipping {sym}")
            continue

        size_pct = _conviction_allocation_pct(
            sig["score"], sig.get("rsi", 50), sig.get("vol_ratio", 1.0)
        )
        # v75 FIX 4 — catalyst sizing boost (earnings within 14 days + score≥0.08)
        size_pct = _apply_catalyst_boost(sym, size_pct, sig["score"])
        deployable = portfolio_value * (1 - MIN_CASH_RESERVE_PCT)
        notional = deployable * size_pct * regime_mult
        min_cash = portfolio_value * MIN_CASH_RESERVE_PCT

        # Hard cap: total exposure to this symbol cannot exceed MAX_SINGLE_POSITION_PCT
        existing_mv = sum(
            float(p["market_value"]) for p in broker.get_positions()
            if p["symbol"] == sym
        )
        max_notional = portfolio_value * MAX_SINGLE_POSITION_PCT
        if existing_mv >= max_notional:
            logger.info(f"[MOM] {sym}: already at position cap ({existing_mv/portfolio_value:.1%}) — skipping")
            continue
        notional = min(notional, max_notional - existing_mv)

        rotated_in = False  # v73 — track whether this entry came via rotation
        if cash - notional < min_cash:
            # Try capital rotation before giving up
            from utils.capital_rotator import find_rotation_candidate, execute_rotation
            rotation_candidate = find_rotation_candidate(
                new_symbol=sym,
                new_score=sig["score"],
                new_notional=notional,
                current_positions=broker.get_positions(),
                broker=broker,
                db_conn=db_conn,
            )
            if rotation_candidate:
                rotated = execute_rotation(
                    sell_symbol=rotation_candidate,
                    buy_symbol=sym,
                    buy_notional=notional,
                    buy_score=sig["score"],
                    broker=broker,
                    db_conn=db_conn,
                    strategy_name=STRATEGY_NAME,
                )
                if not rotated:
                    continue
                rotated_in = True
                # Refresh cash after rotation
                cash, portfolio_value = broker.get_live_cash()
                min_cash = portfolio_value * MIN_CASH_RESERVE_PCT
                if cash - notional < min_cash:
                    logger.info(
                        f"[MOM] {sym}: still insufficient after rotation "
                        f"(cash=${cash:.0f}, need=${notional:.0f}) — skipping"
                    )
                    continue
            else:
                logger.info(
                    f"[MOM] {sym}: insufficient cash (available=${cash:.0f}, "
                    f"need=${notional:.0f}, reserve=${min_cash:.0f}) — skipping"
                )
                continue

        logger.info(
            f"[MOM] ENTER {sym} — score={sig['score']:.4f}, alloc={size_pct:.1%} "
            f"(3m={sig['ret_3m']:.2%}, 1m={sig['ret_1m']:.2%}), "
            f"rsi={sig['rsi']:.1f}, vol_ratio={sig['vol_ratio']:.2f}x, "
            f"notional=${notional:.0f}"
        )

        _buy_result = broker.market_buy(sym, notional, STRATEGY_NAME)
        tag_symbol(sym, STRATEGY_NAME)
        if _buy_result is not None and rotated_in:
            from utils.capital_rotator import mark_rotation_in
            mark_rotation_in(sym)
        log_trade(
            db_conn, STRATEGY_NAME, sym, "buy", 0, sig["price"], 0,
            metadata={
                "notional": notional,
                "momentum_score": sig["score"],
                "ret_3m": sig["ret_3m"],
                "ret_1m": sig["ret_1m"],
                "rsi": sig["rsi"],
                "alloc_pct": size_pct,
            },
        )
        cash, portfolio_value = broker.get_live_cash()
        if cash < portfolio_value * MIN_CASH_RESERVE_PCT:
            logger.warning(f"[{STRATEGY_NAME}] Cash floor hit (${cash:,.0f}) — halting entries")
            break

    _last_rebalance = _now_utc()
    try:
        set_state(db_conn, "momentum_last_rebalance", _last_rebalance.isoformat())
    except Exception:
        pass

    active = len([p for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME])
    logger.info(f"[MOM] Rebalance complete — {active} active positions, next in {MOMENTUM_REBALANCE_DAYS} days")
