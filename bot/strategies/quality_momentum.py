"""
Strategy: Quality Momentum Combo  — v91
---------------------------------------
+66.7% / Sharpe 1.23 / Sortino 1.71 over 2 years in the v91 deep backtest.

Blends a defensive QUALITY factor (low 252-day realised volatility) with a
6-month MOMENTUM factor over the same ~40-name large-cap universe as CS-MOM.
The quality tilt lets it keep trading in CHOP (at half weight) where pure
momentum bleeds.

Monthly rebalance (first Monday of the month):
  quality   = 1 - (vol_rank_ascending / n)     (lower vol → higher quality)
  momentum  = (return_rank_ascending / n)        (6-month return, normalised)
  combined  = 0.4 * quality + 0.6 * momentum
  → buy the top 8 by combined score, 8% equity each (≈64% deployment)

Entry: BULL (full) and CHOP (half size). No new entries in BEAR.
Exit:
  - monthly rebalance: drop names that fall out of the top 16 combined-score
  - 10% hard stop / 25% take-profit (exchange bracket)
  - time stop after 45 trading days
  - BEAR regime: close existing positions that are down > 5%

TP multiples for tp_engine: (2.0, 4.0). Regime compat: ["bull", "chop"].
"""

import gc
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import pandas as pd
import yfinance as yf

from broker import AlpacaBroker, tag_symbol
from config import MIN_CASH_RESERVE_PCT
from db import log_trade, log_signal, get_state, set_state, get_position_state

logger = logging.getLogger("alphabot.quality_momentum")
STRATEGY_NAME = "quality_momentum"

QUALITY_MOM_UNIVERSE = [
    "AAPL", "MSFT", "NVDA", "META", "GOOGL", "AMZN", "CRM", "ADBE", "AMD", "AVGO",
    "QCOM", "MU", "JPM", "GS", "MS", "V", "MA", "UNH", "LLY", "ABBV",
    "CAT", "DE", "HON", "GE", "XOM", "CVX", "COP", "COST", "HD", "NKE",
    "SBUX", "NFLX", "TSLA", "TSM", "BLK", "MRK", "WFC", "AXP", "PFE", "MMM",
]
QUALITY_MOM_UNIVERSE = list(dict.fromkeys(QUALITY_MOM_UNIVERSE))

TOP_N = 8                  # buy the top 8 by combined score
EXIT_RANK = 16             # close when a holding falls out of the top 16
POSITION_PCT = 0.08        # 8% of equity per position (8 × 8% = 64% deployment)
CHOP_SIZE_MULT = 0.5       # half size in CHOP regime
STOP_PCT = 0.10            # 10% hard stop below entry
TP_PCT = 0.25              # 25% take-profit above entry
TIME_STOP_DAYS = 45        # close after 45 trading days
BEAR_LOSS_EXIT_PCT = 0.05  # in BEAR, close holdings down more than 5%
LOOKBACK_DAYS = 280        # daily history to download (needs >= 252 trading rows)
QUALITY_WEIGHT = 0.4
MOMENTUM_WEIGHT = 0.6
MOMENTUM_LOOKBACK = 126    # ~6 months of trading days

_LAST_RUN_MONTH_KEY = "quality_momentum_last_run_month"   # "YYYY-MM" guard


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _this_month() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")


def _is_first_week_monday() -> bool:
    """First Monday of the month — fires once per month on a weekday."""
    now = datetime.now(timezone.utc)
    return now.day <= 7 and now.weekday() == 0


def _trading_days_open(entry_time) -> int:
    if entry_time is None:
        return 0
    try:
        if isinstance(entry_time, str):
            entry_dt = datetime.fromisoformat(entry_time.replace("Z", "+00:00"))
        else:
            entry_dt = entry_time
        if entry_dt.tzinfo is None:
            entry_dt = entry_dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        days = 0
        current = entry_dt
        while current.date() < now.date():
            current += timedelta(days=1)
            if current.weekday() < 5:
                days += 1
        return days
    except Exception:
        return 0


def _download_closes(symbols: list[str]) -> dict[str, pd.Series]:
    """Download daily closes for the whole universe in ONE yfinance call."""
    closes: dict[str, pd.Series] = {}
    try:
        data = yf.download(
            symbols, period=f"{LOOKBACK_DAYS + 60}d", interval="1d",
            progress=False, auto_adjust=True, group_by="ticker", threads=True,
        )
    except Exception as e:
        logger.warning(f"[QMOM] bulk download failed: {e}")
        return closes
    if data is None or len(data) == 0:
        logger.warning("[QMOM] bulk download returned no data")
        return closes
    multi = isinstance(data.columns, pd.MultiIndex)
    for sym in symbols:
        try:
            if multi:
                if sym not in data.columns.get_level_values(0):
                    logger.warning(f"[QMOM] {sym}: missing from download — skipping")
                    continue
                s = data[sym]["Close"].dropna()
            else:
                s = data["Close"].dropna()
            if len(s) >= 252:
                closes[sym] = s
            else:
                logger.warning(f"[QMOM] {sym}: only {len(s)} rows (<252) — skipping")
        except Exception as e:
            logger.warning(f"[QMOM] {sym}: extract failed ({e}) — skipping")
    return closes


def _rank_universe(symbols: list[str]) -> list[dict]:
    """Rank by combined = 0.4*quality + 0.6*momentum. Returns rows sorted desc."""
    closes = _download_closes(symbols)
    raw: list[dict] = []
    for sym, s in closes.items():
        try:
            c_now = float(s.iloc[-1])
            c_6m = float(s.iloc[-MOMENTUM_LOOKBACK])
            if c_now <= 0 or c_6m <= 0:
                continue
            ret_6m = (c_now / c_6m) - 1.0
            vol = float(s.tail(252).pct_change().dropna().std())
            raw.append({"symbol": sym, "price": c_now, "ret_6m": ret_6m, "vol": vol})
        except Exception as e:
            logger.debug(f"[QMOM] {sym}: factor calc failed: {e}")

    n = len(raw)
    if n == 0:
        return []

    # vol_rank ascending: lowest vol → rank 0 → highest quality.
    by_vol = sorted(raw, key=lambda x: x["vol"])
    for rank, row in enumerate(by_vol):
        row["quality"] = 1.0 - (rank / n)
    # return_rank ascending: highest return → rank n-1 → highest momentum.
    by_ret = sorted(raw, key=lambda x: x["ret_6m"])
    for rank, row in enumerate(by_ret):
        row["momentum"] = rank / n

    for row in raw:
        row["combined"] = QUALITY_WEIGHT * row["quality"] + MOMENTUM_WEIGHT * row["momentum"]

    raw.sort(key=lambda x: x["combined"], reverse=True)
    gc.collect()
    return raw


def _close_position(broker: AlpacaBroker, db_conn, pos: dict, reason: str) -> bool:
    sym = pos["symbol"]
    from strategies.regime_exit import _cancel_open_orders_for_symbol
    n = _cancel_open_orders_for_symbol(broker, sym)
    if n:
        logger.info(f"[QMOM] {sym}: cancelled {n} open order(s) before close")
    time.sleep(3)
    result = broker.close_position(sym, STRATEGY_NAME)
    if result is not None:
        logger.info(f"[QMOM] CLOSED {sym} — {reason} (pnl={pos['unrealized_pnl_pct']:.1f}%)")
        log_trade(
            db_conn, STRATEGY_NAME, sym, "sell",
            pos["qty"], pos["current_price"], pos["unrealized_pnl"],
            metadata={"reason": reason},
        )
        from utils.cooldown import set_cooldown
        set_cooldown(sym)
        return True
    logger.warning(f"[QMOM] {sym}: close failed ({reason}) — will retry next cycle")
    return False


def _check_time_stops(broker: AlpacaBroker, db_conn) -> None:
    """45-trading-day time stop — checked every cycle (cheap, DB-driven)."""
    positions = [p for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME]
    for pos in positions:
        state = get_position_state(db_conn, pos["symbol"])
        entry_time = state.get("entry_time") if state else None
        if _trading_days_open(entry_time) >= TIME_STOP_DAYS:
            _close_position(broker, db_conn, pos, f"time stop ({TIME_STOP_DAYS} trading days)")


def _bear_loss_sweep(broker: AlpacaBroker, db_conn) -> None:
    """In BEAR, close any quality_momentum holding down more than 5%."""
    positions = [p for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME]
    for pos in positions:
        if pos["unrealized_pnl_pct"] <= -BEAR_LOSS_EXIT_PCT * 100:
            _close_position(broker, db_conn, pos, f"bear regime, down {pos['unrealized_pnl_pct']:.1f}%")


def run(broker: AlpacaBroker, db_conn):
    """
    Quality-Momentum dispatch:
      - Time stops run every cycle.
      - BEAR: no new entries; close holdings down > 5%.
      - Monthly (first Monday): re-rank, drop names out of the top 16, then enter
        the top 8 not held (BULL full size, CHOP half size).
    """
    regime = (get_state(db_conn, "current_regime") or "bull").lower()

    _check_time_stops(broker, db_conn)

    if regime == "bear":
        _bear_loss_sweep(broker, db_conn)
        return

    # ── Monthly rebalance gate (first Monday of the month, once per month) ────
    if not _is_first_week_monday():
        return
    if get_state(db_conn, _LAST_RUN_MONTH_KEY) == _this_month():
        return

    from utils.market_hours import is_entry_allowed
    if not is_entry_allowed():
        logger.info("[QMOM] Outside safe entry window — rebalance deferred")
        return

    logger.info("=== Quality Momentum: monthly rebalance ===")
    ranked = _rank_universe(QUALITY_MOM_UNIVERSE)
    if not ranked:
        logger.warning("[QMOM] No rankings computed — will retry next cycle")
        return

    logger.info(
        f"[QMOM] Ranked {len(ranked)} names; top {TOP_N}: "
        + ", ".join(f"{r['symbol']}({r['combined']:.3f})" for r in ranked[:TOP_N])
    )

    # ── Rebalance exits: drop holdings out of the top 16 combined-score ──────
    top_syms = {r["symbol"] for r in ranked[:EXIT_RANK]}
    ranked_syms = {r["symbol"] for r in ranked}
    for pos in [p for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME]:
        sym = pos["symbol"]
        if sym in ranked_syms and sym not in top_syms:
            _close_position(broker, db_conn, pos, f"dropped out of top {EXIT_RANK}")

    # ── Entries ──────────────────────────────────────────────────────────────
    size_mult = CHOP_SIZE_MULT if regime == "chop" else 1.0
    held = {p["symbol"] for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME}
    targets = [r for r in ranked[:TOP_N] if r["symbol"] not in held]

    account = broker.get_account()
    equity = account["equity"]
    cash, portfolio_value = broker.get_live_cash()

    for r in targets:
        sym = r["symbol"]

        from utils.cooldown import is_on_cooldown
        if is_on_cooldown(sym):
            logger.debug(f"[QMOM] {sym} on cooldown — skipping")
            continue

        from utils.earnings_calendar import has_upcoming_earnings
        if has_upcoming_earnings(sym):
            logger.info(f"[QMOM] Skipping {sym} — earnings blackout (within 2 days)")
            continue

        notional = equity * POSITION_PCT * size_mult
        min_cash = portfolio_value * MIN_CASH_RESERVE_PCT
        if cash - notional < min_cash:
            logger.info(
                f"[QMOM] {sym}: insufficient cash "
                f"(available=${cash:.0f}, need=${notional:.0f}, reserve=${min_cash:.0f})"
            )
            continue

        entry_ref = r["price"]
        stop_px = round(entry_ref * (1.0 - STOP_PCT), 2)
        tp_px = round(entry_ref * (1.0 + TP_PCT), 2)
        sig_score = max(0.0, min(1.0, r["combined"]))

        log_signal(db_conn, STRATEGY_NAME, sym, "buy", r["combined"],
                   {"combined": r["combined"], "quality": r["quality"],
                    "momentum": r["momentum"], "ret_6m": r["ret_6m"], "vol": r["vol"]})

        logger.info(
            f"[QMOM] ENTER {sym} — combined={r['combined']:.3f} "
            f"(q={r['quality']:.2f}, m={r['momentum']:.2f}), price=${entry_ref:.2f}, "
            f"notional=${notional:.0f}, stop=${stop_px:.2f}, tp=${tp_px:.2f}"
        )

        result = broker.market_buy(
            sym, notional, STRATEGY_NAME,
            tp_target_override=tp_px,
            stop_override=stop_px,
            signal_score=sig_score,
        )
        if result is None:
            logger.info(f"[QMOM] {sym}: entry blocked by broker safety gate")
            continue
        tag_symbol(sym, STRATEGY_NAME)
        log_trade(
            db_conn, STRATEGY_NAME, sym, "buy", 0, entry_ref, 0,
            metadata={"notional": notional, "combined": r["combined"],
                      "stop": stop_px, "tp": tp_px},
        )
        cash, portfolio_value = broker.get_live_cash()
        if cash < portfolio_value * MIN_CASH_RESERVE_PCT:
            logger.warning(f"[QMOM] Cash floor hit (${cash:,.0f}) — halting entries")
            break

    set_state(db_conn, _LAST_RUN_MONTH_KEY, _this_month())
    active = len([p for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME])
    logger.info(f"[QMOM] Rebalance complete — {active} active quality_momentum position(s)")
