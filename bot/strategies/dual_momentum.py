"""
Strategy: Dual Momentum (Antonacci-style)  — v91
-------------------------------------------------
+45.0% / Sharpe 0.98 over 2 years in the v91 deep backtest. Gary Antonacci's
dual-momentum applied to a small cross-asset ETF universe.

Two momentum filters, applied monthly (first Monday of the month):
  ABSOLUTE  — only hold assets whose 12-1 momentum return > 4.8% (a T-bill-like
              floor). When risk assets fail this gate the sleeve rotates to GLD.
  RELATIVE  — among the absolute survivors, rank by 12-1 momentum and hold the
              top 3, equal-weight (~33% each, ≈99% deployed).

If fewer than 3 assets clear the absolute gate the empty slots are filled with
GLD; if none clear it the sleeve is 100% GLD. This keeps the book defensive in
drawdowns rather than going to cash.

Entry: ALL regimes. On each monthly rebalance the whole sleeve is rebuilt:
close every existing dual_momentum position, then buy the new targets equal
weight. No fixed take-profit (let momentum run); the only hard exit is a
catastrophic 15% intra-month stop placed as an exchange bracket.

Exit:
  - monthly rebalance (full teardown + rebuild)
  - 15% catastrophic stop (exchange bracket) — checked every cycle as a backstop

TP multiples for tp_engine: (3.0, 6.0). Regime compat: ["bull", "chop", "bear"].
"""

import gc
import logging
import time
from collections import Counter
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf

from broker import AlpacaBroker, tag_symbol
from config import MIN_CASH_RESERVE_PCT
from db import log_trade, log_signal, get_state, set_state

logger = logging.getLogger("alphabot.dual_momentum")
STRATEGY_NAME = "dual_momentum"

DUAL_MOM_UNIVERSE = ["SPY", "QQQ", "IWM", "GLD", "XLF", "XLE", "XLK", "XLV"]
DUAL_MOM_UNIVERSE = list(dict.fromkeys(DUAL_MOM_UNIVERSE))

SAFE_ASSET = "GLD"             # rotation target when risk assets fail the gate
TOP_N = 3                      # hold the top 3 by relative momentum
ABS_MOM_FLOOR = 0.048          # absolute momentum gate: 12-month return > 4.8%
DEPLOY_PCT = 0.99              # ~99% deployed across the slots
CATASTROPHIC_STOP_PCT = 0.15   # 15% hard stop (the only protective stop)
LOOKBACK_DAYS = 280            # daily history to download (needs >= 252 trading rows)

_LAST_RUN_MONTH_KEY = "dual_momentum_last_run_month"   # "YYYY-MM" guard
_LAST_ENTRY_DATE_KEY = "dual_momentum_last_entry_date"  # "YYYY-MM-DD" first-run guard


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _needs_first_run(broker: AlpacaBroker, db_conn) -> bool:
    """First-run override (v92): build the sleeve immediately instead of waiting
    for the first Monday of the month. Fires when we hold NO dual_momentum
    positions AND have not entered in the last 7 days. Normal monthly schedule
    resumes once positions exist or a recent entry is on record.
    """
    held = [p for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME]
    if held:
        return False
    last = get_state(db_conn, _LAST_ENTRY_DATE_KEY)
    if not last:
        return True
    try:
        last_dt = datetime.strptime(last, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - last_dt).days >= 7
    except Exception:
        return True


def _this_month() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")


def _is_first_week_monday() -> bool:
    """First Monday of the month — fires once per month on a weekday."""
    now = datetime.now(timezone.utc)
    return now.day <= 7 and now.weekday() == 0


def _download_closes(symbols: list[str]) -> dict[str, pd.Series]:
    """Download daily closes for the whole universe in ONE yfinance call."""
    closes: dict[str, pd.Series] = {}
    try:
        data = yf.download(
            symbols, period=f"{LOOKBACK_DAYS + 60}d", interval="1d",
            progress=False, auto_adjust=True, group_by="ticker", threads=True,
        )
    except Exception as e:
        logger.warning(f"[DUAL-MOM] bulk download failed: {e}")
        return closes
    if data is None or len(data) == 0:
        logger.warning("[DUAL-MOM] bulk download returned no data")
        return closes
    multi = isinstance(data.columns, pd.MultiIndex)
    for sym in symbols:
        try:
            if multi:
                if sym not in data.columns.get_level_values(0):
                    logger.warning(f"[DUAL-MOM] {sym}: missing from download — skipping")
                    continue
                s = data[sym]["Close"].dropna()
            else:
                s = data["Close"].dropna()
            if len(s) >= 252:
                closes[sym] = s
            else:
                logger.warning(f"[DUAL-MOM] {sym}: only {len(s)} rows (<252) — skipping")
        except Exception as e:
            logger.warning(f"[DUAL-MOM] {sym}: extract failed ({e}) — skipping")
    return closes


def _rank_universe(symbols: list[str]) -> list[dict]:
    """Compute 12-1 momentum + 12-month return for every symbol.

    score    = (close[-1] / close[-252]) - (close[-1] / close[-21])   (relative)
    ret_12m  = (close[-1] / close[-252]) - 1.0                         (absolute)
    """
    closes = _download_closes(symbols)
    rows: list[dict] = []
    for sym, s in closes.items():
        try:
            c_now = float(s.iloc[-1])
            c_252 = float(s.iloc[-252])
            c_21 = float(s.iloc[-21])
            if c_now <= 0 or c_252 <= 0 or c_21 <= 0:
                continue
            score = (c_now / c_252) - (c_now / c_21)
            ret_12m = (c_now / c_252) - 1.0
            rows.append({"symbol": sym, "score": score, "ret_12m": ret_12m, "price": c_now})
        except Exception as e:
            logger.debug(f"[DUAL-MOM] {sym}: score failed: {e}")
    rows.sort(key=lambda x: x["score"], reverse=True)
    gc.collect()
    return rows


def _select_targets(ranked: list[dict]) -> list[dict]:
    """Apply absolute gate then relative selection; fill empty slots with GLD.

    Returns a list of TOP_N target dicts (one per slot). GLD-fill slots reuse the
    GLD row so each slot carries a real price for sizing/stops.
    """
    by_sym = {r["symbol"] for r in ranked}
    gld_row = next((r for r in ranked if r["symbol"] == SAFE_ASSET), None)

    # ABSOLUTE gate: risk assets must clear the floor; GLD itself can always hold.
    passers = [
        r for r in ranked
        if r["ret_12m"] > ABS_MOM_FLOOR and r["symbol"] != SAFE_ASSET
    ]
    targets = passers[:TOP_N]

    # Fill remaining slots with GLD (defensive rotation).
    if len(targets) < TOP_N:
        if gld_row is None:
            logger.warning(f"[DUAL-MOM] {SAFE_ASSET} unavailable — cannot fill defensive slots")
        else:
            targets = targets + [gld_row] * (TOP_N - len(targets))

    return targets[:TOP_N]


def _close_position(broker: AlpacaBroker, db_conn, pos: dict, reason: str) -> bool:
    sym = pos["symbol"]
    from strategies.regime_exit import _cancel_open_orders_for_symbol
    n = _cancel_open_orders_for_symbol(broker, sym)
    if n:
        logger.info(f"[DUAL-MOM] {sym}: cancelled {n} open order(s) before close")
    time.sleep(3)
    result = broker.close_position(sym, STRATEGY_NAME)
    if result is not None:
        logger.info(f"[DUAL-MOM] CLOSED {sym} — {reason} (pnl={pos['unrealized_pnl_pct']:.1f}%)")
        log_trade(
            db_conn, STRATEGY_NAME, sym, "sell",
            pos["qty"], pos["current_price"], pos["unrealized_pnl"],
            metadata={"reason": reason},
        )
        from utils.cooldown import set_cooldown
        set_cooldown(sym)
        return True
    logger.warning(f"[DUAL-MOM] {sym}: close failed ({reason}) — will retry next cycle")
    return False


def _catastrophic_stop_sweep(broker: AlpacaBroker, db_conn) -> None:
    """Backstop: close any dual_momentum holding down more than 15%."""
    positions = [p for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME]
    for pos in positions:
        if pos["unrealized_pnl_pct"] <= -CATASTROPHIC_STOP_PCT * 100:
            _close_position(broker, db_conn, pos, f"catastrophic stop, down {pos['unrealized_pnl_pct']:.1f}%")


def _close_all(broker: AlpacaBroker, db_conn, reason: str) -> int:
    positions = [p for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME]
    closed = 0
    for pos in positions:
        if _close_position(broker, db_conn, pos, reason):
            closed += 1
    return closed


def run(broker: AlpacaBroker, db_conn):
    """
    Dual-Momentum dispatch (runs in ALL regimes):
      - Catastrophic 15% stop runs every cycle.
      - Monthly (first Monday): tear down the whole sleeve and rebuild it from the
        absolute+relative momentum selection (GLD-filled when risk assets fail).
    """
    _catastrophic_stop_sweep(broker, db_conn)

    # ── Monthly rebalance gate (first Monday of month, or first-run override) ──
    first_run = _needs_first_run(broker, db_conn)
    if not _is_first_week_monday() and not first_run:
        return
    if not first_run and get_state(db_conn, _LAST_RUN_MONTH_KEY) == _this_month():
        return
    if first_run:
        logger.info("[DUAL-MOM] First-run override — deploying without waiting for first Monday")

    from utils.market_hours import is_entry_allowed
    if not is_entry_allowed():
        logger.info("[DUAL-MOM] Outside safe entry window — rebalance deferred")
        return

    logger.info("=== Dual Momentum: monthly rebalance ===")
    ranked = _rank_universe(DUAL_MOM_UNIVERSE)
    if not ranked:
        logger.warning("[DUAL-MOM] No rankings computed — will retry next cycle")
        return

    targets = _select_targets(ranked)
    if not targets:
        logger.warning("[DUAL-MOM] No targets selected — will retry next cycle")
        return

    # Equal-weight slot allocation; GLD-fill collapses duplicate slots into one
    # larger GLD position.
    slot_counts = Counter(t["symbol"] for t in targets)
    target_by_sym = {t["symbol"]: t for t in targets}
    logger.info(
        "[DUAL-MOM] Targets: "
        + ", ".join(f"{sym}×{cnt}" for sym, cnt in slot_counts.items())
    )

    # ── Full teardown: close every existing dual_momentum position ───────────
    n_closed = _close_all(broker, db_conn, "monthly rebalance teardown")
    if n_closed:
        logger.info(f"[DUAL-MOM] Teardown closed {n_closed} position(s)")
        time.sleep(3)

    # ── Rebuild: equal-weight entries ────────────────────────────────────────
    account = broker.get_account()
    equity = account["equity"]
    cash, portfolio_value = broker.get_live_cash()
    per_slot_pct = DEPLOY_PCT / TOP_N

    for sym, slots in slot_counts.items():
        r = target_by_sym[sym]

        from utils.cooldown import is_on_cooldown
        if is_on_cooldown(sym):
            logger.debug(f"[DUAL-MOM] {sym} on cooldown — skipping")
            continue

        notional = equity * per_slot_pct * slots
        min_cash = portfolio_value * MIN_CASH_RESERVE_PCT
        if cash - notional < min_cash:
            logger.info(
                f"[DUAL-MOM] {sym}: insufficient cash "
                f"(available=${cash:.0f}, need=${notional:.0f}, reserve=${min_cash:.0f})"
            )
            continue

        entry_ref = r["price"]
        stop_px = round(entry_ref * (1.0 - CATASTROPHIC_STOP_PCT), 2)
        # v92: dual momentum is high-conviction by design (it IS the portfolio
        # layer) — fixed 0.90 score so every slot clears the 0.85 displacement gate.
        sig_score = 0.90

        log_signal(db_conn, STRATEGY_NAME, sym, "buy", r["score"],
                   {"score": r["score"], "ret_12m": r["ret_12m"], "slots": slots})

        logger.info(
            f"[DUAL-MOM] ENTER {sym} ({slots} slot(s)) — score={r['score']:.3f}, "
            f"ret_12m={r['ret_12m']*100:.1f}%, price=${entry_ref:.2f}, "
            f"notional=${notional:.0f}, catastrophic_stop=${stop_px:.2f}"
        )

        result = broker.market_buy(
            sym, notional, STRATEGY_NAME,
            stop_override=stop_px,
            signal_score=sig_score,
        )
        if result is None:
            logger.info(f"[DUAL-MOM] {sym}: entry blocked by broker safety gate")
            continue
        tag_symbol(sym, STRATEGY_NAME)
        log_trade(
            db_conn, STRATEGY_NAME, sym, "buy", 0, entry_ref, 0,
            metadata={"notional": notional, "score": r["score"],
                      "ret_12m": r["ret_12m"], "stop": stop_px, "slots": slots},
        )
        cash, portfolio_value = broker.get_live_cash()
        if cash < portfolio_value * MIN_CASH_RESERVE_PCT:
            logger.warning(f"[DUAL-MOM] Cash floor hit (${cash:,.0f}) — halting entries")
            break

    set_state(db_conn, _LAST_RUN_MONTH_KEY, _this_month())
    set_state(db_conn, _LAST_ENTRY_DATE_KEY, _today())
    active = len([p for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME])
    logger.info(f"[DUAL-MOM] Rebalance complete — {active} active dual_momentum position(s)")
