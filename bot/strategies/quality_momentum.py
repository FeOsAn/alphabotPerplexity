"""
Strategy: Quality Momentum Combo  — v93
---------------------------------------
+66.7% / Sharpe 1.23 / Sortino 1.71 over 2 years in the v91 deep backtest.

Blends a defensive QUALITY factor (low 252-day realised volatility) with a
6-month MOMENTUM factor over the same ~40-name large-cap universe as CS-MOM.
The quality tilt lets it keep trading in CHOP (at half weight) where pure
momentum bleeds.

v93 — the combined factor score is now a SCREEN, not the entry trigger. It
narrows the universe to a top-15 shortlist; an actual BUY then requires the
trend to be actively strengthening AND the business to clear a fundamental
quality bar. A high combined score alone is no longer a reason to buy.

Monthly rebalance (first Monday of the month):
  quality   = 1 - (vol_rank_ascending / n)     (lower vol → higher quality)
  momentum  = (return_rank_ascending / n)        (6-month return, normalised)
  combined  = 0.4 * quality + 0.6 * momentum
  → top 15 by combined score form the shortlist; from it, BUY only names that
    pass ALL of:
      1. Momentum accelerating — return_1m > 0 AND return_1m > return_3m / 3
      2. Trend rising — close > MA50 AND MA50 today > MA50 20 days ago
      3. RSI(14) in [45, 68] — not oversold/broken, not overbought/extended
      4. No earnings within 5 trading days
      5. Quality fundamentals — trailingPE < 40 and (if available) ROE > 0.10
  Up to 5 positions (was 8), 13% equity each. If fewer than 2 pass, deploy
  nothing this month — cash is a position, no forced deployment.

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

import numpy as np
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

SHORTLIST_N = 15           # rank to a top-15 shortlist; gates pick the actual buys
MAX_POSITIONS = 5          # at most 5 new positions per cycle (was 8)
MIN_PASSERS = 2            # if fewer than this pass ALL gates, open nothing
EXIT_RANK = 16             # close when a holding falls out of the top 16
POSITION_PCT = 0.13        # 13% of equity per position
CHOP_SIZE_MULT = 0.5       # half size in CHOP regime
STOP_PCT = 0.10            # 10% hard stop below entry
TP_PCT = 0.25              # 25% take-profit above entry
TIME_STOP_DAYS = 45        # close after 45 trading days
BEAR_LOSS_EXIT_PCT = 0.05  # in BEAR, close holdings down more than 5%
LOOKBACK_DAYS = 280        # daily history to download (needs >= 252 trading rows)
QUALITY_WEIGHT = 0.4
MOMENTUM_WEIGHT = 0.6
MOMENTUM_LOOKBACK = 126    # ~6 months of trading days

# ── Entry gate thresholds (v93) ────────────────────────────────────────────────
RSI_MIN = 45.0             # RSI(14) lower bound (not oversold/broken)
RSI_MAX = 68.0             # RSI(14) upper bound (not overbought/extended)
MA_TREND = 50              # close must be above the 50-day MA, which must be rising
MA_RISING_LOOKBACK = 20    # MA50 today vs MA50 20 days ago
EARNINGS_BLACKOUT_DAYS = 5 # skip if earnings within 5 trading days
MAX_PE = 40.0              # trailingPE must be below this
MIN_ROE = 0.10             # returnOnEquity floor (only if the field is available)

_LAST_RUN_MONTH_KEY = "quality_momentum_last_run_month"   # "YYYY-MM" guard
_LAST_ENTRY_DATE_KEY = "quality_momentum_last_entry_date"  # "YYYY-MM-DD" first-run guard


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _needs_first_run(broker: AlpacaBroker, db_conn) -> bool:
    """First-run override (v92): deploy immediately instead of waiting for the
    first Monday of the month. Fires when we hold NO quality_momentum positions
    AND have not entered in the last 7 days. Normal monthly schedule resumes once
    positions exist or a recent entry is on record.
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


def _rsi(series: pd.Series, period: int = 14) -> float:
    """Wilder-style RSI; returns the latest value (NaN-safe → 50.0 fallback)."""
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    val = rsi.iloc[-1]
    return float(val) if pd.notna(val) else 50.0


def _trading_days_until(target_date) -> int:
    """Mon-Fri days from today (UTC) to target_date. Negative if already past."""
    today = datetime.now(timezone.utc).date()
    if target_date < today:
        return -1
    days = 0
    current = today
    while current < target_date:
        current += timedelta(days=1)
        if current.weekday() < 5:
            days += 1
    return days


def _earnings_within(sym: str, n_trading_days: int, info: dict | None = None) -> bool:
    """True if `sym` reports earnings within `n_trading_days`. Uses the supplied
    .info dict when given, else fetches it; falls back to the ticker calendar.
    Fail-open (False) on any error so a flaky source never blocks every name."""
    dates: list = []
    try:
        ticker = yf.Ticker(sym)
        if info is None:
            try:
                info = ticker.info or {}
            except Exception:
                info = {}
        ts = info.get("earningsTimestamp") or info.get("earningsTimestampStart")
        if ts:
            try:
                dates.append(datetime.fromtimestamp(int(ts), tz=timezone.utc).date())
            except Exception:
                pass
        if not dates:
            cal = ticker.calendar
            raw = []
            if isinstance(cal, dict):
                ed = cal.get("Earnings Date", [])
                raw = ed if isinstance(ed, list) else ([ed] if ed else [])
            elif cal is not None and hasattr(cal, "columns") and "Earnings Date" in getattr(cal, "columns", []):
                raw = cal["Earnings Date"].dropna().tolist()
            for d in raw:
                try:
                    dates.append(d.date() if hasattr(d, "date") else d)
                except Exception:
                    continue
    except Exception as e:
        logger.debug(f"[QMOM] {sym}: earnings lookup failed: {e}")
        return False

    for d in dates:
        td = _trading_days_until(d)
        if 0 <= td <= n_trading_days:
            return True
    return False


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


def _rank_universe(symbols: list[str]) -> tuple[list[dict], dict[str, pd.Series]]:
    """Rank by combined = 0.4*quality + 0.6*momentum. Returns (rows sorted desc,
    {symbol: close_series}) so entry gates can reuse the downloaded closes."""
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
        return [], closes

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
    return raw, closes


def _passes_entry_gates(sym: str, s: pd.Series) -> bool:
    """Concrete entry trigger required for a BUY (v93). The combined factor put
    this name on the shortlist; the trend must be actively strengthening and the
    business must clear a fundamental quality bar to justify buying. Logs the
    failing gate."""
    try:
        c_now = float(s.iloc[-1])

        # 1. Momentum accelerating — recent pace faster than the 3-month average.
        ret_1m = (c_now / float(s.iloc[-21])) - 1.0
        ret_3m = (c_now / float(s.iloc[-63])) - 1.0
        if not (ret_1m > 0 and ret_1m > ret_3m / 3.0):
            logger.info(
                f"[QUAL_MOM] {sym} rejected: momentum not accelerating "
                f"(ret_1m={ret_1m*100:.1f}%, ret_3m/3={ret_3m/3*100:.1f}%)"
            )
            return False

        # 2. Trend actively rising — above MA50 AND MA50 higher than 20 days ago.
        ma_now = float(s.tail(MA_TREND).mean())
        ma_prev = float(s.iloc[-(MA_TREND + MA_RISING_LOOKBACK):-MA_RISING_LOOKBACK].mean())
        if c_now <= ma_now:
            logger.info(f"[QUAL_MOM] {sym} rejected: close={c_now:.2f} <= MA{MA_TREND}={ma_now:.2f}")
            return False
        if ma_now <= ma_prev:
            logger.info(f"[QUAL_MOM] {sym} rejected: MA{MA_TREND} not rising ({ma_now:.2f} <= {ma_prev:.2f})")
            return False

        # 3. RSI(14) in the sweet spot.
        rsi = _rsi(s, 14)
        if not (RSI_MIN <= rsi <= RSI_MAX):
            logger.info(f"[QUAL_MOM] {sym} rejected: RSI={rsi:.1f} outside [{RSI_MIN:.0f}, {RSI_MAX:.0f}]")
            return False

        # Fetch .info once for the earnings + fundamental gates.
        info = {}
        try:
            info = yf.Ticker(sym).info or {}
        except Exception:
            info = {}

        # 4. No earnings within the blackout window.
        if _earnings_within(sym, EARNINGS_BLACKOUT_DAYS, info=info):
            logger.info(f"[QUAL_MOM] {sym} rejected: earnings within {EARNINGS_BLACKOUT_DAYS} trading days")
            return False

        # 5. Fundamental quality — skip fields that are unavailable (don't disqualify on missing data).
        pe = info.get("trailingPE")
        if pe is not None and pe >= MAX_PE:
            logger.info(f"[QUAL_MOM] {sym} rejected: trailingPE={pe:.1f} >= {MAX_PE:.0f}")
            return False
        roe = info.get("returnOnEquity")
        if roe is not None and roe <= MIN_ROE:
            logger.info(f"[QUAL_MOM] {sym} rejected: returnOnEquity={roe:.2f} <= {MIN_ROE}")
            return False

        logger.info(
            f"[QUAL_MOM] {sym} PASSED all gates: ret_1m={ret_1m*100:.1f}%, "
            f"RSI={rsi:.1f}, MA{MA_TREND} rising, "
            f"PE={pe if pe is None else round(pe,1)}, ROE={roe if roe is None else round(roe,2)}"
        )
        return True
    except Exception as e:
        logger.warning(f"[QUAL_MOM] {sym}: gate evaluation failed ({e}) — skipping")
        return False


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

    # ── Monthly rebalance gate (first Monday of month, or first-run override) ──
    first_run = _needs_first_run(broker, db_conn)
    if not _is_first_week_monday() and not first_run:
        return
    if not first_run and get_state(db_conn, _LAST_RUN_MONTH_KEY) == _this_month():
        return
    if first_run:
        logger.info("[QMOM] First-run override — deploying without waiting for first Monday")

    from utils.market_hours import is_entry_allowed
    if not is_entry_allowed():
        logger.info("[QMOM] Outside safe entry window — rebalance deferred")
        return

    logger.info("=== Quality Momentum: monthly rebalance ===")
    ranked, closes = _rank_universe(QUALITY_MOM_UNIVERSE)
    if not ranked:
        logger.warning("[QMOM] No rankings computed — will retry next cycle")
        return

    logger.info(
        f"[QMOM] Ranked {len(ranked)} names; shortlist top {SHORTLIST_N}: "
        + ", ".join(f"{r['symbol']}({r['combined']:.3f})" for r in ranked[:SHORTLIST_N])
    )

    # ── Rebalance exits: drop holdings out of the top 16 combined-score ──────
    top_syms = {r["symbol"] for r in ranked[:EXIT_RANK]}
    ranked_syms = {r["symbol"] for r in ranked}
    for pos in [p for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME]:
        sym = pos["symbol"]
        if sym in ranked_syms and sym not in top_syms:
            _close_position(broker, db_conn, pos, f"dropped out of top {EXIT_RANK}")

    # ── Entries: combined score screened the universe; a strengthening trend and
    #    a fundamental quality bar justify the actual buy. No forced deployment. ─
    size_mult = CHOP_SIZE_MULT if regime == "chop" else 1.0
    held = {p["symbol"] for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME}
    shortlist = [r for r in ranked[:SHORTLIST_N] if r["symbol"] not in held]

    from utils.cooldown import is_on_cooldown
    passers: list[dict] = []
    for r in shortlist:
        sym = r["symbol"]
        if is_on_cooldown(sym):
            logger.debug(f"[QMOM] {sym} on cooldown — skipping")
            continue
        s = closes.get(sym)
        if s is None:
            logger.debug(f"[QMOM] {sym}: no price series — skipping")
            continue
        if _passes_entry_gates(sym, s):
            passers.append(r)
        if len(passers) >= MAX_POSITIONS:
            break

    if len(passers) < MIN_PASSERS:
        logger.info(
            f"[QMOM] Only {len(passers)} name(s) passed all gates (need >= {MIN_PASSERS}) "
            f"— no forced deployment, holding cash this month"
        )
        set_state(db_conn, _LAST_RUN_MONTH_KEY, _this_month())
        set_state(db_conn, _LAST_ENTRY_DATE_KEY, _today())
        return

    targets = passers[:MAX_POSITIONS]
    logger.info(
        f"[QMOM] {len(targets)} name(s) cleared all gates: "
        + ", ".join(t["symbol"] for t in targets)
    )

    account = broker.get_account()
    equity = account["equity"]
    cash, portfolio_value = broker.get_live_cash()

    for r in targets:
        sym = r["symbol"]

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
        # v92: normalise the combined factor to a 0.85–1.0 signal_score by rank so
        # top selections clear the broker's 0.85 displacement gate. Top of shortlist → 1.0.
        rank_idx = next((i for i, rr in enumerate(ranked) if rr["symbol"] == sym), 0)
        sig_score = max(0.85, 1.0 - 0.15 * (rank_idx / max(1, SHORTLIST_N - 1)))

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
    set_state(db_conn, _LAST_ENTRY_DATE_KEY, _today())
    active = len([p for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME])
    logger.info(f"[QMOM] Rebalance complete — {active} active quality_momentum position(s)")
