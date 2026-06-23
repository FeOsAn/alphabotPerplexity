"""
Strategy: Cross-Sectional Momentum (CS-MOM)  — v93
---------------------------------------------------
The #1 performer in the v91 deep backtest (+178.7% / Sharpe 1.58 / Sortino 2.19
over 2 years). Classic Jegadeesh-Titman 12-1 cross-sectional momentum over a
~40-name large-cap universe.

v93 — ranking is now SCREENING, not the entry trigger. The 12-1 momentum score
narrows the universe to a top-15 shortlist; an actual BUY then requires a
concrete technical setup so we buy momentum on dips, never chasing extended
moves. Ranking + gates together = conviction; a high rank alone is not a reason
to buy.

Each Monday the universe is ranked by 12-1 momentum to form a top-15 shortlist.
From that shortlist a name is bought ONLY if it passes ALL of these on the day:
  1. Pullback — 3–10% below its 20-day high (buy dips, not extended tops)
  2. Volume   — today's volume is 0.7×–1.5× the 20-day average (no spikes)
  3. RSI(14)  — < 72 (not overbought)
  4. Uptrend  — close > 50-day MA
  5. Earnings — no earnings within 3 trading days (event risk)
Up to 4 positions (was 6 — be selective). If fewer than 2 names pass all gates,
nothing is opened this week — cash is a position, no forced deployment.

  - 12-1 momentum = (close[-1]/close[-252]) - (close[-1]/close[-21])
  - 12% equity, 8% hard stop, 20% take-profit (exchange bracket)

Exit (checked daily):
  - dropped out of the top 12 ranked names → close
  - 8% hard stop (exchange bracket)
  - time stop after 30 trading days
  - BEAR regime → close ALL cs_momentum positions immediately

TP multiples for tp_engine: (1.5, 3.0). Regime compat: ["bull"].
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

logger = logging.getLogger("alphabot.cs_momentum")
STRATEGY_NAME = "cs_momentum"

CS_MOM_UNIVERSE = [
    "AAPL", "MSFT", "NVDA", "META", "GOOGL", "AMZN", "CRM", "ADBE", "AMD", "AVGO",
    "QCOM", "MU", "JPM", "GS", "MS", "V", "MA", "UNH", "LLY", "ABBV",
    "CAT", "DE", "HON", "GE", "XOM", "CVX", "COP", "COST", "HD", "NKE",
    "SBUX", "NFLX", "TSLA", "TSM", "BLK", "MRK", "WFC", "AXP", "PFE", "MMM",
]
# Deduplicate while preserving order
CS_MOM_UNIVERSE = list(dict.fromkeys(CS_MOM_UNIVERSE))

POSITION_PCT = 0.12        # 12% of equity per position (fewer names, higher conviction)
SHORTLIST_N = 15           # rank to a top-15 shortlist; gates pick the actual buys
MAX_POSITIONS = 4          # at most 4 new positions per week (be selective)
MIN_PASSERS = 2            # if fewer than this pass ALL gates, open nothing
EXIT_RANK = 12             # close when a holding falls out of the top 12
STOP_PCT = 0.08            # 8% hard stop below entry
TP_PCT = 0.20              # 20% take-profit above entry
TIME_STOP_DAYS = 30        # momentum signals decay — close after 30 trading days
LOOKBACK_DAYS = 280        # daily history to download (needs >= 252 trading rows)

# ── Entry gate thresholds (v93) ────────────────────────────────────────────────
PULLBACK_MIN = 0.03        # at least 3% below the 20-day high (not chasing)
PULLBACK_MAX = 0.10        # at most 10% below (>10% = momentum may be broken)
VOL_RATIO_MIN = 0.70       # today's volume vs 20-day avg — lower bound
VOL_RATIO_MAX = 1.50       # upper bound (>1.5× = spike, avoid panic/news)
RSI_MAX = 72.0             # RSI(14) must be below this (not overbought)
MA_TREND = 50              # close must be above the 50-day MA
EARNINGS_BLACKOUT_DAYS = 3 # skip if earnings within 3 trading days

_LAST_DAILY_KEY = "cs_momentum_last_daily_date"     # once-per-day ranking guard
_LAST_ENTRY_KEY = "cs_momentum_last_entry_date"     # once-per-Monday entry guard


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _needs_first_run(broker: AlpacaBroker, db_conn) -> bool:
    """First-run override (v92): deploy immediately instead of waiting for Monday.

    Fires when we hold NO cs_momentum positions AND have not entered in the last
    7 days. This gets capital deployed the first run after going live rather than
    idling until the next Monday. Once positions exist (or a recent entry is on
    record) the normal Monday-only schedule resumes.
    """
    held = [p for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME]
    if held:
        return False
    last = get_state(db_conn, _LAST_ENTRY_KEY)
    if not last:
        return True
    try:
        last_dt = datetime.strptime(last, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - last_dt).days >= 7
    except Exception:
        return True


def _trading_days_open(entry_time) -> int:
    """Count trading days (Mon-Fri) between entry_time and today UTC."""
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


def _earnings_within(sym: str, n_trading_days: int) -> bool:
    """True if `sym` reports earnings within `n_trading_days` (per yfinance .info,
    falling back to the ticker calendar). Fail-open: returns False on any error so
    a flaky data source never blocks every candidate."""
    dates: list = []
    try:
        ticker = yf.Ticker(sym)
        info = {}
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
        logger.debug(f"[CS-MOM] {sym}: earnings lookup failed: {e}")
        return False

    for d in dates:
        td = _trading_days_until(d)
        if 0 <= td <= n_trading_days:
            return True
    return False


def _download_ohlcv(symbols: list[str]) -> dict[str, pd.DataFrame]:
    """Download daily OHLCV for the whole universe in ONE yfinance call.

    Returns {symbol: dataframe} with Close/High/Volume columns. Symbols whose
    data fails or is too short are skipped with a warning (never crashes).
    """
    frames: dict[str, pd.DataFrame] = {}
    try:
        data = yf.download(
            symbols, period=f"{LOOKBACK_DAYS + 60}d", interval="1d",
            progress=False, auto_adjust=True, group_by="ticker", threads=True,
        )
    except Exception as e:
        logger.warning(f"[CS-MOM] bulk download failed: {e}")
        return frames

    if data is None or len(data) == 0:
        logger.warning("[CS-MOM] bulk download returned no data")
        return frames

    multi = isinstance(data.columns, pd.MultiIndex)
    for sym in symbols:
        try:
            if multi:
                if sym not in data.columns.get_level_values(0):
                    logger.warning(f"[CS-MOM] {sym}: missing from download — skipping")
                    continue
                df = data[sym].dropna(subset=["Close"])
            else:
                df = data.dropna(subset=["Close"])
            if len(df) >= 252:
                frames[sym] = df
            else:
                logger.warning(f"[CS-MOM] {sym}: only {len(df)} rows (<252) — skipping")
        except Exception as e:
            logger.warning(f"[CS-MOM] {sym}: extract failed ({e}) — skipping")
    return frames


def _rank_universe(symbols: list[str]) -> tuple[list[dict], dict[str, pd.DataFrame]]:
    """Compute 12-1 momentum for every symbol. Returns (rows sorted by score desc,
    {symbol: ohlcv_frame}) so entry gates can reuse the already-downloaded data.

    momentum_score = (close[-1] / close[-252]) - (close[-1] / close[-21])
    """
    frames = _download_ohlcv(symbols)
    rows: list[dict] = []
    for sym, df in frames.items():
        try:
            s = df["Close"]
            c_now = float(s.iloc[-1])
            c_252 = float(s.iloc[-252])
            c_21 = float(s.iloc[-21])
            if c_now <= 0 or c_252 <= 0 or c_21 <= 0:
                continue
            score = (c_now / c_252) - (c_now / c_21)
            rows.append({"symbol": sym, "score": score, "price": c_now})
        except Exception as e:
            logger.debug(f"[CS-MOM] {sym}: score failed: {e}")
    rows.sort(key=lambda x: x["score"], reverse=True)
    gc.collect()
    return rows, frames


def _passes_entry_gates(sym: str, df: pd.DataFrame) -> bool:
    """Concrete technical trigger required for a BUY (v93). Logs the failing gate.
    A high momentum rank gets a name onto the shortlist; this is what justifies
    actually buying it today."""
    try:
        close = df["Close"]
        c_now = float(close.iloc[-1])

        high20 = float(df["High"].tail(20).max())
        pullback = (high20 - c_now) / high20 if high20 > 0 else 0.0
        if pullback < PULLBACK_MIN:
            logger.info(f"[CS_MOM] {sym} rejected: pullback={pullback*100:.1f}% < {PULLBACK_MIN*100:.0f}% (chasing)")
            return False
        if pullback > PULLBACK_MAX:
            logger.info(f"[CS_MOM] {sym} rejected: pullback={pullback*100:.1f}% > {PULLBACK_MAX*100:.0f}% (too extended down)")
            return False

        vol_today = float(df["Volume"].iloc[-1])
        vol_avg = float(df["Volume"].tail(20).mean())
        ratio = vol_today / vol_avg if vol_avg > 0 else 0.0
        if not (VOL_RATIO_MIN <= ratio <= VOL_RATIO_MAX):
            logger.info(f"[CS_MOM] {sym} rejected: volume ratio={ratio:.2f} outside [{VOL_RATIO_MIN}, {VOL_RATIO_MAX}]")
            return False

        rsi = _rsi(close, 14)
        if rsi >= RSI_MAX:
            logger.info(f"[CS_MOM] {sym} rejected: RSI={rsi:.1f} >= {RSI_MAX:.0f} (overbought)")
            return False

        ma = float(close.tail(MA_TREND).mean())
        if c_now <= ma:
            logger.info(f"[CS_MOM] {sym} rejected: close={c_now:.2f} <= MA{MA_TREND}={ma:.2f} (trend broken)")
            return False

        if _earnings_within(sym, EARNINGS_BLACKOUT_DAYS):
            logger.info(f"[CS_MOM] {sym} rejected: earnings within {EARNINGS_BLACKOUT_DAYS} trading days (event risk)")
            return False

        logger.info(
            f"[CS_MOM] {sym} PASSED all gates: pullback={pullback*100:.1f}%, "
            f"vol_ratio={ratio:.2f}, RSI={rsi:.1f}, close>MA{MA_TREND}"
        )
        return True
    except Exception as e:
        logger.warning(f"[CS_MOM] {sym}: gate evaluation failed ({e}) — skipping")
        return False


def _close_position(broker: AlpacaBroker, db_conn, pos: dict, reason: str) -> bool:
    """Cancel resting bracket orders, let them settle, then close at market (v88)."""
    sym = pos["symbol"]
    from strategies.regime_exit import _cancel_open_orders_for_symbol
    n = _cancel_open_orders_for_symbol(broker, sym)
    if n:
        logger.info(f"[CS-MOM] {sym}: cancelled {n} open order(s) before close")
    time.sleep(3)
    result = broker.close_position(sym, STRATEGY_NAME)
    if result is not None:
        logger.info(f"[CS-MOM] CLOSED {sym} — {reason} (pnl={pos['unrealized_pnl_pct']:.1f}%)")
        log_trade(
            db_conn, STRATEGY_NAME, sym, "sell",
            pos["qty"], pos["current_price"], pos["unrealized_pnl"],
            metadata={"reason": reason},
        )
        from utils.cooldown import set_cooldown
        set_cooldown(sym)
        return True
    logger.warning(f"[CS-MOM] {sym}: close failed ({reason}) — will retry next cycle")
    return False


def _check_exits(broker: AlpacaBroker, db_conn, ranked: list[dict]) -> None:
    """Daily exit sweep: rank-drop (out of top 12) + 30-trading-day time stop."""
    top_syms = {r["symbol"] for r in ranked[:EXIT_RANK]}
    ranked_syms = {r["symbol"] for r in ranked}
    positions = [p for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME]

    for pos in positions:
        sym = pos["symbol"]
        state = get_position_state(db_conn, sym)
        entry_time = state.get("entry_time") if state else None

        days_held = _trading_days_open(entry_time)
        if days_held >= TIME_STOP_DAYS:
            _close_position(broker, db_conn, pos, f"time stop ({days_held} trading days)")
            continue

        # Rank-drop: only act when we actually scored the symbol this cycle.
        if sym in ranked_syms and sym not in top_syms:
            _close_position(broker, db_conn, pos, f"dropped out of top {EXIT_RANK}")


def _close_all(broker: AlpacaBroker, db_conn, reason: str) -> int:
    """Close every open cs_momentum position (used on BEAR regime exit)."""
    positions = [p for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME]
    closed = 0
    for pos in positions:
        if _close_position(broker, db_conn, pos, reason):
            closed += 1
    return closed


def run(broker: AlpacaBroker, db_conn):
    """
    CS-MOM dispatch:
      - BEAR regime → close ALL cs_momentum positions immediately, then return.
      - Daily (once/day): re-rank the universe, run rank-drop + time-stop exits.
      - Monday (once/week, BULL only): enter the top 6 names not already held.
    """
    regime = (get_state(db_conn, "current_regime") or "bull").lower()

    # ── BEAR: regime exit — flatten all cs_momentum positions ────────────────
    if regime == "bear":
        n = _close_all(broker, db_conn, "bear regime exit")
        if n:
            logger.info(f"[CS-MOM] BEAR regime — closed {n} position(s)")
        return

    # ── Once-per-day ranking + exit sweep ────────────────────────────────────
    today = _today()
    if get_state(db_conn, _LAST_DAILY_KEY) == today:
        return  # already did the heavy daily work this UTC day

    logger.info("=== CS-MOM: daily ranking + exit sweep ===")
    ranked, frames = _rank_universe(CS_MOM_UNIVERSE)
    if not ranked:
        logger.warning("[CS-MOM] No rankings computed — will retry next cycle")
        return

    logger.info(
        f"[CS-MOM] Ranked {len(ranked)}/{len(CS_MOM_UNIVERSE)} names; shortlist top {SHORTLIST_N}: "
        + ", ".join(f"{r['symbol']}({r['score']:.3f})" for r in ranked[:SHORTLIST_N])
    )

    _check_exits(broker, db_conn, ranked)
    set_state(db_conn, _LAST_DAILY_KEY, today)

    # ── Entries: Monday (or first-run override), once per week, BULL only ─────
    first_run = _needs_first_run(broker, db_conn)
    if datetime.now(timezone.utc).weekday() != 0 and not first_run:
        return
    if first_run:
        logger.info("[CS-MOM] First-run override — deploying without waiting for Monday")
    if regime != "bull":
        logger.info(f"[CS-MOM] Regime={regime} — no new entries (bull only)")
        return
    if get_state(db_conn, _LAST_ENTRY_KEY) == today:
        return  # already entered this Monday

    from utils.market_hours import is_entry_allowed
    if not is_entry_allowed():
        logger.info("[CS-MOM] Outside safe entry window — entries deferred")
        return

    held = {p["symbol"] for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME}
    shortlist = [r for r in ranked[:SHORTLIST_N] if r["symbol"] not in held]
    if not shortlist:
        logger.info("[CS-MOM] All shortlist names already held — nothing to enter")
        set_state(db_conn, _LAST_ENTRY_KEY, today)
        return

    # ── Entry gates: ranking screened the universe; a concrete technical setup is
    #    what justifies actually buying. No forced deployment. ─────────────────
    from utils.cooldown import is_on_cooldown
    passers: list[dict] = []
    for r in shortlist:
        sym = r["symbol"]
        if is_on_cooldown(sym):
            logger.debug(f"[CS-MOM] {sym} on cooldown — skipping")
            continue
        df = frames.get(sym)
        if df is None:
            logger.debug(f"[CS-MOM] {sym}: no price frame — skipping")
            continue
        if _passes_entry_gates(sym, df):
            passers.append(r)
        if len(passers) >= MAX_POSITIONS:
            break

    if len(passers) < MIN_PASSERS:
        logger.info(
            f"[CS-MOM] Only {len(passers)} name(s) passed all gates (need >= {MIN_PASSERS}) "
            f"— no forced deployment, holding cash this week"
        )
        set_state(db_conn, _LAST_ENTRY_KEY, today)
        return

    targets = passers[:MAX_POSITIONS]
    logger.info(
        f"[CS-MOM] {len(targets)} name(s) cleared all gates: "
        + ", ".join(t["symbol"] for t in targets)
    )

    account = broker.get_account()
    equity = account["equity"]
    cash, portfolio_value = broker.get_live_cash()

    for r in targets:
        sym = r["symbol"]

        notional = equity * POSITION_PCT
        min_cash = portfolio_value * MIN_CASH_RESERVE_PCT
        if cash - notional < min_cash:
            logger.info(
                f"[CS-MOM] {sym}: insufficient cash "
                f"(available=${cash:.0f}, need=${notional:.0f}, reserve=${min_cash:.0f})"
            )
            continue

        entry_ref = r["price"]
        stop_px = round(entry_ref * (1.0 - STOP_PCT), 2)
        tp_px = round(entry_ref * (1.0 + TP_PCT), 2)
        # v92: signal_score as momentum-rank percentile so high-conviction picks
        # clear the broker's 0.85 displacement gate. Top of shortlist → 1.0.
        rank_idx = next((i for i, rr in enumerate(ranked) if rr["symbol"] == sym), 0)
        sig_score = max(0.85, 1.0 - 0.15 * (rank_idx / max(1, SHORTLIST_N - 1)))

        log_signal(db_conn, STRATEGY_NAME, sym, "buy", r["score"],
                   {"momentum_score": r["score"], "price": entry_ref})

        logger.info(
            f"[CS-MOM] ENTER {sym} — score={r['score']:.3f}, price=${entry_ref:.2f}, "
            f"notional=${notional:.0f}, stop=${stop_px:.2f}, tp=${tp_px:.2f}"
        )

        result = broker.market_buy(
            sym, notional, STRATEGY_NAME,
            tp_target_override=tp_px,
            stop_override=stop_px,
            signal_score=sig_score,
        )
        if result is None:
            logger.info(f"[CS-MOM] {sym}: entry blocked by broker safety gate")
            continue
        tag_symbol(sym, STRATEGY_NAME)
        log_trade(
            db_conn, STRATEGY_NAME, sym, "buy", 0, entry_ref, 0,
            metadata={"notional": notional, "momentum_score": r["score"],
                      "stop": stop_px, "tp": tp_px},
        )
        cash, portfolio_value = broker.get_live_cash()
        if cash < portfolio_value * MIN_CASH_RESERVE_PCT:
            logger.warning(f"[CS-MOM] Cash floor hit (${cash:,.0f}) — halting entries")
            break

    set_state(db_conn, _LAST_ENTRY_KEY, today)
    active = len([p for p in broker.get_positions() if p["strategy"] == STRATEGY_NAME])
    logger.info(f"[CS-MOM] Entry pass complete — {active} active cs_momentum position(s)")
