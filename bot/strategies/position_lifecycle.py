"""
position_lifecycle.py — Stale position re-evaluator and conviction-based displacement engine.

v79:
- 7 trading-day re-evaluation timer on every open position
- Max 2 re-evals (21 trading days hard cap) then close
- Displacement: when high-conviction signal (score >= 0.85) fires, re-score
  all positions and evict the weakest to free capital
- Capital guard: 25% NAV floor, bypass only for score >= 0.85 with displacement
"""
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List
import yfinance as yf
import pandas_ta as ta

logger = logging.getLogger("alphabot.position_lifecycle")

# ── Constants ─────────────────────────────────────────────────────────────────
STALE_TRADING_DAYS       = 7      # re-evaluate after this many trading days
MAX_REEVALS              = 2      # max re-evaluations before force-close (21 days total)
MIN_CASH_PCT             = 0.25   # 25% NAV always reserved
HIGH_CONVICTION_SCORE    = 0.85   # score threshold to trigger displacement check
DISPLACEMENT_LOSS_MAX    = -0.03  # never displace a position down more than -3%
NEVER_DISPLACE_PROFIT    = 0.05   # never displace if up > 5% (it's working)
MIN_OPEN_DAYS_DISPLACE   = 3      # position must be >= 3 trading days old to be displaced
MAX_DISPLACEMENTS_PER_DAY = 1     # prevent cascade evictions

_displacements_today: Dict[str, int] = {}  # date -> count


def _trading_days_open(entry_date) -> int:
    """Count trading days between entry_date and today (excludes weekends)."""
    if entry_date is None:
        return 0
    try:
        if isinstance(entry_date, str):
            entry_dt = datetime.fromisoformat(entry_date.replace("Z", "+00:00"))
        else:
            entry_dt = entry_date
        now = datetime.now(timezone.utc)
        days = 0
        current = entry_dt
        while current.date() < now.date():
            current += timedelta(days=1)
            if current.weekday() < 5:  # Mon-Fri
                days += 1
        return days
    except Exception:
        return 0


def _rescore_thesis(symbol: str, strategy: str, entry_price: float) -> float:
    """
    Re-score the original thesis for a position (0.0 = dead, 1.0 = strong).
    Uses the same indicators the entry strategies use.
    Returns float 0.0-1.0.
    """
    try:
        df = yf.download(symbol, period="60d", interval="1d", progress=False, auto_adjust=True)
        if df is None or len(df) < 21:
            return 0.5  # neutral if no data

        close = df["Close"]
        current_price = float(close.iloc[-1])
        score = 0.0
        factors = 0

        # Factor 1: Price vs MA20 and MA50
        ma20 = float(close.rolling(20).mean().iloc[-1])
        ma50 = float(close.rolling(50).mean().iloc[-1])
        if strategy not in ("mean_reversion", "short_hedge"):
            if current_price > ma20: score += 1; factors += 1
            if current_price > ma50: score += 1; factors += 1
        else:
            if current_price < ma20: score += 1; factors += 1

        # Factor 2: RSI momentum
        rsi_series = ta.rsi(close, length=14)
        if rsi_series is not None and not rsi_series.dropna().empty:
            rsi = float(rsi_series.dropna().iloc[-1])
            if strategy == "mean_reversion":
                score += 1 if rsi < 45 else 0  # still recovering
            elif strategy in ("momentum", "breakout", "trend_following"):
                score += 1 if 50 < rsi < 75 else 0  # strong but not overbought
            else:
                score += 1 if rsi > 45 else 0
            factors += 1

        # Factor 3: Recent momentum (5d return vs entry)
        ret_5d = (current_price - float(close.iloc[-6])) / float(close.iloc[-6]) if len(close) >= 6 else 0
        if strategy in ("momentum", "breakout", "trend_following"):
            score += 1 if ret_5d > 0 else 0
        else:
            score += 0.5  # neutral for other strategies
        factors += 1

        # Factor 4: Position vs entry
        pnl_pct = (current_price - entry_price) / entry_price if entry_price > 0 else 0
        if pnl_pct > 0.02: score += 1; factors += 1
        elif pnl_pct < -0.03: score += 0; factors += 1
        else: score += 0.5; factors += 1

        return round(score / factors, 3) if factors > 0 else 0.5

    except Exception as e:
        logger.debug(f"[Lifecycle] rescore failed for {symbol}: {e}")
        return 0.5


def _displacement_score(pnl_pct: float, thesis_score: float, staleness_days: int) -> float:
    """
    Higher score = better candidate for displacement (evict this one first).
    Priority: profitable + weak thesis first, losing + strong thesis last.
    """
    return (
        (1 - thesis_score) * 0.5 +   # weaker thesis = higher score
        max(pnl_pct, 0) * 0.3 +       # more profit = more displaceable (lock the gain)
        min(staleness_days / 21, 1.0) * 0.2  # older = more displaceable
    )


def sync_exchange_fills(broker, db_conn):
    """
    Check for exchange orders (stops/TPs) that filled since last cycle.
    Cleans up position_state for any symbols no longer in open positions.
    """
    try:
        open_syms = {p.get("symbol") for p in broker.get_positions()}
        # Find symbols in our DB that are no longer open positions
        if open_syms:
            placeholders = ",".join("?" * len(open_syms))
            rows = db_conn.execute(
                f"SELECT symbol FROM position_state WHERE symbol NOT IN ({placeholders})",
                list(open_syms)
            ).fetchall()
        else:
            rows = db_conn.execute(
                "SELECT symbol FROM position_state"
            ).fetchall()
        for (sym,) in rows:
            logger.info(f"[Lifecycle] {sym}: position closed by exchange order — syncing state")
            db_conn.execute("DELETE FROM position_state WHERE symbol=?", (sym,))
            db_conn.commit()
            # Clear in-memory trade management state
            try:
                from strategies.trade_management import clear_symbol
                clear_symbol(sym)
            except Exception:
                pass
    except Exception as e:
        logger.debug(f"[Lifecycle] sync_exchange_fills error: {e}")


def check_stale_positions(broker, db_conn) -> List[str]:
    """
    Called every cycle. For each position open >= STALE_TRADING_DAYS,
    re-score the thesis and decide hold or close.
    Returns list of symbols closed.
    """
    closed = []
    try:
        positions = broker.get_positions()
        for pos in positions:
            symbol = pos.get("symbol")
            strategy = pos.get("strategy", "unknown")
            # broker.get_positions() returns avg_entry and unrealized_pnl_pct
            entry_price = float(pos.get("avg_entry", 0) or 0)
            current_price = float(pos.get("current_price", 0) or 0)
            # unrealized_pnl_pct is already in percentage form (e.g. 2.5 for 2.5%)
            pnl_pct = float(pos.get("unrealized_pnl_pct", 0) or 0) / 100.0
            qty = float(pos.get("qty", 0) or 0)

            # Get entry date and re-eval count from DB
            row = db_conn.execute(
                "SELECT entry_date, reeval_count FROM position_state WHERE symbol=?",
                (symbol,)
            ).fetchone()
            if not row:
                continue

            entry_date, reeval_count = row
            reeval_count = reeval_count or 0
            days_open = _trading_days_open(entry_date)

            # Check if due for re-evaluation
            if days_open < STALE_TRADING_DAYS * (reeval_count + 1):
                continue

            # Hard cap: 21 trading days max regardless
            if reeval_count >= MAX_REEVALS or days_open >= 21:
                logger.info(
                    f"[Lifecycle] {symbol}: hard cap reached ({days_open}d, {reeval_count} re-evals) — closing"
                )
                try:
                    broker.market_sell(symbol, qty=abs(qty), strategy="lifecycle_hard_cap")
                    closed.append(symbol)
                except Exception as _e:
                    logger.warning(f"[Lifecycle] Could not close {symbol}: {_e}")
                continue

            # Re-score the thesis
            thesis = _rescore_thesis(symbol, strategy, entry_price)

            # ATR movement check — is the stock genuinely stale?
            atr_moved = False
            try:
                df = yf.download(symbol, period="20d", interval="1d", progress=False, auto_adjust=True)
                if df is not None and len(df) >= 14:
                    atr_s = ta.atr(df["High"], df["Low"], df["Close"], length=14)
                    if atr_s is not None and not atr_s.dropna().empty:
                        atr = float(atr_s.dropna().iloc[-1])
                        price_move = abs(current_price - entry_price)
                        atr_moved = price_move > 0.5 * atr
            except Exception:
                atr_moved = True  # assume moved if can't check

            # Decision logic
            if thesis >= 0.6 and (pnl_pct > 0.01 or atr_moved):
                # Still valid and either profitable or moving — hold, reset timer
                logger.info(
                    f"[Lifecycle] {symbol}: re-eval {reeval_count+1} → thesis={thesis:.2f}, "
                    f"pnl={pnl_pct:+.1%} — HOLD, resetting timer"
                )
                db_conn.execute(
                    "UPDATE position_state SET reeval_count=? WHERE symbol=?",
                    (reeval_count + 1, symbol)
                )
                db_conn.commit()
            else:
                # Weak thesis or stale and not profitable — close
                logger.info(
                    f"[Lifecycle] {symbol}: re-eval {reeval_count+1} → thesis={thesis:.2f}, "
                    f"pnl={pnl_pct:+.1%}, atr_moved={atr_moved} — CLOSING (thesis expired)"
                )
                try:
                    broker.market_sell(symbol, qty=abs(qty), strategy="lifecycle_thesis_expired")
                    closed.append(symbol)
                except Exception as _e:
                    logger.warning(f"[Lifecycle] Could not close {symbol}: {_e}")

    except Exception as e:
        logger.warning(f"[Lifecycle] check_stale_positions error: {e}")

    return closed


def check_capital_and_displace(broker, db_conn, new_signal: dict) -> bool:
    """
    Called when a new signal wants to enter but cash < MIN_CASH_PCT of NAV.

    new_signal must have: symbol, strategy, score, expected_rr (risk/reward ratio)

    Returns True if capital was freed (displacement happened or cash was already available).
    Returns False if no displacement was possible/warranted.
    """
    try:
        # Check if signal meets high-conviction threshold
        score = float(new_signal.get("score", 0))
        expected_rr = float(new_signal.get("expected_rr", 0))

        if score < HIGH_CONVICTION_SCORE:
            logger.info(
                f"[Lifecycle] {new_signal.get('symbol')}: score={score:.2f} < {HIGH_CONVICTION_SCORE} "
                f"— no displacement, skip signal"
            )
            return False

        if expected_rr < 3.0:
            logger.info(
                f"[Lifecycle] {new_signal.get('symbol')}: R:R={expected_rr:.1f} < 3.0 — no displacement"
            )
            return False

        # Check displacement daily cap
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        daily_count = _displacements_today.get(today_str, 0)
        if daily_count >= MAX_DISPLACEMENTS_PER_DAY:
            logger.info(f"[Lifecycle] Daily displacement cap ({MAX_DISPLACEMENTS_PER_DAY}) reached — skip")
            return False

        # Re-score ALL open positions fresh
        positions = broker.get_positions()
        if not positions:
            return False

        logger.info(
            f"[Lifecycle] High-conviction signal {new_signal.get('symbol')} (score={score:.2f}) "
            f"— re-scoring all {len(positions)} open positions for displacement"
        )

        candidates = []
        for pos in positions:
            sym = pos.get("symbol")
            strategy = pos.get("strategy", "unknown")
            entry_price = float(pos.get("avg_entry", 0) or 0)
            # unrealized_pnl_pct is in percentage form
            pnl_pct = float(pos.get("unrealized_pnl_pct", 0) or 0) / 100.0
            qty = float(pos.get("qty", 0) or 0)

            # Hard exclusions — never displace these
            if pnl_pct > NEVER_DISPLACE_PROFIT:
                logger.debug(f"[Lifecycle] {sym}: excluded (up {pnl_pct:+.1%} > {NEVER_DISPLACE_PROFIT:.0%})")
                continue
            if pnl_pct < DISPLACEMENT_LOSS_MAX:
                logger.debug(f"[Lifecycle] {sym}: excluded (down {pnl_pct:+.1%} < {DISPLACEMENT_LOSS_MAX:.0%}, let SL handle)")
                continue

            # Check TP1 already hit
            row = db_conn.execute(
                "SELECT tp1_hit, entry_date FROM position_state WHERE symbol=?", (sym,)
            ).fetchone()
            if row and row[0]:
                logger.debug(f"[Lifecycle] {sym}: excluded (TP1 already hit)")
                continue

            # Check minimum open days
            days_open = _trading_days_open(row[1] if row else None)
            if days_open < MIN_OPEN_DAYS_DISPLACE:
                logger.debug(f"[Lifecycle] {sym}: excluded (only {days_open}d open, need {MIN_OPEN_DAYS_DISPLACE})")
                continue

            # Re-score thesis
            thesis = _rescore_thesis(sym, strategy, entry_price)
            disp_score = _displacement_score(pnl_pct, thesis, days_open)

            logger.info(
                f"[Lifecycle] {sym}: thesis={thesis:.2f}, pnl={pnl_pct:+.1%}, "
                f"days={days_open} → displacement_score={disp_score:.3f}"
            )
            candidates.append((sym, disp_score, thesis, pnl_pct, qty, pos))

        if not candidates:
            logger.info("[Lifecycle] No displacement candidates found — all positions protected")
            return False

        # Sort by displacement score descending — highest score gets evicted
        candidates.sort(key=lambda x: x[1], reverse=True)
        evict_sym, evict_score, evict_thesis, evict_pnl, evict_qty, evict_pos = candidates[0]

        logger.info(
            f"[Lifecycle] DISPLACING {evict_sym} (score={evict_score:.3f}, "
            f"thesis={evict_thesis:.2f}, pnl={evict_pnl:+.1%}) "
            f"to fund {new_signal.get('symbol')} (conviction={score:.2f})"
        )

        broker.market_sell(evict_sym, qty=abs(evict_qty), strategy=f"displaced_by_{new_signal.get('symbol')}")

        _displacements_today[today_str] = daily_count + 1
        return True

    except Exception as e:
        logger.warning(f"[Lifecycle] check_capital_and_displace error: {e}")
        return False
