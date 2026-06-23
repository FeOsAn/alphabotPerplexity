"""
conviction_long.py — v89

Conviction Long: a slow-moving, second-layer strategy that sits on top of the
daily short-term strategies. It runs a WEEKLY scan (Sunday nights, driven by
weekly_scan.py — NOT the daily main loop) over a ~50-name large-cap universe,
scores every tradeable name 0–100 across four dimensions (momentum, earnings
quality, analyst sentiment, AI research signal), and opens 2–4 high-conviction
multi-week long positions (4–12 week holds) at ~12% equity each with wide GTC
brackets (8% stop / 20% TP).

Position management (`manage_conviction_positions`) runs EVERY day from the
daily loop so existing conviction holds get trailing ratchets, a 60-trading-day
time stop, and a bear-regime exit — even though new entries are only opened on
the weekly cron.
"""
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import yfinance as yf

logger = logging.getLogger("alphabot.conviction_long")

STRATEGY_NAME = "conviction_long"

# Regime compatibility — conviction longs are bull/chop only. No new entries in
# bear, but existing positions are still managed (and exited if they turn down).
REGIME_COMPAT = ["bull", "chop"]

# Position sizing / risk
ALLOCATION_PCT = 0.12      # 12% of EQUITY per position
MAX_POSITIONS = 4          # never more than 4 conviction holds at once
STOP_PCT = 0.08            # 8% hard stop (wider than daily strategies)
TP_PCT = 0.20              # 20% take-profit (conviction hold target)
MIN_SCORE = 55             # quality bar — only trade names scoring >= 55
TIME_STOP_TRADING_DAYS = 60
REGIME_EXIT_GRACE_DAYS = 2  # calendar-day grace before a bear exit fires

_CANCEL_SETTLE_SECONDS = 3
_MANAGE_DEDUP_SECONDS = 120  # avoid double-managing within one 5-min cycle

CONVICTION_UNIVERSE = [
    # Tech
    "AAPL", "MSFT", "NVDA", "META", "GOOGL", "AMZN", "CRM", "ADBE", "AMD", "AVGO",
    # Financials
    "JPM", "GS", "MS", "BLK", "V", "MA",
    # Healthcare
    "UNH", "LLY", "ABBV", "JNJ", "MRK",
    # Industrials
    "CAT", "DE", "HON", "GE",
    # Energy
    "XOM", "CVX", "COP",
    # Consumer
    "COST", "HD", "NKE", "SBUX",
    # Communications
    "NFLX", "DIS",
    # Semi/Hardware
    "TSM", "QCOM", "MU", "INTC",
    # ETFs (for regime confirmation, not traded)
    "SPY", "QQQ", "XLK", "XLF", "XLE",
]
_NON_TRADEABLE = {"SPY", "QQQ", "XLK", "XLF", "XLE"}
TRADEABLE_UNIVERSE = [t for t in CONVICTION_UNIVERSE if t not in _NON_TRADEABLE]


# ── scoring ──────────────────────────────────────────────────────────────────
def _compute_rsi(close, period: int = 14) -> Optional[float]:
    """Wilder RSI on a close-price series. None if not enough data."""
    try:
        if close is None or len(close) < period + 1:
            return None
        delta = close.diff().dropna()
        gain = delta.clip(lower=0.0)
        loss = (-delta).clip(lower=0.0)
        avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean().iloc[-1]
        avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean().iloc[-1]
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return float(100.0 - (100.0 / (1.0 + rs)))
    except Exception:
        return None


def momentum_score(symbol: str) -> tuple[float, str]:
    """Technical momentum (0–25) from ~1y of daily data."""
    try:
        hist = yf.Ticker(symbol).history(period="1y")
        if hist is None or hist.empty or len(hist) < 50:
            return 0.0, "momentum: no data"
        close = hist["Close"].dropna()
        price = float(close.iloc[-1])
        ma50 = float(close.tail(50).mean())
        ma200 = float(close.tail(min(200, len(close))).mean())
        rsi = _compute_rsi(close, 14)
        high_52w = float(hist["High"].max())

        score = 0.0
        parts = []
        if price > ma50:
            score += 8
            parts.append(">MA50")
        if price > ma200:
            score += 8
            parts.append(">MA200")
        if rsi is not None and 50.0 <= rsi <= 70.0:
            score += 5
            parts.append(f"RSI{rsi:.0f}")
        if high_52w > 0 and price >= high_52w * 0.95:
            score += 4
            parts.append("near52wH")
        return score, "momentum:" + ("+".join(parts) if parts else "weak")
    except Exception as e:
        logger.debug(f"[ConvL] momentum_score {symbol}: {e}")
        return 0.0, "momentum: error"


def earnings_score(symbol: str) -> tuple[float, str]:
    """Earnings quality (0–25) from yfinance .info fundamentals."""
    try:
        info = yf.Ticker(symbol).info or {}
        eps_growth = info.get("earningsGrowth")
        rev_growth = info.get("revenueGrowth")
        fwd_pe = info.get("forwardPE")

        score = 0.0
        parts = []
        if eps_growth is not None and float(eps_growth) > 0.10:
            score += 10
            parts.append(f"EPS+{float(eps_growth)*100:.0f}%")
        if rev_growth is not None and float(rev_growth) > 0.08:
            score += 8
            parts.append(f"Rev+{float(rev_growth)*100:.0f}%")
        if fwd_pe is not None and 0 < float(fwd_pe) < 30:
            score += 7
            parts.append(f"fwdPE{float(fwd_pe):.0f}")
        return score, "earnings:" + ("+".join(parts) if parts else "weak")
    except Exception as e:
        logger.debug(f"[ConvL] earnings_score {symbol}: {e}")
        return 0.0, "earnings: error"


def analyst_score(symbol: str) -> tuple[float, str]:
    """Analyst sentiment (0–25) from recommendationMean + coverage breadth."""
    try:
        info = yf.Ticker(symbol).info or {}
        rec = info.get("recommendationMean")
        n_analysts = info.get("numberOfAnalystOpinions") or 0
        if rec is None:
            return 0.0, "analyst: no rating"
        rec = float(rec)
        if rec < 2.0:
            score = 25.0
        elif rec < 2.5:
            score = 18.0
        elif rec < 3.0:
            score = 10.0
        else:
            score = 0.0
        # Thin coverage (<=5 analysts) → cap the credit at +15.
        if int(n_analysts) <= 5:
            score = min(score, 15.0)
        return score, f"analyst:mean{rec:.2f}/{int(n_analysts)}an"
    except Exception as e:
        logger.debug(f"[ConvL] analyst_score {symbol}: {e}")
        return 0.0, "analyst: error"


_NEG_KEYWORDS = (
    "downgrade", "downgraded", "cuts price target", "cut price target",
    "lowers price target", "lowered price target", "price target cut",
    "earnings miss", "misses estimates", "missed estimates", "revenue miss",
    "lowered guidance", "cuts guidance", "guidance cut", "slashes",
)
_UPGRADE_KEYWORDS = (
    "upgrade", "upgraded", "raises price target", "raised price target",
    "price target raise", "raises target", "initiates buy", "raised to buy",
    "boosts price target",
)
_BEAT_KEYWORDS = ("earnings beat", "beat estimates", "tops estimates", "beats estimates")


def _perplexity_query(prompt: str) -> Optional[str]:
    """Single Perplexity (sonar) query via the OpenAI-compatible SDK. None on failure."""
    import os
    api_key = os.environ.get("PERPLEXITY_API_KEY")
    if not api_key:
        return None
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key, base_url="https://api.perplexity.ai")
        response = client.chat.completions.create(
            model="sonar",
            messages=[{"role": "user", "content": prompt}],
        )
        return response.choices[0].message.content or ""
    except Exception as e:
        logger.warning(f"[ConvL] Perplexity query failed: {e}")
        return None


def research_score(symbol: str) -> tuple[float, str]:
    """AI research signal (0–25) via Perplexity. Defaults to 12 (neutral) if unavailable."""
    import os
    if not os.environ.get("PERPLEXITY_API_KEY"):
        logger.warning(f"[ConvL] PERPLEXITY_API_KEY not set — research_score={12} (neutral) for {symbol}")
        return 12.0, "research: neutral(no-key)"

    prompts = [
        f"{symbol} stock analyst upgrade price target 2026",
        f"{symbol} earnings beat revenue growth recent",
    ]
    texts = []
    for p in prompts:
        r = _perplexity_query(p)
        if r:
            texts.append(r)
    if not texts:
        logger.warning(f"[ConvL] Perplexity returned nothing for {symbol} — research_score=12 (neutral)")
        return 12.0, "research: neutral(no-result)"

    text = " ".join(texts).lower()
    has_neg = any(k in text for k in _NEG_KEYWORDS)
    has_upgrade = any(k in text for k in _UPGRADE_KEYWORDS)
    has_beat = any(k in text for k in _BEAT_KEYWORDS)
    has_guidance_pos = ("guidance" in text or "outlook" in text) and (
        "raise" in text or "raised" in text or "strong" in text or "above" in text
    )

    if has_upgrade and not has_neg:
        return 25.0, "research: upgrade/PT-raise"
    if has_beat and has_guidance_pos and not has_neg:
        return 20.0, "research: beat+guidance"
    if has_neg:
        return 0.0, "research: negative"
    return 10.0, "research: mixed"


def score_ticker(symbol: str) -> dict:
    """Score one ticker 0–100 across the four conviction dimensions."""
    m_score, m_txt = momentum_score(symbol)
    e_score, e_txt = earnings_score(symbol)
    a_score, a_txt = analyst_score(symbol)
    r_score, r_txt = research_score(symbol)
    total = round(m_score + e_score + a_score + r_score, 1)
    reasoning = " | ".join([m_txt, e_txt, a_txt, r_txt])
    logger.info(
        f"[ConvL] {symbol}: total={total} "
        f"(mom={m_score} earn={e_score} analyst={a_score} research={r_score})"
    )
    return {
        "symbol": symbol,
        "total_score": total,
        "momentum_score": round(m_score, 1),
        "earnings_score": round(e_score, 1),
        "analyst_score": round(a_score, 1),
        "research_score": round(r_score, 1),
        "reasoning": reasoning,
    }


# ── weekly scan ──────────────────────────────────────────────────────────────
def _log_scan_results(scored: list[dict], selected: set[str]) -> None:
    """Persist every scored row to conviction_scan_log."""
    try:
        from db import get_connection
        conn = get_connection()
        try:
            scan_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            for row in scored:
                conn.execute(
                    """
                    INSERT INTO conviction_scan_log
                        (scan_date, symbol, total_score, momentum_score,
                         earnings_score, analyst_score, research_score,
                         reasoning, was_selected)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        scan_date, row["symbol"], row["total_score"],
                        row["momentum_score"], row["earnings_score"],
                        row["analyst_score"], row["research_score"],
                        row["reasoning"],
                        1 if row["symbol"] in selected else 0,
                    ),
                )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.warning(f"[ConvL] could not log scan results: {e}")


def run_weekly_scan() -> list[dict]:
    """
    Score the full tradeable universe, keep names >= MIN_SCORE, and return the
    top 4 by total_score. Every scored row (selected or not) is logged to
    conviction_scan_log for later review.
    """
    logger.info(f"[ConvL] Weekly scan over {len(TRADEABLE_UNIVERSE)} tickers")
    scored = []
    for sym in TRADEABLE_UNIVERSE:
        try:
            scored.append(score_ticker(sym))
        except Exception as e:
            logger.warning(f"[ConvL] scoring failed for {sym}: {e}")

    qualified = [r for r in scored if r["total_score"] >= MIN_SCORE]
    qualified.sort(key=lambda r: r["total_score"], reverse=True)
    top = qualified[:MAX_POSITIONS]
    selected_syms = {r["symbol"] for r in top}

    _log_scan_results(scored, selected_syms)

    logger.info(
        f"[ConvL] {len(qualified)} names >= {MIN_SCORE}; selected top {len(top)}: "
        f"{[(r['symbol'], r['total_score']) for r in top]}"
    )
    return top


# ── position opening (weekly cron) ───────────────────────────────────────────
def _count_conviction_positions(broker) -> int:
    """How many conviction_long positions are currently open (DB is source of truth)."""
    try:
        from db import get_connection
        conn = get_connection()
        try:
            rows = conn.execute(
                "SELECT symbol FROM positions_state "
                "WHERE strategy=? OR opening_strategy=?",
                (STRATEGY_NAME, STRATEGY_NAME),
            ).fetchall()
            return len(rows)
        finally:
            conn.close()
    except Exception as e:
        logger.debug(f"[ConvL] count via DB failed: {e}")
        # Fall back to live tags
        try:
            return sum(
                1 for p in broker.get_positions()
                if p.get("strategy") == STRATEGY_NAME
            )
        except Exception:
            return 0


def open_conviction_positions(broker, candidates: list[dict], account_info: dict) -> list[str]:
    """
    Open up to (MAX_POSITIONS - existing) conviction positions from candidates.
    Each is sized at 12% of EQUITY with an 8% GTC stop and 20% GTC take-profit.
    Returns the list of symbols entered.
    """
    from broker import tag_symbol
    from db import get_connection, log_trade

    opened: list[str] = []
    equity = float(account_info.get("equity", 0) or 0)
    if equity <= 0:
        logger.error("[ConvL] equity <= 0 — cannot size positions, aborting open")
        return opened

    existing = _count_conviction_positions(broker)
    slots = MAX_POSITIONS - existing
    if slots <= 0:
        logger.info(f"[ConvL] already holding {existing} conviction positions — no slots free")
        return opened

    notional = round(equity * ALLOCATION_PCT, 2)
    logger.info(
        f"[ConvL] equity=${equity:,.0f}, {existing} open, {slots} slot(s), "
        f"notional/position=${notional:,.0f}"
    )

    conn = get_connection()
    try:
        for cand in candidates[:slots]:
            sym = cand["symbol"]
            price = broker._latest_price(sym)
            if not price or price <= 0:
                logger.warning(f"[ConvL] {sym}: no price — skipping entry")
                continue
            stop_px = round(price * (1.0 - STOP_PCT), 2)
            tp_px = round(price * (1.0 + TP_PCT), 2)
            sig_score = min(1.0, max(0.0, float(cand["total_score"]) / 100.0))

            logger.info(
                f"[ConvL] ENTER {sym} score={cand['total_score']} "
                f"~${notional:,.0f} @ ${price:.2f} stop=${stop_px:.2f} tp=${tp_px:.2f}"
            )
            result = broker.market_buy(
                sym, notional, STRATEGY_NAME,
                tp_target_override=tp_px,
                stop_override=stop_px,
                signal_score=sig_score,
            )
            if result is None:
                logger.info(f"[ConvL] {sym}: entry blocked by broker safety gate")
                continue
            tag_symbol(sym, STRATEGY_NAME)
            opened.append(sym)
            try:
                log_trade(
                    conn, STRATEGY_NAME, sym, "buy", 0, price, 0,
                    metadata={
                        "notional": notional,
                        "total_score": cand["total_score"],
                        "stop": stop_px,
                        "tp": tp_px,
                        "reasoning": cand.get("reasoning", ""),
                    },
                )
            except Exception as e:
                logger.debug(f"[ConvL] log_trade skipped for {sym}: {e}")
    finally:
        conn.close()

    logger.info(f"[ConvL] opened {len(opened)} conviction position(s): {opened}")
    return opened


# ── daily management ─────────────────────────────────────────────────────────
def _cancel_open_orders_for_symbol(broker, symbol: str) -> int:
    cancelled = 0
    try:
        from alpaca.trading.requests import GetOrdersRequest
        from alpaca.trading.enums import QueryOrderStatus
        open_orders = broker.trading.get_orders(
            GetOrdersRequest(status=QueryOrderStatus.OPEN, symbols=[symbol])
        )
        for o in (open_orders or []):
            try:
                broker.trading.cancel_order_by_id(str(o.id))
                cancelled += 1
            except Exception as e:
                logger.debug(f"[ConvL] {symbol}: could not cancel {o.id}: {e}")
    except Exception as e:
        logger.warning(f"[ConvL] {symbol}: order cancel sweep failed: {e}")
    return cancelled


def _place_protection(broker, symbol: str, qty: int, stop_px: float,
                      tp_px: Optional[float]) -> None:
    """Place a fresh GTC stop (and optional GTC TP limit) for the full qty."""
    try:
        from alpaca.trading.requests import StopOrderRequest, LimitOrderRequest
        from alpaca.trading.enums import OrderSide, TimeInForce
        abs_qty = int(abs(qty))
        if abs_qty < 1:
            return
        broker.trading.submit_order(StopOrderRequest(
            symbol=symbol, qty=abs_qty, side=OrderSide.SELL,
            stop_price=round(stop_px, 2), time_in_force=TimeInForce.GTC,
        ))
        if tp_px and tp_px > 0:
            broker.trading.submit_order(LimitOrderRequest(
                symbol=symbol, qty=abs_qty, side=OrderSide.SELL,
                limit_price=round(tp_px, 2), time_in_force=TimeInForce.GTC,
            ))
        broker._open_orders_cache_ts = 0.0
        logger.info(f"[ConvL] {symbol}: protection re-set stop=${stop_px:.2f} tp={tp_px}")
    except Exception as e:
        logger.warning(f"[ConvL] {symbol}: could not place protection: {e}")


def _close_position(broker, symbol: str, reason: str) -> bool:
    """Cancel open orders, let them settle, then close at market (v88 pattern)."""
    n = _cancel_open_orders_for_symbol(broker, symbol)
    if n:
        logger.info(f"[ConvL] {symbol}: cancelled {n} open order(s) before close")
    time.sleep(_CANCEL_SETTLE_SECONDS)
    try:
        result = broker.close_position(symbol, strategy=STRATEGY_NAME)
        ok = result is not None
    except Exception as e:
        logger.warning(f"[ConvL] {symbol}: close failed: {e}")
        ok = False
    if ok:
        logger.info(f"[ConvL] CLOSED {symbol} — {reason}")
        try:
            from db import get_connection, del_state
            _c = get_connection()
            try:
                del_state(_c, f"conv_ratchet_{symbol}")
            finally:
                _c.close()
        except Exception:
            pass
    return ok


def _ratchet(broker, symbol: str, qty: float, entry: float, pnl_pct: float,
             tp_target: Optional[float]) -> None:
    """
    Trailing ratchet:
      up >15% → move stop to entry + 8%
      up >10% → move stop to breakeven (entry)
    Each level fires once (tracked in bot_state) to avoid re-placing every cycle.
    """
    from db import get_connection, get_state, set_state
    stage_key = f"conv_ratchet_{symbol}"
    conn = get_connection()
    try:
        stage = get_state(conn, stage_key) or "none"
        new_stage = stage
        new_stop = None
        if pnl_pct > 15.0 and stage != "lock8":
            new_stop = round(entry * 1.08, 2)
            new_stage = "lock8"
        elif pnl_pct > 10.0 and stage in ("none",):
            new_stop = round(entry, 2)
            new_stage = "breakeven"
        if new_stop is not None:
            _cancel_open_orders_for_symbol(broker, symbol)
            time.sleep(_CANCEL_SETTLE_SECONDS)
            _place_protection(broker, symbol, int(abs(qty)), new_stop, tp_target)
            set_state(conn, stage_key, new_stage)
            logger.info(
                f"[ConvL] {symbol}: ratchet → {new_stage} stop=${new_stop:.2f} (pnl={pnl_pct:.1f}%)"
            )
    finally:
        conn.close()


def _days_since(entry_time: str) -> Optional[float]:
    try:
        et = datetime.fromisoformat(str(entry_time))
        if et.tzinfo is None:
            et = et.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - et).total_seconds() / 86400.0
    except Exception:
        return None


_LAST_MANAGE_TS = 0.0


def manage_conviction_positions(broker, positions: list[dict], orders: list[dict],
                                db_conn=None) -> list[str]:
    """
    Daily management for open conviction_long positions:
      • trailing ratchet (breakeven at +10%, lock +8% at +15%)
      • time stop after 60 trading days
      • bear-regime exit when down (2 calendar-day grace period)

    Idempotent within a single 5-min cycle so it can be safely invoked from both
    the explicit daily call and the bull/chop strategy dispatch.
    """
    global _LAST_MANAGE_TS
    now_ts = time.time()
    if now_ts - _LAST_MANAGE_TS < _MANAGE_DEDUP_SECONDS:
        logger.debug("[ConvL] manage skipped — already ran this cycle")
        return []
    _LAST_MANAGE_TS = now_ts

    from db import get_connection, get_position_state, get_state

    acted: list[str] = []
    owns_conn = db_conn is None
    conn = get_connection() if owns_conn else db_conn
    try:
        regime = (get_state(conn, "current_regime") or "bull").lower()
        for pos in positions or []:
            symbol = pos.get("symbol")
            if not symbol:
                continue
            state = get_position_state(conn, symbol)
            if not state:
                continue
            strat = state.get("opening_strategy") or state.get("strategy")
            if strat != STRATEGY_NAME:
                continue

            qty = float(pos.get("qty") or 0.0)
            if qty <= 0:
                continue
            entry = float(state.get("entry_price") or 0.0)
            pnl_pct = float(pos.get("unrealized_pnl_pct") or 0.0)
            tp_target = state.get("tp_target")

            # 1) Bear-regime exit (down + out of grace window) — takes precedence.
            age_days = _days_since(state.get("entry_time"))
            if regime == "bear" and pnl_pct < 0:
                if age_days is not None and age_days < REGIME_EXIT_GRACE_DAYS:
                    logger.info(
                        f"[ConvL] {symbol}: bear+down but {age_days:.1f}d old — grace, skipping"
                    )
                else:
                    if _close_position(broker, symbol, f"bear regime exit (pnl={pnl_pct:.1f}%)"):
                        acted.append(symbol)
                    continue

            # 2) Time stop — close after ~60 trading days.
            if age_days is not None:
                approx_trading_days = age_days * (5.0 / 7.0)
                if approx_trading_days >= TIME_STOP_TRADING_DAYS:
                    if _close_position(
                        broker, symbol,
                        f"time stop ~{approx_trading_days:.0f} trading days",
                    ):
                        acted.append(symbol)
                    continue

            # 3) Trailing ratchet for winners still held.
            if entry > 0 and pnl_pct > 10.0:
                _ratchet(broker, symbol, qty, entry, pnl_pct, tp_target)
    finally:
        if owns_conn:
            conn.close()

    if acted:
        logger.info(f"[ConvL] management acted on: {acted}")
    return acted


def run(broker, db_conn) -> None:
    """
    Dispatch entry point used by the daily bull/chop strategy loop. Conviction
    longs only OPEN on the weekly cron (weekly_scan.py); from the daily loop this
    performs management only.
    """
    try:
        positions = broker.get_positions()
    except Exception as e:
        logger.warning(f"[ConvL] run: could not fetch positions: {e}")
        return
    try:
        orders = broker.get_orders(status="open")
    except Exception:
        orders = []
    manage_conviction_positions(broker, positions, orders, db_conn)
