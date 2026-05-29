"""
Strategy: Short Interest Squeeze Screener
-----------------------------------------
Academic edge: heavily-shorted stocks that begin rising on rising volume force
short sellers to cover, accelerating the move. Documented +8-15% over 10 days
for stocks with >20% short interest combined with positive price action and
volume expansion (Lamont & Thaler 2003, Cohen, Diether & Malloy 2007).

This is systematic early detection — NOT meme-stock chasing.

Signal logic:
  1. Scan UNIVERSE for short_pct_of_float >= 0.15 (yfinance info field).
  2. 5-day return >= +3% (the squeeze is starting, not stuck).
  3. Current volume >= 1.5x 20-day average (covering pressure visible).
  4. RSI in [45, 72] (in the squeeze zone, not exhausted).
  5. Regime gate: is_bull_market() only.
  6. Score = short_pct * price_5d_ret * vol_ratio.
  7. Score >= 0.015 required to enter.

Sizing (tiered):
  - score >= 0.05  → 5% of portfolio
  - score >= 0.025 → 3% of portfolio
  - score >= 0.015 → 2% of portfolio

Exits (handled in trade_management.py for trailing + hard stop / TP):
  - +12% take profit
  - -5% hard stop
  - 10 trading day timeout
  - ATR trailing stop

Data: yfinance (info["shortPercentOfFloat"] updated bi-weekly by FINRA).
Max positions: 4 concurrent squeeze_screener positions.
Runs once daily, 9:45-15:30 ET window. _ran_today persisted to DB.
"""

import logging, gc
from utils.clock import now_utc as _now_utc
from datetime import datetime, timezone
from typing import Optional
import yfinance as yf

from broker import AlpacaBroker
from config import UNIVERSE, MIN_CASH_RESERVE_PCT
from db import log_signal, get_state, set_state

logger = logging.getLogger("alphabot.squeeze_screener")
STRATEGY_NAME = "squeeze_screener"
MAX_POSITIONS = 4

SHORT_PCT_MIN     = 0.15
PRICE_5D_RET_MIN  = 0.03
VOL_RATIO_MIN     = 1.5
RSI_LOW           = 45.0
RSI_HIGH          = 72.0

SCORE_MIN         = 0.015
SCORE_MID         = 0.025
SCORE_HIGH        = 0.05

ALLOC_LOW   = 0.02
ALLOC_MID   = 0.03
ALLOC_HIGH  = 0.05

COOLDOWN_DAYS = 7
DB_RAN_KEY = "squeeze_screener_ran_date"

_cooldown: dict[str, datetime] = {}
_ran_today: str = ""


def _compute_signal(sym: str) -> Optional[dict]:
    """
    Pull short interest + price/volume from yfinance and compute squeeze score.
    Returns dict if all gates pass, else None.
    """
    try:
        tk = yf.Ticker(sym)

        info = {}
        try:
            info = tk.info or {}
        except Exception:
            info = {}
        short_pct = info.get("shortPercentOfFloat")
        if short_pct is None:
            return None
        try:
            short_pct = float(short_pct)
        except Exception:
            return None
        if short_pct < SHORT_PCT_MIN:
            return None

        hist = tk.history(period="3mo", interval="1d")
        if hist.empty or len(hist) < 25:
            return None

        closes = hist["Close"].dropna()
        volumes = hist["Volume"].dropna()
        if len(closes) < 6 or len(volumes) < 21:
            return None

        last_close = float(closes.iloc[-1])
        prior_close = float(closes.iloc[-6])
        if prior_close <= 0:
            return None
        price_5d_ret = (last_close / prior_close) - 1.0
        if price_5d_ret < PRICE_5D_RET_MIN:
            return None

        avg_vol_20 = float(volumes.iloc[-21:-1].mean())
        if avg_vol_20 <= 0:
            return None
        # Use prior completed day (iloc[-2]) — today's bar is partial during RTH
        # and reads ~0.15x at 10am, falsely blocking real volume signals.
        cur_vol = float(volumes.iloc[-2]) if len(volumes) >= 2 else float(volumes.iloc[-1])
        vol_ratio = cur_vol / avg_vol_20
        if vol_ratio < VOL_RATIO_MIN:
            return None

        delta = closes.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        last_gain = float(gain.iloc[-1]) if gain.iloc[-1] == gain.iloc[-1] else 0.0
        last_loss = float(loss.iloc[-1]) if loss.iloc[-1] == loss.iloc[-1] else 0.0
        rs = last_gain / last_loss if last_loss > 0 else 100.0
        rsi = 100.0 - 100.0 / (1.0 + rs)
        if rsi < RSI_LOW or rsi > RSI_HIGH:
            return None

        score = short_pct * price_5d_ret * vol_ratio
        if score < SCORE_MIN:
            return None

        return {
            "sym": sym,
            "short_pct": round(short_pct, 4),
            "price_5d_ret": round(price_5d_ret, 4),
            "vol_ratio": round(vol_ratio, 2),
            "rsi": round(rsi, 1),
            "score": round(score, 4),
            "price": last_close,
        }
    except Exception as e:
        logger.debug(f"[Squeeze] {sym} error: {e}")
        return None


def _alloc_for_score(score: float) -> float:
    if score >= SCORE_HIGH:
        return ALLOC_HIGH
    if score >= SCORE_MID:
        return ALLOC_MID
    return ALLOC_LOW


def run(broker: AlpacaBroker, db_conn):
    """Main entry — called once per day inside the 9:45-15:30 ET window."""
    global _ran_today

    today = _now_utc().strftime("%Y-%m-%d")
    if _ran_today == today:
        return

    if db_conn is not None:
        try:
            if get_state(db_conn, DB_RAN_KEY) == today:
                _ran_today = today
                return
        except Exception:
            pass

    from utils.market_hours import is_entry_allowed
    if not is_entry_allowed():
        return

    try:
        from utils.regime import is_bull_market
        if not is_bull_market():
            logger.info("[Squeeze] Regime gate: not a bull market — skipping")
            _ran_today = today
            return
    except Exception as e:
        logger.debug(f"[Squeeze] Regime check error: {e}")
        return

    positions = broker.get_positions()
    existing = [p for p in positions if p.get("strategy") == STRATEGY_NAME]
    if len(existing) >= MAX_POSITIONS:
        logger.info(f"[Squeeze] Max positions ({MAX_POSITIONS}) reached — skipping scan")
        _ran_today = today
        if db_conn is not None:
            try:
                set_state(db_conn, DB_RAN_KEY, today)
            except Exception:
                pass
        return

    cash, pv = broker.get_live_cash()
    if cash < 0 or (pv > 0 and cash / pv < MIN_CASH_RESERVE_PCT):
        logger.warning("[Squeeze] Cash floor hit — skipping")
        _ran_today = today
        if db_conn is not None:
            try:
                set_state(db_conn, DB_RAN_KEY, today)
            except Exception:
                pass
        return

    logger.info("=== Squeeze Screener: Daily Scan ===")

    now_prune = _now_utc()
    stale = [s for s, dt in _cooldown.items() if (now_prune - dt).days >= COOLDOWN_DAYS]
    for s in stale:
        del _cooldown[s]

    signals = []
    held_symbols = {p["symbol"] for p in positions}
    for sym in UNIVERSE:
        if sym in held_symbols:
            continue
        if sym in _cooldown and (_now_utc() - _cooldown[sym]).days < COOLDOWN_DAYS:
            continue
        sig = _compute_signal(sym)
        if sig:
            signals.append(sig)

    if not signals:
        logger.info("[Squeeze] No qualifying squeeze candidates today")
        _ran_today = today
        if db_conn is not None:
            try:
                set_state(db_conn, DB_RAN_KEY, today)
            except Exception:
                pass
        return

    signals.sort(key=lambda x: x["score"], reverse=True)
    logger.info(f"[Squeeze] {len(signals)} candidate(s): "
                f"{[(s['sym'], s['score']) for s in signals]}")

    slots = MAX_POSITIONS - len(existing)
    entered = 0

    for sig in signals[:slots]:
        sym = sig["sym"]
        alloc_pct = _alloc_for_score(sig["score"])
        notional = pv * alloc_pct

        log_signal(db_conn, STRATEGY_NAME, sym, "short_squeeze", sig["score"], sig)

        try:
            order = broker.market_buy(sym, notional, strategy=STRATEGY_NAME)
            if order:
                _cooldown[sym] = _now_utc()
                entered += 1
                logger.info(
                    f"[Squeeze] Entered {sym} — score={sig['score']:.3f}, "
                    f"short={sig['short_pct']*100:.1f}% float, "
                    f"5d={sig['price_5d_ret']*100:+.1f}%, "
                    f"vol={sig['vol_ratio']:.1f}x, RSI={sig['rsi']:.0f}, "
                    f"${notional:,.0f} ({alloc_pct:.0%})"
                )

                cash, pv = broker.get_live_cash()
                if cash < 0 or (pv > 0 and cash / pv < MIN_CASH_RESERVE_PCT):
                    logger.warning("[Squeeze] Cash floor hit after entry — halting")
                    break
        except Exception as e:
            logger.error(f"[Squeeze] Order failed {sym}: {e}")

    _ran_today = today
    if db_conn is not None:
        try:
            set_state(db_conn, DB_RAN_KEY, today)
        except Exception:
            pass
    logger.info(f"[Squeeze] Scan complete — {entered} new positions")
