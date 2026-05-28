"""
Strategy: Options Flow — Unusual Call Volume
--------------------------------------------
Large call volume relative to open interest = institutional positioning signal.
When smart money expects a move, they buy options before it happens.
Documented alpha: 3-5% over 10 days following unusual call volume
(Pan & Poteshman 2006, Easley, O'Hara & Srinivas 1998).

Signal logic:
  1. For each symbol in UNIVERSE, fetch nearest 2 expiry option chains via yfinance.
  2. Find calls where:
     a. Volume/OpenInterest ratio > 3.0 (unusual accumulation)
     b. Volume > 1000 (not just a couple of contracts)
     c. Strike within 5% of current price (near-the-money = directional bet, not hedge)
     d. OTM calls only (strike > current price * 1.01) — buying OTM calls = bullish directional
  3. Compute flow_score = (volume / open_interest) * sqrt(volume) / 10
     Higher score = more unusual and larger the flow.
  4. Filter: flow_score >= 2.0 to proceed.
  5. Regime + RSI filter (same as insider_buying).
  6. Size: flow_score 2-5 = 3% portfolio, >5 = 5% portfolio.
  7. Hold: 7 trading days (options flow signals are shorter-term than insider buying).
  8. Exit: +8% take profit (options flow tends to be faster), -4% stop, or 7-day timeout.

Data source: yfinance option chains (free).
Max positions: 3 concurrent options_flow positions.
"""

import logging, gc, math
from utils.clock import now_utc as _now_utc
from datetime import datetime, timezone
from typing import Optional
import yfinance as yf

from broker import AlpacaBroker
from config import UNIVERSE, MIN_CASH_RESERVE_PCT
from db import log_trade, log_signal

logger = logging.getLogger("alphabot.options_flow")
STRATEGY_NAME = "options_flow"
MAX_POSITIONS = 3
HOLD_DAYS = 7
COOLDOWN_DAYS = 5
VOL_OI_MIN = 5.0          # volume/OI ratio threshold
MIN_VOLUME = 3000          # minimum contract volume
FLOW_SCORE_LOW = 20.0      # entry threshold (skip below)
FLOW_SCORE_MID = 30.0      # medium conviction → 3% allocation
FLOW_SCORE_HIGH = 50.0     # high-conviction threshold → 5% allocation
MIN_DTE = 5                # skip contracts expiring in < 5 days
MIN_NOTIONAL = 500_000     # minimum $-notional of unusual activity
ALLOC_MID = 0.03
ALLOC_HIGH = 0.05

_cooldown: dict[str, datetime] = {}
_ran_today: str = ""


def _compute_flow_score(sym: str) -> Optional[dict]:
    """
    Fetch option chain for sym, find unusual OTM call activity.
    """
    try:
        tk = yf.Ticker(sym)
        hist = tk.history(period="5d", interval="1d")
        if hist.empty:
            return None
        current_price = float(hist["Close"].iloc[-1])

        expiries = tk.options
        if not expiries:
            return None

        best = None
        today = datetime.now(timezone.utc).date()

        for expiry in expiries[:2]:
            try:
                try:
                    exp_date = datetime.strptime(expiry, "%Y-%m-%d").date()
                except Exception:
                    continue
                dte = (exp_date - today).days
                if dte < MIN_DTE:
                    continue

                chain = tk.option_chain(expiry)
                calls = chain.calls

                otm_mask = (
                    (calls["strike"] > current_price * 1.01) &
                    (calls["strike"] < current_price * 1.05) &
                    (calls["volume"] > MIN_VOLUME) &
                    (calls["openInterest"] > 0)
                )
                candidates = calls[otm_mask].copy()
                if candidates.empty:
                    continue

                candidates["vol_oi"] = candidates["volume"] / candidates["openInterest"]
                unusual = candidates[candidates["vol_oi"] >= VOL_OI_MIN]
                if unusual.empty:
                    continue

                top = unusual.sort_values("vol_oi", ascending=False).iloc[0]
                last_price = float(top.get("lastPrice", 0) or 0)
                notional = last_price * float(top["volume"]) * 100.0
                if notional < MIN_NOTIONAL:
                    continue

                flow_score = (top["vol_oi"] * math.sqrt(float(top["volume"]))) / 10.0

                if best is None or flow_score > best["flow_score"]:
                    best = {
                        "sym": sym,
                        "flow_score": round(flow_score, 2),
                        "strike": float(top["strike"]),
                        "volume": int(top["volume"]),
                        "open_interest": int(top["openInterest"]),
                        "vol_oi": round(float(top["vol_oi"]), 2),
                        "expiry": expiry,
                        "dte": dte,
                        "notional": round(notional, 0),
                        "current_price": current_price,
                        "iv": round(float(top["impliedVolatility"]), 3),
                    }
            except Exception:
                continue

        return best

    except Exception as e:
        logger.debug(f"[OptionsFlow] {sym} error: {e}")
        return None


def _passes_filters(sym: str) -> bool:
    """Regime + not overbought."""
    try:
        from utils.regime import current_regime
        regime = current_regime()
        if regime not in ("BULL_STRONG", "BULL_NORMAL"):
            return False
        tk = yf.Ticker(sym)
        hist = tk.history(period="3mo", interval="1d")
        if hist.empty or len(hist) < 15:
            return False
        closes = hist["Close"].dropna()
        delta = closes.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rs = gain.iloc[-1] / loss.iloc[-1] if loss.iloc[-1] != 0 else 100
        rsi = 100 - 100 / (1 + rs)
        return rsi <= 75
    except Exception:
        return False


def run(broker: AlpacaBroker, db_conn):
    """Main entry — called once per day."""
    global _ran_today

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _ran_today == today:
        return

    from utils.market_hours import is_entry_allowed
    if not is_entry_allowed():
        return

    positions = broker.get_positions()
    existing = [p for p in positions if p.get("strategy") == STRATEGY_NAME]
    if len(existing) >= MAX_POSITIONS:
        _ran_today = today
        return

    cash, pv = broker.get_live_cash()
    if cash < 0 or cash / pv < MIN_CASH_RESERVE_PCT:
        _ran_today = today
        return

    logger.info("=== Options Flow Strategy: Daily Scan ===")

    # Prune stale cooldown entries
    now_prune = _now_utc()
    stale = [s for s, dt in _cooldown.items() if (now_prune - dt).days >= COOLDOWN_DAYS]
    for s in stale:
        del _cooldown[s]

    from utils.cooldown import is_on_cooldown

    signals = []
    for sym in UNIVERSE:
        if sym in _cooldown:
            if (_now_utc() - _cooldown[sym]).days < COOLDOWN_DAYS:
                continue
        if is_on_cooldown(sym):
            logger.debug(f"[OptionsFlow] {sym} on cooldown, skipping")
            continue
        if any(p["symbol"] == sym for p in positions):
            continue
        result = _compute_flow_score(sym)
        if result:
            if result["flow_score"] >= FLOW_SCORE_LOW:
                signals.append(result)
            else:
                logger.debug(
                    f"[OptionsFlow] {sym} score={result['flow_score']:.1f} below "
                    f"FLOW_SCORE_LOW={FLOW_SCORE_LOW}, skipping"
                )

    if not signals:
        logger.info("[OptionsFlow] No unusual call flow detected today")
        _ran_today = today
        return

    signals.sort(key=lambda x: x["flow_score"], reverse=True)
    logger.info(f"[OptionsFlow] {len(signals)} signal(s) found: {[s['sym'] for s in signals]}")

    slots = MAX_POSITIONS - len(existing)
    entered = 0

    for sig in signals[:slots]:
        sym = sig["sym"]
        if not _passes_filters(sym):
            continue

        alloc_pct = ALLOC_HIGH if sig["flow_score"] >= FLOW_SCORE_HIGH else ALLOC_MID
        notional = pv * alloc_pct

        log_signal(db_conn, STRATEGY_NAME, sym, "unusual_calls", sig["flow_score"], sig)

        try:
            order = broker.market_buy(sym, notional, strategy=STRATEGY_NAME)
            if order:
                _cooldown[sym] = _now_utc()
                entered += 1
                logger.info(
                    f"[OptionsFlow] Entered {sym} — score={sig['flow_score']:.1f}, "
                    f"vol/OI={sig['vol_oi']:.1f}x, strike=${sig['strike']:.0f} "
                    f"exp {sig['expiry']}, ${notional:,.0f} ({alloc_pct:.0%})"
                )
                cash, pv = broker.get_live_cash()
                if cash < 0 or cash / pv < MIN_CASH_RESERVE_PCT:
                    logger.warning("[OptionsFlow] Cash floor — halting")
                    break
        except Exception as e:
            logger.error(f"[OptionsFlow] Order failed {sym}: {e}")

    _ran_today = today
    logger.info(f"[OptionsFlow] Scan complete — {entered} new positions")
