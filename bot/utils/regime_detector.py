"""
HMM-style Regime Detector — AlphaBot
Runs as a background thread, updates market regime every 30 minutes.
Detects: BULL_STRONG, BULL_NORMAL, CHOPPY, BEAR_MILD, BEAR_STRONG
Uses 5 signals: SPY trend, VIX level, breadth (advance/decline proxy),
momentum dispersion, and credit spread proxy (HYG/LQD ratio).
"""
import gc
import logging
import threading
import time
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Shared regime state — read by strategies
current_regime: str = "BULL_NORMAL"
regime_confidence: float = 0.5
_regime_lock = threading.Lock()
_running = False
_thread = None

UPDATE_INTERVAL = 1800  # 30 minutes

# v95 — short cache for the composite regime context (base, strategy-agnostic).
# compute_regime_score() already caches its downloads for 1h; this guards the
# state-machine read + dict build against per-order recompute storms.
_CTX_TTL = 60
_ctx_cache: dict = {}
_ctx_cache_ts: float = 0.0
_ctx_lock = threading.Lock()


def get_regime() -> tuple:
    """Thread-safe regime read. Returns (regime_str, confidence)."""
    with _regime_lock:
        return current_regime, regime_confidence


def _base_context() -> dict:
    """
    Build the strategy-agnostic regime context from the composite score (Idea 3)
    passed through the N-day confirmation state machine (Idea 2). Cached ~60s.
    """
    global _ctx_cache, _ctx_cache_ts
    now = time.time()
    with _ctx_lock:
        if _ctx_cache and (now - _ctx_cache_ts) < _CTX_TTL:
            return dict(_ctx_cache)

    from config import (
        TRANSITION_BAND_PCT, TRANSITION_VIX_HALF, TRANSITION_VIX_BLOCK,
        TRANSITION_VIX_EMERGENCY,
    )
    from utils.regime_scorer import compute_regime_score

    score = compute_regime_score()
    raw_regime = score["regime"]          # bull | transition | bear
    vix = float(score["vix"])
    spy_dist = float(score["spy_dist_pct"])
    in_band = abs(spy_dist) < TRANSITION_BAND_PCT

    # N-day confirmation (Idea 2) — needs a DB connection.
    confirmed = raw_regime
    confidence = 1.0
    candidate = None
    days = 0
    try:
        from db import get_connection
        from utils.regime_state_machine import RegimeStateMachine
        conn = get_connection()
        try:
            sm = RegimeStateMachine(conn)
            res = sm.update(raw_regime)
            confirmed = res["confirmed_regime"]
            confidence = res["transition_confidence"]
            candidate = res["candidate_regime"]
            days = res["days_confirming"]
        finally:
            conn.close()
    except Exception as e:
        logger.debug(f"[RegimeContext] state machine unavailable: {e}")

    emergency = vix > TRANSITION_VIX_EMERGENCY

    # Base (no strategy) VIX-driven multiplier — Idea 1 VIX overlay.
    if vix > TRANSITION_VIX_EMERGENCY:
        base_mult = 0.0
    elif vix > TRANSITION_VIX_BLOCK:
        base_mult = 0.0
    elif vix > TRANSITION_VIX_HALF:
        base_mult = 0.5
    else:
        base_mult = 1.0

    ctx = {
        "regime": confirmed,
        "raw_regime": raw_regime,
        "in_transition_band": in_band,
        "spy_distance_pct": spy_dist,
        "vix": vix,
        "entry_multiplier": base_mult,
        "emergency_stop_compression": emergency,
        "transition_confidence": confidence,
        "candidate_regime": candidate,
        "days_confirming": days,
        "score": score["score"],
    }
    with _ctx_lock:
        _ctx_cache = dict(ctx)
        _ctx_cache_ts = now
    return ctx


def get_regime_context(strategy: str = None) -> dict:
    """
    v95 — unified regime context for the transition-protection gate (Idea 1).

    Returns a dict with the confirmed regime, transition-band state, VIX, and a
    strategy-aware `entry_multiplier`:

      - 0.0 if the strategy is momentum-type AND SPY is inside the ±1% MA50 band
      - 0.0 if VIX > 25 (or > 30, which also sets emergency_stop_compression)
      - 0.5 if VIX 20–25
      - otherwise 1.0
      - scaled down further by transition_confidence while a flip is unconfirmed

    `strategy=None` returns the base (VIX-only) multiplier without the
    momentum-band block.
    """
    from config import TRANSITION_BLOCKED_STRATEGIES

    ctx = _base_context()
    mult = ctx["entry_multiplier"]

    # Momentum-type strategies are blocked entirely inside the transition band.
    if strategy and ctx["in_transition_band"] and strategy in TRANSITION_BLOCKED_STRATEGIES:
        mult = 0.0

    # During an unconfirmed regime flip, scale entries down by confidence (Idea 2).
    conf = ctx.get("transition_confidence", 1.0)
    if conf < 1.0:
        mult *= conf

    ctx = dict(ctx)
    ctx["entry_multiplier"] = round(mult, 4)
    ctx["strategy"] = strategy
    return ctx


def _compute_regime() -> tuple:
    """
    Compute current market regime from 5 signals.
    Returns (regime_str, confidence_float).
    """
    import yfinance as yf
    from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError

    signals = {}

    def fetch(sym, period, interval):
        try:
            t = yf.Ticker(sym)
            h = t.history(period=period, interval=interval, auto_adjust=True)
            return h
        except Exception:
            pass
            return None

    def _safe_result(f, name):
        try:
            return f.result(timeout=15)
        except FuturesTimeoutError:
            logger.warning(f"[RegimeDetector] {name} fetch timed out — using None")
            return None
        except Exception as e:
            logger.warning(f"[RegimeDetector] {name} fetch error: {e}")
            return None

    # Fetch in parallel — result collection INSIDE the with block so timeout is enforced
    with ThreadPoolExecutor(max_workers=6) as ex:
        f_spy_1y = ex.submit(fetch, "SPY", "1y", "1wk")
        f_spy_1m = ex.submit(fetch, "SPY", "1mo", "1d")
        f_vix    = ex.submit(fetch, "^VIX", "5d", "1d")
        f_hyg    = ex.submit(fetch, "HYG", "1mo", "1d")
        f_lqd    = ex.submit(fetch, "LQD", "1mo", "1d")
        f_qqq    = ex.submit(fetch, "QQQ", "1mo", "1d")

        spy_1y = _safe_result(f_spy_1y, "SPY 1y")
        spy_1m = _safe_result(f_spy_1m, "SPY 1m")
        vix_h  = _safe_result(f_vix,    "VIX")
        hyg_h  = _safe_result(f_hyg,    "HYG")
        lqd_h  = _safe_result(f_lqd,    "LQD")
        qqq_h  = _safe_result(f_qqq,    "QQQ")

    score = 0.0   # positive = bull, negative = bear
    weight = 0.0

    # Signal 1: SPY 12-week trend (iloc[-13] for true 12-week lookback)
    try:
        if spy_1y is not None and len(spy_1y) >= 13:
            ret_12w = (spy_1y["Close"].iloc[-1] - spy_1y["Close"].iloc[-13]) / spy_1y["Close"].iloc[-13]
            if ret_12w > 0.05:
                score += 2.0
            elif ret_12w > 0.0:
                score += 1.0
            elif ret_12w > -0.05:
                score -= 1.0
            else:
                score -= 2.0
            weight += 2.0
    except Exception:
        pass

    # Signal 2: VIX level
    try:
        if vix_h is not None and len(vix_h) >= 1:
            vix = float(vix_h["Close"].iloc[-1])
            signals["vix"] = vix
            if vix < 15:
                score += 2.0
            elif vix < 20:
                score += 1.0
            elif vix < 30:
                score -= 1.0
            else:
                score -= 2.0
            weight += 2.0
    except Exception:
        pass

    # Signal 3: SPY 1-month momentum
    try:
        if spy_1m is not None and len(spy_1m) >= 2:
            ret_1m = (spy_1m["Close"].iloc[-1] - spy_1m["Close"].iloc[0]) / spy_1m["Close"].iloc[0]
            signals["spy_1m"] = ret_1m
            if ret_1m > 0.03:
                score += 1.5
            elif ret_1m > 0:
                score += 0.5
            elif ret_1m > -0.03:
                score -= 0.5
            else:
                score -= 1.5
            weight += 1.5
    except Exception:
        pass

    # Signal 4: QQQ vs SPY relative strength (tech leadership)
    try:
        if qqq_h is not None and spy_1m is not None and len(qqq_h) >= 2 and len(spy_1m) >= 2:
            qqq_ret = (qqq_h["Close"].iloc[-1] - qqq_h["Close"].iloc[0]) / qqq_h["Close"].iloc[0]
            spy_ret = (spy_1m["Close"].iloc[-1] - spy_1m["Close"].iloc[0]) / spy_1m["Close"].iloc[0]
            rs = qqq_ret - spy_ret
            if rs > 0.02:
                score += 1.0   # tech leading = risk-on
            elif rs < -0.02:
                score -= 1.0   # tech lagging = risk-off
            weight += 1.0
    except Exception:
        pass

    # Signal 5: Credit spread proxy (HYG/LQD ratio trend)
    try:
        if hyg_h is not None and lqd_h is not None and len(hyg_h) >= 2 and len(lqd_h) >= 2:
            hyg_ret = (hyg_h["Close"].iloc[-1] - hyg_h["Close"].iloc[0]) / hyg_h["Close"].iloc[0]
            lqd_ret = (lqd_h["Close"].iloc[-1] - lqd_h["Close"].iloc[0]) / lqd_h["Close"].iloc[0]
            spread_chg = hyg_ret - lqd_ret  # positive = spreads tightening = bullish
            if spread_chg > 0.01:
                score += 1.0
            elif spread_chg < -0.01:
                score -= 1.0
            weight += 1.0
    except Exception:
        pass

    # Normalise
    if weight == 0:
        return "BULL_NORMAL", 0.5

    norm = score / weight  # range roughly -1 to +1

    if norm > 0.6:
        regime, conf = "BULL_STRONG", min(0.95, 0.7 + norm * 0.25)
    elif norm > 0.2:
        regime, conf = "BULL_NORMAL", min(0.85, 0.6 + norm * 0.25)
    elif norm > -0.2:
        regime, conf = "CHOPPY", 0.5 + abs(norm) * 0.1
    elif norm > -0.6:
        regime, conf = "BEAR_MILD", min(0.85, 0.6 + abs(norm) * 0.25)
    else:
        regime, conf = "BEAR_STRONG", min(0.95, 0.7 + abs(norm) * 0.25)

    vix_str = f"{signals['vix']:.1f}" if 'vix' in signals else '?'
    spy_str = f"{signals.get('spy_1m', 0):.2%}"
    logger.info(
        f"[RegimeDetector] score={score:.2f}/weight={weight:.1f} → norm={norm:.3f} "
        f"→ {regime} (conf {conf:.2f}) | VIX={vix_str} SPY_1m={spy_str}"
    )
    return regime, conf


def _worker():
    global current_regime, regime_confidence
    while _running:
        try:
            regime, conf = _compute_regime()
            with _regime_lock:
                current_regime = regime
                regime_confidence = conf
        except Exception as e:
            logger.error(f"[RegimeDetector] Error: {e}")
        time.sleep(UPDATE_INTERVAL)


def start():
    """Start background regime detector thread."""
    global _running, _thread
    if _running:
        return
    _running = True
    _thread = threading.Thread(target=_worker, daemon=True, name="regime_detector")
    _thread.start()
    logger.info("[RegimeDetector] Started (updates every 30 min)")


def stop():
    global _running
    _running = False
