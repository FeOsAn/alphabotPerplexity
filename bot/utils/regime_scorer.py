"""
v95 — Multi-Signal Composite Regime Score (Idea 3).

Replaces the fragile binary SPY-vs-MA50 flip with a 0–100 composite score built
from four free yfinance signals:

  1. SPY trend vs MA50      (35 pts) — directional bias
  2. VIX level              (35 pts) — volatility / fear
  3. VIX term structure     (15 pts) — IVTS = VIX / VIX3M (contango = calm)
  4. Credit spread direction(15 pts) — HYG/LQD 5-day change (risk-on/off)

Score → regime:
    score >= 70  → "bull"
    score >= 40  → "transition"   (NEW state — defensive only, reduced size)
    score <  40  → "bear"

Result is cached for 1 hour so the main loop (every 5 min) doesn't re-download.
"""
import logging
import threading
import time

logger = logging.getLogger(__name__)

_CACHE_TTL = 3600  # 1 hour
_cache: dict = {}
_cache_ts: float = 0.0
_lock = threading.Lock()


def _compute() -> dict:
    import yfinance as yf

    data = yf.download(
        ["SPY", "^VIX", "^VIX3M", "HYG", "LQD"],
        period="90d", interval="1d", progress=False, auto_adjust=True,
    )
    close = data["Close"]

    # ── Component 1: SPY vs MA50 (35 pts) ────────────────────────────────────
    spy = close["SPY"].dropna()
    ma50 = spy.rolling(50).mean().iloc[-1]
    spy_now = float(spy.iloc[-1])
    spy_dist = (spy_now - ma50) / ma50
    if spy_dist > 0.02:
        c1 = 35
    elif spy_dist > 0.01:
        c1 = 25
    elif spy_dist > -0.01:
        c1 = 15
    elif spy_dist > -0.02:
        c1 = 8
    else:
        c1 = 0

    # ── Component 2: VIX level (35 pts) ──────────────────────────────────────
    vix = float(close["^VIX"].dropna().iloc[-1])
    if vix < 15:
        c2 = 35
    elif vix < 18:
        c2 = 28
    elif vix < 22:
        c2 = 20
    elif vix < 27:
        c2 = 10
    else:
        c2 = 0

    # ── Component 3: VIX term structure / IVTS (15 pts) ──────────────────────
    try:
        vix3m = float(close["^VIX3M"].dropna().iloc[-1])
        ivts = vix / vix3m
        if ivts < 0.85:
            c3 = 15
        elif ivts < 0.95:
            c3 = 10
        elif ivts < 1.05:
            c3 = 5
        else:
            c3 = 0
    except Exception:
        ivts = None
        c3 = 8  # neutral default if VIX3M unavailable

    # ── Component 4: HYG/LQD credit spread direction (15 pts) ────────────────
    try:
        hyg = close["HYG"].dropna()
        lqd = close["LQD"].dropna()
        ratio_now = hyg.iloc[-1] / lqd.iloc[-1]
        ratio_5d = hyg.iloc[-6] / lqd.iloc[-6]
        credit_change = (ratio_now - ratio_5d) / ratio_5d
        if credit_change > 0.002:
            c4 = 15
        elif credit_change > -0.002:
            c4 = 8
        else:
            c4 = 0
    except Exception:
        credit_change = None
        c4 = 8  # neutral default

    score = c1 + c2 + c3 + c4

    if score >= 70:
        regime = "bull"
    elif score >= 40:
        regime = "transition"
    else:
        regime = "bear"

    result = {
        "score": score,
        "regime": regime,
        "components": {"spy_trend": c1, "vix_level": c2, "ivts": c3, "credit_spread": c4},
        "spy_dist_pct": float(spy_dist),
        "vix": vix,
        "ivts": ivts,
        "credit_change": credit_change,
    }
    logger.info(
        f"[RegimeScore] {score}/100 → {regime.upper()} | "
        f"SPY_dist={spy_dist:+.2%} VIX={vix:.1f} "
        f"comp(trend={c1},vix={c2},ivts={c3},credit={c4})"
    )
    return result


def compute_regime_score(force: bool = False) -> dict:
    """
    Composite 0–100 regime score with component breakdown. Cached for 1 hour.
    Returns a dict (see module docstring). On total failure returns a neutral
    "transition" fallback so callers never crash.
    """
    global _cache, _cache_ts
    now = time.time()
    with _lock:
        if not force and _cache and (now - _cache_ts) < _CACHE_TTL:
            return _cache
    try:
        result = _compute()
    except Exception as e:
        logger.warning(f"[RegimeScore] compute failed: {e} — neutral transition fallback")
        result = {
            "score": 50,
            "regime": "transition",
            "components": {"spy_trend": 15, "vix_level": 20, "ivts": 8, "credit_spread": 8},
            "spy_dist_pct": 0.0,
            "vix": 20.0,
            "ivts": None,
            "credit_change": None,
        }
    with _lock:
        _cache = result
        _cache_ts = now
    return result
