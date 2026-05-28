"""
Self-Calibrating Adaptive Filter System
----------------------------------------
Every hour this module scans live market data and derives thresholds
directly from what the market is actually doing — not from static
preset tables.

The old system had preset tables (BULL_NORMAL → rsi_max=78 etc.) that
were hardcoded and never updated when market conditions changed. So when
the tariff-relief rally hit and 1m momentum > 3m momentum everywhere,
the score filter blocked 100% of entries without anyone noticing.

This system works differently:
  1. CALIBRATE — scan 20 liquid ETFs/indices to measure the real
     distribution of RSI, volume, momentum, and breadth right now.
  2. DERIVE — set each threshold as a percentile of that live
     distribution. E.g. RSI max = 80th percentile RSI across the
     universe. This means thresholds automatically rise in strong
     bull markets and tighten in bear markets.
  3. REGIME — classify regime from macro signals (SPY MAs, VIX,
     breadth). Used to scale aggressiveness, not as a hard gate.
  4. SANITY CHECK — hard floors/ceilings to prevent extreme values
     during data glitches.
  5. CACHE — results valid for 60 minutes, thread-safe.

The key insight: thresholds should be RELATIVE to the current market,
not ABSOLUTE numbers written months ago. If every stock has RSI 75
today, blocking at RSI 78 is fine. If every stock has RSI 55 today,
blocking at RSI 78 means zero entries.
"""

import logging
import gc
from datetime import datetime, timezone, timedelta
from typing import Optional
import yfinance as yf
import pandas_ta as _pta
import numpy as np

logger = logging.getLogger(__name__)

# ── Calibration universe — 20 liquid ETFs/large-caps, fast to fetch ──────────
# Wide enough to get real distribution stats, small enough to run in <30s.
CALIBRATION_UNIVERSE = [
    # Broad market
    "SPY", "QQQ", "IWM", "DIA",
    # Sectors
    "XLK", "XLF", "XLE", "XLV", "XLI", "XLY",
    # Individual bellwethers
    "AAPL", "MSFT", "NVDA", "AMZN", "META",
    "JPM", "V", "UNH", "CAT", "AVGO",
]

# ── Hard safety bounds — thresholds never go outside these ───────────────────
BOUNDS = {
    "momentum_rsi_max":          (60,  90),   # never block below RSI 60, never allow above 90
    "momentum_score_min":        (-0.05, 0.10), # never require >10% 3m return to enter
    "breakout_vol_min":          (0.7,  2.0),
    "breakout_proximity":        (0.85, 0.97),
    "breakout_rsi_max":          (60,  88),
    "mr_rsi_oversold":           (25,  45),
    "tf_adx_min":                (15,  35),
    "max_new_positions_per_cycle": (1,   6),
}

# ── Cache ─────────────────────────────────────────────────────────────────────
_cache: dict = {}
CACHE_TTL_MINUTES = 60


# ─────────────────────────────────────────────────────────────────────────────
# Step 1: Calibrate — measure live market distribution
# ─────────────────────────────────────────────────────────────────────────────

def _calibrate_market() -> dict:
    """
    Fetch 6 months of daily data for the calibration universe.
    Returns a stats dict describing the current distribution of signals.
    Falls back to safe defaults on any failure.
    """
    rsi_values    = []
    vol_ratios    = []
    mom_3m_values = []
    above_ma50_count = 0
    total = 0

    # M12: parallel fetch — was sequential 20× ~300ms = 6s blocking
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _fetch_one(sym):
        try:
            return sym, yf.Ticker(sym).history(period="6mo")
        except Exception as e:
            return sym, None

    hist_map: dict = {}
    try:
        with ThreadPoolExecutor(max_workers=6) as ex:
            futures = {ex.submit(_fetch_one, s): s for s in CALIBRATION_UNIVERSE}
            for fut in as_completed(futures):
                try:
                    sym, hist = fut.result(timeout=20)
                    hist_map[sym] = hist
                except Exception as e:
                    logger.debug(f"[Calibrate] fetch error: {e}")
    except Exception as e:
        logger.warning(f"[Calibrate] Parallel fetch failed, falling back to defaults: {e}")
        return _safe_defaults()

    for sym in CALIBRATION_UNIVERSE:
        try:
            hist = hist_map.get(sym)
            if hist is None or hist.empty or len(hist) < 65:
                continue

            close  = hist["Close"].dropna()
            volume = hist["Volume"].dropna()

            # RSI
            rsi_s = _pta.rsi(close, length=14)
            rsi   = float(rsi_s.iloc[-1]) if not rsi_s.empty else None

            # Volume ratio (prev completed day)
            if len(volume) >= 21:
                vol_avg = float(volume.tail(21).iloc[:-1].mean())
                vol_r   = float(volume.iloc[-2]) / vol_avg if vol_avg > 0 else None
            else:
                vol_r = None

            # 3-month return
            if len(close) >= 64:
                mom_3m = (float(close.iloc[-1]) - float(close.iloc[-64])) / float(close.iloc[-64])
            else:
                mom_3m = None

            # MA50
            if len(close) >= 50:
                ma50 = float(close.tail(50).mean())
                above_ma50_count += int(float(close.iloc[-1]) > ma50)

            if rsi    is not None: rsi_values.append(rsi)
            if vol_r  is not None: vol_ratios.append(vol_r)
            if mom_3m is not None: mom_3m_values.append(mom_3m)
            total += 1

        except Exception as e:
            logger.debug(f"[Calibrate] {sym}: {e}")

    if total < 5:
        logger.warning("[Calibrate] Too few symbols fetched — using safe defaults")
        return _safe_defaults()

    breadth_pct = above_ma50_count / total  # fraction of universe above MA50

    stats = {
        "rsi_p25":    float(np.percentile(rsi_values, 25)),
        "rsi_p50":    float(np.percentile(rsi_values, 50)),
        "rsi_p75":    float(np.percentile(rsi_values, 75)),
        "rsi_p85":    float(np.percentile(rsi_values, 85)),
        "vol_p50":    float(np.percentile(vol_ratios, 50)),
        "vol_p25":    float(np.percentile(vol_ratios, 25)),
        "mom3m_p25":  float(np.percentile(mom_3m_values, 25)),
        "mom3m_p50":  float(np.percentile(mom_3m_values, 50)),
        "mom3m_p75":  float(np.percentile(mom_3m_values, 75)),
        "breadth":    round(breadth_pct, 3),
        "n_symbols":  total,
    }

    logger.info(
        f"[Calibrate] n={total} | "
        f"RSI p50={stats['rsi_p50']:.1f} p85={stats['rsi_p85']:.1f} | "
        f"Vol p50={stats['vol_p50']:.2f} | "
        f"3m-mom p50={stats['mom3m_p50']:+.1%} | "
        f"Breadth={stats['breadth']:.0%}"
    )
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# Step 2: Macro regime from SPY + VIX
# ─────────────────────────────────────────────────────────────────────────────

def _assess_regime() -> tuple[str, dict]:
    """Classify macro regime from SPY MAs + VIX. Returns (regime, details)."""
    try:
        spy  = yf.Ticker("SPY")
        hist = spy.history(period="1y", interval="1d")

        if hist.empty or len(hist) < 50:
            return "BULL_NORMAL", {}

        close    = hist["Close"]
        price    = float(close.iloc[-1])
        ma50     = float(close.tail(50).mean())
        ma200    = float(close.tail(200).mean()) if len(close) >= 200 else price
        mom_20d  = (price - float(close.iloc[-21])) / float(close.iloc[-21]) if len(close) >= 21 else 0.0

        try:
            vix_h    = yf.Ticker("^VIX").history(period="5d")
            vix_proxy = float(vix_h["Close"].iloc[-1]) if not vix_h.empty else 20.0
        except Exception:
            vix_proxy = 20.0

        details = {
            "spy_price":   round(price, 2),
            "ma50":        round(ma50, 2),
            "ma200":       round(ma200, 2),
            "above_ma50":  price > ma50,
            "above_ma200": price > ma200,
            "vix_proxy":   round(vix_proxy, 2),
            "mom_20d_pct": round(mom_20d * 100, 2),
        }

        bearish = sum([
            price < ma50,
            price < ma200,
            vix_proxy > 25,
            mom_20d < 0,
        ])

        if price < ma200 or vix_proxy > 35 or mom_20d < -0.03:
            regime = "BEAR_STRONG"
        elif bearish >= 2:
            regime = "BEAR_MILD"
        elif price > ma50 and price > ma200 and vix_proxy < 18 and mom_20d > 0.02:
            regime = "BULL_STRONG"
        else:
            regime = "BULL_NORMAL"

        logger.info(
            f"[Regime] {regime} | SPY=${price:.2f} MA50=${ma50:.2f} "
            f"VIX~{vix_proxy:.1f} Mom20d={mom_20d:+.1%} bearish={bearish}/4"
        )
        return regime, details

    except Exception as e:
        logger.warning(f"[Regime] Assessment failed ({e}) — defaulting to BULL_NORMAL")
        return "BULL_NORMAL", {}


# ─────────────────────────────────────────────────────────────────────────────
# Step 3: Derive thresholds from live stats + regime
# ─────────────────────────────────────────────────────────────────────────────

def _derive_thresholds(stats: dict, regime: str) -> dict:
    """
    Set each threshold as a function of the live distribution.
    Regime controls how aggressive the scaling is.
    """

    # Regime aggressiveness multiplier
    # BULL_STRONG: loosen everything, enter aggressively
    # BULL_NORMAL: standard
    # BEAR_MILD:   tighten meaningfully
    # BEAR_STRONG: very tight, almost no new entries
    aggr = {
        "BULL_STRONG": 1.0,
        "BULL_NORMAL": 0.85,
        "BEAR_MILD":   0.60,
        "BEAR_STRONG": 0.30,
    }.get(regime, 0.85)

    # ── RSI max: allow entries up to the 80th–85th percentile of current RSIs
    # In a strong bull: rsi_p85 might be 78 → allow up to 82
    # In a bear: rsi_p85 might be 65 → allow up to 65
    rsi_p85 = stats.get("rsi_p85", 75.0)
    momentum_rsi_max = rsi_p85 + (5 * aggr)   # headroom above p85

    # ── Momentum score min: require score above the 25th percentile of 3m returns
    # If market is broadly up 10%, 25th pct might be +3% — only buy stocks that
    # beat that. If market is flat, 25th pct is ~0%, so requirement is near zero.
    mom3m_p25 = stats.get("mom3m_p25", 0.0)
    momentum_score_min = mom3m_p25 * aggr   # scale down in bear markets

    # ── Volume min: require above the 25th percentile of yesterday's volumes
    # In high-activity markets the bar is higher; in slow markets it's lower.
    vol_p25 = stats.get("vol_p25", 0.7)
    breakout_vol_min = vol_p25 * (0.9 + 0.2 * aggr)  # slightly below median in bull

    # ── Breakout proximity: how close to 52w high
    # In strong bull: allow entries further from high (0.88)
    # In bear: only buy stocks almost at their high (0.96)
    breakout_proximity = 0.96 - (0.10 * aggr)

    # ── Breakout RSI max: same logic as momentum RSI
    breakout_rsi_max = rsi_p85 + (3 * aggr)

    # ── Mean reversion RSI oversold: lower in bear (more stocks are oversold)
    rsi_p25 = stats.get("rsi_p25", 35.0)
    mr_rsi_oversold = rsi_p25 + (5 * aggr)  # in bull: ~40 oversold; in bear: ~30

    # ── Trend ADX minimum: lower in bull (more trends worth following)
    tf_adx_min = 35 - (15 * aggr)  # bull_strong=20, bull_normal=22, bear_mild=26

    # ── Max positions per cycle: more in bull, fewer in bear
    max_pos = max(1, round(6 * aggr))

    raw = {
        "momentum_rsi_max":          momentum_rsi_max,
        "momentum_score_min":        momentum_score_min,
        "breakout_vol_min":          breakout_vol_min,
        "breakout_proximity":        breakout_proximity,
        "breakout_rsi_max":          breakout_rsi_max,
        "mr_rsi_oversold":           mr_rsi_oversold,
        "tf_adx_min":                tf_adx_min,
        "max_new_positions_per_cycle": max_pos,
    }

    # Apply hard safety bounds
    clamped = {}
    for key, val in raw.items():
        lo, hi = BOUNDS[key]
        clamped[key] = round(max(lo, min(hi, val)), 3)

    logger.info(
        f"[Thresholds] regime={regime} aggr={aggr:.2f} | "
        f"RSI_max={clamped['momentum_rsi_max']:.1f} "
        f"score_min={clamped['momentum_score_min']:+.3f} "
        f"vol_min={clamped['breakout_vol_min']:.2f} "
        f"proximity={clamped['breakout_proximity']:.3f} "
        f"adx_min={clamped['tf_adx_min']:.1f} "
        f"max_pos={clamped['max_new_positions_per_cycle']}"
    )

    return clamped


# ─────────────────────────────────────────────────────────────────────────────
# Safe defaults — used only if calibration fails entirely
# ─────────────────────────────────────────────────────────────────────────────

def _safe_defaults() -> dict:
    return {
        "rsi_p85": 75.0, "rsi_p25": 35.0, "vol_p25": 0.8,
        "mom3m_p25": 0.0, "breadth": 0.6, "n_symbols": 0,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def get_thresholds() -> dict:
    """
    Return self-calibrated thresholds for the current market.
    Refreshes every 60 minutes. Safe to call every cycle — cached.
    On any failure, returns conservative-but-workable defaults.
    """
    global _cache
    now = datetime.now(timezone.utc)

    # Return cache if fresh
    last_update = _cache.get("updated_at")
    if last_update and (now - last_update).total_seconds() < CACHE_TTL_MINUTES * 60:
        return _cache["thresholds"]

    logger.info("[AdaptiveFilters] Recalibrating thresholds from live market data...")

    try:
        stats          = _calibrate_market()
        regime, details = _assess_regime()
        thresholds      = _derive_thresholds(stats, regime)

        thresholds["regime"]  = regime
        thresholds["details"] = details
        thresholds["stats"]   = stats  # expose raw stats for logging/debugging

        _cache = {
            "regime":     regime,
            "thresholds": thresholds,
            "updated_at": now,
            "stats":      stats,
        }
        return thresholds

    except Exception as e:
        logger.warning(f"[AdaptiveFilters] Calibration failed ({e}) — using safe fallback")
        fallback = _derive_thresholds(_safe_defaults(), "BULL_NORMAL")
        fallback["regime"] = "BULL_NORMAL"
        fallback["details"] = {}
        return fallback


def get_regime() -> str:
    """Convenience — returns just the regime name."""
    return get_thresholds().get("regime", "BULL_NORMAL")


def force_recalibrate():
    """Force an immediate recalibration, bypassing the cache. Call on startup."""
    global _cache
    _cache = {}
    logger.info("[AdaptiveFilters] Cache cleared — will recalibrate on next get_thresholds() call")
