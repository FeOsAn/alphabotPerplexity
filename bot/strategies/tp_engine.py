"""
tp_engine.py — Strategy-aware take-profit target calculator.
Returns TP1 (partial, ~50% take) and TP2 (full exit) price levels
based on entry price, ATR, strategy type, and signal metadata.

v79: exchange-side limit orders placed at entry.
"""
import logging
from typing import Optional, Tuple
import yfinance as yf
import pandas as pd
import pandas_ta as ta

logger = logging.getLogger("alphabot.tp_engine")

# ATR multiples by strategy — how far we expect each strategy's moves to run.
#
# v100.6: trend/momentum-family tp2 -> 7.0x ATR, matching the tested no-trailing
# winner (backtests/exit_atr_robust.py W3: stop~6%/tp2=7x/NO ratchet beats the
# tight-trailing config in ALL THREE sub-periods on Sharpe AND CAGR at equal
# MaxDD). Pairs with _ATR_RATCHET_TIERS=[] in trade_management (trailing off) —
# see the evidence note there, incl. the live 2026-07 whipsaw week. tp1 (partial
# take) retained for first-half risk control. Quick snap-back strategies
# (mean_reversion, vwap, spy_dip, vix_reversal, 52wh_vol) keep tight targets —
# their edge is the fast reversion, not a runner.
_TP_MULTIPLES = {
    # strategy: (tp1_atr_mult, tp2_atr_mult)
    "momentum":        (1.5, 7.0),   # trending — let the runner run (was 3.0)
    "trend_pullback":  (1.5, 7.0),   # pullback continuation (was 3.0)
    "multi_tf_rsi":    (1.5, 7.0),   # multi-timeframe RSI (was 3.0)
    "breakout":        (2.0, 7.0),   # breakouts can run hard (was 4.0; measured-move still overrides)
    "52wh_vol":        (1.0, 2.0),   # 52wk-high breakout: quick single TP — unchanged
    "trend_following": (2.0, 7.0),   # longest duration trades (was 5.0)
    "mean_reversion":  (1.0, 2.0),   # snap-back, tighter target — unchanged
    "vwap_reclaim":    (1.0, 2.0),   # intraday reclaim, quick — unchanged
    "gap_scanner":     (1.5, 3.0),   # disabled; unchanged
    "earnings_drift":  (1.5, 4.0),   # PEAD drift — let drift run (was 2.5)
    "event_driven":    (1.5, 4.0),   # catalyst drift (was 2.5)
    "ts_momentum":     (1.5, 7.0),   # macro/sector trend (was 3.0)
    "squeeze_screener":(2.0, 4.0),   # disabled; unchanged
    "spy_dip":         (1.0, 2.0),   # quick dip-buy — unchanged
    "vix_reversal":    (1.0, 2.0),   # quick fear bounce — unchanged
    "ai_research":     (1.5, 7.0),   # research-driven momentum — let winners run (was 3.0)
    "insider_buying":  (1.5, 3.0),   # disabled; unchanged
    "options_flow":    (1.5, 3.0),   # disabled; unchanged
    "earnings_prediction": (1.5, 4.0),  # earnings catalyst drift (was 2.5)
    # earnings_nlp deprecated in v90 — dead/orphaned strategy (never dispatched,
    # superseded by earnings_prediction); config removed.
    "conviction_long": (2.0, 7.0),   # multi-week conviction holds — wide runner (was 4.0)
    "cs_momentum":     (1.5, 7.0),   # 12-1 cross-sectional momentum (was 3.0)
    "quality_momentum":(2.0, 7.0),   # quality+momentum combo (was 4.0)
    "dual_momentum":   (3.0, 6.0),   # cross-asset dual momentum — already wide, unchanged
    "sector_rotation": (0.0, 0.0),   # no fixed TP — rotation managed separately
    "pairs_trading":   (0.0, 0.0),   # TP = z-score target, managed separately
    "short_hedge":     (0.0, 0.0),   # hedge, no TP
    "donchian_trend":  (0.0, 0.0),   # trend-following: NO TP — exit is the 20d channel
    "crypto_trend":    (0.0, 0.0),   # self-managed 200DMA exit; no equity bracket flow
    "gold_trend":      (0.0, 0.0),   # 200DMA trend exit is the manager; stop = backstop
}

MIN_ATR_FALLBACK_PCT = 0.02  # if ATR unavailable, use 2% of price as proxy


def get_atr(symbol: str, period: int = 14) -> Optional[float]:
    """Fetch 14-period ATR for a symbol."""
    try:
        df = yf.download(symbol, period="30d", interval="1d", progress=False, auto_adjust=True)
        if df is None or len(df) < period + 1:
            return None
        atr_series = ta.atr(df["High"], df["Low"], df["Close"], length=period)
        if atr_series is None or atr_series.dropna().empty:
            return None
        return float(atr_series.dropna().iloc[-1])
    except Exception as e:
        logger.debug(f"[TP] ATR fetch failed for {symbol}: {e}")
        return None


def get_bb_midline(symbol: str) -> Optional[float]:
    """Fetch Bollinger Band midline (20-period SMA) — used as MR TP."""
    try:
        df = yf.download(symbol, period="30d", interval="1d", progress=False, auto_adjust=True)
        if df is None or len(df) < 20:
            return None
        return float(df["Close"].rolling(20).mean().iloc[-1])
    except Exception as e:
        logger.debug(f"[TP] BB midline fetch failed for {symbol}: {e}")
        return None


def get_breakout_target(symbol: str, entry_price: float) -> Optional[float]:
    """
    Breakout TP: measure base formation height (52w low to breakout level)
    and project it above the entry. Classic measured move.
    """
    try:
        df = yf.download(symbol, period="1y", interval="1d", progress=False, auto_adjust=True)
        if df is None or len(df) < 50:
            return None
        low_52w = float(df["Low"].min())
        # Target = entry + (entry - 52w_low) * 0.5  (half the base height as conservative target)
        base_height = entry_price - low_52w
        return round(entry_price + base_height * 0.5, 2)
    except Exception as e:
        logger.debug(f"[TP] Breakout target failed for {symbol}: {e}")
        return None


def calculate_tp_levels(
    symbol: str,
    entry_price: float,
    strategy: str,
    is_short: bool = False,
    signal_score: float = 0.5,
) -> Tuple[Optional[float], Optional[float]]:
    """
    Returns (tp1_price, tp2_price) for a position.
    tp1 = partial take (50% of position)
    tp2 = full exit target

    For strategies with no fixed TP (pairs, sector_rotation, short_hedge),
    returns (None, None) — those are managed by their own logic.

    signal_score (0-1) scales the TP upward for high-conviction entries:
    A score of 1.0 gets 1.3x the base multiple, score of 0.5 gets 1.0x.
    """
    mult1, mult2 = _TP_MULTIPLES.get(strategy, (1.5, 3.0))

    # No fixed TP for these strategies
    if mult1 == 0.0 and mult2 == 0.0:
        return None, None

    # Scale by signal conviction (0.5 = neutral = 1.0x, 1.0 = best = 1.3x)
    conviction_scale = 1.0 + (signal_score - 0.5) * 0.6
    mult1 = mult1 * conviction_scale
    mult2 = mult2 * conviction_scale

    # Special case: mean reversion TP = BB midline
    if strategy == "mean_reversion":
        bb_mid = get_bb_midline(symbol)
        if bb_mid:
            tp1 = round(bb_mid * 0.97, 2) if not is_short else round(bb_mid * 1.03, 2)  # slightly inside midline
            tp2 = round(bb_mid, 2)
            logger.info(f"[TP] {symbol} MR: tp1=${tp1:.2f} (97% BB mid), tp2=${tp2:.2f} (BB mid)")
            return (tp1, tp2) if not is_short else (tp2, tp1)

    # Special case: breakout uses measured move
    if strategy == "breakout":
        brk_target = get_breakout_target(symbol, entry_price)
        if brk_target:
            tp1 = round(entry_price + (brk_target - entry_price) * 0.5, 2)
            tp2 = brk_target
            logger.info(f"[TP] {symbol} BRK: measured move tp1=${tp1:.2f}, tp2=${tp2:.2f}")
            return (tp1, tp2) if not is_short else (
                round(entry_price - (entry_price - brk_target) * 0.5, 2),
                round(entry_price - (entry_price - brk_target), 2)
            )

    # General case: ATR multiples
    atr = get_atr(symbol)
    if atr is None:
        atr = entry_price * MIN_ATR_FALLBACK_PCT
        logger.debug(f"[TP] {symbol}: ATR unavailable, using 2% fallback ({atr:.2f})")

    if not is_short:
        tp1 = round(entry_price + mult1 * atr, 2)
        tp2 = round(entry_price + mult2 * atr, 2)
    else:
        tp1 = round(entry_price - mult1 * atr, 2)
        tp2 = round(entry_price - mult2 * atr, 2)

    logger.info(
        f"[TP] {symbol} {strategy}: ATR={atr:.2f}, conviction={conviction_scale:.2f}x "
        f"→ tp1=${tp1:.2f} ({mult1:.1f}x ATR), tp2=${tp2:.2f} ({mult2:.1f}x ATR)"
    )
    return tp1, tp2
