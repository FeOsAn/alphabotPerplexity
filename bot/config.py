"""
AlphaBot Configuration
Multi-factor trading bot for Alpaca Markets
"""
import os

# ============================================================
# API KEYS — set these via environment variables or .env file
# ============================================================
ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY", "")
ALPACA_BASE_URL = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

# ============================================================
# Universe — 50 large-cap S&P 500 stocks (kept small for Railway 512MB RAM)
# ============================================================
UNIVERSE = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "LLY", "PYPL",
    "JPM", "V", "XOM", "AVGO", "PG", "MA", "JNJ", "HD", "MRK", "ABBV",
    "CVX", "COST", "CRM", "BAC", "NFLX", "AMD", "ADBE", "WMT",
    "MCD", "CSCO", "ORCL", "TXN",
    "COP", "RTX", "AMGN", "INTU", "SPGI", "CAT", "BKNG", "GE",
    "HON", "AXP", "MS", "GS", "LMT", "ISRG", "VRTX",
    "NOW", "PANW", "REGN", "KLAC",
]

# ============================================================
# Risk Management
# ============================================================
MAX_TOTAL_POSITIONS = 15         # hard cap on total positions (across all strategies)
STOP_LOSS_PCT = 0.05             # 5% stop loss per position
# v94: minimum dollar value to open ANY position. Fractional residuals below
# this are not worth holding — qty × price < MIN_ENTRY_NOTIONAL is blocked at
# the central entry gate and swept by the residual-cleanup pass.
MIN_ENTRY_NOTIONAL = 500
# v94: cross-strategy symbol lock. Once ANY strategy stops out of / takes profit
# on a symbol, NO strategy may re-enter it for this many hours. Stored in the
# dedicated `symbol_cooldown` DB table so it survives restarts and is global.
SYMBOL_COOLDOWN_HOURS = 48
ATR_STOP_MULT = 1.0          # v80: stop = max(STOP_LOSS_PCT, ATR_STOP_MULT × ATR/price)
ATR_STOP_MAX_PCT = 0.12      # v80: cap ATR stop at 12% — never risk more than this
TAKE_PROFIT_PCT = 0.20           # 20% take profit target

# v80: dynamic vol_ratio threshold by VIX regime
# VIX < 20 → 1.0 (calm trending market), VIX 20-30 → 1.5 (normal), VIX > 30 → 2.0 (high vol)
MIN_VOL_RATIO = 1.5              # default / fallback
VOL_RATIO_LOW_VIX  = 1.0        # VIX < 20
VOL_RATIO_MID_VIX  = 1.5        # VIX 20–30
VOL_RATIO_HIGH_VIX = 2.0        # VIX > 30
VOL_RATIO_SCORE_OVERRIDE = 0.75  # v80.1: floor for high-conviction score override (score > 3.5 + above MA50)
SCORE_OVERRIDE_THRESHOLD = 3.5   # v80.1: momentum score threshold to trigger vol floor override

# --- Conviction-based sizing (no hard slot caps) ---
# Allocation tiers: score determines base % of portfolio
CONVICTION_TIER_MAX   = 0.15   # score >= 0.50 (exceptional, +50% in 3m)  → up to 15%
CONVICTION_TIER_HIGH  = 0.10   # score >= 0.25 (+25% in 3m) → up to 10%
CONVICTION_TIER_MID   = 0.07   # score >= 0.10 (+10% in 3m) → up to 7%
CONVICTION_TIER_LOW   = 0.04   # score >= 0.03 (+3% in 3m)  → up to 4%
CONVICTION_TIER_MIN   = 0.02   # score < 0.03                → up to 2%

# RSI bonus: in the 50-72 sweet spot → +1.5% to allocation
CONVICTION_RSI_BONUS  = 0.015
# Volume bonus: vol_ratio >= 1.2 (institutional buying) → +1.5% to allocation
CONVICTION_VOL_BONUS  = 0.015
# Hard max single position: never exceed 20% of portfolio regardless of conviction
MAX_SINGLE_POSITION_PCT = 0.15  # hard cap per symbol across all buys
# Cash floor: keep at least 15% cash at all times
MIN_CASH_RESERVE_PCT = 0.15
# Non-momentum strategies that lack a score-based conviction use this flat base.
DEFAULT_STRATEGY_ALLOCATION_PCT = 0.05

# Portfolio-wide gross exposure ceiling: (|long MV| + |short MV|) / equity
# Pairs trading + short_hedge can push gross above 1.0 even when net is flat.
MAX_GROSS_EXPOSURE_PCT = 1.5

# Per-strategy capital ceiling — keep one strategy from starving the rest.
STRATEGY_CAPITAL_LIMITS = {
    "momentum":         0.30,
    "ai_research":      0.20,
    "sector_rotation":  0.25,
    "breakout":         0.20,
    "ts_momentum":      0.20,
    "trend_following":  0.15,
    "pairs_trading":    0.15,
    "conviction_long":  0.60,  # 2–4 multi-week holds @ ~12% equity each
    "cs_momentum":      0.65,  # 6 holds @ 10% equity each (~60% deployment)
    "quality_momentum": 0.70,  # 8 holds @ 8% equity each (~64% deployment)
    "dual_momentum":    1.00,  # 3 cross-asset slots @ ~33% each (~99% deployment)
    "default":          0.15,  # all other strategies
}

# Kelly-derived sizing from 4yr backtest (1,875 combinations). Dual momentum reduced from 99% to 24% — biggest improvement.
# Kelly-derived position sizes — optimised via 4yr backtest (Jun 2022-Jun 2026)
# Half-Kelly fractions, capped at 20% per position
STRATEGY_POSITION_SIZES = {
    "cs_momentum":      0.085,   # was 0.12 — Kelly says smaller, higher conviction
    "quality_momentum": 0.060,   # was 0.13
    "dual_momentum":    0.120,   # was 0.33 — BIGGEST CHANGE: was taking 99% of portfolio
    "mean_reversion":   0.025,   # was 0.06
    "breakout":         0.080,   # unchanged
    "trend_pullback":   0.030,   # was 0.08
    "short_hedge":      0.030,   # was 0.06
    "52wh_vol":         0.080,   # unchanged
    "vwap_reclaim":     0.040,   # was 0.06 (estimate)
    "multi_tf_rsi":     0.040,   # was 0.06 (estimate)
    "conviction_long":  0.120,   # unchanged
    "spy_dip":          0.040,   # estimate
    "earnings_drift":   0.040,   # estimate
}

# Hard cap: never deploy > 80% of equity across all long positions. Prevents the
# old problem of dual_momentum alone trying to take 99% of equity.
MAX_PORTFOLIO_EXPOSURE = 0.80

# ============================================================
# Strategy Parameters
# ============================================================

# Momentum strategy
MOMENTUM_LOOKBACK = 200        # ~10 months of trading history required for full signal
MOMENTUM_SKIP = 21             # Skip last month (reversal avoidance)
MOMENTUM_REBALANCE_DAYS = 21   # Rebalance every ~month (academic standard for 3m momentum)

# Mean Reversion strategy
MR_RSI_PERIOD = 14
MR_RSI_OVERSOLD = 25           # v76: tightened — RSI<25 produces 2.495% avg 5d at 70.8% win rate vs 0.758% for RSI 25-32
MR_RSI_OVERBOUGHT = 68         # Sell signal threshold
MR_BB_PERIOD = 20
MR_BB_STD = 2.0
MR_MAX_POSITIONS = 5

# Trend Following strategy
TREND_FAST_EMA = 9
TREND_SLOW_EMA = 21
TREND_VIX_MAX = 25             # VIXY price ~25 ≈ VIX ~20 (moderate stress) — pause trend-following above this
TREND_MAX_POSITIONS = 5

# Post-Earnings Announcement Drift (PEAD) strategy
PEAD_MIN_SURPRISE_PCT = 5.0   # Minimum EPS beat % to trigger entry
PEAD_MAX_POSITIONS = 4        # Max concurrent PEAD positions
PEAD_HOLD_DAYS = 15           # Hold for ~15 trading days then exit
PEAD_WATCHLIST = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA",
    "JPM", "V", "MA", "BAC", "GS", "WMT", "HD", "COST",
    "NFLX", "ADBE", "CRM", "ORCL", "AMD", "AVGO",
    "LLY", "JNJ", "ABBV", "MRK",
    "XOM", "CVX", "COP", "CAT", "HON", "GE",
    "SPGI", "MS", "AXP", "AMGN", "ISRG",
]

# Sector Rotation strategy
SR_TOP_N = 3                  # top 3 sectors — academic standard for 10-ETF universe
SR_LOOKBACK_DAYS = 63         # ~3 months of trading days
SR_REBALANCE_DAYS = 21         # Sector rotation monthly is correct
SR_MAX_POSITION_PCT = 0.08    # 8% per sector ETF (individual names can go higher)
SR_MAX_ETF_SLOTS = 3          # Cap ETF positions at 3 if individual signals are firing
                               # ETFs are backup exposure, not the main bet

# ============================================================
# v75 — No overnight losers (FIX 1)
# ============================================================
OVERNIGHT_LOSS_THRESHOLD = -0.005      # -0.5% — exit if losing at pre-close
OVERNIGHT_EXIT_WINDOW_START = "20:15"  # BST
OVERNIGHT_EXIT_WINDOW_END   = "20:29"  # BST — must finish before 20:30

# ============================================================
# v75 — Trailing stop for active winners (FIX 2)
# ============================================================
TRAIL_ACTIVATE_PCT    = 0.01   # activate trailing once up 1%
TRAIL_DISTANCE_PCT    = 0.015  # trail at 1.5% below peak (long) / above peak (short)
TRAIL_TIGHTEN_PCT     = 0.01   # tighten to 1.0% once up 3% (lock in more)
TRAIL_TIGHTEN_AT_PCT  = 0.03   # tighten threshold

# ============================================================
# v75 — Catalyst sizing (FIX 4)
# ============================================================
CATALYST_SIZING_BOOST   = 1.5   # multiply base allocation by this for catalyst signals
CATALYST_MIN_SCORE      = 0.08  # minimum momentum score to qualify for boost
CATALYST_EARNINGS_DAYS  = 14    # within 14 days of earnings = catalyst window
# MAX_CATALYST_POSITION_PCT alias — boost never exceeds MAX_SINGLE_POSITION_PCT (0.15)
MAX_CATALYST_POSITION_PCT = MAX_SINGLE_POSITION_PCT

# ============================================================
# v95 — Regime Transition Protection (band gate + N-day confirm + composite score)
# ============================================================
# Optimised via 432-combination backtest grid, Jun 2024-Jun 2026. Sharpe 1.330, MaxDD -17.4%
TRANSITION_BAND_PCT = 0.01       # 1.0% — unchanged, confirmed optimal
TRANSITION_VIX_HALF = 22         # was 20 — less restrictive in normal markets; VIX above this → 50% size cut
TRANSITION_VIX_BLOCK = 25        # VIX above this → no new entries
TRANSITION_VIX_EMERGENCY = 30    # VIX above this → compress all open stops to 3%
TRANSITION_EMERGENCY_STOP_PCT = 0.03  # emergency stop distance when VIX > 30

# Momentum/breakout-type strategies — BLOCKED from new entries inside the
# transition band (|SPY - MA50| < 1%). These are the trend-followers that bleed
# when the market drifts across the MA50 boundary.
TRANSITION_BLOCKED_STRATEGIES = {
    "cs_momentum", "quality_momentum", "breakout", "52wh_vol",
    "trend_pullback", "momentum", "sector_rotation", "gap_scanner",
    "earnings_drift", "dual_momentum", "conviction_long",
}

# Defensive / mean-reversion strategies — ALLOWED inside the transition band
# (still subject to the VIX size cut). These are counter-trend and don't depend
# on a clean regime read.
TRANSITION_ALLOWED_STRATEGIES = {
    "mean_reversion", "vwap_reclaim", "short_hedge",
    "vix_reversal", "multi_tf_rsi", "spy_dip",
}

# In the composite-score "transition" regime, defensive strategies run at this
# fraction of normal size; dual_momentum runs full (its own filter handles it).
TRANSITION_DEFENSIVE_SIZE = 0.75

# ============================================================
# Scheduling
# ============================================================
MARKET_OPEN_BUFFER_MIN = 15    # Wait 15min after open before trading
MARKET_CLOSE_BUFFER_MIN = 15   # Stop 15min before close
CHECK_INTERVAL_MIN = 5         # Check signals every 5 minutes
