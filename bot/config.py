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
ATR_STOP_MULT = 1.0          # v80: stop = max(STOP_LOSS_PCT, ATR_STOP_MULT × ATR/price)
ATR_STOP_MAX_PCT = 0.12      # v80: cap ATR stop at 12% — never risk more than this
TAKE_PROFIT_PCT = 0.20           # 20% take profit target

# v80: dynamic vol_ratio threshold by VIX regime
# VIX < 20 → 1.0 (calm trending market), VIX 20-30 → 1.5 (normal), VIX > 30 → 2.0 (high vol)
MIN_VOL_RATIO = 1.5              # default / fallback
VOL_RATIO_LOW_VIX  = 1.0        # VIX < 20
VOL_RATIO_MID_VIX  = 1.5        # VIX 20–30
VOL_RATIO_HIGH_VIX = 2.0        # VIX > 30

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
    "default":          0.15,  # all other strategies
}

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
# Scheduling
# ============================================================
MARKET_OPEN_BUFFER_MIN = 15    # Wait 15min after open before trading
MARKET_CLOSE_BUFFER_MIN = 15   # Stop 15min before close
CHECK_INTERVAL_MIN = 5         # Check signals every 5 minutes
