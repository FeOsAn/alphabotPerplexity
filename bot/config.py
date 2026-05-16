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
MAX_POSITION_PCT = 0.10          # was 8% — raised to 10% so each position is meaningful
MAX_TOTAL_EQUITY_POSITIONS = 12  # was 30 — focus on 12 best ideas, not 30 mediocre ones
MAX_TOTAL_POSITIONS = 15         # hard cap
STOP_LOSS_PCT = 0.05             # 5% stop loss per position
TAKE_PROFIT_PCT = 0.20           # 20% take profit target
MIN_CASH_RESERVE_PCT = 0.05     # was 10% — keep only 5% cash, deploy the rest

# ============================================================
# Strategy Parameters
# ============================================================

# Momentum strategy
MOMENTUM_LOOKBACK = 120        # ~6 months (safe under yFinance ~179 day limit)
MOMENTUM_SKIP = 21             # Skip last month (reversal avoidance)
MOMENTUM_TOP_N = 6             # was 10 — top 6 highest-conviction names
MOMENTUM_REBALANCE_DAYS = 21   # Rebalance every ~month

# Mean Reversion strategy
MR_RSI_PERIOD = 14
MR_RSI_OVERSOLD = 32           # Buy signal threshold
MR_RSI_OVERBOUGHT = 68         # Sell signal threshold
MR_BB_PERIOD = 20
MR_BB_STD = 2.0
MR_MAX_POSITIONS = 5

# Trend Following strategy
TREND_FAST_EMA = 9
TREND_SLOW_EMA = 21
TREND_VIX_MAX = 35             # Pause trend-following above this VIX level
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
SR_TOP_N = 5                  # was 3 — hold top 5 sectors for more exposure
SR_LOOKBACK_DAYS = 63         # ~3 months of trading days
SR_REBALANCE_DAYS = 21        # Rebalance monthly
SR_MAX_POSITION_PCT = 0.08    # 8% per sector ETF (individual names can go higher)
SR_MAX_ETF_SLOTS = 3          # Cap ETF positions at 3 if individual signals are firing
                               # ETFs are backup exposure, not the main bet

# ============================================================
# Position Sizing — Conviction Multipliers
# ============================================================
# Base notional = MAX_POSITION_PCT * portfolio_value
# Multiplied by conviction score. Widened range so the best signals get
# meaningfully more capital than weak ones.
# Hard cap: no single position > MAX_POSITION_PCT * 2.0 of portfolio
SIZING_MIN_MULT  = 0.5   # Weak signal — half size (was 0.75)
SIZING_MID_MULT  = 1.0   # Standard signal — base size
SIZING_HIGH_MULT = 1.5   # Strong signal (was 1.25)
SIZING_MAX_MULT  = 2.0   # Exceptional conviction — double size (was 1.5)
                          # e.g. AMD: 3m=+104%, at 52w high, RSI 80, vol 1.4x = 2.0x

# ============================================================
# Scheduling
# ============================================================
MARKET_OPEN_BUFFER_MIN = 15    # Wait 15min after open before trading
MARKET_CLOSE_BUFFER_MIN = 15   # Stop 15min before close
CHECK_INTERVAL_MIN = 5         # Check signals every 5 minutes
