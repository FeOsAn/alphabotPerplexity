"""
AlphaBot Configuration
Multi-factor trading bot for Alpaca Markets
"""
import os

# ============================================================
# API KEYS — set these via environment variables or .env file
# ============================================================
ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY", "PKLLH3DOI2OWUQ4HKGS4QCRD7M")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY", "5yxUCTa9VpXhRG6uKLWGAUS2TajyK9mUQPzRtskgemPV")
ALPACA_BASE_URL = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

# ============================================================
# Universe — Top 50 large-cap S&P 500 stocks (memory optimized)
# ============================================================
UNIVERSE = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "UNH", "LLY", "JPM",
    "V", "XOM", "AVGO", "PG", "MA", "JNJ", "HD", "MRK", "ABBV", "CVX",
    "COST", "CRM", "BAC", "NFLX", "AMD", "ADBE", "WMT", "MCD", "ORCL", "TXN",
    "AMGN", "INTU", "SPGI", "CAT", "BKNG", "GS", "MS", "ISRG", "NOW", "PANW",
    "REGN", "LRCX", "KLAC", "AMAT", "MU", "ADI", "SCHW", "AXP", "BLK", "GE",
]

# ============================================================
# Risk Management
# ============================================================
MAX_POSITION_PCT = 0.05
MAX_TOTAL_EQUITY_POSITIONS = 12
MAX_TOTAL_POSITIONS = 25
STOP_LOSS_PCT = 0.07
TAKE_PROFIT_PCT = 0.20
MIN_CASH_RESERVE_PCT = 0.15

# ============================================================
# Strategy Parameters
# ============================================================

# Momentum strategy
MOMENTUM_LOOKBACK = 200
MOMENTUM_SKIP = 21
MOMENTUM_TOP_N = 10
MOMENTUM_REBALANCE_DAYS = 21

# Mean Reversion strategy
MR_RSI_PERIOD = 14
MR_RSI_OVERSOLD = 32
MR_RSI_OVERBOUGHT = 68
MR_BB_PERIOD = 20
MR_BB_STD = 2.0
MR_MAX_POSITIONS = 5

# Trend Following strategy
TREND_FAST_EMA = 9
TREND_SLOW_EMA = 21
TREND_VIX_MAX = 35
TREND_MAX_POSITIONS = 5

# Post-Earnings Announcement Drift (PEAD) strategy
PEAD_MIN_SURPRISE_PCT = 5.0
PEAD_MAX_POSITIONS = 4
PEAD_HOLD_DAYS = 15
PEAD_WATCHLIST = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA",
    "JPM", "V", "MA", "BAC", "GS", "WMT", "HD", "COST",
    "NFLX", "ADBE", "CRM", "ORCL", "AMD", "AVGO", "INTC",
    "LLY", "JNJ", "MRK", "UNH", "XOM", "CVX", "CAT",
    "GE", "SCHW", "AXP", "BLK", "AMGN", "ISRG",
]

# Sector Rotation strategy
SR_TOP_N = 3
SR_LOOKBACK_DAYS = 63
SR_REBALANCE_DAYS = 21
SR_MAX_POSITION_PCT = 0.08

# ============================================================
# Scheduling
# ============================================================
MARKET_OPEN_BUFFER_MIN = 15
MARKET_CLOSE_BUFFER_MIN = 15
CHECK_INTERVAL_MIN = 5
