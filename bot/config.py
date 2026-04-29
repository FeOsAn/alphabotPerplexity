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
# Universe — Large-cap S&P 500 stocks with high liquidity
# ============================================================
UNIVERSE = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "UNH", "LLY", "PYPL",
    "JPM", "V", "XOM", "AVGO", "PG", "MA", "JNJ", "HD", "MRK", "ABBV",
    "CVX", "COST", "CRM", "BAC", "NFLX", "AMD", "PEP", "KO", "ADBE", "WMT",
    "TMO", "MCD", "ACN", "CSCO", "ABT", "ORCL", "LIN", "DHR", "TXN", "INTC",
    "COP", "UPS", "RTX", "AMGN", "INTU", "PM", "SPGI", "CAT", "BKNG", "GE",
    "HON", "LOW", "AXP", "MS", "GS", "BLK", "SYK", "ELV", "PLD", "MDT",
    "DE", "ADP", "SCHW", "ADI", "LMT", "ISRG", "CI", "TGT", "VRTX", "MO",
    "MMM", "SO", "DUK", "EOG", "SLB", "BSX", "NOW", "PANW", "REGN", "ZTS",
    "APD", "PSA", "ITW", "HUM", "D", "KLAC", "LRCX", "MU", "MRVL", "AMAT",
    "CME", "ICE", "PGR", "TRV", "AFL", "AIG", "AEP", "EXC", "WM", "ECL",
]

# ============================================================
# Risk Management
# ============================================================
MAX_POSITION_PCT = 0.05       # Max 5% of portfolio per position
MAX_TOTAL_EQUITY_POSITIONS = 12  # Max equity positions managed by bot (not counting options)
MAX_TOTAL_POSITIONS = 25      # Hard cap including all existing positions
STOP_LOSS_PCT = 0.07          # 7% stop loss per position
TAKE_PROFIT_PCT = 0.20        # 20% take profit target
MIN_CASH_RESERVE_PCT = 0.15  # Keep at least 15% cash (raised due to existing options exposure)

# ============================================================
# Strategy Parameters
# ============================================================

# Momentum strategy
MOMENTUM_LOOKBACK = 200        # 10-month lookback (trading days) — IEX data limit
MOMENTUM_SKIP = 21             # Skip last month (reversal avoidance)
MOMENTUM_TOP_N = 10            # Hold top-N momentum stocks
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
    "NFLX", "ADBE", "CRM", "ORCL", "AMD", "AVGO", "INTC",
    "LLY", "JNJ", "PFE", "ABBV", "MRK", "UNH", "CVS",
    "XOM", "CVX", "COP", "CAT", "DE", "HON", "GE", "UPS",
    "SPGI", "BLK", "MS", "SCHW", "AXP", "AMGN", "ISRG",
]

# Sector Rotation strategy
SR_TOP_N = 3                  # Hold top N sectors
SR_LOOKBACK_DAYS = 63         # ~3 months of trading days
SR_REBALANCE_DAYS = 21        # Rebalance monthly
SR_MAX_POSITION_PCT = 0.08    # 8% per sector ETF (larger — ETFs are diversified)

# ============================================================
# Scheduling
# ============================================================
MARKET_OPEN_BUFFER_MIN = 15    # Wait 15min after open before trading
MARKET_CLOSE_BUFFER_MIN = 15   # Stop 15min before close
CHECK_INTERVAL_MIN = 5         # Check signals every 5 minutes
