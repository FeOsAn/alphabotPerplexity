"""
Cash deployment floor.
If cash > 35% of portfolio at market open, buy the top 2 momentum signals at half-size.
Runs once per day, 13:30–14:00 UTC (9:30–10:00 ET), weekdays only.
"""
import gc
import logging
from datetime import datetime, timezone

import yfinance as yf

logger = logging.getLogger("alphabot.cash_deployer")

CASH_FLOOR_PCT = 0.35        # trigger if cash > 35% of equity
DEPLOY_POSITION_PCT = 0.04   # 4% of portfolio per deployment (half of normal ~8%)
MAX_DEPLOY_SYMBOLS = 2       # deploy into at most 2 symbols
STRATEGY_NAME = "cash_deployer"

_deployed_date = ""

# Symbols to consider — top liquid momentum names (keep list short for RAM)
DEPLOY_UNIVERSE = [
    "NVDA", "MSFT", "META", "GOOGL", "AAPL", "AMZN",
    "AVGO", "AMD", "CRM", "PANW", "CRWD", "PLTR",
    "JPM", "GS", "MS", "BAC",
    "XOM", "CVX",
    "TSLA", "UBER",
]


def _score_symbol(sym: str) -> float:
    """Quick 1-month momentum score. Returns float, 0.0 on error."""
    try:
        ticker = yf.Ticker(sym)
        hist = ticker.history(period="1mo", interval="1d", auto_adjust=True)
        if hist.empty or len(hist) < 2:
            return 0.0
        score = (hist["Close"].iloc[-1] - hist["Close"].iloc[0]) / hist["Close"].iloc[0]
        return float(score)
    except Exception:
        return 0.0
    finally:
        gc.collect()


def run(broker, db_conn=None):
    """Deploy idle cash at market open if cash > CASH_FLOOR_PCT.
    `broker` is the AlpacaBroker wrapper from broker.py.
    """
    global _deployed_date

    try:
        now = datetime.now(timezone.utc)
        today = now.strftime("%Y-%m-%d")
        weekday = now.weekday()  # 0=Mon, 4=Fri

        # Only run 13:30–14:00 UTC, weekdays, once per day
        if weekday >= 5:
            return
        minutes_now = now.hour * 60 + now.minute
        if not (13 * 60 + 30 <= minutes_now <= 14 * 60):
            return
        if _deployed_date == today:
            return

        _deployed_date = today  # mark immediately to prevent double-run

        account = broker.get_account()
        equity = float(account["equity"])
        cash = float(account["cash"])
        if equity <= 0:
            return
        cash_pct = cash / equity

        if cash_pct <= CASH_FLOOR_PCT:
            logger.info(
                f"[CashDeploy] Cash {cash_pct:.1%} <= {CASH_FLOOR_PCT:.0%} floor — no action needed"
            )
            return

        logger.info(
            f"[CashDeploy] Cash {cash_pct:.1%} > {CASH_FLOOR_PCT:.0%} — scanning for top picks"
        )

        # Get existing position symbols to avoid doubling up
        existing = {p["symbol"] for p in broker.get_positions()}

        # Score universe (one at a time, RAM-safe)
        scores = {}
        count = 0
        for sym in DEPLOY_UNIVERSE:
            if sym in existing:
                continue
            score = _score_symbol(sym)
            if score > 0:
                scores[sym] = score
            count += 1
            if count % 5 == 0:
                gc.collect()

        if not scores:
            logger.warning("[CashDeploy] No positive-scoring symbols found")
            return

        # Take top N by score
        top = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:MAX_DEPLOY_SYMBOLS]

        for sym, score in top:
            try:
                trade_value = equity * DEPLOY_POSITION_PCT
                logger.info(
                    f"[CashDeploy] Deploying ${trade_value:.0f} into {sym} (score {score:+.4f})"
                )
                broker.market_buy(sym, trade_value, STRATEGY_NAME)
            except Exception as e:
                logger.error(f"[CashDeploy] Order failed for {sym}: {e}")
            finally:
                gc.collect()

    except Exception as e:
        logger.error(f"[CashDeploy] Run failed: {e}")
