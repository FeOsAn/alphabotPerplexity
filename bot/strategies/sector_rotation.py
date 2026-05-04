"""
Strategy 6: Sector Rotation — Monthly Relative Momentum
---------------------------------------------------------
Rotate into the top 2-3 performing S&P 500 sectors every month based on
3-month relative momentum. Uses sector ETFs as proxies (XLK, XLF, etc.).
Classic Faber (2007) tactical asset allocation — proven to outperform
buy-and-hold over full market cycles by avoiding weak sectors.

Uses yFinance for historical data (Alpaca IEX only has ~15 days, not enough
for 3-month momentum calculation).
Expected alpha: 2-5% annualized with lower drawdowns than SPY.
"""

import gc
import logging
import pandas as pd
import yfinance as yf
from datetime import datetime
from typing import Optional
from broker import AlpacaBroker, tag_symbol
from config import (
    SR_TOP_N, SR_LOOKBACK_DAYS, SR_REBALANCE_DAYS,
    SR_MAX_POSITION_PCT, MAX_TOTAL_EQUITY_POSITIONS,
    STOP_LOSS_PCT, MIN_CASH_RESERVE_PCT,
    SIZING_MIN_MULT, SIZING_MID_MULT, SIZING_HIGH_MULT
)


def _conviction_multiplier(momentum: float) -> float:
    """
    Scale sector position size by 3-month momentum strength.
    >15% momentum (like XLE's 17.4%) = 1.25x. Weak <5% = 0.75x.
    Sector ETFs are capped at 1.25x max — they're already 8% of portfolio each.
    """
    if momentum >= 0.15:
        return SIZING_HIGH_MULT  # 1.25x — very strong sector trend
    elif momentum >= 0.08:
        return SIZING_MID_MULT   # 1.0x — solid momentum
    else:
        return SIZING_MIN_MULT   # 0.75x — weak, borderline top-3


from db import log_trade, log_signal

logger = logging.getLogger("alphabot.sector_rotation")
STRATEGY_NAME = "sector_rotation"

# S&P 500 sector ETFs — liquid, low spread, highly representative
SECTOR_ETFS = {
    "XLK":  "Technology",
    "XLF":  "Financials",
    "XLV":  "Health Care",
    "XLY":  "Consumer Discretionary",
    "XLP":  "Consumer Staples",
    "XLE":  "Energy",
    "XLI":  "Industrials",
    "XLB":  "Materials",
    "XLU":  "Utilities",
    "XLRE": "Real Estate",
    "XLC":  "Communication Services",
}

_last_rebalance: Optional[datetime] = None


def _load_last_rebalance(db_conn) -> Optional[datetime]:
    """Read last rebalance timestamp from DB (survives restarts)."""
    try:
        row = db_conn.execute("""
            SELECT created_at FROM trades
            WHERE strategy = 'sector_rotation' AND side = 'buy'
            ORDER BY created_at DESC LIMIT 1
        """).fetchone()
        if row:
            return datetime.fromisoformat(row["created_at"])
    except Exception:
        pass
    return None


def _should_rebalance(db_conn, broker: "AlpacaBroker") -> bool:
    """
    Rebalance only if:
    1. We don't already hold the top sectors (live Alpaca check — survives restarts), AND
    2. Either the in-memory timer has expired OR it's a fresh start with no DB record

    Checking live positions is the primary guard — it prevents double-buying after
    container restarts where the DB is wiped (Railway ephemeral filesystem).
    """
    global _last_rebalance

    # Primary guard: check what we actually hold right now on Alpaca
    current_sr_symbols = {
        p["symbol"] for p in broker.get_positions()
        if p["strategy"] == STRATEGY_NAME
    }
    if current_sr_symbols:
        # Already holding sector rotation positions — only rebalance if timer expired
        if _last_rebalance is not None:
            if (datetime.now() - _last_rebalance).days < SR_REBALANCE_DAYS:
                return False
        else:
            # No in-memory timer but we have live positions — set timer now and skip
            _last_rebalance = datetime.now()
            return False

    # No live positions — check DB for when we last bought
    db_ts = _load_last_rebalance(db_conn)
    if db_ts is not None:
        _last_rebalance = db_ts
        return (datetime.now() - db_ts).days >= SR_REBALANCE_DAYS

    # Truly no positions and no history — rebalance now
    return True


def _get_sector_scores(etfs: list[str]) -> dict[str, float]:
    """
    Return momentum score for each sector ETF relative to SPY.
    Score = ETF 20-day return minus SPY 20-day return.
    Positive = outperforming, Negative = underperforming.
    """
    scores = {}
    try:
        spy = yf.Ticker("SPY")
        spy_hist = spy.history(period="2mo", interval="1d")
        gc.collect()
        if spy_hist.empty or len(spy_hist) < 20:
            return {etf: 0.0 for etf in etfs}
        spy_ret = (spy_hist["Close"].iloc[-1] - spy_hist["Close"].iloc[-20]) / spy_hist["Close"].iloc[-20]

        for etf in etfs:
            try:
                ticker = yf.Ticker(etf)
                hist = ticker.history(period="2mo", interval="1d")
                gc.collect()
                if hist.empty or len(hist) < 20:
                    scores[etf] = 0.0
                    continue
                ret = (hist["Close"].iloc[-1] - hist["Close"].iloc[-20]) / hist["Close"].iloc[-20]
                scores[etf] = ret - spy_ret  # relative momentum
            except Exception:
                scores[etf] = 0.0
    except Exception as e:
        logger.warning(f"[SR] Sector scoring failed: {e}")
        return {etf: 0.0 for etf in etfs}

    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    logger.info(f"[SR] Sector heat map: {[(s, f'{v:+.1%}') for s, v in sorted_scores]}")
    return scores


def _score_sectors() -> pd.Series:
    """
    Compute 3-month momentum for each sector ETF via yFinance.
    Downloads one ETF at a time to stay within Railway 512MB RAM.
    """
    scores = {}

    for etf, sector_name in SECTOR_ETFS.items():
        try:
            ticker = yf.Ticker(etf)
            hist = ticker.history(period="6mo")
            if hist.empty or len(hist) < SR_LOOKBACK_DAYS:
                logger.debug(f"[SR] {etf}: only {len(hist)} rows, need {SR_LOOKBACK_DAYS}")
                continue

            series = hist["Close"].dropna()
            price_now = float(series.iloc[-1])
            price_then = float(series.iloc[-SR_LOOKBACK_DAYS])
            if price_then <= 0:
                continue

            momentum = (price_now - price_then) / price_then
            ma50 = float(series.tail(50).mean())

            if price_now > ma50:
                scores[etf] = momentum
                logger.debug(f"[SR] {etf} ({sector_name}): {momentum:.2%} momentum, above MA50")
            else:
                logger.debug(f"[SR] {etf} ({sector_name}): {momentum:.2%} momentum, BELOW MA50 — excluded")

        except Exception as e:
            logger.warning(f"[SR] Error fetching {etf}: {e}")
        finally:
            gc.collect()

    logger.info(f"[SR] Scored {len(scores)}/{len(SECTOR_ETFS)} sectors")
    return pd.Series(scores).sort_values(ascending=False)


def run(broker: AlpacaBroker, db_conn):
    """Run sector rotation rebalance if due."""
    if not _should_rebalance(db_conn, broker):
        # Still enforce stop losses between rebalances
        _check_stops(broker, db_conn)
        return

    global _last_rebalance
    logger.info("=== Sector Rotation Strategy: Monthly Rebalance ===")

    scores = _score_sectors()
    if scores.empty:
        logger.warning("[SR] No sector scores computed — skipping")
        return

    top_sectors = scores.head(SR_TOP_N).index.tolist()
    logger.info(f"[SR] Top sectors (3m momentum): {[(s, SECTOR_ETFS[s], f'{scores[s]:.1%}') for s in top_sectors]}")
    logger.info(f"[SR] All scores: {scores.to_dict()}")

    # ── Sector heat map filter: only rotate into sectors outperforming SPY (20d) ──
    rel_scores = _get_sector_scores(top_sectors)
    outperforming = [s for s in top_sectors if rel_scores.get(s, 0.0) > 0]
    if not outperforming:
        logger.info("[SR] Heat map: no top sector is outperforming SPY on 20-day basis — skipping rotation")
        _check_stops(broker, db_conn)
        return
    # Preserve top-N rank but restrict to outperformers
    top_sectors = outperforming
    logger.info(f"[SR] Heat map filter — rotating only into outperformers: {top_sectors}")

    # Log all signals
    for etf in scores.index:
        log_signal(db_conn, STRATEGY_NAME, etf, "buy" if etf in top_sectors else "hold",
                   float(scores[etf]), {"sector": SECTOR_ETFS[etf], "momentum": float(scores[etf])})

    account = broker.get_account()
    portfolio_value = account["portfolio_value"]
    cash = account["cash"]
    notional_per_sector = portfolio_value * SR_MAX_POSITION_PCT

    all_positions = broker.get_positions()
    sr_positions = [p for p in all_positions if p["strategy"] == STRATEGY_NAME]
    current_symbols = {p["symbol"] for p in sr_positions}

    # Exit sectors no longer in top N
    for pos in sr_positions:
        if pos["symbol"] not in top_sectors:
            logger.info(f"[SR] EXIT {pos['symbol']} ({SECTOR_ETFS.get(pos['symbol'], '?')}) — rotated out")
            broker.close_position(pos["symbol"], STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, pos["symbol"], "sell",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])
            cash += pos["market_value"]

    # Enter new top sectors
    for etf in top_sectors:
        if etf in current_symbols:
            continue

        total_equity = len([p for p in broker.get_positions() if p.get("asset_class", "equity") == "equity"])
        if total_equity >= MAX_TOTAL_EQUITY_POSITIONS:
            logger.info(f"[SR] Max equity positions reached — skipping {etf}")
            break

        min_cash = portfolio_value * MIN_CASH_RESERVE_PCT
        if cash - notional_per_sector < min_cash:
            logger.info(f"[SR] Insufficient cash for {etf}")
            continue

        mult = _conviction_multiplier(float(scores[etf]))
        notional = min(notional_per_sector * mult, cash * 0.9)
        if notional < 1:
            break

        logger.info(
            f"[SR] ENTER {etf} ({SECTOR_ETFS[etf]}) — "
            f"{scores[etf]:.1%} 3m momentum, conviction={mult:.2f}x, ${notional:.0f}"
        )
        broker.market_buy(etf, notional, STRATEGY_NAME)
        tag_symbol(etf, STRATEGY_NAME)
        log_trade(db_conn, STRATEGY_NAME, etf, "buy", 0, 0, 0, metadata={
            "notional": notional,
            "sector": SECTOR_ETFS[etf],
            "momentum": float(scores[etf]),
        })
        cash -= notional

    _last_rebalance = datetime.now()
    logger.info("[SR] Sector rotation rebalance complete")


def _check_stops(broker: AlpacaBroker, db_conn):
    """Enforce stop losses on sector positions between monthly rebalances."""
    for pos in broker.get_positions():
        if pos["strategy"] != STRATEGY_NAME:
            continue
        if pos["unrealized_pnl_pct"] <= -STOP_LOSS_PCT * 100:
            logger.info(f"[SR] STOP LOSS {pos['symbol']} @ {pos['unrealized_pnl_pct']:.1f}%")
            broker.close_position(pos["symbol"], STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, pos["symbol"], "sell_stop",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])
