"""
Strategy: Donchian Trend (turtle-style breakout)
------------------------------------------------
Classic trend-following. Entry: price breaks above its prior 40-day high. Exit:
price breaks below its prior 20-day low (channel exit) — plus the shared
trade_management protective stop (wide, trend-following needs room) and NO fixed
take-profit (let winners run — that is the whole edge).

Backtest (backtests/donchian_validate.py, 2015-2026): standalone Sharpe 1.35,
Sortino 1.79, CAGR 20.4%, robust across all sub-periods (1.35/1.26/1.63). It is
~0.78 correlated to the equity book (not a diversifier), but a HIGHER-quality
trend implementation than the older breakout/52wh/momentum sleeves — added as an
extra trend source that also helps close the chronic under-deployment. Total risk
stays capped by the vol-targeting exposure overlay regardless.

Stateless exits (survive restarts): the 40d-high / 20d-low channels are recomputed
from data every cycle, so no per-position peak/stop state is needed.
"""
import logging
import yfinance as yf
from broker import AlpacaBroker, tag_symbol
from config import MIN_CASH_RESERVE_PCT
from db import log_trade, log_signal

logger = logging.getLogger("alphabot.donchian_trend")
STRATEGY_NAME = "donchian_trend"

ENTRY_CHANNEL = 40      # break above prior 40-day high to enter
EXIT_CHANNEL  = 20      # break below prior 20-day low to exit
ALLOCATION_PCT = 0.04   # 4% per position — modest new sleeve
MAX_POSITIONS  = 8

# Most-liquid names from the universe (trend-following wants deep liquidity).
UNIVERSE = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO", "JPM",
    "V", "MA", "HD", "COST", "NFLX", "AMD", "CRM", "ADBE", "WMT", "XOM", "LLY",
]


def _get_signals() -> dict:
    """Compute Donchian entry/exit signals per symbol (stateless, from data)."""
    signals = {}
    for sym in UNIVERSE:
        try:
            hist = yf.Ticker(sym).history(period="6mo")
            if hist is None or len(hist) < ENTRY_CHANNEL + 5:
                continue
            close = hist["Close"].dropna()
            price = float(close.iloc[-1])
            # PRIOR channel excludes today's bar (shift by 1) so today's close is
            # the bar that breaks it.
            high_40 = float(close.iloc[:-1].tail(ENTRY_CHANNEL).max())
            low_20 = float(close.iloc[:-1].tail(EXIT_CHANNEL).min())
            signals[sym] = {
                "price": price,
                "high_40": high_40,
                "low_20": low_20,
                "buy_signal": price >= high_40,
                "exit_signal": price <= low_20,
            }
        except Exception as e:
            logger.debug(f"[Donchian] signal error {sym}: {e}")
    return signals


def run(broker: AlpacaBroker, db_conn):
    """Donchian trend: exits first (channel breakdown), then breakout entries."""
    logger.info("=== Donchian Trend: scanning ===")
    signals = _get_signals()
    if not signals:
        logger.warning("[Donchian] No signals — skipping")
        return

    all_positions = broker.get_positions()
    current_symbols = {p["symbol"] for p in all_positions}
    my_positions = [p for p in all_positions if p.get("strategy") == STRATEGY_NAME]

    # ── 1. Exits — channel breakdown (runs every regime) ──────────────────────
    for pos in my_positions:
        sym = pos["symbol"]
        sig = signals.get(sym, {})
        if sig.get("exit_signal"):
            logger.info(f"[Donchian] EXIT {sym} — closed below {EXIT_CHANNEL}d low "
                        f"(${sig['price']:.2f} <= ${sig['low_20']:.2f})")
            try:
                broker.close_position(sym, STRATEGY_NAME)
                log_trade(db_conn, STRATEGY_NAME, sym, "sell_channel",
                          pos.get("qty", 0), pos.get("current_price", sig["price"]),
                          pos.get("unrealized_pnl", 0))
                current_symbols.discard(sym)
            except Exception as e:
                logger.error(f"[Donchian] exit failed {sym}: {e}")

    # ── 2. Entries — regime-gated (trend-following: bull/chop only) ────────────
    try:
        from utils.regime_weights import get_multiplier as _rm
        if _rm(STRATEGY_NAME) == 0.0:
            logger.info("[Donchian] Regime weight 0.0 — exits only")
            return
    except Exception:
        pass

    held = len([p for p in broker.get_positions() if p.get("strategy") == STRATEGY_NAME])
    if held >= MAX_POSITIONS:
        logger.info(f"[Donchian] At max positions ({MAX_POSITIONS}) — exits only")
        return

    account = broker.get_account()
    portfolio_value = float(account["portfolio_value"])
    cash = float(account["cash"])
    min_cash = portfolio_value * MIN_CASH_RESERVE_PCT

    # Strongest breakouts first (furthest above the channel).
    candidates = sorted(
        [(s, sig) for s, sig in signals.items()
         if sig.get("buy_signal") and s not in current_symbols],
        key=lambda kv: kv[1]["price"] / kv[1]["high_40"], reverse=True,
    )

    for sym, sig in candidates:
        if held >= MAX_POSITIONS:
            break
        notional = portfolio_value * ALLOCATION_PCT
        if cash - notional < min_cash:
            logger.info(f"[Donchian] Cash floor — skipping {sym}")
            continue
        logger.info(f"[Donchian] ENTER {sym} — ${notional:.0f} | "
                    f"broke {ENTRY_CHANNEL}d high ${sig['high_40']:.2f} @ ${sig['price']:.2f}")
        log_signal(db_conn, STRATEGY_NAME, sym, "buy", sig["price"] / sig["high_40"],
                   {"price": sig["price"], "high_40": sig["high_40"]})
        try:
            order = broker.market_buy(sym, notional, STRATEGY_NAME)
            if order:
                tag_symbol(sym, STRATEGY_NAME)
                log_trade(db_conn, STRATEGY_NAME, sym, "buy", 0, sig["price"], 0,
                          metadata={"notional": notional, "high_40": sig["high_40"]})
                held += 1
                cash, portfolio_value = broker.get_live_cash()
                if cash < portfolio_value * MIN_CASH_RESERVE_PCT:
                    logger.warning("[Donchian] Cash floor hit — halting entries")
                    break
        except Exception as e:
            logger.error(f"[Donchian] entry failed {sym}: {e}")

    logger.info("[Donchian] Scan complete")
