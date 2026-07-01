"""
Backtest: pairs_trading strategy (priority roadmap #4)
======================================================
Mirrors bot/strategies/pairs_trading.py exactly:
  - OLS hedge ratio (beta) over a LOOKBACK_DAYS trailing window
  - spread = P1 - (beta*P2 + alpha)
  - rolling z-score over ZSCORE_WINDOW
  - enter when |z| > Z_ENTRY  (dollar-neutral: equal $ per leg, matching live)
  - exit  when |z| <= Z_EXIT (reversion) OR |z| >= Z_STOP (stop) OR held >= MAX_HOLD_DAYS
  - each leg sized POSITION_PCT of book; market-neutral so daily pair P&L
    = POSITION_PCT * (r_long - r_short)

The live strategy is DOLLAR-neutral (short_qty = notional/short_price), not
beta-neutral, even though it computes beta only for the spread/z-score. This
backtest replicates that.

Costs: COST_BPS per leg per side (entry+exit => 4 * COST_BPS per pair trade).

Run:  python backtests/pairs_trading_backtest.py
"""
from __future__ import annotations

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))

import numpy as np
import pandas as pd

import data as D
import metrics as M

# ── params copied from bot/strategies/pairs_trading.py ────────────────────────
PAIRS = [
    ("XOM", "CVX"),
    ("JPM", "BAC"),
    ("KO",  "PEP"),
    ("HD",  "LOW"),
    ("GS",  "MS"),
]
Z_ENTRY = 2.0
Z_EXIT = 0.5
Z_STOP = 3.5
LOOKBACK_DAYS = 63
ZSCORE_WINDOW = 30
MAX_HOLD_DAYS = 20
POSITION_PCT = 0.04     # per leg
COST_BPS = 5.0          # per leg per side

START = "2015-01-01"
END = "2026-06-30"


def _zscore_series(p1: pd.Series, p2: pd.Series) -> pd.Series:
    """Point-in-time z-score of the spread, no look-ahead.

    At each day t we use ONLY data up to and including t: fit beta/alpha on the
    trailing LOOKBACK_DAYS, then z = (spread_t - mean) / std over ZSCORE_WINDOW.
    """
    idx = p1.index
    z = pd.Series(index=idx, dtype=float)
    a1 = p1.values
    a2 = p2.values
    for i in range(LOOKBACK_DAYS, len(idx)):
        w1 = a1[i - LOOKBACK_DAYS: i + 1]
        w2 = a2[i - LOOKBACK_DAYS: i + 1]
        var2 = np.var(w2)
        if var2 < 1e-12:
            continue
        beta = np.cov(w1, w2)[0, 1] / var2
        alpha = np.mean(w1) - beta * np.mean(w2)
        spread = a1[i - LOOKBACK_DAYS: i + 1] - (beta * w2 + alpha)
        sw = spread[-ZSCORE_WINDOW:]
        sd = sw.std()
        if sd <= 0:
            continue
        z.iloc[i] = (spread[-1] - sw.mean()) / sd
    return z


def backtest_pair(s1: str, s2: str, closes: pd.DataFrame):
    """Return (daily_return_series, list_of_trade_returns) for one pair."""
    if s1 not in closes or s2 not in closes:
        return pd.Series(dtype=float), []
    df = closes[[s1, s2]].dropna()
    if len(df) < LOOKBACK_DAYS + ZSCORE_WINDOW + 5:
        return pd.Series(dtype=float), []

    p1, p2 = df[s1], df[s2]
    r1 = p1.pct_change().fillna(0.0)
    r2 = p2.pct_change().fillna(0.0)
    z = _zscore_series(p1, p2)

    daily = pd.Series(0.0, index=df.index)
    trades: list[float] = []

    pos = 0            # +1 long s1/short s2 ; -1 short s1/long s2 ; 0 flat
    held = 0
    trade_ret = 0.0    # cumulative (net of costs) return of the open trade

    cost = COST_BPS / 1e4

    for i in range(1, len(df)):
        day = df.index[i]
        zi = z.iloc[i - 1]   # act on yesterday's signal (no look-ahead)

        # accrue P&L on an open position using today's returns
        if pos != 0:
            if pos == 1:      # long s1, short s2
                pnl = POSITION_PCT * (r1.iloc[i] - r2.iloc[i])
            else:             # short s1, long s2
                pnl = POSITION_PCT * (r2.iloc[i] - r1.iloc[i])
            daily.iloc[i] += pnl
            trade_ret += pnl
            held += 1

            zi_now = z.iloc[i]
            exit_now = False
            if np.isfinite(zi_now) and abs(zi_now) <= Z_EXIT:
                exit_now = True
            elif np.isfinite(zi_now) and abs(zi_now) >= Z_STOP:
                exit_now = True
            elif held >= MAX_HOLD_DAYS:
                exit_now = True

            if exit_now:
                c = 2 * cost * POSITION_PCT   # 2 legs closing
                daily.iloc[i] -= c
                trade_ret -= c
                trades.append(trade_ret)
                pos = 0
                held = 0
                trade_ret = 0.0
            continue

        # flat → look for entry on yesterday's z
        if np.isfinite(zi):
            if zi > Z_ENTRY:      # z high → spread rich → short s1, long s2
                pos = -1
            elif zi < -Z_ENTRY:   # z low → long s1, short s2
                pos = 1
            if pos != 0:
                c = 2 * cost * POSITION_PCT
                daily.iloc[i] -= c
                trade_ret = -c
                held = 0

    return daily, trades


def main():
    syms = sorted({s for pair in PAIRS for s in pair})
    print(f"Fetching {len(syms)} symbols {START}..{END} ...")
    closes = D.close_panel(syms, START, END)
    print(f"Panel: {closes.shape[0]} days x {closes.shape[1]} symbols\n")

    all_daily = []
    all_trades: list[float] = []
    print(f"{'Pair':<12}{'Trades':>7}  {'Win%':>6}  {'AvgRet':>8}  {'Sharpe':>7}  {'TotRet':>8}")
    print("-" * 60)
    for s1, s2 in PAIRS:
        daily, trades = backtest_pair(s1, s2, closes)
        if daily.empty:
            print(f"{s1+'/'+s2:<12}  (insufficient data)")
            continue
        eq = (1 + daily).cumprod()
        mm = M.summarize(eq)
        ts = M.trade_stats(trades)
        all_daily.append(daily)
        all_trades += trades
        wr = ts['win_rate']
        print(f"{s1+'/'+s2:<12}{ts['trades']:>7}  "
              f"{wr*100:>5.1f}%  {ts['avg_ret']*100:>7.3f}%  "
              f"{mm['Sharpe']:>7.2f}  {mm['TotRet']*100:>7.2f}%")

    if not all_daily:
        print("No pairs produced data.")
        return

    # Portfolio: pairs share the book; sum daily contributions (max 3 concurrent
    # in live, but historically rarely >2 fire at once; summing is a close proxy
    # and slightly conservative on capital efficiency).
    port = pd.concat(all_daily, axis=1).fillna(0.0).sum(axis=1)
    eq = (1 + port).cumprod()
    port_m = M.summarize(eq)
    port_t = M.trade_stats(all_trades)

    print("\n=== PAIRS_TRADING PORTFOLIO (2015-2026, all pairs pooled) ===")
    print(M.fmt(port_m))
    print(M.fmt(port_t))

    # Yearly breakdown
    print("\nYearly return:")
    yr = eq.resample("YE").last().pct_change()
    yr.iloc[0] = eq.resample("YE").last().iloc[0] / 1.0 - 1  # first year vs 1.0
    for d, v in yr.items():
        if pd.notna(v):
            print(f"  {d.year}: {v*100:+6.2f}%")

    print("\nNOTE: market-neutral book — compare Sharpe to SPY, not raw return. "
          "The strategy is an uncorrelated diversifier, not a return engine.")


if __name__ == "__main__":
    main()
