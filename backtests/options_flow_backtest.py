"""
Backtest: options_flow strategy (priority roadmap #1)
====================================================
HONEST LIMITATION FIRST
-----------------------
options_flow's signal is "unusual near-the-money OTM call volume relative to
open interest". That requires HISTORICAL per-strike option volume + open
interest. No free source provides it:
  - yfinance option_chain() returns only a CURRENT snapshot (today's chain).
  - Yahoo's chart API (used here) is underlying OHLCV only.
  - Historical options data (ORATS, CBOE DataShop, Polygon options) is paid.

Therefore the flow signal itself CANNOT be backtested with free data. Any claim
that "unusual call flow backtests to +X%" without paid options history is
fabricated. We do not do that here.

WHAT WE CAN DO
--------------
Measure the EXIT STRUCTURE as a floor. options_flow buys the UNDERLYING (not the
option) and holds 7 days with +8% TP / -4% stop. We test that bracket on the
live UNIVERSE using a neutral, always-eligible entry (buy any day the stock is
above its 20d MA — a weak momentum proxy) to see what the hold/exit geometry
alone returns. If even a favourable-momentum entry with this bracket is not
clearly +EV, the strategy has no cushion for a noisy real-world flow signal.

This is a CONTROL / sanity floor, NOT a validation of the flow alpha.

Run:  python backtests/options_flow_backtest.py
"""
from __future__ import annotations
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np
import data as D, metrics as M

LIVE_UNIVERSE = ["AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","JPM","V",
                 "XOM","AVGO","MA","HD","AMD","NFLX","CRM","ADBE","COST","AMGN","CAT"]
TP = 0.08
STOP = 0.04
HOLD = 7
START, END = "2015-01-01", "2026-06-30"


def bracket_trades(df):
    c = df["Close"]; h = df["High"]; lo = df["Low"]; o = df["Open"]
    ma20 = c.rolling(20).mean()
    trades = []
    i = 21; n = len(df)
    while i < n - 1:
        # weak momentum proxy: above 20d MA (options_flow also RSI<75 gate)
        if not (np.isfinite(ma20.iloc[i]) and c.iloc[i] > ma20.iloc[i]):
            i += 1; continue
        entry = o.iloc[i + 1]
        if not np.isfinite(entry) or entry <= 0:
            i += 1; continue
        exit_ret = None
        for k in range(i + 1, min(i + 1 + HOLD, n)):
            if lo.iloc[k] <= entry * (1 - STOP):
                exit_ret = -STOP; exit_i = k; break
            if h.iloc[k] >= entry * (1 + TP):
                exit_ret = TP; exit_i = k; break
        if exit_ret is None:
            exit_i = min(i + HOLD, n - 1)
            exit_ret = c.iloc[exit_i] / entry - 1
        trades.append(exit_ret)
        i = exit_i + 1
    return trades


def main():
    print(__doc__.split("Run:")[0])
    panel = D.get_panel(LIVE_UNIVERSE, START, END)
    tr = []
    for s, df in panel.items():
        tr += bracket_trades(df)
    st = M.trade_stats(tr)
    exp = st["win_rate"]*st["avg_win"] + (1-st["win_rate"])*st["avg_loss"]
    print("=== CONTROL: +8%/-4%/7d bracket, above-20dMA entry, live universe ===")
    print(M.fmt(st))
    print(f"Expectancy/trade: {exp*100:+.3f}%")
    print("\nInterpretation: this is the exit-geometry floor only. The unusual-call")
    print("flow signal cannot be validated without paid historical options data.")
    print("Recommendation: keep options_flow OFF for live capital until a paid")
    print("options-history backtest demonstrates the flow signal adds alpha.")


if __name__ == "__main__":
    main()
