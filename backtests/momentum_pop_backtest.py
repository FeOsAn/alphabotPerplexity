"""
Backtest: MegaCap Momentum-Pop entry under the bot's ACTUAL exit engine
=======================================================================
This validates the repurposed squeeze_screener entry (roadmap #1/#2) the honest
way: the entry signal is measured against the REAL exit logic the live bot uses,
not a convenient fixed bracket.

Entry (the tradeable core, short-interest gate removed):
  - 5-day return >= +3%
  - volume >= 1.5x 20-day average
  - RSI(14) in [45, 72]
  - enter at NEXT day's open (no look-ahead)

Exit engine — faithful replica of bot/strategies/trade_management.py:
  - initial hard stop 5% below entry
  - peak-based trailing ratchet (tier floors: peak +25%->lock +18%, +18%->+12%,
    +12%->+7%, +8%->+4%, +5%->+2%, +3%->breakeven). Using the floor alone is
    CONSERVATIVE vs the live max(floor, peak - 1*ATR) rule (real stops sit >= floor).
  - 20% take-profit cap
  - dead-money timeout: flat within +/-2.5% for >= 14 trading days -> close
  - hard cap at 60 trading days for boundedness

Run:  python backtests/momentum_pop_backtest.py
"""
from __future__ import annotations
import sys, pathlib, itertools
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import data as D, metrics as M
from squeeze_backtest import SQUEEZE_UNIVERSE

LIVE_UNIVERSE = [
    "AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","LLY","PYPL","JPM","V",
    "XOM","AVGO","PG","MA","JNJ","HD","MRK","ABBV","CVX","COST","CRM","BAC",
    "NFLX","AMD","ADBE","WMT","MCD","CSCO","ORCL","TXN","COP","RTX","AMGN",
    "INTU","SPGI","CAT","BKNG","GE","HON","AXP","MS","GS","LMT","ISRG","VRTX",
    "NOW","PANW","REGN","KLAC",
]
START, END = "2015-01-01", "2026-06-30"

# exit-engine constants (from trade_management.py / config.py)
INIT_STOP = 0.05
TP = 0.20
DEAD_DAYS = 14
DEAD_BAND = 0.025
HARD_CAP = 60
_TIERS = [(0.25, 0.18), (0.18, 0.12), (0.12, 0.07), (0.08, 0.04), (0.05, 0.02), (0.03, 0.0)]


def _trail_floor(peak_gain: float) -> float:
    for mg, lf in _TIERS:
        if peak_gain >= mg:
            return lf
    return -INIT_STOP  # below lowest tier -> original stop


def rsi(c: pd.Series, n=14):
    d = c.diff(); g = d.clip(lower=0).rolling(n).mean(); l = (-d.clip(upper=0)).rolling(n).mean()
    return 100 - 100 / (1 + g / l.replace(0, np.nan))


def simulate(df, ret5_min, vol_min, rsi_lo, rsi_hi):
    if len(df) < 40:
        return []
    c, v, h, lo, o = df["Close"], df["Volume"], df["High"], df["Low"], df["Open"]
    ret5 = c / c.shift(5) - 1
    volavg = v.rolling(20).mean()
    r = rsi(c)
    trades = []
    i, n = 25, len(df)
    while i < n - 1:
        vr = v.iloc[i] / volavg.iloc[i] if volavg.iloc[i] > 0 else 0
        if not (np.isfinite(ret5.iloc[i]) and np.isfinite(r.iloc[i]) and
                ret5.iloc[i] >= ret5_min and vr >= vol_min and rsi_lo <= r.iloc[i] <= rsi_hi):
            i += 1; continue
        entry = o.iloc[i + 1]
        if not np.isfinite(entry) or entry <= 0:
            i += 1; continue
        peak = entry
        exit_ret = None
        exit_i = min(i + HARD_CAP, n - 1)
        for k in range(i + 1, min(i + 1 + HARD_CAP, n)):
            peak = max(peak, h.iloc[k])
            peak_gain = peak / entry - 1
            stop_px = entry * (1 + _trail_floor(peak_gain))
            if lo.iloc[k] <= stop_px:                      # trailing/hard stop
                exit_ret = stop_px / entry - 1; exit_i = k; break
            if h.iloc[k] >= entry * (1 + TP):              # take profit
                exit_ret = TP; exit_i = k; break
            held = k - i
            if held >= DEAD_DAYS and abs(c.iloc[k] / entry - 1) <= DEAD_BAND:
                exit_ret = c.iloc[k] / entry - 1; exit_i = k; break
        if exit_ret is None:
            exit_ret = c.iloc[exit_i] / entry - 1
        trades.append(exit_ret)
        i = exit_i + 1
    return trades


_PANELS: dict = {}


def _panel(universe_key, universe):
    if universe_key not in _PANELS:
        _PANELS[universe_key] = D.get_panel(universe, START, END)
    return _PANELS[universe_key]


def _run(universe, universe_key="live", **kw):
    panel = _panel(universe_key, universe)
    trades = []
    for s, df in panel.items():
        trades += simulate(df, kw.get("ret5_min", 0.03), kw.get("vol_min", 1.5),
                           kw.get("rsi_lo", 45), kw.get("rsi_hi", 72))
    return trades, len(panel)


def _report(name, trades, nsyms, years=11.46):
    st = M.trade_stats(trades)
    exp = st["win_rate"]*st["avg_win"] + (1-st["win_rate"])*st["avg_loss"]
    tpy = st["trades"] / years
    print(f"{name}")
    print("  " + M.fmt(st))
    print(f"  Expectancy {exp*100:+.3f}%/trade | ~{tpy:.0f} trades/yr | "
          f"gross edge ~{exp*tpy*100:+.1f}%/yr (before overlap/sizing)")
    return exp, tpy


def main():
    print("=== MegaCap Momentum-Pop under the LIVE exit engine (2015-2026) ===\n")
    mt, mn = _run(LIVE_UNIVERSE)
    _report(f"MEGA-CAP universe ({mn} names):", mt, mn)
    print()
    st_, sn = _run(SQUEEZE_UNIVERSE, universe_key="squeeze")
    _report(f"SQUEEZE-PRONE universe ({sn} names) [control — expect worse]:", st_, sn)

    # #3: how big can the edge get? small entry-param sweep on mega-caps
    print("\n=== Entry-param sweep (mega-caps, live exit engine) — biggest edge ===")
    print(f"{'ret5':>5}{'vol':>5}{'rsiLo':>6}{'rsiHi':>6}  {'N':>5}{'Win%':>6}{'Exp%':>7}{'PF':>5}{'edge/yr':>8}")
    rows = []
    for r5, vm, rl, rh in itertools.product([0.02,0.03,0.05],[1.3,1.5,2.0],[40,45,50],[68,72,78]):
        tr, _ = _run(LIVE_UNIVERSE, ret5_min=r5, vol_min=vm, rsi_lo=rl, rsi_hi=rh)
        st = M.trade_stats(tr)
        if st["trades"] < 100:
            continue
        exp = st["win_rate"]*st["avg_win"] + (1-st["win_rate"])*st["avg_loss"]
        rows.append((r5, vm, rl, rh, st, exp, exp*(st["trades"]/11.46)))
    rows.sort(key=lambda x: x[6], reverse=True)
    for r5, vm, rl, rh, st, exp, edge in rows[:8]:
        print(f"{r5:>5}{vm:>5}{rl:>6}{rh:>6}  {st['trades']:>5}{st['win_rate']*100:>5.0f}%"
              f"{exp*100:>6.2f}%{st['profit_factor']:>5.2f}{edge*100:>7.1f}%")


if __name__ == "__main__":
    main()
