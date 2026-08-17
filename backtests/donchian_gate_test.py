"""
Pre-registered (2026-08-17): does chop-gating donchian entries help or hurt?
Live wiring sets donchian weight 0 in CHOPPY/TRANSITION; validation never had
that gate. Proxy for the live chop flag: |SPY 63d return| < 4% (sideways tape).
Pre-reg: the gate STAYS only if gated Sharpe > ungated Sharpe + 0.05 AND gated
MaxDD is better. Otherwise the gate goes (restore the validated config).
"""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import data as D, metrics as M
from scorecard import UNIVERSE

START, END = "2014-01-01", "2026-08-14"
N_ENTRY, N_EXIT, SLOTS, ALLOC = 40, 20, 12, 1.0/12


def run(block_chop):
    panel = D.get_panel(UNIVERSE, START, END)
    spy = D.get_history("SPY", START, END)["Close"]
    chop = (spy / spy.shift(63) - 1).abs() < 0.04
    sigs = {}
    for s in UNIVERSE:
        px = panel[s]["Close"].dropna()
        if len(px) < 80: continue
        hi = px.rolling(N_ENTRY).max().shift(1); lo = px.rolling(N_EXIT).min().shift(1)
        st = pd.Series(np.nan, index=px.index)
        st[px >= hi] = 1.0; st[px <= lo] = 0.0
        sigs[s] = {"state": st.ffill().fillna(0.0), "px": px,
                   "entry": (st == 1.0) & (st.ffill().shift(1) != 1.0)}
    cal = sorted(set().union(*[s["px"].index for s in sigs.values()]))
    cal = pd.DatetimeIndex(cal)
    held, daily, entries_blocked = {}, [], 0
    for i, d in enumerate(cal[1:], 1):
        r = 0.0
        for s in list(held):
            px = sigs[s]["px"]
            if d in px.index:
                j = px.index.get_loc(d)
                if j > 0: r += ALLOC * (px.iloc[j]/px.iloc[j-1] - 1)
                if sigs[s]["state"].get(d, 1.0) == 0.0: held.pop(s)
        daily.append(r)
        is_chop = bool(chop.reindex([d]).ffill().iloc[-1]) if len(chop) else False
        for s, sg in sigs.items():
            if len(held) >= SLOTS: break
            if s in held or d not in sg["entry"].index: continue
            if sg["entry"].get(d, False):
                if block_chop and is_chop:
                    entries_blocked += 1; continue
                held[s] = True
    d = pd.Series(daily, index=cal[1:])
    m = M.summarize((1+d).cumprod())
    dd22 = M.summarize((1+d.loc["2021-12-31":"2023-01-31"]).cumprod())["MaxDD"]
    recent = d.loc["2026-06-01":]
    return m, dd22, entries_blocked, (1+recent).prod()-1, chop


for gated in (False, True):
    m, dd22, nb, rec, chop = run(gated)
    tag = "GATED (block entries in chop)" if gated else "UNGATED (validated config)"
    print(f"{tag:<32} CAGR {m['CAGR']*100:5.1f}%  Sh {m['Sharpe']:5.2f}  DD {m['MaxDD']*100:6.1f}%"
          f"  2022DD {dd22*100:6.1f}%  blocked={nb}  Jun-Aug26 {rec*100:+.1f}%")
c = chop.loc["2026-06-01":]
print(f"\nchop-proxy active {c.mean()*100:.0f}% of days Jun-Aug 2026 ({int(c.sum())}/{len(c)})")
