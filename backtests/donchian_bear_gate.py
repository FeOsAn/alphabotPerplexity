"""
Pre-registered 2026-08-25: (T1) donchian BEAR gate — entries blocked when
SPY<200DMA vs never. Same survival rule as the chop-gate test: the gate STAYS
only if gated Sharpe > ungated + 0.05 AND gated MaxDD better. (T2) reconstruct
the sleeve's Aug 17-25 live contribution (12 slots, 3.3%/slot). Attribution only.
"""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import data as D, metrics as M
from scorecard import UNIVERSE
START, END = "2014-01-01", "2026-08-25"

def sim(block_bear):
    panel = D.get_panel(UNIVERSE, START, END)
    spy = D.get_history("SPY", START, END)["Close"]
    bear = spy < spy.rolling(200).mean()
    sigs = {}
    for s in UNIVERSE:
        px = panel[s]["Close"].dropna()
        if len(px) < 80: continue
        hi = px.rolling(40).max().shift(1); lo = px.rolling(20).min().shift(1)
        st = pd.Series(np.nan, index=px.index); st[px>=hi]=1.0; st[px<=lo]=0.0
        sigs[s] = {"st": st.ffill().fillna(0.0), "px": px,
                   "en": (st==1.0)&(st.ffill().shift(1)!=1.0)}
    cal = pd.DatetimeIndex(sorted(set().union(*[v["px"].index for v in sigs.values()])))
    held, daily = {}, []
    for d in cal[1:]:
        r = 0.0
        for s in list(held):
            px = sigs[s]["px"]
            if d in px.index:
                j = px.index.get_loc(d)
                if j>0: r += (1/12)*(px.iloc[j]/px.iloc[j-1]-1)
                if sigs[s]["st"].get(d,1.0)==0.0: held.pop(s)
        daily.append(r)
        isb = bool(bear.reindex([d]).ffill().iloc[-1])
        for s,sg in sigs.items():
            if len(held)>=12: break
            if s in held: continue
            if sg["en"].get(d,False):
                if block_bear and isb: continue
                held[s]=True
    return pd.Series(daily, index=cal[1:]), panel

for gated in (False, True):
    d,panel = sim(gated)
    m = M.summarize((1+d).cumprod())
    dd22 = M.summarize((1+d.loc["2021-12-31":"2023-01-31"]).cumprod())["MaxDD"]
    tag = "BEAR-GATED" if gated else "UNGATED  "
    print(f"{tag}  CAGR {m['CAGR']*100:5.1f}%  Sh {m['Sharpe']:5.2f}  DD {m['MaxDD']*100:6.1f}%  2022DD {dd22*100:6.1f}%")

# T2: Aug 17-25 reconstruction, sleeve assumed empty Aug 17 (worst case: all-new entries)
print("\n=== T2: donchian entries taken Aug 17-25 (12 slots, first-come) ===")
panel = D.get_panel(UNIVERSE, "2026-04-01", END)
taken, pnl = [], 0.0
for s in UNIVERSE:
    px = panel[s]["Close"].dropna()
    hi = px.rolling(40).max().shift(1)
    w = px.loc["2026-08-17":]
    for d,v in w.items():
        if len(taken)>=12: break
        if v >= hi.get(d, np.inf) and s not in [t[0] for t in taken]:
            taken.append((s, d.date(), v, px.iloc[-1]))
            break
for s,d,e,c in taken:
    r=(c/e-1); pnl += 0.033*r
    print(f"  {s:<6} entered {d} @{e:8.2f}  now {c:8.2f}  {r*100:+5.1f}%")
print(f"entries: {len(taken)}/12 slots  -> est. book contribution {pnl*100:+.2f}pts")
