"""
Survivorship robustness test — the one uncleared overfit exposure.
The live UNIVERSE is today's winners (hindsight). Here we rebuild the basket
from what was KNOWABLE at the start: the largest US large-caps as of end-2014,
INCLUDING the era's future laggards/losers (GE, IBM, XOM, WFC, C, T, ...).
Selection uses zero future information. The engine overlay is identical.

The delta between this and the hindsight universe = the survivorship premium
baked into every absolute CAGR reported so far. If the 2014-universe engine
still beats SPY risk-adjusted, the STRUCTURAL claims survive.

Residual caveat (stated, unavoidable free): a few 2014 mega-caps merged or
renamed (UTX->RTX kept via RTX history, PCLN->BKNG, FB->META n/a in 2014);
fully delisted names (e.g. MON 2018) can't be fetched, which leaves a small
UPWARD residual bias — so treat this as a lower-bound haircut, not zero bias.

Run:  python backtests/survivorship_test.py
"""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import data as D, metrics as M
from frontier_shift import overlay

# Largest US-listed large-caps by market cap as of late 2014 (knowable then).
UNIVERSE_2014 = [
    "AAPL", "XOM", "MSFT", "JNJ", "WMT", "WFC", "GE", "PG", "JPM", "CVX",
    "VZ", "PFE", "KO", "T", "ORCL", "BAC", "MRK", "IBM", "DIS", "INTC",
    "CSCO", "HD", "PM", "GILD", "UNH", "MCD", "MMM", "BA", "AMZN", "GOOGL",
    "V", "MA", "C", "SLB", "PEP", "CMCSA", "ABBV", "BMY", "AMGN", "CAT",
    "GS", "AXP", "RTX", "COP", "QCOM", "SBUX", "NKE", "USB", "UPS", "LLY",
]
START, END = "2014-01-01", "2026-06-30"


def main():
    panel = D.get_panel(UNIVERSE_2014, START, END)
    aux = D.get_panel(["SPY"], START, END)
    print(f"2014-universe names fetched: {len(panel)}/{len(UNIVERSE_2014)}")
    closes = pd.DataFrame({s: panel[s]["Close"] for s in panel}).sort_index()
    basket = closes.pct_change().mean(axis=1).dropna()
    spy = aux["SPY"]["Close"].reindex(basket.index).ffill()
    eng = overlay(basket, spy)          # identical overlay: vt 0.15, ma200 0.6, cap 0.9
    spy_r = spy.pct_change().fillna(0.0)

    def rep(name, d):
        m = M.summarize((1 + d.dropna()).cumprod())
        dd22 = M.summarize((1 + d.loc["2021-12-31":"2023-01-31"]).cumprod())["MaxDD"]
        subs = [M.summarize((1 + d.loc[a:b]).cumprod())["Sharpe"] for a, b in
                [("2015-01-01", "2019-01-01"), ("2019-01-01", "2023-01-01"),
                 ("2023-01-01", "2026-06-30")]]
        print(f"{name:<30} CAGR {m['CAGR']*100:5.1f}%  Sharpe {m['Sharpe']:.2f}  "
              f"MaxDD {m['MaxDD']*100:6.1f}%  Calmar {m['Calmar']:.2f}  2022DD {dd22*100:5.1f}%  "
              f"subs[{subs[0]:.2f}/{subs[1]:.2f}/{subs[2]:.2f}]")
        return m

    m_e = rep("ENGINE on 2014 universe", eng)
    m_s = rep("SPY buy & hold", spy_r)

    print("\nAnnual, engine-2014 vs SPY:")
    def annual(d):
        eq = (1 + d).cumprod(); ye = eq.resample("YE").last()
        out = ye.pct_change(); out.iloc[0] = ye.iloc[0] - 1; return out
    a_e, a_s = annual(eng), annual(spy_r)
    for dt in a_e.index:
        beat = "BEAT" if a_e[dt] > a_s.get(dt, np.nan) else "lag "
        print(f"  {dt.year}: {a_e[dt]*100:+6.1f}%  vs SPY {a_s.get(dt, float('nan'))*100:+6.1f}%  {beat}")

    print("\nReference (hindsight universe engine): CAGR 17.7% Sharpe 1.31 MaxDD -15.9%")
    print(f"SURVIVORSHIP PREMIUM ESTIMATE: {17.7 - m_e['CAGR']*100:.1f} CAGR points "
          f"(lower bound — fully delisted names unfetchable)")


if __name__ == "__main__":
    main()
