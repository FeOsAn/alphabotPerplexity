"""
Workstream 3: scale the best sleeve. Donchian currently trades 20 names / 8
slots / 40-20 channels. Test: full 50-name universe, more slots, channel grid.

Run:  python backtests/donchian_expand.py
"""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import data as D, metrics as M
import donchian_validate as DV
from scorecard import UNIVERSE

START, END = "2014-06-01", "2026-06-30"
CUR20 = ["AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","AVGO","JPM","V",
         "MA","HD","COST","NFLX","AMD","CRM","ADBE","WMT","XOM","LLY"]


def run(panel, N, Mx, K):
    DV.N_ENTRY, DV.M_EXIT, DV.K = N, Mx, K
    eq = DV.donchian_portfolio(panel)
    m = M.summarize(eq)
    subs = []
    for a, b in [("2015-01-01","2019-01-01"),("2019-01-01","2023-01-01"),("2023-01-01","2026-06-30")]:
        subs.append(M.summarize(eq.loc[a:b])["Sharpe"])
    return m, subs


def main():
    p50 = D.get_panel(UNIVERSE, START, END)
    p20 = {s: p50[s] for s in CUR20 if s in p50}
    print(f"{'config':<34}{'CAGR':>7}{'Sharpe':>7}{'MaxDD':>8}{'Calmar':>7}   [sub Sharpes]")
    print("-" * 86)
    for label, panel, N, Mx, K in [
        ("CURRENT 20n/8s/40-20", p20, 40, 20, 8),
        ("50 names / 8 slots / 40-20", p50, 40, 20, 8),
        ("50 names / 12 slots / 40-20", p50, 40, 20, 12),
        ("50 names / 12 slots / 55-20", p50, 55, 20, 12),
        ("50 names / 16 slots / 40-20", p50, 40, 20, 16),
        ("20 names / 8 slots / 55-20", p20, 55, 20, 8),
    ]:
        m, subs = run(panel, N, Mx, K)
        print(f"{label:<34}{m['CAGR']*100:>6.1f}%{m['Sharpe']:>7.2f}{m['MaxDD']*100:>7.1f}%"
              f"{m['Calmar']:>7.2f}   [{subs[0]:.2f}/{subs[1]:.2f}/{subs[2]:.2f}]")


if __name__ == "__main__":
    main()
