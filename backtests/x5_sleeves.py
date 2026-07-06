"""
5x round, rig 2: new/upgraded sleeves + optimal sleeve-weight grid.
  A. Single-asset trend sleeves done RIGHT (the old cross-asset test diluted
     them in an equal-weight mush): GLD alone, TLT alone, 200DMA long/flat.
  B. Crypto sleeve upgrades: +SOL, and Donchian 40/20 gate vs 200DMA.
  C. Capstone: weight grid over engine / donchian / crypto / gold with
     sub-period validation -> the best implementable book.

Run:  python backtests/x5_sleeves.py
"""
import sys, pathlib, itertools
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import data as D, metrics as M
from frontier_shift import load, basket_ew, overlay
import donchian_validate as DV

def sleeve_trend(sym, cal, ma=200):
    px = D.get_history(sym, "2014-01-01", "2026-06-30")["Close"]
    if px.empty: return None
    sig = (px > px.rolling(ma).mean()).shift(1).fillna(False)
    eq = (1 + px.pct_change().fillna(0)*sig).cumprod()
    return eq.reindex(eq.index.union(cal)).ffill().reindex(cal).pct_change().fillna(0.0)

def sleeve_donch(sym, cal, N=40, Mx=20):
    px = D.get_history(sym, "2014-01-01", "2026-06-30")["Close"]
    if px.empty: return None
    hi = px.rolling(N).max().shift(1); lo = px.rolling(Mx).min().shift(1)
    sig = pd.Series(np.nan, index=px.index)
    sig[px >= hi] = 1.0; sig[px <= lo] = 0.0
    sig = sig.ffill().fillna(0.0).shift(1).fillna(0.0)
    eq = (1 + px.pct_change().fillna(0)*sig).cumprod()
    return eq.reindex(eq.index.union(cal)).ffill().reindex(cal).pct_change().fillna(0.0)

def summ(name, d, engine=None):
    d = d.dropna()
    m = M.summarize((1+d).cumprod())
    dd22 = M.summarize((1+d.loc["2021-12-31":"2023-01-31"]).cumprod())["MaxDD"]
    c = ""
    if engine is not None:
        j = pd.concat([d.rename("x"), engine.rename("e")], axis=1).dropna()
        c = f" corr={j['x'].corr(j['e']):.2f}"
    print(f"{name:<34}{m['CAGR']*100:>6.1f}%{m['Sharpe']:>7.2f}{m['MaxDD']*100:>8.1f}%"
          f"{m['Calmar']:>7.2f}{dd22*100:>8.1f}%{c}")
    return m

def main():
    panel, closes, rets, spy = load()
    eng = overlay(basket_ew(rets, closes), spy)
    cal = eng.index
    print(f"{'sleeve':<34}{'CAGR':>7}{'Sharpe':>7}{'MaxDD':>8}{'Calmar':>7}{'2022DD':>8}")
    print("="*82)

    print("— A. single-asset trend sleeves (undiluted) —")
    gld = sleeve_trend("GLD", cal); tlt = sleeve_trend("TLT", cal)
    if gld is not None: summ("GLD 200DMA long/flat", gld, eng)
    if tlt is not None: summ("TLT 200DMA long/flat", tlt, eng)

    print("— B. crypto sleeve upgrades —")
    btc = sleeve_trend("BTC-USD", cal); eth = sleeve_trend("ETH-USD", cal)
    sol = sleeve_trend("SOL-USD", cal)
    cur = 0.5*btc + 0.5*eth
    summ("BTC+ETH 200DMA (current)", cur, eng)
    if sol is not None:
        summ("BTC+ETH+SOL 200DMA (1/3 each)", (btc+eth+sol)/3, eng)
    btc_d = sleeve_donch("BTC-USD", cal); eth_d = sleeve_donch("ETH-USD", cal)
    summ("BTC+ETH Donchian 40/20 gate", 0.5*btc_d + 0.5*eth_d, eng)

    print("— C. sleeve-weight grid (engine/donchian/crypto/gold), step .05 —")
    DV.N_ENTRY, DV.M_EXIT, DV.K = 40, 20, 12
    donch = DV.donchian_portfolio(panel).pct_change().reindex(cal).fillna(0.0)
    sleeves = {"eng": eng, "don": donch, "cry": cur, "gld": gld}
    best = []
    for we, wd, wc, wg in itertools.product(
            [0.45,0.55,0.65], [0.20,0.30], [0.05,0.10,0.15], [0.0,0.05,0.10]):
        if abs(we+wd+wc+wg - 1.0) > 1e-9: continue
        d = we*eng + wd*donch + wc*cur + wg*gld
        m = M.summarize((1+d).cumprod())
        dd22 = M.summarize((1+d.loc["2021-12-31":"2023-01-31"]).cumprod())["MaxDD"]
        best.append((m["Sharpe"], we, wd, wc, wg, m, dd22))
    best.sort(reverse=True)
    print(f"{'weights e/d/c/g':<22}{'CAGR':>7}{'Sharpe':>8}{'MaxDD':>8}{'Calmar':>8}{'2022DD':>8}")
    for sh, we, wd, wc, wg, m, dd22 in best[:6]:
        print(f"{we:.2f}/{wd:.2f}/{wc:.2f}/{wg:.2f}{'':<4}{m['CAGR']*100:>6.1f}%{sh:>8.2f}"
              f"{m['MaxDD']*100:>7.1f}%{m['Calmar']:>8.2f}{dd22*100:>7.1f}%")
    # sub-period check on the winner
    _, we, wd, wc, wg, _, _ = best[0]
    win = we*eng + wd*donch + wc*cur + wg*gld
    subs = [M.summarize((1+win.loc[a:b]).cumprod())["Sharpe"] for a, b in
            [("2015-01-01","2019-01-01"),("2019-01-01","2023-01-01"),("2023-01-01","2026-06-30")]]
    print(f"winner {we:.2f}/{wd:.2f}/{wc:.2f}/{wg:.2f} sub-period Sharpes: "
          f"[{subs[0]:.2f}/{subs[1]:.2f}/{subs[2]:.2f}]")

if __name__ == "__main__":
    main()
