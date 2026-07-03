"""
Validate two additions before wiring live:
  1. Cross-asset trend sleeve (the "uncorrelated return stream"): time-series
     momentum (long/flat by 200DMA + 12m>0) on a diversified ETF set. Ship only
     if standalone Sharpe >= 0.6 AND correlation to the equity engine < 0.4.
  2. VIX-spike circuit-breaker: cut exposure hard when VIX spikes, to catch the
     one-day tail (Volmageddon) the 20d vol signal misses. Ship if it improves
     tail drawdown without materially hurting CAGR.

Run:  python backtests/validate_additions.py
"""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import data as D, metrics as M
from scorecard import UNIVERSE

START, END = "2014-01-01", "2026-06-30"
# diversified, liquid, non-equity-beta ETFs (all exist pre-2015)
XASSET = ["TLT","IEF","GLD","DBC","UUP","VNQ","HYG","EFA","EEM"]


def engine_series():
    panel = D.get_panel(UNIVERSE, START, END)
    aux = D.get_panel(["SPY"], START, END)
    vixp = D.get_history("^VIX", START, END)
    from deep_analysis import basket_and_overlay
    basket, spy, vix, engine = basket_and_overlay(panel, aux, vixp)
    return engine, spy, vix


def xasset_trend():
    """Long/flat TSMOM on the ETF set: hold each ETF that is >200DMA AND 12m>0,
    equal-weight the holdings, monthly rebalance. Uncorrelated by construction."""
    px = {}
    for s in XASSET:
        h = D.get_history(s, START, END)
        if not h.empty and len(h) > 260:
            px[s] = h["Close"]
    closes = pd.DataFrame(px).dropna(how="all")
    monthly = closes.resample("ME").last().index
    daily_ret = closes.pct_change()
    weights = pd.DataFrame(0.0, index=closes.index, columns=closes.columns)
    for j in range(13, len(monthly)-1):
        d = monthly[j]
        i = closes.index.get_indexer([d], method="ffill")[0]
        if i < 252:
            continue
        hold = []
        for s in closes.columns:
            c = closes[s]
            if not np.isfinite(c.iloc[i]) or not np.isfinite(c.iloc[i-252]):
                continue
            ma200 = c.iloc[i-199:i+1].mean()
            if c.iloc[i] > ma200 and c.iloc[i]/c.iloc[i-252]-1 > 0:
                hold.append(s)
        nxt = monthly[j+1]
        ni = closes.index.get_indexer([nxt], method="ffill")[0]
        if hold:
            weights.iloc[i+1:ni+1] = 0.0
            for s in hold:
                weights.loc[closes.index[i+1:ni+1], s] = 1.0/len(hold)
    port = (weights.shift(1)*daily_ret).sum(axis=1)
    return port.dropna()


def vix_breaker(engine, vix):
    """Cut exposure to 0.25 the day AFTER VIX closes > 35 OR jumps > 40% in a day."""
    v = vix.reindex(engine.index).ffill()
    spike = (v > 35) | (v.pct_change() > 0.40)
    scal = pd.Series(1.0, index=engine.index)
    scal[spike] = 0.25
    scal = scal.shift(1).fillna(1.0)
    return scal*engine


def summ(name, s):
    eq=(1+s).cumprod(); m=M.summarize(eq)
    return f"{name:<20} CAGR {m['CAGR']*100:5.1f}%  Sharpe {m['Sharpe']:.2f}  MaxDD {m['MaxDD']*100:6.1f}%"


def main():
    engine, spy, vix = engine_series()

    print("=== 1. Cross-asset trend sleeve (uncorrelated diversifier test) ===")
    xa = xasset_trend()
    m = M.summarize((1+xa).cumprod())
    print(summ("x-asset trend", xa) + f"  Sortino {m['Sortino']:.2f}")
    j = pd.concat([xa.rename("xa"), engine.rename("eng")], axis=1).dropna()
    corr = j["xa"].corr(j["eng"])
    print(f"  Correlation to equity engine: {corr:.2f}")
    # 50/50 blend
    blend = (0.5*j["xa"]+0.5*j["eng"])
    bm = M.summarize((1+blend).cumprod())
    print(f"  70/30 eng/xa blend Sharpe: "
          f"{M.summarize((1+(0.7*j['eng']+0.3*j['xa'])).cumprod())['Sharpe']:.2f} "
          f"| 50/50: {bm['Sharpe']:.2f} (engine alone 1.30)")
    ship1 = (m["Sharpe"]>=0.6) and (abs(corr)<0.4)
    print(f"  VERDICT: {'SHIP — real diversifier' if ship1 else 'weak — report only'}")

    print("\n=== 2. VIX-spike circuit-breaker ===")
    vb = vix_breaker(engine, vix)
    print(summ("engine (base)", engine))
    print(summ("engine + VIXbreak", vb))
    for name,(a,b) in {"Volmageddon":("2018-02-01","2018-02-12"),
                       "COVID":("2020-02-19","2020-03-23")}.items():
        e0=M.summarize((1+engine.loc[a:b]).cumprod())["MaxDD"]
        e1=M.summarize((1+vb.loc[a:b]).cumprod())["MaxDD"]
        print(f"  {name:<12} base DD {e0*100:5.1f}%  ->  with breaker {e1*100:5.1f}%")


if __name__ == "__main__":
    main()
