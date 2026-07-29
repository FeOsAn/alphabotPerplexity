"""
Pre-registered validation of the three unvalidated LIVE sleeves (registry:
revalidate_by 2026-09-30). Thresholds declared before running — see ledger.

  spy_dip        : ship iff expectancy > 0 AND >= random-entry ETF control
  sector_rotation: ship iff monthly Sharpe > equal-weight-sectors Sharpe
  ts_momentum    : ship iff monthly Sharpe > 0.7 AND corr(engine) < 0.4

Run:  python backtests/validate_small_sleeves.py
"""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import data as D, metrics as M
from frontier_shift import load, basket_ew, overlay

SECTORS = ["XLK", "XLF", "XLE", "XLV", "XLI", "XLP", "XLU", "XLB", "XLY"]
TS_UNI = ["SPY", "QQQ", "IWM", "EFA", "EEM", "TLT", "IEF", "HYG", "LQD",
          "GLD", "SLV", "USO", "DBC", "XLF", "XLE", "XLK", "XLV", "XLI"]
START, END = "2014-06-01", "2026-06-30"


def rsi(c, n=14):
    d = c.diff(); g = d.clip(lower=0).ewm(alpha=1/n, adjust=False).mean()
    l = (-d.clip(upper=0)).ewm(alpha=1/n, adjust=False).mean()
    return 100 - 100 / (1 + g / l.replace(0, np.nan))


def bracket(df, i, tp=0.08, stop=0.04, cap=40):
    o, h, l, c = df["Open"], df["High"], df["Low"], df["Close"]
    n = len(df)
    if i + 1 >= n: return None
    e = o.iloc[i+1]
    if not np.isfinite(e) or e <= 0: return None
    for k in range(i+1, min(i+1+cap, n)):
        if l.iloc[k] <= e*(1-stop): return -stop, k
        if h.iloc[k] >= e*(1+tp): return tp, k
    k = min(i+cap, n-1)
    return c.iloc[k]/e - 1, k


def spy_dip_test():
    trades, ctrl = [], []
    for sym in ["SPY", "QQQ"]:
        df = D.get_history(sym, START, END)
        c = df["Close"]; ma50 = c.rolling(50).mean(); ma20 = c.rolling(20).mean()
        r = rsi(c); hi20 = c.rolling(20).max()
        off = (hi20 - c) / hi20
        slope = ma20 > ma20.shift(5)
        sig = ((c > ma50) & (off >= 0.02) & (off <= 0.08) & (r >= 35) & (r <= 55) & slope)
        i, n = 55, len(df)
        while i < n - 1:                       # strategy
            if sig.iloc[i]:
                res = bracket(df, i)
                if res is None: break
                trades.append(res[0]); i = res[1] + 1
            else: i += 1
        i = 55
        while i < n - 1:                       # control: every 10 days
            res = bracket(df, i)
            if res is None: break
            ctrl.append(res[0]); i = max(res[1] + 1, i + 10)
    ts, tc = M.trade_stats(trades), M.trade_stats(ctrl)
    print(f"spy_dip   : N={ts['trades']:>3} exp {ts['avg_ret']*100:+.2f}% PF {ts['profit_factor']:.2f} "
          f"| control N={tc['trades']} exp {tc['avg_ret']*100:+.2f}% PF {tc['profit_factor']:.2f}")
    verdict = ts["avg_ret"] > 0 and ts["avg_ret"] >= tc["avg_ret"]
    print(f"  -> {'PASS (ship)' if verdict else 'FAIL (cull)'}  [pre-reg: exp>0 AND >= control]")
    return verdict


def sector_rotation_test():
    panel = {s: D.get_history(s, START, END)["Close"] for s in SECTORS}
    px = pd.DataFrame(panel).dropna()
    me = px.resample("ME").last()
    mom = me / me.shift(3) - 1                 # 3-month momentum, monthly grid
    fwd = me.shift(-1) / me - 1                # next-month return
    rot, ew = [], []
    for dt in me.index[3:-1]:
        row = mom.loc[dt].dropna()
        top = row.sort_values(ascending=False).index[:3]
        rot.append(fwd.loc[dt, top].mean())
        ew.append(fwd.loc[dt].mean())
    rot, ew = pd.Series(rot), pd.Series(ew)
    sh = lambda x: x.mean()/x.std()*np.sqrt(12)
    print(f"sector_rot: rotation Sharpe {sh(rot):.2f} CAGR {((1+rot).prod()**(12/len(rot))-1)*100:.1f}% "
          f"| EW-sectors Sharpe {sh(ew):.2f} CAGR {((1+ew).prod()**(12/len(ew))-1)*100:.1f}%")
    verdict = sh(rot) > sh(ew)
    print(f"  -> {'PASS (ship)' if verdict else 'FAIL (cull)'}  [pre-reg: Sharpe > equal-weight]")
    return verdict


def ts_momentum_test(engine):
    px = pd.DataFrame({s: D.get_history(s, START, END)["Close"] for s in TS_UNI}).dropna(how="all")
    me = px.resample("ME").last()
    sig = (me.shift(1) / me.shift(13) - 1) > 0      # 12-1 at prior month end
    fwd = me / me.shift(1) - 1
    port = (fwd[sig].mean(axis=1)).dropna()          # EW of long ETFs, flat else
    sh = port.mean()/port.std()*np.sqrt(12)
    em = (1+engine).cumprod().resample("ME").last().pct_change()
    j = pd.concat([port.rename("ts"), em.rename("e")], axis=1).dropna()
    corr = j["ts"].corr(j["e"])
    print(f"ts_mom    : monthly Sharpe {sh:.2f} CAGR {((1+port).prod()**(12/len(port))-1)*100:.1f}% corr(engine) {corr:.2f}")
    verdict = sh > 0.7 and abs(corr) < 0.4
    print(f"  -> {'PASS (ship)' if verdict else 'FAIL (cull)'}  [pre-reg: Sharpe>0.7 AND corr<0.4]")
    return verdict


if __name__ == "__main__":
    panel, closes, rets, spy = load()
    engine = overlay(basket_ew(rets, closes), spy)
    a = spy_dip_test()
    b = sector_rotation_test()
    c = ts_momentum_test(engine)
    print(f"\nVERDICTS: spy_dip={'KEEP' if a else 'CULL'} sector_rotation={'KEEP' if b else 'CULL'} ts_momentum={'KEEP' if c else 'CULL'}")
