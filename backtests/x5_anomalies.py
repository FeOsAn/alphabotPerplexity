"""
5x round, rig 1: market anomalies + overlay-signal upgrades.
  A. Overnight vs intraday split of the basket (close->open vs open->close)
  B. Turn-of-month tilt (last 4 + first 3 trading days)
  C. IVTS (VIX/VIX3M) as exposure signal vs/with the 200DMA
  D. MA-speed ensemble gate (100/200/300 vote) vs single 200DMA
  E. Bear-side: net-short below 200DMA; Donchian short sleeve in downtrends

Run:  python backtests/x5_anomalies.py
"""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import data as D, metrics as M
from frontier_shift import load, basket_ew, overlay, FLOOR

def summ(name, d, extra=""):
    d = d.dropna()
    m = M.summarize((1+d).cumprod())
    dd22 = M.summarize((1+d.loc["2021-12-31":"2023-01-31"]).cumprod())["MaxDD"]
    print(f"{name:<36}{m['CAGR']*100:>6.1f}%{m['Sharpe']:>7.2f}{m['MaxDD']*100:>8.1f}%"
          f"{m['Calmar']:>7.2f}{dd22*100:>8.1f}%  {extra}")
    return m

def main():
    panel, closes, rets, spy = load()
    ew = basket_ew(rets, closes)
    print(f"{'variant':<36}{'CAGR':>7}{'Sharpe':>7}{'MaxDD':>8}{'Calmar':>7}{'2022DD':>8}")
    print("="*80)

    # ── A. Overnight vs intraday ─────────────────────────────────────────────
    print("— A. overnight anomaly (equal-weight basket, unlevered legs) —")
    op = pd.DataFrame({s: panel[s]["Open"] for s in panel}).sort_index()
    cl = pd.DataFrame({s: panel[s]["Close"] for s in panel}).sort_index()
    overnight = (op / cl.shift(1) - 1).mean(axis=1)          # prior close -> open
    intraday  = (cl / op - 1).mean(axis=1)                   # open -> close
    summ("overnight only", overnight)
    summ("intraday only", intraday)
    summ("full close-to-close (ref)", ew)

    # ── B. Turn-of-month ─────────────────────────────────────────────────────
    print("— B. turn-of-month (basket day returns) —")
    idx = ew.index
    mnth = pd.Series(idx.month, index=idx)
    pos_in_month = pd.Series(0, index=idx)
    # day counters from month start / to month end
    grp = pd.Series(idx, index=idx).groupby([idx.year, idx.month])
    first_flags = pd.Series(False, index=idx); last_flags = pd.Series(False, index=idx)
    for _, days in grp:
        first_flags.loc[days.iloc[:3]] = True
        last_flags.loc[days.iloc[-4:]] = True
    tom = first_flags | last_flags
    print(f"  ToM days: mean {ew[tom].mean()*1e4:.1f} bps/d (n={tom.sum()}) | "
          f"rest: {ew[~tom].mean()*1e4:.1f} bps/d (n={(~tom).sum()})")

    # ── C. IVTS overlay ──────────────────────────────────────────────────────
    print("— C. IVTS (VIX/VIX3M) exposure signal —")
    vix = D.get_history("^VIX", "2014-01-01", "2026-06-30")["Close"].reindex(ew.index).ffill()
    vix3 = D.get_history("^VIX3M", "2014-01-01", "2026-06-30")["Close"].reindex(ew.index).ffill()
    ivts = (vix / vix3)
    rv = ew.rolling(20).std()*np.sqrt(252)
    vt = (0.15/rv).clip(lower=FLOOR, upper=1.0)
    ma200 = spy.rolling(200).mean()
    ma_m = pd.Series(1.0, index=ew.index); ma_m[spy<=ma200]=0.60
    iv_m = pd.Series(1.0, index=ew.index); iv_m[ivts>=0.97]=0.60; iv_m[ivts>=1.05]=0.30
    for name, gate in [("vt+MA200 (current)", ma_m), ("vt+IVTS only", iv_m),
                       ("vt+min(MA200,IVTS)", np.minimum(ma_m, iv_m))]:
        scal = np.minimum(vt, gate).shift(1).fillna(0.0)
        summ(name, 0.90*scal*ew)

    # ── D. MA ensemble ───────────────────────────────────────────────────────
    print("— D. MA-speed ensemble (100/200/300 vote -> mult 1/.75/.5/.3) —")
    votes = sum((spy > spy.rolling(n).mean()).astype(int) for n in (100, 200, 300))
    ens = votes.map({3:1.0, 2:0.75, 1:0.5, 0:0.30})
    scal = np.minimum(vt, ens).shift(1).fillna(0.0)
    summ("vt+MA-ensemble", 0.90*scal*ew)

    # ── E. bear-side tests ───────────────────────────────────────────────────
    print("— E. short side (expect nulls; honesty checks) —")
    short_m = pd.Series(1.0, index=ew.index); short_m[spy<=ma200] = -0.20
    scal = (np.minimum(vt, 1.0)*short_m).shift(1).fillna(0.0)
    summ("net-short -0.2x below 200DMA", 0.90*scal*ew)

if __name__ == "__main__":
    main()
