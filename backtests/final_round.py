"""
Final round:
  A. WALK-FORWARD validation of the sleeve stack — weights re-chosen every 6mo
     from the TRAILING 24mo only, applied out-of-sample to the next 6mo. The
     honest answer to "how much of Sharpe 1.9 is selection bias?"
  B. Vol-targeted crypto sleeve — scale each coin's weight by target/realized
     vol instead of fixed 5% slices (crypto vol ranges 40-120%/yr).
  C. Donchian upgrades — speed ensemble (20/10 + 40/20 + 55/20 averaged) and
     true-turtle ATR sizing (risk-normalized slots).

Run:  python backtests/final_round.py
"""
import sys, pathlib, itertools
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import data as D, metrics as M, engine as E
from frontier_shift import load, basket_ew, overlay
import donchian_validate as DV
from x5_sleeves import sleeve_trend


def summ(name, d, sub=False):
    d = d.dropna()
    m = M.summarize((1 + d).cumprod())
    dd22 = M.summarize((1 + d.loc["2021-12-31":"2023-01-31"]).cumprod())["MaxDD"]
    extra = ""
    if sub:
        s = [M.summarize((1+d.loc[a:b]).cumprod())["Sharpe"] for a, b in
             [("2017-01-01","2020-01-01"),("2020-01-01","2023-01-01"),("2023-01-01","2026-06-30")]]
        extra = f"  subs[{s[0]:.2f}/{s[1]:.2f}/{s[2]:.2f}]"
    print(f"{name:<38}{m['CAGR']*100:>6.1f}%{m['Sharpe']:>7.2f}{m['MaxDD']*100:>8.1f}%"
          f"{m['Calmar']:>7.2f}{dd22*100:>8.1f}%{extra}")
    return m


def main():
    panel, closes, rets, spy = load()
    eng = overlay(basket_ew(rets, closes), spy)
    cal = eng.index

    DV.N_ENTRY, DV.M_EXIT, DV.K = 40, 20, 12
    don = DV.donchian_portfolio(panel).pct_change().reindex(cal).fillna(0.0)
    btc, eth, sol = (sleeve_trend(s, cal) for s in ["BTC-USD", "ETH-USD", "SOL-USD"])
    cry_fixed = (btc + eth + sol) / 3
    gld = sleeve_trend("GLD", cal)

    print(f"{'variant':<38}{'CAGR':>7}{'Sharpe':>7}{'MaxDD':>8}{'Calmar':>7}{'2022DD':>8}")
    print("=" * 90)

    # ── A. WALK-FORWARD ──────────────────────────────────────────────────────
    print("— A. walk-forward OOS (weights from trailing 24mo, applied next 6mo) —")
    sleeves = pd.DataFrame({"eng": eng, "don": don, "cry": cry_fixed, "gld": gld}).dropna()
    grid = [(we, wd, wc, wg) for we, wd, wc, wg in itertools.product(
        [0.35, 0.45, 0.55, 0.65], [0.10, 0.20, 0.30], [0.0, 0.05, 0.10, 0.15],
        [0.0, 0.05, 0.10]) if abs(we + wd + wc + wg - 1.0) < 1e-9]
    halves = pd.date_range("2017-01-01", "2026-06-30", freq="6MS")
    oos = []
    for i in range(len(halves) - 1):
        a, b = halves[i], halves[i + 1]
        train = sleeves.loc[a - pd.DateOffset(months=24):a - pd.Timedelta(days=1)]
        if len(train) < 250:
            continue
        best, bw = -9, None
        for w in grid:
            tr = (train * w).sum(axis=1)
            sh = tr.mean() / tr.std() * np.sqrt(252) if tr.std() > 0 else -9
            if sh > best:
                best, bw = sh, w
        seg = (sleeves.loc[a:b - pd.Timedelta(days=1)] * bw).sum(axis=1)
        oos.append(seg)
    oos_ret = pd.concat(oos)
    summ("WALK-FORWARD OOS stack", oos_ret, sub=True)
    static = (sleeves * (0.45, 0.30, 0.15, 0.10)).sum(axis=1).loc[oos_ret.index]
    summ("static 45/30/15/10 (same window)", static, sub=True)
    spy_r = spy.pct_change().reindex(oos_ret.index).fillna(0.0)
    summ("SPY (same window)", spy_r)

    # ── B. vol-targeted crypto sleeve ────────────────────────────────────────
    print("— B. vol-targeted crypto sleeve (target 50% ann vol per coin, cap 1) —")
    def vt_coin(r):
        rv = r.rolling(30).std() * np.sqrt(252)
        sc = (0.50 / rv).clip(upper=1.0).shift(1).fillna(0.0)
        return sc * r
    cry_vt = (vt_coin(btc) + vt_coin(eth) + vt_coin(sol)) / 3
    summ("crypto sleeve FIXED (ref)", cry_fixed)
    summ("crypto sleeve VOL-TARGETED", cry_vt)
    book_f = 0.45*eng + 0.30*don + 0.15*cry_fixed + 0.10*gld
    book_v = 0.45*eng + 0.30*don + 0.15*cry_vt + 0.10*gld
    summ("BOOK w/ fixed crypto (ref)", book_f)
    summ("BOOK w/ vol-targeted crypto", book_v)

    # ── C. Donchian upgrades ─────────────────────────────────────────────────
    print("— C. donchian: speed ensemble + ATR sizing —")
    DV.N_ENTRY, DV.M_EXIT, DV.K = 20, 10, 12
    d2010 = DV.donchian_portfolio(panel).pct_change().reindex(cal).fillna(0.0)
    DV.N_ENTRY, DV.M_EXIT, DV.K = 55, 20, 12
    d5520 = DV.donchian_portfolio(panel).pct_change().reindex(cal).fillna(0.0)
    DV.N_ENTRY, DV.M_EXIT, DV.K = 40, 20, 12
    ens = (don + d2010 + d5520) / 3
    summ("donchian 40/20 single (ref)", don)
    summ("donchian ensemble 20/10+40/20+55/20", ens)
    book_e = 0.45*eng + 0.30*ens + 0.15*cry_vt + 0.10*gld
    summ("BOOK w/ ensemble + vt-crypto", book_e)


if __name__ == "__main__":
    main()
