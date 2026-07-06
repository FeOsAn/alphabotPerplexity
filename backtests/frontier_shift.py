"""
Frontier-shift experiments: can we IMPROVE Sharpe/Calmar at similar drawdown,
rather than just trading CAGR vs DD along the cap/vol-target frontier?

Levers tested (all point-in-time, yesterday's data only):
  BASKET WEIGHTING (monthly rebalance):
    ew        — equal weight (current engine)
    invvol    — inverse 60d-vol weights (risk-parity-lite)
    mom25     — top 25 of 50 by 12-1 momentum, equal weight
    mom25iv   — top 25 by 12-1 momentum, inverse-vol weighted
  VOL ESTIMATOR for the targeting overlay (target 0.15, cap 0.9, floor 0.30):
    roll20    — 20d rolling std (current live)
    ewma94    — RiskMetrics EWMA lambda=0.94 (faster, less lag)
    roll60    — slower/steadier
  MA200 CUT DEPTH: 0.45 / 0.60 (current) / 0.75
  DONCHIAN BLEND: 85/15 and 75/25 engine/donchian sleeves.

Run:  python backtests/frontier_shift.py
"""
import sys, pathlib, itertools
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import data as D, metrics as M
from scorecard import UNIVERSE

START, END = "2014-01-01", "2026-06-30"
CAP, VT, FLOOR = 0.90, 0.15, 0.30


def load():
    panel = D.get_panel(UNIVERSE, START, END)
    aux = D.get_panel(["SPY"], START, END)
    closes = pd.DataFrame({s: panel[s]["Close"] for s in panel}).sort_index()
    rets = closes.pct_change()
    spy = aux["SPY"]["Close"].reindex(closes.index).ffill()
    return panel, closes, rets, spy


def month_ends(idx):
    return pd.Series(idx, index=idx).resample("ME").last().dropna()


def basket_ew(rets, closes):
    return rets.mean(axis=1)


def _weighted(rets, closes, pick_fn):
    """Monthly weights from pick_fn(i) -> {sym: w}; applied the following month."""
    idx = closes.index
    me = month_ends(idx)
    w = pd.DataFrame(0.0, index=idx, columns=closes.columns)
    for j in range(13, len(me) - 1):
        i = idx.get_loc(me.iloc[j])
        weights = pick_fn(i, closes)
        if not weights:
            continue
        nxt = idx.get_loc(me.iloc[j + 1])
        for s, ww in weights.items():
            w.iloc[i + 1:nxt + 1, w.columns.get_loc(s)] = ww
    return (w.shift(1) * rets).sum(axis=1)


def pick_invvol(i, closes):
    out = {}
    for s in closes.columns:
        c = closes[s].iloc[max(0, i - 60):i + 1].pct_change().dropna()
        if len(c) > 30:
            v = c.std()
            if v > 0:
                out[s] = 1.0 / v
    tot = sum(out.values())
    return {s: w / tot for s, w in out.items()} if tot > 0 else {}


def _mom121(i, closes, s):
    c = closes[s]
    if i < 252 or not np.isfinite(c.iloc[i - 252]) or not np.isfinite(c.iloc[i]):
        return None
    return (c.iloc[i] / c.iloc[i - 252] - 1) - (c.iloc[i] / c.iloc[i - 21] - 1)


def pick_mom25(i, closes):
    scored = [(m, s) for s in closes.columns if (m := _mom121(i, closes, s)) is not None]
    scored.sort(reverse=True)
    top = [s for _, s in scored[:25]]
    return {s: 1.0 / len(top) for s in top} if top else {}


def pick_mom25iv(i, closes):
    top = list(pick_mom25(i, closes).keys())
    if not top:
        return {}
    sub = pick_invvol(i, closes[top])
    return sub


def overlay(daily, spy, vol_kind="roll20", ma_mult=0.60, cap=CAP, vt=VT):
    if vol_kind == "roll20":
        rv = daily.rolling(20).std() * np.sqrt(252)
    elif vol_kind == "roll60":
        rv = daily.rolling(60).std() * np.sqrt(252)
    else:  # ewma94
        rv = daily.ewm(alpha=0.06).std() * np.sqrt(252)
    vtsc = (vt / rv).clip(lower=FLOOR, upper=1.0)
    ma200 = spy.rolling(200).mean()
    mam = pd.Series(1.0, index=daily.index)
    mam[spy.reindex(daily.index).ffill() <= ma200.reindex(daily.index).ffill()] = ma_mult
    scal = np.minimum(vtsc, mam).shift(1).fillna(0.0)
    return cap * scal * daily


def score(name, d):
    eq = (1 + d.dropna()).cumprod()
    m = M.summarize(eq)
    dd22 = M.summarize((1 + d.loc["2021-12-31":"2023-01-31"]).cumprod())["MaxDD"]
    print(f"{name:<34}{m['CAGR']*100:>6.1f}%{m['Sharpe']:>7.2f}{m['Sortino']:>8.2f}"
          f"{m['MaxDD']*100:>7.1f}%{m['Calmar']:>7.2f}{dd22*100:>7.1f}%")
    return m


def main():
    panel, closes, rets, spy = load()
    print(f"{'variant':<34}{'CAGR':>7}{'Sharpe':>7}{'Sortino':>8}{'MaxDD':>8}{'Calmar':>7}{'2022DD':>7}")
    print("-" * 78)

    baskets = {
        "ew": basket_ew(rets, closes),
        "invvol": _weighted(rets, closes, pick_invvol),
        "mom25": _weighted(rets, closes, pick_mom25),
        "mom25iv": _weighted(rets, closes, pick_mom25iv),
    }

    # 1) basket weighting (vol=roll20, ma=0.60)
    print("— basket weighting —")
    for name, b in baskets.items():
        score(f"{name} / roll20 / ma0.60", overlay(b, spy))

    # 2) vol estimator on the best-Sharpe basket will be re-run below; first on ew
    print("— vol estimator (ew basket) —")
    for vk in ["roll20", "ewma94", "roll60"]:
        score(f"ew / {vk} / ma0.60", overlay(baskets["ew"], spy, vol_kind=vk))

    # 3) ma200 depth (ew, roll20)
    print("— 200DMA cut depth (ew, roll20) —")
    for mm in [0.45, 0.60, 0.75]:
        score(f"ew / roll20 / ma{mm}", overlay(baskets["ew"], spy, ma_mult=mm))

    # 4) Donchian blend
    print("— Donchian blend (vs pure engine) —")
    from donchian_validate import donchian_portfolio
    deq = donchian_portfolio(panel)
    dret = deq.pct_change().reindex(baskets["ew"].index).fillna(0.0)
    eng = overlay(baskets["ew"], spy)
    for wd in [0.15, 0.25]:
        score(f"engine {int((1-wd)*100)}/{int(wd*100)} donchian",
              (1 - wd) * eng + wd * dret)

    # 5) combos of winners (printed for manual pick)
    print("— combos —")
    for bn in ["invvol", "mom25iv"]:
        for vk in ["roll20", "ewma94"]:
            score(f"{bn} / {vk} / ma0.60", overlay(baskets[bn], spy, vol_kind=vk))


if __name__ == "__main__":
    main()
