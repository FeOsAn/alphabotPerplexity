"""
Workstream 1: does vol-targeted LEVERAGE add real CAGR at acceptable risk?
Two implementations compared:
  A. Margin: allow the exposure scalar above 1.0 (up to L), pay 6%/yr on the
     borrowed fraction (exposure beyond 1.0 of equity).
  B. Leveraged-ETF sleeve: replace w of the basket with QLD (2x QQQ) but ONLY
     while QQQ > its 200DMA (trend gate); no margin, no borrow cost.
Both still run under the standard overlay (vol target 0.15, 200DMA 0.60 cut).

Run:  python backtests/leverage_tests.py
"""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import data as D, metrics as M
from frontier_shift import load, basket_ew, overlay, FLOOR

MARGIN_RATE = 0.06


def sub(d, a, b):
    return M.summarize((1 + d.loc[a:b]).cumprod())["Sharpe"]


def report(name, d):
    m = M.summarize((1 + d.dropna()).cumprod())
    dd22 = M.summarize((1 + d.loc["2021-12-31":"2023-01-31"]).cumprod())["MaxDD"]
    print(f"{name:<34}{m['CAGR']*100:>6.1f}%{m['Sharpe']:>7.2f}{m['MaxDD']*100:>8.1f}%"
          f"{m['Calmar']:>7.2f}{dd22*100:>8.1f}%   "
          f"[{sub(d,'2015-01-01','2019-01-01'):.2f}/{sub(d,'2019-01-01','2023-01-01'):.2f}/{sub(d,'2023-01-01','2026-06-30'):.2f}]")


def main():
    panel, closes, rets, spy = load()
    ew = basket_ew(rets, closes)
    qld = D.get_history("QLD", "2014-01-01", "2026-06-30")
    qqq = D.get_history("QQQ", "2014-01-01", "2026-06-30")
    qld_r = qld["Close"].reindex(ew.index).ffill().pct_change().fillna(0.0)
    qqq_c = qqq["Close"].reindex(ew.index).ffill()

    print(f"{'variant':<34}{'CAGR':>7}{'Sharpe':>7}{'MaxDD':>8}{'Calmar':>7}{'2022DD':>8}   [sub-period Sharpes]")
    print("-" * 100)
    base = overlay(ew, spy)
    report("BASE engine (cap .9, vt .15)", base)

    # A. margin leverage: scalar clipped at L instead of 1.0
    for L in [1.25, 1.5]:
        rv = ew.rolling(20).std() * np.sqrt(252)
        vtsc = (0.15 / rv).clip(lower=FLOOR, upper=L)
        ma200 = spy.rolling(200).mean()
        mam = pd.Series(1.0, index=ew.index)
        mam[spy <= ma200] = 0.60
        scal = np.minimum(vtsc, L * mam).shift(1).fillna(0.0)
        expo = 0.90 * scal
        borrow = (expo - 1.0).clip(lower=0.0)
        d = expo * ew - borrow * (MARGIN_RATE / 252)
        report(f"A margin, max scalar {L}", d)

    # B. QLD sleeve, trend-gated, no margin
    gate = (qqq_c > qqq_c.rolling(200).mean()).shift(1).fillna(False)
    for w in [0.20, 0.30, 0.40]:
        comp = (1 - w) * ew + w * np.where(gate, qld_r, 0.0)
        comp = pd.Series(comp, index=ew.index)
        report(f"B QLD sleeve w={w:.0%} (trend-gated)", overlay(comp, spy))


if __name__ == "__main__":
    main()
