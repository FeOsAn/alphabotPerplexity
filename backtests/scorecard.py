"""
Strategy scorecard: run every backtestable live strategy through the REAL exit
engine and rank by genuine edge. Keep/kill verdicts.

  KEEP     : expectancy > 0 AND profit factor >= 1.2 AND >= 100 trades
  MARGINAL : expectancy > 0 but PF < 1.2 (or thin sample)
  KILL     : expectancy <= 0

Rotation sleeves (dual_momentum) are judged on Sharpe of their monthly series,
not per-trade stats.

Run:  python backtests/scorecard.py
"""
from __future__ import annotations
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import data as D, metrics as M, engine as E, strategies_bt as S

UNIVERSE = [
    "AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","LLY","PYPL","JPM","V",
    "XOM","AVGO","PG","MA","JNJ","HD","MRK","ABBV","CVX","COST","CRM","BAC",
    "NFLX","AMD","ADBE","WMT","MCD","CSCO","ORCL","TXN","COP","RTX","AMGN",
    "INTU","SPGI","CAT","BKNG","GE","HON","AXP","MS","GS","LMT","ISRG","VRTX",
    "NOW","PANW","REGN","KLAC",
]
ETFS = ["SPY","QQQ","IWM","GLD","XLF","XLE","XLK","XLV"]
START, END, YEARS = "2015-01-01", "2026-06-30", 11.46


def verdict(exp, pf, n):
    if exp <= 0:
        return "KILL"
    if pf >= 1.2 and n >= 100:
        return "KEEP"
    return "MARGINAL"


def summarize_trades(name, trades):
    st = M.trade_stats(trades)
    if st["trades"] == 0:
        return None
    # True expectancy is the mean of ALL trade returns (incl. breakeven exits).
    # The win%*avgWin + loss%*avgLoss reconstruction is wrong when the trailing
    # stop produces exactly-0 trades (counted as neither win nor loss).
    exp = st["avg_ret"]
    tpy = st["trades"]/YEARS
    return dict(name=name, n=st["trades"], win=st["win_rate"], pf=st["profit_factor"],
                exp=exp, tpy=tpy, edge=exp*tpy, verdict=verdict(exp, st["profit_factor"], st["trades"]))


def main():
    print("Fetching universe (this is cached after first run)...")
    panel = D.get_panel(UNIVERSE, START, END)
    etf_panel = D.get_panel(ETFS, START, END)
    full = {**panel, **etf_panel}
    print(f"Equity names: {len(panel)} | ETFs: {len(etf_panel)}\n")

    rows = []

    # ── BASELINE CONTROLS (survivorship check) ────────────────────────────────
    # If a strategy doesn't beat "just be long these mega-caps", its edge is
    # survivorship beta, not signal. baseline_ma50 = enter whenever above MA50
    # (trivial long filter); baseline_10d = enter every 10 days unconditionally.
    import engine as _E
    def _above_ma50(df):
        return df["Close"] > _E.sma(df["Close"], 50)
    def _every10(df):
        s = pd.Series(False, index=df.index)
        s.iloc[::10] = True
        return s
    base_ma50, _ = E.run_event(panel, _above_ma50, "momentum")
    base_10d, _ = E.run_event(panel, _every10, "momentum")
    base_ma50_exp = M.trade_stats(base_ma50)["avg_ret"]
    base_10d_exp = M.trade_stats(base_10d)["avg_ret"]
    rows.append(summarize_trades("BASELINE_ma50", base_ma50))
    rows.append(summarize_trades("BASELINE_10d", base_10d))
    baseline = max(base_ma50_exp, base_10d_exp)

    # EVENT strategies
    for name, (fn, native) in S.EVENT.items():
        trades, _ = E.run_event(panel, fn, name, native_exit=native)
        r = summarize_trades(name, trades)
        if r:
            rows.append(r)

    # RANKED strategies
    for name, cfg in S.RANKED.items():
        trades, _ = S.run_ranked(panel, name, **cfg)
        r = summarize_trades(name, trades)
        if r:
            rows.append(r)

    # edge vs baseline: does the signal beat trivially-being-long the winners?
    for r in rows:
        r["vs_base"] = r["exp"] - baseline
        if not r["name"].startswith("BASELINE"):
            r["verdict"] = "REAL EDGE" if r["vs_base"] > 0.0005 else \
                           ("~BASELINE" if r["vs_base"] > -0.0005 else "WORSE")
    rows.sort(key=lambda x: x["exp"], reverse=True)

    print("="*96)
    print(f"{'strategy':<18}{'verdict':<12}{'N':>6}{'win%':>7}{'PF':>6}"
          f"{'exp/trade':>11}{'vs_base':>10}")
    print(f"(baseline = best of always-long-MA50 / every-10-days on same universe = "
          f"{baseline*100:+.2f}%/trade)")
    print("-"*96)
    for r in rows:
        print(f"{r['name']:<18}{r['verdict']:<12}{r['n']:>6}"
              f"{r['win']*100:>6.0f}%{r['pf']:>6.2f}{r['exp']*100:>10.2f}%{r['vs_base']*100:>9.2f}%")
    print("="*96)

    # ROTATION: dual_momentum monthly series
    dm = S.dual_momentum_returns(full)
    if len(dm) > 12:
        eq = (1+dm).cumprod()
        m = M.summarize(eq)
        # monthly Sharpe -> annualised
        sharpe = dm.mean()/dm.std()*np.sqrt(12) if dm.std() > 0 else float("nan")
        print(f"\ndual_momentum (rotation sleeve, monthly): "
              f"CAGR {m['CAGR']*100:.2f}% | Sharpe {sharpe:.2f} | MaxDD {m['MaxDD']*100:.1f}% "
              f"| months {len(dm)}")
        # SPY benchmark
        spy = full.get("SPY")
        if spy is not None:
            spm = spy["Close"].resample("ME").last().pct_change().dropna()
            spm = spm.loc[dm.index[0]:dm.index[-1]]
            spsh = spm.mean()/spm.std()*np.sqrt(12) if spm.std() > 0 else float("nan")
            print(f"  SPY same window: Sharpe {spsh:.2f}")

    print("\nNotes:")
    print("- Exits = live engine (per-strategy stop + trailing ratchet + 20% TP + dead-money).")
    print("- cs_momentum run with NO take-profit (tp=None) per its live design.")
    print("- vwap_reclaim/spy_dip/vix_reversal/short_hedge/ai_research/earnings_*/insider/")
    print("  event_driven NOT here: intraday, short-side, or external-data — need separate rigs.")


if __name__ == "__main__":
    main()
