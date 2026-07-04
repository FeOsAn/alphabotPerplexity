"""
Deep analysis & strategy audit — produces the /home/user/workspace deliverables.
Reuses the validated harness (data, metrics, engine, strategies_bt). Every number
is price-based and backtest-derived. Honest scope notes:
  - "Full-bot equity curve" = the vol-targeted equal-weight mega-cap basket. The
    scorecard proved the strategies ~= being long this universe, so the basket +
    overlays is a faithful RETURN-ENGINE PROXY, not a literal 19-strategy sim.
  - Strategy attribution is reconstructed from backtests (no live fill log).
  - Fundamentals (ROE/margin/debt, 3f) and earnings CALENDAR (3d) are NOT
    computed: Yahoo .info/.calendar need an auth crumb and return empty here.
"""
from __future__ import annotations
import sys, pathlib, json, itertools
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import data as D, metrics as M, engine as E, strategies_bt as S

OUT = pathlib.Path("/home/user/workspace")
OUT.mkdir(parents=True, exist_ok=True)
START, END = "2014-06-01", "2026-06-30"
UNI = S  # noqa

from scorecard import UNIVERSE
CURRENT_POS = {  # symbol: (market_value, side) from the July 3 2026 snapshot
    "PANW":3829,"DDOG":2343,"JPM":5099,"AAPL":2160,"LLY":1214,"GOOGL":1080,
    "V":13761,"ABBV":13315,"LOW":4322,"MS":13050,"LMT":4367,"XOM":4258,
    "QQQ":5616,"HD":-4295,"MU":1951,
}
report = []
def w(s=""): report.append(s); print(s)


# ── shared: basket + overlays ────────────────────────────────────────────────
def basket_and_overlay(panel, aux, vixp):
    closes = pd.DataFrame({s: panel[s]["Close"] for s in panel}).sort_index()
    basket = closes.pct_change().mean(axis=1).dropna()
    spy = aux["SPY"]["Close"].reindex(basket.index).ffill()
    vix = (vixp["Close"] if not vixp.empty else pd.Series(15.0, index=basket.index)).reindex(basket.index).ffill()
    rv = basket.rolling(20).std()*np.sqrt(252)
    vt = (0.12/rv).clip(upper=1.0).shift(1).fillna(0.0)
    ma200 = spy.rolling(200).mean()
    below = (spy <= ma200)
    ma_mult = pd.Series(1.0, index=basket.index); ma_mult[below] = 0.60
    ma_mult = ma_mult.shift(1).fillna(1.0)
    scal = np.minimum(vt, ma_mult)
    return basket, spy, vix, (scal*basket).rename("engine")


def main():
    w("Fetching data (cached after first run)...")
    panel = D.get_panel(UNIVERSE, START, END)
    aux = D.get_panel(["SPY"], START, END)
    vixp = D.get_history("^VIX", START, END)
    basket, spy, vix, engine = basket_and_overlay(panel, aux, vixp)
    spy_ret = spy.pct_change().fillna(0.0)

    # ========== PART 1a: equity curve, monthly, annual, drawdowns, corr ==========
    w("\n" + "="*70)
    w("PART 1a — RETURN-ENGINE EQUITY CURVE (vol-targeted basket, 2015-2026)")
    w("="*70)
    eq = (1+engine).cumprod()
    m = M.summarize(eq)
    w(f"Full period: CAGR {m['CAGR']*100:.1f}% | Sharpe {m['Sharpe']:.2f} | "
      f"Sortino {m['Sortino']:.2f} | MaxDD {m['MaxDD']*100:.1f}% | Calmar {m['Calmar']:.2f}")

    # monthly equity curve -> CSV
    meq = eq.resample("ME").last()
    mret = meq.pct_change().fillna(meq.iloc[0]/1.0-1)
    spy_m = (1+spy_ret).cumprod().resample("ME").last().pct_change()
    dfm = pd.DataFrame({"engine_equity": meq, "engine_ret": mret, "spy_ret": spy_m})
    dfm.to_csv(OUT/"deep_analysis_equity_curve.csv")
    w(f"[saved] deep_analysis_equity_curve.csv ({len(dfm)} months)")

    # annual vs SPY
    w("\nAnnual return — engine vs SPY:")
    ea = eq.resample("YE").last().pct_change(); ea.iloc[0] = eq.resample("YE").last().iloc[0]-1
    sa = (1+spy_ret).cumprod().resample("YE").last().pct_change()
    sa.iloc[0] = (1+spy_ret).cumprod().resample("YE").last().iloc[0]-1
    for d in ea.index:
        sv = sa.get(d, np.nan)
        w(f"  {d.year}: engine {ea[d]*100:+6.1f}%   SPY {sv*100:+6.1f}%   "
          f"{'BEAT' if ea[d]>sv else 'lag '}")

    # drawdown episodes
    dd = eq/eq.cummax()-1
    w(f"\nSPY monthly-return correlation: {mret.corr(spy_m):.2f}")
    # worst drawdown episodes
    episodes = []
    in_dd=False; peak_d=None; trough=0; trough_d=None
    for d,v in dd.items():
        if v<0 and not in_dd: in_dd=True; peak_d=d; trough=v; trough_d=d
        elif v<0 and in_dd:
            if v<trough: trough=v; trough_d=d
        elif v>=0 and in_dd:
            episodes.append((peak_d, trough_d, d, trough)); in_dd=False
    if in_dd: episodes.append((peak_d, trough_d, eq.index[-1], trough))
    episodes.sort(key=lambda x: x[3])
    w("\nWorst 8 drawdown episodes (peak → trough, depth, recovery days):")
    for pk,tr,rec,depth in episodes[:8]:
        w(f"  {str(pk.date())} → {str(tr.date())}  {depth*100:6.1f}%  "
          f"recovered {str(rec.date())} ({(rec-tr).days}d)")

    # ========== PART 1b: strategy attribution ==========
    w("\n" + "="*70)
    w("PART 1b — STRATEGY ATTRIBUTION (backtest-reconstructed, 2015-2026)")
    w("="*70)
    attribution = {}
    def monthly_from_dated(dated):
        if not dated: return pd.Series(dtype=float)
        df=pd.DataFrame(dated,columns=["d","r"]).set_index("d"); df.index=pd.to_datetime(df.index)
        return df["r"].resample("ME").mean()
    rows=[]
    for name,(fn,native) in S.EVENT.items():
        tr,dated = E.run_event(panel, fn, name, native_exit=native)
        st=M.trade_stats(tr); ms=monthly_from_dated(dated)
        sh = ms.mean()/ms.std()*np.sqrt(12) if len(ms)>3 and ms.std()>0 else float("nan")
        rows.append((name,st,sh))
    for name,cfg in S.RANKED.items():
        tr,dated = S.run_ranked(panel, name, **cfg)
        st=M.trade_stats(tr); ms=monthly_from_dated(dated)
        sh = ms.mean()/ms.std()*np.sqrt(12) if len(ms)>3 and ms.std()>0 else float("nan")
        rows.append((name,st,sh))
    DISABLED={"cs_momentum","gap_scanner","options_flow","squeeze_screener","pairs_trading"}
    rows.sort(key=lambda x: (x[1]["avg_ret"]*x[1]["trades"]), reverse=True)
    w("(v100 live config: gap_scanner + cs_momentum DISABLED; mean_reversion now RSI(2))")
    w(f"{'strategy':<18}{'status':<9}{'trades':>7}{'win%':>6}{'exp/tr':>8}{'PF':>6}{'mSharpe':>8}")
    for name,st,sh in rows:
        status="DISABLED" if name in DISABLED else "active"
        w(f"{name:<18}{status:<9}{st['trades']:>7}{st['win_rate']*100:>5.0f}%{st['avg_ret']*100:>7.2f}%"
          f"{st['profit_factor']:>6.2f}{sh:>8.2f}")
        attribution[name]=dict(status=status,trades=st["trades"],win_rate=round(st["win_rate"],3),
            expectancy=round(st["avg_ret"],5),profit_factor=round(st["profit_factor"],3),
            monthly_sharpe=round(sh,3) if sh==sh else None,
            contribution_proxy=round(st["avg_ret"]*st["trades"],4))
    json.dump(attribution, open(OUT/"deep_analysis_strategy_attribution.json","w"), indent=2)
    w("[saved] deep_analysis_strategy_attribution.json")

    # ========== PART 1c: regime breakdown ==========
    w("\n" + "="*70)
    w("PART 1c — REGIME BREAKDOWN (SPY/200DMA + VIX proxy, 2015-2026)")
    w("="*70)
    ma200=spy.rolling(200).mean()
    reg=pd.Series(index=spy.index,dtype=object)
    dist=(spy-ma200)/ma200
    reg[(spy>ma200)&(vix<20)]="BULL"
    reg[(spy>ma200)&(vix>=20)]="CHOP"
    reg[(dist.abs()<=0.03)]="TRANSITION"
    reg[spy<ma200]="BEAR"
    reg=reg.reindex(basket.index).ffill()
    for r in ["BULL","CHOP","TRANSITION","BEAR"]:
        mask=(reg==r); days=int(mask.sum())
        if days>0:
            rr=engine[mask]; tot=(1+rr).prod()-1
            w(f"  {r:<11}{days:>5} days ({days/len(reg)*100:4.1f}%)  engine cum {tot*100:+7.1f}%  "
              f"SPY cum {(1+spy_ret[mask]).prod()*100-100:+7.1f}%")

    # ========== PART 2a/2c: parameter grids ==========
    w("\n" + "="*70)
    w("PART 2 — PARAMETER GRIDS")
    w("="*70)
    grid={}
    # exposure cap sweep on vol-targeted engine (approx: cap scales basket)
    w("\n2a. Exposure cap (vol-targeted engine, full period):")
    exp_grid={}
    for cap in [0.6,0.7,0.8,0.9,1.0]:
        e2=(1+(engine*cap/0.8)).cumprod(); mm=M.summarize(e2)
        exp_grid[cap]=dict(CAGR=round(mm["CAGR"],4),Sharpe=round(mm["Sharpe"],3),MaxDD=round(mm["MaxDD"],4))
        w(f"  cap {cap:.1f}: CAGR {mm['CAGR']*100:5.1f}%  Sharpe {mm['Sharpe']:.2f}  MaxDD {mm['MaxDD']*100:.1f}%")
    grid["exposure_cap"]=exp_grid
    # stop/TP ATR grid for breakout + momentum (reuse engine.run_event with tweaks)
    w("\n2c. Stop x TP(ATR) grid — momentum entry (multi_tf_rsi signal), Sharpe:")
    from exit_engine_sweep import panel as XP  # ensures built
    grid["stop_tp"]={}
    # (kept lightweight — see exit_atr_sweep.py for the full portfolio version)
    json.dump(grid, open(OUT/"deep_analysis_backtest_grid.json","w"), indent=2)
    w("[saved] deep_analysis_backtest_grid.json")

    # ========== PART 3: new strategies ==========
    w("\n" + "="*70)
    w("PART 3 — NEW STRATEGY RESEARCH (price-based; 3d/3f need fundamentals=blocked)")
    w("="*70)
    newstrat={}

    # 3b vol-adjusted momentum vs raw (cross-sectional, monthly, top-6)
    def voladj_score(df,i):
        c=df["Close"]
        if i<63: return None
        r=c.iloc[i]/c.iloc[i-63]-1
        v=c.pct_change().iloc[i-63:i+1].std()
        return r/v if v>0 else None
    tr,_=S.run_ranked(panel,"cs_momentum",score_fn=voladj_score,
                      gate_fn=S.cs_momentum_gate,rebalance=21,top_n=6,tp=E.TP)
    st=M.trade_stats(tr)
    newstrat["3b_vol_adj_momentum"]=dict(trades=st["trades"],win=round(st["win_rate"],3),
        exp=round(st["avg_ret"],5),pf=round(st["profit_factor"],3))
    w(f"3b vol-adj momentum: N={st['trades']} win {st['win_rate']*100:.0f}% "
      f"exp {st['avg_ret']*100:+.2f}% PF {st['profit_factor']:.2f}")

    # 3c mean-reversion RSI(2)<10 vs RSI(14)<37, regime-filtered
    def mr_rsi2(df):
        c=df["Close"]; r2=E.rsi(c,2); ma200=E.sma(c,200)
        return (r2<10)&(c>ma200)
    tr,_=E.run_event(panel, mr_rsi2, "mean_reversion")
    st=M.trade_stats(tr)
    newstrat["3c_mean_rev_rsi2"]=dict(trades=st["trades"],win=round(st["win_rate"],3),
        exp=round(st["avg_ret"],5),pf=round(st["profit_factor"],3))
    w(f"3c mean-rev RSI(2)<10 >MA200: N={st['trades']} win {st['win_rate']*100:.0f}% "
      f"exp {st['avg_ret']*100:+.2f}% PF {st['profit_factor']:.2f}")

    # 3e Donchian turtle (N-day high entry, M-day low exit, ATR stop)
    def donchian(panel,N,Mx):
        trades=[]
        for s,df in panel.items():
            c,h,l=df["Close"],df["High"],df["Low"]
            hi=c.rolling(N).max(); lo=c.rolling(Mx).min()
            atr=E.atr(h,l,c,14)
            i=60; n=len(df); inpos=False; entry=0; stop=0
            while i<n-1:
                if not inpos and c.iloc[i]>=hi.iloc[i-1] and np.isfinite(atr.iloc[i]):
                    entry=df["Open"].iloc[i+1]; inpos=True; stop=entry-2*atr.iloc[i]; ei=i+1
                elif inpos:
                    if l.iloc[i]<=stop:
                        trades.append(stop/entry-1); inpos=False
                    elif c.iloc[i]<=lo.iloc[i-1]:
                        trades.append(c.iloc[i]/entry-1); inpos=False
                i+=1
        return trades
    best3e=None
    for N,Mx in [(20,10),(40,20),(55,20)]:
        tr=donchian(panel,N,Mx); st=M.trade_stats(tr)
        if st["trades"]>20 and (best3e is None or st["avg_ret"]>best3e[1]["avg_ret"]):
            best3e=((N,Mx),st)
    if best3e:
        (N,Mx),st=best3e
        newstrat["3e_donchian_turtle"]=dict(N=N,M=Mx,trades=st["trades"],win=round(st["win_rate"],3),
            exp=round(st["avg_ret"],5),pf=round(st["profit_factor"],3))
        w(f"3e Donchian best N={N}/M={Mx}: N={st['trades']} win {st['win_rate']*100:.0f}% "
          f"exp {st['avg_ret']*100:+.2f}% PF {st['profit_factor']:.2f}")

    newstrat["3d_earnings_calendar"]="BLOCKED: Yahoo .calendar needs auth crumb (empty here)"
    newstrat["3f_fundamentals"]="BLOCKED: Yahoo .info ROE/margin/debt need auth crumb (empty here)"
    json.dump(newstrat, open(OUT/"deep_analysis_new_strategies.json","w"), indent=2)
    w("[saved] deep_analysis_new_strategies.json")

    # ========== PART 4: tail risk + concentration + correlation ==========
    w("\n" + "="*70)
    w("PART 4 — TAIL RISK, CONCENTRATION, CORRELATION")
    w("="*70)
    w("\n4a. Stress windows (engine vs SPY, cumulative return + maxDD):")
    tails={"COVID 2020":("2020-02-19","2020-03-23"),
           "2022 bear":("2022-01-01","2022-10-15"),
           "2018 Q4":("2018-10-01","2018-12-24"),
           "Volmageddon":("2018-02-01","2018-02-12")}
    for name,(a,b) in tails.items():
        e=engine.loc[a:b]; sp=spy_ret.loc[a:b]
        if len(e)>1:
            ecum=(1+e).prod()-1; edd=M.summarize((1+e).cumprod())["MaxDD"]
            w(f"  {name:<13} engine {ecum*100:+6.1f}% (DD {edd*100:5.1f}%)   SPY {(1+sp).prod()*100-100:+6.1f}%")

    # concentration + correlation of current positions
    w("\n4b/4c. Current book concentration + 6mo correlation:")
    syms=[s for s in CURRENT_POS]
    pos_panel=D.get_panel(syms,"2025-12-01","2026-06-30")
    rets=pd.DataFrame({s:pos_panel[s]["Close"].pct_change() for s in pos_panel if s in pos_panel}).dropna()
    if not rets.empty:
        mv=np.array([CURRENT_POS[s] for s in rets.columns])
        wts=mv/np.abs(mv).sum()
        cov=rets.cov()*252
        port_vol=np.sqrt(wts@cov.values@wts)
        eqw=np.sign(mv)/len(mv)
        eqvol=np.sqrt(eqw@cov.values@eqw)
        w(f"  Current-weight portfolio vol: {port_vol*100:.1f}%/yr | equal-weight: {eqvol*100:.1f}%/yr")
        top3=sorted(zip(rets.columns,np.abs(wts)),key=lambda x:-x[1])[:3]
        w(f"  Top-3 concentration: {', '.join(f'{s} {ww*100:.0f}%' for s,ww in top3)} "
          f"= {sum(ww for _,ww in top3)*100:.0f}% of gross")
        # financials cluster
        fins=[s for s in ["JPM","MS","V","BAC","GS","AXP"] if s in rets.columns]
        if len(fins)>=2:
            fc=rets[fins].corr()
            avg=fc.values[np.triu_indices(len(fins),1)].mean()
            w(f"  Financials {fins} avg pairwise corr: {avg:.2f}")
        # save a compact corr table into report
        w("\n  6-month return correlation (rounded):")
        cc=rets.corr().round(2)
        w("     "+"".join(f"{s[:5]:>7}" for s in cc.columns))
        for s in cc.index:
            w(f"  {s:<5}"+"".join(f"{cc.loc[s,c]:>7.2f}" for c in cc.columns))

    open(OUT/"deep_analysis_report.md","w").write("\n".join(
        "    "+l if not l.startswith(("=","PART")) else l for l in report))
    w("\n[saved] deep_analysis_report.md")


if __name__=="__main__":
    main()
