"""Parameter sweep for pairs_trading — is there ANY config with Sharpe>0.8?
Optimised: precompute the point-in-time z-score once per (lookback,zwin),
then reuse across (z_entry,z_exit,z_stop,hold) combos.
"""
import sys, pathlib, itertools
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import data as D, metrics as M
import pairs_trading_backtest as P

closes = D.close_panel(sorted({s for pr in P.PAIRS for s in pr}), P.START, P.END)

# Precompute z-scores + returns per pair per (lookback, zwin)
Z_CACHE = {}
def zcache(lookback, zwin):
    key = (lookback, zwin)
    if key in Z_CACHE:
        return Z_CACHE[key]
    P.LOOKBACK_DAYS, P.ZSCORE_WINDOW = lookback, zwin
    out = {}
    for s1, s2 in P.PAIRS:
        df = closes[[s1, s2]].dropna()
        z = P._zscore_series(df[s1], df[s2])
        out[(s1, s2)] = (df, z, df[s1].pct_change().fillna(0), df[s2].pct_change().fillna(0))
    Z_CACHE[key] = out
    return out

def sim(df, z, r1, r2, z_entry, z_exit, z_stop, hold):
    cost = P.COST_BPS/1e4; pp = P.POSITION_PCT
    daily = np.zeros(len(df)); trades=[]
    pos=0; held=0; tr=0.0
    zv=z.values; a1=r1.values; a2=r2.values
    for i in range(1,len(df)):
        zi=zv[i-1]
        if pos!=0:
            pnl = pp*(a1[i]-a2[i]) if pos==1 else pp*(a2[i]-a1[i])
            daily[i]+=pnl; tr+=pnl; held+=1
            zn=zv[i]
            ex = (np.isfinite(zn) and (abs(zn)<=z_exit or abs(zn)>=z_stop)) or held>=hold
            if ex:
                c=2*cost*pp; daily[i]-=c; tr-=c; trades.append(tr); pos=0; held=0; tr=0.0
            continue
        if np.isfinite(zi):
            if zi>z_entry: pos=-1
            elif zi<-z_entry: pos=1
            if pos!=0:
                c=2*cost*pp; daily[i]-=c; tr=-c; held=0
    return daily, trades

def run(z_entry,z_exit,z_stop,hold,lookback,zwin):
    cache=zcache(lookback,zwin)
    dailies=[]; trades=[]
    for pair in P.PAIRS:
        df,z,r1,r2=cache[pair]
        d,t=sim(df,z,r1,r2,z_entry,z_exit,z_stop,hold)
        dailies.append(pd.Series(d,index=df.index)); trades+=t
    port=pd.concat(dailies,axis=1).fillna(0).sum(axis=1)
    eq=(1+port).cumprod()
    return M.summarize(eq), M.trade_stats(trades)

grid=list(itertools.product([1.5,2.0,2.5],[0.0,0.5,1.0],[3.0,4.0,5.0],[15,20,40],[63,90],[20,30]))
print(f"Sweeping {len(grid)} combos...",flush=True)
results=[]
for combo in grid:
    ze,zx,zs,h,lb,zw=combo
    m,ts=run(ze,zx,zs,h,lb,zw)
    results.append((combo,m,ts))
results.sort(key=lambda r:(r[1]['Sharpe'] if r[1]['Sharpe']==r[1]['Sharpe'] else -9),reverse=True)
print(f"\n{'z_ent':>5}{'z_ex':>5}{'z_st':>5}{'hold':>5}{'lb':>4}{'zw':>4}  {'Sharpe':>7}{'CAGR':>8}{'MaxDD':>8}{'Trades':>7}{'Win%':>6}{'PF':>5}")
for combo,m,ts in results[:12]:
    ze,zx,zs,h,lb,zw=combo
    print(f"{ze:>5}{zx:>5}{zs:>5}{h:>5}{lb:>4}{zw:>4}  {m['Sharpe']:>7.2f}{m['CAGR']*100:>7.2f}%{m['MaxDD']*100:>7.2f}%{ts['trades']:>7}{ts['win_rate']*100:>5.0f}%{ts['profit_factor']:>5.2f}")
print("\nBest Sharpe:",round(results[0][1]['Sharpe'],3),flush=True)
