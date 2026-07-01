# AlphaBot Backtest Harness & Roadmap #1/#4 Findings

Reusable backtesting infrastructure plus the first rigorous validation of the
three priority-roadmap strategies (`options_flow`, `squeeze_screener`,
`pairs_trading`). Built to honour the project's golden rule:

> **Backtest every change before implementing it. Only implement when the
> backtest proves it's an improvement.**

## Headline (full-fleet scorecard, added later)

After the three roadmap strategies, the harness was pointed at every
backtestable equity strategy in the bot (`scorecard.py`). The single most
important result of this whole exercise:

> **No entry signal beats a trivial baseline.** "Enter these mega-caps every 10
> days at random and use the bot's own exit engine" returns **+0.42%/trade
> (PF 1.28)**. Every strategy either *matches* that (`multi_tf_rsi`,
> `quality_momentum`) or is *worse* than random entry timing (`breakout`,
> `momentum`, `52wh_vol`, `trend_pullback`, `cs_momentum`), with `gap_scanner`
> and `mean_reversion` barely above zero.

The bot's returns come from **survivorship beta + the exit engine (trailing
ratchet + 20% TP) + being long quality names** — *not* from entry alpha. The
19-strategy entry apparatus is, at best, neutral vs random on this universe.
See §4 for the table and the (important) caveats.

## TL;DR — what the numbers say

| Strategy | Backtestable free? | Verdict | Evidence |
|---|---|---|---|
| `pairs_trading` (#4) | ✅ fully | **No edge** — do not rely on it | Shipped config Sharpe **0.07**, CAGR 0.05% (2015–2026). Best of **324** param combos = Sharpe **0.45**. Never clears the 0.8 bar. |
| `squeeze_screener` (#1) | ⚠️ core only | **Misconfigured + premise backwards** | (a) Live UNIVERSE is all mega-caps (short float ~1–3%) vs a 15% gate → **never fires in prod**. (b) On a genuinely squeeze-prone universe the entry core is **−0.31%/trade** (30% win, PF 0.91). |
| `options_flow` (#1) | ❌ no | **Cannot be validated** with free data | Flow signal needs historical per-strike option volume/OI (paid only). Exit geometry floor is a benign +0.44%/trade — not flow alpha. |

**Bonus finding (with a sting in the tail):** the squeeze entry *core* (5-day
return ≥ +3%, volume ≥ 1.5× 20d, RSI 45–72) looks like a momentum-continuation
edge on quality mega-caps — **but only under a fixed +12%/−5%/10-day bracket**
(+1.09%/trade, robust across sub-periods). Run through the bot's *actual* exit
engine (trailing 5% ratchet stop + 20% TP, which is what squeeze positions get
in prod) the **same entry is −0.94%/trade** and negative in all 81 sweep combos.
The "edge" is exit-structure-dependent, not a robust entry edge — see §2(c).

## Files

| File | Purpose |
|---|---|
| `data.py` | Daily OHLCV fetcher via Yahoo's chart API over `requests` (yfinance's curl_cffi transport fails TLS through the agent proxy). Disk-cached (pickle), split/div-adjusted. |
| `metrics.py` | Sharpe, Sortino, CAGR, MaxDD, Calmar, win rate, profit factor, expectancy. |
| `pairs_trading_backtest.py` | Mirrors the live pairs logic (OLS hedge ratio, z-score entry/exit/stop, dollar-neutral legs, 20d max hold). Point-in-time, no look-ahead. |
| `pairs_sweep.py` | 324-combo parameter sweep over z_entry/z_exit/z_stop/hold/lookback/zwin. |
| `squeeze_backtest.py` | Squeeze entry core on a curated squeeze-prone universe **and** on the live mega-cap universe (control). |
| `options_flow_backtest.py` | Documents why the flow signal is unbacktestable free; measures the +8%/−4%/7d exit floor only. |
| `momentum_pop_backtest.py` | Re-tests the mega-cap momentum-pop entry under a faithful replica of the LIVE exit engine (trailing ratchet + 20% TP). Shows the §2 edge is exit-dependent and −EV in production. |

## Running

```bash
pip install yfinance pandas numpy scipy tzdata   # requests comes with yfinance
python backtests/pairs_trading_backtest.py
python backtests/pairs_sweep.py
python backtests/squeeze_backtest.py
python backtests/options_flow_backtest.py
```

First run fetches ~11 years for ~60 tickers (cached to `backtests/.cache/`,
gitignored); subsequent runs are instant.

## Detailed findings

### 1. `pairs_trading` — no edge (roadmap #4)

Classic large-cap duopolies (XOM/CVX, JPM/BAC, KO/PEP, HD/LOW, GS/MS),
market-neutral z-score reversion, exactly as shipped.

```
Pair       Trades  Win%   AvgRet  Sharpe  TotRet
XOM/CVX     104    59.6%  -0.001%  -0.02   -0.10%
JPM/BAC     104    58.7%  -0.016%  -0.41   -1.67%
KO/PEP      104    56.7%  -0.004%  -0.10   -0.44%
HD/LOW      104    55.8%   0.002%   0.03    0.13%
GS/MS       104    70.2%   0.026%   0.66    2.76%
PORTFOLIO:  Sharpe 0.07 | CAGR 0.05% | MaxDD -3.60% | PF 1.04
```

Win rate ~60% but avg loss > avg win (z-stop at 3.5 lets losers run, 20-day cap
truncates winners). A 324-combo sweep could not lift portfolio Sharpe above
**0.45**. This is textbook crowded-pairs decay on liquid large-caps.

**Recommendation:** disable for live capital, or keep only GS/MS as tiny
uncorrelated ballast. Do not treat it as a return source.

### 2. `squeeze_screener` — misconfigured, premise backwards (roadmap #1)

Two independent problems:

**(a) It can't fire in production.** The live `UNIVERSE` in `config.py` is 46
mega-caps (AAPL, MSFT, NVDA, …) whose short-float sits ~1–3%, far below
`SHORT_PCT_MIN = 0.15`. The daily scan finds zero candidates every day. (The
`score = short_pct × ret5 × vol_ratio ≥ 0.015` gate is also mis-scaled: the
minimum qualifying trade scores ~0.009 and would be rejected anyway.)

**(b) The premise is backwards.** Pointed at a genuinely squeeze-prone universe
(41 persistently-high-short-interest names — GME, AMC, MARA, FCEL, PLUG, …), the
entry core loses money:

```
Squeeze-prone universe: 2162 trades | Win 30% | PF 0.91 | Expectancy -0.31%/trade
Live mega-cap universe:  521 trades | Win 54% | PF 1.59 | Expectancy +1.09%/trade
```

High-short-interest low-quality names *dump* on a 3% volume pop; quality names
*continue*. The momentum-continuation edge is real — on the wrong universe the
sign flips. Sub-period robustness on mega-caps:

```
2015-2018: N=179 win 53% PF 1.49 exp +0.96%
2019-2022: N=181 win 51% PF 1.44 exp +0.87%
2023-2026: N=149 win 58% PF 1.83 exp +1.44%
```

**Note on survivorship:** the squeeze-prone universe *excludes* names already
delisted (BBBY, WISH, etc. — 404 on fetch), i.e. some of the worst blowups drop
out. So the true −EV of chasing these names is, if anything, understated here.

**(c) The mega-cap edge is EXIT-DEPENDENT — and −EV under the live engine.**
The +1.09%/trade above uses a fixed +12%/−5%/10-day bracket. But squeeze
positions in production go through the shared exit engine
(`trade_management.py`): a 5% initial stop that **trails/ratchets up** as the
position gains, a 20% TP cap, and no fixed timeout. Re-running the *same*
mega-cap momentum-pop entry through a faithful replica of that engine
(`momentum_pop_backtest.py`) flips the sign:

```
Fixed +12%/-5%/10d bracket : +1.09%/trade  (54% win, PF 1.59)
LIVE trailing/ratchet/20%TP : -0.94%/trade  (38% win, PF 1.13)  <-- what prod gives
```

An 81-combo entry-param sweep under the live engine is negative in **every**
combo (best −0.78%/trade). The trailing stop shakes the position out of fast
pops before they resolve; the entry has no robust edge independent of a
short-horizon fixed bracket.

**Recommendation:** do **not** repurpose `squeeze_screener` to this momentum-pop
core under the default exit engine — it loses money. The edge only survives with
a dedicated fixed +12%/−5%/10-day bracket that bypasses the trailing ratchet,
which is a deliberate per-strategy exit change (some strategies already carry
custom time-stops, so it's feasible — but it's invasive on a live account and
should be an explicit decision, not a silent repurpose). Absent that, disable
`squeeze_screener` (it never fires today anyway).

### 3. `options_flow` — unbacktestable with free data (roadmap #1)

The signal is unusual near-the-money OTM call volume vs open interest. Validating
it needs **historical per-strike option volume + OI**, which is paid-only (ORATS,
CBOE DataShop, Polygon options). `yfinance.option_chain()` returns only today's
snapshot. Any "+X% over 10 days" claim without paid options history is
unverifiable.

The +8%/−4%/7d **exit geometry** on the underlying returns a benign +0.44%/trade
on mega-caps — but that's just drift, not flow alpha.

**Recommendation:** keep `options_flow` off for live capital until a paid
options-history backtest demonstrates the flow signal itself adds edge.

## Why these changes were NOT auto-applied to the live bot

All three strategies are currently registered in `bot/main.py` and run against a
live (paper) account. Disabling or repurposing them changes live trading
behaviour, so those edits are left for the account owner to approve rather than
applied unilaterally. This directory is purely additive analysis — it does not
touch `bot/`.

---

## 4. Full-fleet strategy scorecard (`scorecard.py`, `engine.py`, `strategies_bt.py`)

Every backtestable equity strategy, run through a faithful replica of the LIVE
exit engine (per-strategy stop + peak-based trailing ratchet + 20% TP +
dead-money timeout), 2015–2026, on the live 50-name mega-cap universe. Entries
are point-in-time (gates through bar i, fill at bar i+1 open).

Ranked by per-trade expectancy. `vs_base` = expectancy minus the best trivial
baseline (always-long-above-MA50 / enter-every-10-days on the same universe).

```
strategy          verdict          N   win%    PF  exp/trade   vs_base
(baseline = +0.42%/trade, PF 1.28)
BASELINE_10d      —             8565    43%  1.28      0.42%     0.00%
multi_tf_rsi      ~BASELINE     3296    43%  1.26      0.41%    -0.01%
quality_momentum  ~BASELINE      374    49%  1.24      0.39%    -0.03%
cs_momentum       WORSE          501    45%  1.20      0.34%    -0.08%
breakout          WORSE         2367    43%  1.23      0.33%    -0.09%
trend_pullback    WORSE         3568    43%  1.21      0.32%    -0.10%
52wh_vol          WORSE         1728    42%  1.22      0.30%    -0.12%
momentum          WORSE          496    47%  1.18      0.28%    -0.13%
BASELINE_ma50     —             8548    42%  1.16      0.27%    -0.15%
gap_scanner       WORSE         2272    40%  1.03      0.05%    -0.37%
mean_reversion    WORSE         1976    42%  1.02      0.03%    -0.39%

dual_momentum (rotation sleeve): CAGR 10.64% | Sharpe 0.90 | MaxDD -16.9%
  SPY same window: Sharpe 1.04   -> underperforms buy-and-hold risk-adjusted,
  but lower drawdown = mild defensive/diversification value.
```

### How to read this

- **Absolute**: most strategies are mildly +EV (they make money) — because the
  universe trended up and the exit engine is good.
- **Relative (the real test)**: none beat random-timed entry on the same names.
  `gap_scanner` and `mean_reversion` are the only two that barely clear zero and
  are the worst vs baseline — they actively pick bad moments (chasing gaps /
  catching knives on mega-caps).
- **The lever is the exit + universe + sizing, not the entry.** This matches the
  earlier momentum-pop finding (§2c): the same entry was +1.09% or −0.94%/trade
  purely from the exit rule. Effort spent tuning entries is low-leverage; effort
  on the exit engine, position sizing, and cutting correlated sleeves is high.

### Caveats (why this is "no alpha vs baseline", not "these lose money")

1. **Survivorship** inflates *all* long results, baseline included — which is
   exactly why the comparison is baseline-*relative*. The relative ranking is
   fair; the absolute numbers are optimistic.
2. **Replica fidelity**: adaptive thresholds are set to documented defaults, and
   regime gating / position caps / cross-strategy cooldowns are not modelled.
   The live bot's regime gating (no longs in bear) likely improves real
   drawdowns vs this ungated test — but the baseline is ungated too, so the
   "entries ≈ random" conclusion holds like-for-like.
3. **No transaction costs.** At ~0.3–0.4%/trade expectancy, 5–10 bps round-trip
   on liquid mega-caps eats a meaningful slice but doesn't flip the ranking.
4. **Not modelled here** (need separate rigs): `vwap_reclaim` (intraday),
   `spy_dip`/`vix_reversal`/`short_hedge` (market-timed/short-side),
   `ai_research`/`earnings_*`/`insider_buying`/`event_driven` (external data).

### Recommendation

- **Cut the two genuine laggards:** `gap_scanner` and `mean_reversion` — barely
  break even and are the worst vs baseline. (Live-behaviour change → owner's call.)
- **Keep the rest, but stop treating them as alpha.** They harvest beta + the
  exit engine. The 7 long-momentum sleeves are highly correlated (see main
  findings §3) — consolidating them wouldn't cost much return and would cut
  complexity and correlated drawdown.
- **Redirect effort to the exit engine, position sizing, and drawdown/regime
  behaviour** — that's where the P&L actually comes from.
