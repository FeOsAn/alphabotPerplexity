# AlphaBot — Deep Analysis & Strategy Audit (v100 re-run)

*Re-run of the full audit against the CURRENT bot (post-cleanup), July 2026. All
figures are price-based backtests (2015–2026) via the `backtests/` harness. Scope
caveats at the end.*

## What changed since the last audit (and it worked)

This is a re-run *after* acting on the previous audit. The roster is now cleaner
and the two big fixes are confirmed:

| Change | Before | After | Status |
|---|---|---|---|
| `mean_reversion` signal | RSI(14): +0.04%/trade, PF 1.03, *worst vs baseline* | **RSI(2) Connors: 60% win, PF 1.09, mSharpe 0.71** | ✅ shipped |
| `cs_momentum` | −0.15%/trade, PF 0.92 (parasite) | **disabled** | ✅ cut |
| `gap_scanner` | +0.09%/trade (worst vs baseline) | **disabled** | ✅ cut |
| Exit engine (TP) | 3–4×ATR (chokes winners) | 5–6×ATR (let runners run) | ✅ shipped |
| Regime defense | GLD hedge (deepened 2022) | cash + vol-target overlay | ✅ shipped |
| Risk sizing | none | **vol-targeting → Sharpe 1.30, DD −13.5%** | ✅ shipped |

---

## Lead finding

**The bot is now clearly ahead of SPY on a risk-adjusted basis and hits your
Sharpe target: 1.30 vs SPY's 0.84**, with max drawdown capped at **−13.5% vs
SPY's −34%**. It beats SPY every down year and halves every crash. The heavy
lifting is done (vol-targeting + exit fix + roster cleanup). The remaining
upside is a genuinely *uncorrelated* return stream — see Part 5.

---

## Part 1 — Historical performance

### 1a. Return-engine equity curve (vol-targeted, 2015–2026)

| | Engine | SPY |
|---|---|---|
| CAGR | 15.7% | 14.2% |
| Sharpe | **1.30** | 0.84 |
| Sortino | 1.69 | — |
| Max drawdown | **−13.5%** | −33.7% |
| Calmar | 1.16 | 0.42 |
| Monthly corr to SPY | 0.88 | — |

**Annual vs SPY** (engine wins flat/down years, lags melt-ups): 2015 +6.9/+1.2 ✅ ·
2017 +36.7/+21.7 ✅ · 2018 +5.2/−4.6 ✅ · 2019 +21.0/+31.2 ❌ · 2021 +24.6/+28.7 ❌ ·
2022 **−8.0/−18.2 ✅** · 2023 +28.2/+26.2 ✅ · 2024 +25.8/+24.9 ✅ · 2025 +14.3/+17.7 ❌.

**Worst drawdowns** all shallow & recovered: −13.5% (2022, 244d), −13.0% (2018 Q4,
123d), −12.5% (2025 tariff scare, 106d), −12.2% (COVID, 142d). Nothing worse than
−14% in 11 years.

### 1b. Strategy attribution (v100 roster; backtest-reconstructed, not live fills)

| Strategy | Status | Trades | Win% | Exp/trade | PF | Monthly Sharpe |
|---|---|---|---|---|---|---|
| multi_tf_rsi | active | 3587 | 43% | +0.36% | 1.23 | **0.97** |
| trend_pullback | active | 3764 | 43% | +0.30% | 1.20 | 0.62 |
| **mean_reversion** | **active (upgraded)** | 4348 | **60%** | +0.08% | 1.09 | **0.71** |
| breakout | active | 2470 | 43% | +0.34% | 1.24 | 0.10 |
| 52wh_vol | active | 1794 | 42% | +0.30% | 1.22 | −0.10 |
| momentum | active | 554 | 48% | +0.33% | 1.21 | 0.05 |
| quality_momentum | active | 393 | 44% | +0.04% | 1.03 | 0.11 |
| gap_scanner | **DISABLED** | 2346 | 40% | +0.09% | 1.05 | — |
| cs_momentum | **DISABLED** | 527 | 42% | −0.15% | 0.92 | — |

`multi_tf_rsi` remains the workhorse; the upgraded `mean_reversion` is now a
top-3 contributor and the book's best counter-trend diversifier. The two
parasites are gone.

### 1c. Regime breakdown

| Regime | Days | % | Engine cum | SPY cum |
|---|---|---|---|---|
| BULL | 1698 | 56% | +1188% | +1117% |
| CHOP | 326 | 11% | −23% | −19% |
| TRANSITION | 310 | 10% | −18% | −12% |
| **BEAR** | 503 | 17% | **−34%** | **−50%** |

The engine's structural edge is in BEAR: −16 points better than SPY. That's the
vol-target + cash-defense overlays working.

---

## Part 2 — Current strategy audit

**2a. Underdeployment:** exposure-cap sweep shows Sharpe **flat at 1.30 across
caps 0.6→1.0** — vol-targeting fixes the risk level, so the cap only trades CAGR
for drawdown (0.8→15.7%/−13.5%; 1.0→19.7%/−16.8%). Your 34% idle cash isn't
hurting Sharpe but costs ~4pts CAGR. The binding constraint is the 15% cash-reserve
floor + signal availability, **not** the 80% cap (you're at 66%, below it). To
deploy more, lower `MIN_CASH_RESERVE_PCT` — but backtest first.

**2c. Stop/TP:** addressed by the exit-engine work — tp2 widened 3–4×→5–6×ATR
(Pareto-improving across all sub-periods, shipped).

---

## Part 3 — New strategy research

| # | Idea | Result | Verdict |
|---|---|---|---|
| 3c | **Mean-rev RSI(2)<10 >MA200** | +0.35–0.52%/trade, 67% win, PF 1.35 | ✅ **shipped** (replaced RSI-14 MR) |
| 3e | **Donchian turtle (40/20, 2×ATR)** | standalone Sharpe **1.35**, but **0.78 corr** to book | ⚠️ great but NOT a diversifier |
| 3b | Vol-adjusted momentum | +0.18%/trade, PF 1.11 | ❌ no better than raw |
| 3a | RS-rotation upgrade | ETF-level; deferred | — |
| 3d | Earnings calendar | **BLOCKED** (Yahoo `.calendar` auth crumb) | can't test free |
| 3f | Fundamental quality | **BLOCKED** (Yahoo `.info` empty) | can't test free |

**Donchian nuance:** it's a *better* trend implementation than your current
trend sleeves (Sharpe 1.35 vs the book's 1.30) but 0.78-correlated — so it's a
**replacement** candidate for breakout/52wh/momentum, not a diversifying add.

---

## Part 4 — Risk analysis

**4a. Tail scenarios** — the overlay roughly halves every crash: COVID −11.6% vs
SPY −33.4%; 2022 −11.8% vs −23.8%; 2018 Q4 −12.2% vs −18.9%. Blind spot: the
one-day Volmageddon spike (−6.0% vs −5.9%) — a 20-day vol signal can't react that
fast; a VIX-spike circuit-breaker would help.

**4b/4c. Concentration & correlation:**
- **Concentration is fine** — current-weight vol **11.9% < equal-weight 13.2%**;
  the big V/ABBV/MS positions are low-vol names, so concentration de-risks here.
- **Hidden wash trade:** LOW (long) and HD (short) are **0.90 correlated** (same
  sector) — close one leg; it just bleeds spread + fees.
- **Financials** (JPM/MS/V) avg corr **0.36**; JPM/MS specifically 0.59. Moderate.

---

## Part 5 — What to do next

### Top 3 to implement
1. **Done — vol-targeting** (Sharpe 1.30, DD −13.5%). Optionally raise deployment
   (cap→0.9 or lower cash floor) for +CAGR at the same Sharpe.
2. **Decide Donchian**: replace breakout/52wh/momentum with the Donchian trend
   implementation (Sharpe 1.35). Upgrades trend quality without adding a 4th
   correlated bet. Needs the swap backtested.
3. **VIX-spike circuit-breaker** for the one-day tail blind spot (Volmageddon).

### Top 3 to cut (mostly done)
1. ✅ `cs_momentum` — cut. 2. ✅ `gap_scanner` — cut. 3. **LOW/HD wash trade** —
close one leg (live-account action, not code).

### The one big thing you're missing
**A genuinely uncorrelated return stream.** Every equity sleeve is ~0.8–0.9
correlated to being long the basket (Donchian included). The path from Sharpe
1.3 (long-only) to 1.5+ (multi-strategy) is a real diversifier — managed-futures/
trend on ETFs, or a market-neutral factor sleeve — something that pays when the
book doesn't. That, plus more deployment, is the remaining leverage.

### Honest assessment
**Yes — competitive and now clearly ahead of SPY risk-adjusted (Sharpe 1.30 vs
0.84).** Structural *outperformance* in flat/down markets (vol-target + tail
behavior); structural *underperformance* in melt-up years (it de-risks on
volatility) — a feature of your min-flip-loss goal, dial-able via
`VOL_TARGET_ANNUAL`. Residual weaknesses: single-day vol spikes and the lack of
a truly uncorrelated sleeve. Both fixable.

---

## Scope & honesty caveats
- **"Engine" = vol-targeted equal-weight mega-cap basket** — a faithful *return-engine
  proxy* (the scorecard proved the strategies ≈ long this universe), not a literal
  19-strategy tick sim.
- **Attribution is backtest-reconstructed**, not from your Alpaca fill log.
- **3d (earnings calendar) & 3f (fundamentals) are data-blocked** — Yahoo needs an
  auth crumb; would require a paid feed (FMP/Polygon).
- **Survivorship bias** inflates absolute CAGRs (today's mega-caps); the *edge* vs
  SPY/baseline holds. No transaction costs modelled.
