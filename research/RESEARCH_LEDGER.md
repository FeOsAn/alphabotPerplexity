# Research Ledger — every hypothesis tested (July 2026 program)

Purpose: (1) never re-test a known null; (2) pre-register future hypotheses
BEFORE testing (multiple-testing discipline). ~40 hypotheses were tested this
program; ~10 shipped. On ~3 market cycles of daily data, the effective budget
of independent conclusions is small — spend it deliberately.

## Pre-registration template (fill BEFORE running the backtest)
- Hypothesis (market behavior it depends on):
- Failure looks like:
- Test design (period, controls, sub-period splits):
- Ship threshold (decided in advance):

## SHIPPED (validated)
| Idea | Evidence | Where |
|---|---|---|
| Vol-targeted exposure (0.15 target) | halves DD, Sharpe 1.31 flat across caps | profit_max_sweep, dd_reduction |
| 200DMA cash-defense (0.60 cut) | best flip protection; GLD hedge was harmful | regime_overlay |
| Wide TP (7xATR) + NO trailing tiers | beats tight-trailing all 3 sub-periods; live-confirmed Jul 2026 | exit_atr_sweep/robust |
| RSI(2)<10 >MA200 mean reversion | +0.35%/tr, 67% win, robust | rsi2_validate |
| Donchian 40/20, 50 names, 12 slots | sleeve Sharpe 1.39 | donchian_validate/expand |
| Crypto trend sleeve (BTC/ETH/SOL 200DMA) | corr 0.03; book Sharpe +0.3 | crypto_sleeve, x5 |
| Gold trend sleeve (10%) | corr 0.02; book Sharpe 1.70->1.81 | x5_sleeves |
| Sleeve weights 45/30/15/10 (+tilt) | walk-forward OOS 2.01 ~= in-sample 2.03 | final_round |
| Daily-loss circuit breaker 4% | ~zero cost, tail insurance | circuit_breaker |
| Cull: 4 momentum sleeves (v101) | entries worse than random; ~750 trades/yr friction | scorecard |

## REJECTED (do not re-test without new data/reason)
pairs trading (324 combos, best Sharpe 0.45) · squeeze premise (-EV on squeeze
names) · options flow (unvalidatable free) · gap chasing & RSI14-BB mean-rev
(worst vs baseline) · cs_momentum 12-1 (negative) · inverse-vol & momentum-tilt
basket weighting · EWMA & 60d vol estimators · MA-ensemble & IVTS exposure
gates · deeper/shallower 200DMA cuts · margin leverage (Sharpe down) · QLD
sleeve (vol drag) · bonds/commodities TSMOM (Sharpe 0.56 blend-neutral) · TLT
trend (-0.18) · crypto Donchian gate (worse 2022) · vol-targeted crypto sizing
(book worse) · Donchian speed ensemble (ops risk > gain) · overnight-only
(Sharpe flat, huge turnover) · turn-of-month tilt (too small) · VIX-spike
breaker (whipsaws) · net-short below 200DMA (loses) · dual_momentum >25%
(neutral) · SOL-less crypto (SOL adds) · fixed +12/-5/10d squeeze bracket under
live engine (-0.94%/tr).

## STANDING VERDICTS
- Entries ~= random baseline (+0.42%/tr) on this universe under this exit
  engine. New entry ideas must beat THAT, pre-registered.
- Survivorship haircut ~6.7 CAGR pts/yr (survivorship_test). Risk claims
  survive; raw-CAGR-beats-SPY does not, for the equity engine alone.
- Exits/sizing/universe drive P&L; entry cleverness does not.

## Why no autonomous discovery engine (decision, 2026-07-29)
A continuously-running hypothesis generator on ~2,900 daily bars / ~3 market
cycles of 50 correlated mega-caps is a false-discovery factory: at any
reasonable test rate the survivors of automated feature-space search are
selection artifacts (deflated-Sharpe problem). Our manual 40-hypothesis program
IS the empirical yield estimate for this data: ~1 real edge per 4 tested, all
of them simple. The registry + decay checks + pre-registration above capture
the valuable 20% of that architecture at 2% of the cost. Revisit only with:
point-in-time constituent data, intraday data, or a genuinely new asset class.

## 2026-07-29 round: pre-registered small-sleeve validation (all CULLED)
spy_dip (+0.78%/tr vs +1.56% random control — dip-waiting costs money on index
ETFs) · sector_rotation (Sharpe 0.82 < 0.87 EW-sectors — selection worthless) ·
ts_momentum (Sharpe 0.77, corr 0.76 — diluted engine beta). Thresholds were
declared before running; verdicts executed without renegotiation.
