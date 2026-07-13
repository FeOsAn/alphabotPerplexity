# AlphaBot — Losing-Week Review (Jul 6–13, 2026) & v100.6 Fixes

## What happened

**Account: −1.65% (109,666 → 107,852) over four sessions in which SPY ROSE ~+0.9%.**
This was not market beta. It was **whipsaw bleed**: the market chopped hard
(QQQ: −1.9%, +0.3%, +1.7%, +0.3%) and the exit machinery converted that chop
into realized losses.

### The mechanism, trade by trade
- **QQQ**: stopped out Wed Jul 8 @ $701 — the exact low of the week — then
  re-bought Fri Jul 10 @ $724 (3.3% higher, and bigger: $10k). One whipsaw ≈
  **−$480** including the foregone rebound.
- **LOW** stopped Jul 8 (−$190), **LMT** stopped Jul 10 (−$220), **MRVL**
  entered Thu @ $247.5 and stopped Fri @ $235 — **−5.1% in one day** (−$165).
- Fri Jul 10 the bot bought **$26.4k of ~0.9-correlated index beta in one
  session** (QQQ $10k + XLK $10.6k + META $2.6k + IWM $3.2k) on the bounce —
  all of it fell together Monday (−$300+).
- Winners did fine: DDOG banked +17%, MU exited +8.7%, AAPL/GOOGL partials
  took profit. The bleed came from the loss side churning.

### Root causes (each verified against live orders)
1. **Trailing tier ladder too tight for chop.** By Monday, JPM sat 1.0% from
   its ratcheted stop, JNJ 1.1%, MS 1.3%, ABBV 2.5%, PANW 2.7% — five churn
   candidates. The backtest already knew this: the no-trailing + wide-TP
   config beats the tight-trailing config in ALL THREE sub-periods
   (Sharpe 0.90/0.95/1.47 vs 0.78/0.57/0.76) at equal MaxDD. Live has now
   confirmed it the expensive way.
2. **OCO orders have NEVER worked.** Zero OCO orders exist in the account
   despite dozens of attempts since v81 — the constructor passed both a parent
   `limit_price` and a `take_profit` leg; in alpaca-py `take_profit` belongs to
   BRACKET class, so every OCO 422'd and silently degraded to a plain stop (or
   to nothing: IWM/QQQ/TGT were TP-only-naked when audited).
3. **No beta-cluster control.** Four strategies independently bought the same
   trade (index/tech beta) on the same day.
4. **Concentration STILL untrimmed** (user action, 3rd reminder): V $13.3k +
   ABBV $12.7k + MS $13.6k = 36.7% of equity in three names; HD short still open.

### What worked this week (credit where due)
- **v100.4 is deployed and the stop watchdog is functioning**: PANW's broken
  stop ($274, 6/11 shares) was repaired to $317 full-qty; JNJ got covered;
  JPM/MS carry profit-locked stops. Deployment ≈ 84.5% (cash floor working).
- **Crypto/gold sleeves correctly flat** — BTC/ETH/SOL/GLD all below their
  200DMAs. The gates are keeping the book out of falling assets.
- MU churn stopped after the cooldown-sync fix.

## Fixes shipped (v100.6)
1. **Trailing ratchet tiers DISABLED** (`_ATR_RATCHET_TIERS = []`, watchdog
   `_TIERS = []`): positions now ride the per-strategy base stop (4–8%) until
   TP / channel / dead-money exit. tp2 aligned to the tested winner (7×ATR for
   the trend/momentum family). Expected effect per backtest: +~4.6pts CAGR,
   +0.11 Sharpe on the exit engine, equal MaxDD; far fewer chop stop-outs.
   Existing tight stops are never loosened — they will resolve themselves.
2. **OCO construction fixed** in all four sites (drop the take_profit leg) +
   the watchdog now falls back to a plain stop *immediately* if OCO fails —
   a position is never left naked awaiting the next pass.
3. **Index/sector-ETF cluster cap** (12% of equity) in the central entry gate —
   Friday's QQQ+XLK+IWM stack becomes impossible.
4. VERSION → v100.6 (deployment verifiable in the startup log line).

## Honest expectations
Disabling the ratchet means winners are protected by base stop + TP only —
single-position givebacks can be larger (that's the tested trade-off buying the
higher CAGR/Sharpe). The vol-target overlay, circuit breaker and cluster cap
carry the book-level risk. One losing week is weather; these fixes address the
*mechanisms* the week exposed, all of which had prior backtest evidence.

## User actions (unchanged, now 3rd request)
1. Redeploy Railway; confirm `AlphaBot Starting (v100.6)` in logs.
2. Trim V (−14sh), ABBV (−17sh), MS (−20sh); decide the HD short.
3. Rotate the Alpaca keys.
