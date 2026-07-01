# AlphaBot

An algorithmic paper-trading bot running on [Alpaca Markets](https://alpaca.markets/) (paper account). It orchestrates 19 strategies on a fixed cycle, adapts position sizing and entries to the detected market regime, and persists state to SQLite plus a local JSON slot tracker. Deployed on Railway via Docker.

**Current version: v99**

---

## Contents

- [Architecture](#architecture)
- [Quickstart](#quickstart)
- [Environment variables](#environment-variables)
- [Regime system](#regime-system)
- [Strategies](#strategies)
- [Portfolio constraints](#portfolio-constraints)
- [Conviction long tracking](#conviction-long-tracking)
- [Scheduled tasks](#scheduled-tasks)
- [Adding a new strategy](#adding-a-new-strategy)
- [Version history](#version-history)
- [Backtested performance](#backtested-performance)
- [Outstanding items](#outstanding-items)

---

## Architecture

| Path | Responsibility |
|------|----------------|
| `bot/main.py` | Entry point. Orchestrates every strategy each cycle. |
| `bot/config.py` | All parameters, thresholds, and sizing constants. |
| `bot/broker.py` | All Alpaca API interactions (orders, positions, account). |
| `bot/db.py` | SQLite persistence (`alphabot.db`). |
| `bot/weekly_scan.py` | Conviction-long weekly scanner. |
| `bot/strategies/` | One file per strategy (19 active). |
| `bot/utils/` | Regime detection, position sizing, cooldown tracking, yfinance cache, notifications. |
| `api/` | FastAPI dashboard endpoints. |
| `dashboard/` | Frontend dashboard. |
| `conviction_positions.json` | Local JSON tracking for conviction-long slots (persisted at `/app/`). |

The main loop runs every 5 minutes during market hours, evaluates each strategy against the current regime, places orders through `broker.py`, and logs trades and portfolio snapshots to SQLite.

---

## Quickstart

```bash
# 1. Clone
git clone https://github.com/FeOsAn/alphabotPerplexity.git
cd alphabotPerplexity

# 2. Set environment variables (create a .env file — see below)
cat > .env <<'EOF'
ALPACA_API_KEY=your_paper_key
ALPACA_SECRET_KEY=your_paper_secret
ALPACA_BASE_URL=https://paper-api.alpaca.markets
PERPLEXITY_API_KEY=your_perplexity_key
EOF

# 3. Build the Docker image
docker build -t alphabot .

# 4. Run
docker run --env-file .env alphabot
```

### Running locally without Docker

```bash
pip install -r requirements.txt
export $(cat .env | xargs)
python bot/main.py
```

The API server and dashboard can be run separately; see `SETUP.md` for local API/dashboard instructions.

---

## Environment variables

Configured in the Railway dashboard under **Variables** (or a local `.env`):

```bash
ALPACA_API_KEY=      # Paper account key
ALPACA_SECRET_KEY=   # Paper account secret
ALPACA_BASE_URL=https://paper-api.alpaca.markets
PERPLEXITY_API_KEY=  # Conviction scanner research signal (dimension 4)
```

> **`PERPLEXITY_API_KEY` is currently NOT set.** Without it the conviction scanner degrades gracefully to 3-dimension scoring, substituting a quantitative proxy for the research signal. Add the key in Railway → Variables to enable full 4-dimension scoring.

---

## Regime system

A composite score from **0–100** is computed from four signals:

1. **SPY vs MA50** — trend direction relative to the 50-day moving average.
2. **VIX level** — absolute volatility.
3. **IVTS** — term structure, `VIX / VIX3M`.
4. **Credit spread proxy** — risk appetite.

The score maps to a regime, which gates which strategies may fire and how positions are sized:

| Regime | Condition |
|--------|-----------|
| **BULL** | score 70–100 |
| **CHOP** | score 40–69 |
| **TRANSITION** | SPY within 2% of MA50 |
| **BEAR** | score 0–39 |

### PRE_TRANSITION_ALERT (v97)

Fires when **IVTS ≥ 0.97 AND SPY within 2% of MA50**. Effects:

- Tightens all momentum stops to **5%**.
- Blocks new momentum entries.
- Opens a hedge rotation: **GLD (10%) + SH (5%)**.

---

## Strategies

19 active strategies, gated by regime. Percentages are target equity allocation; `× N` indicates max simultaneous slots.

### BULL only
- `cs_momentum` — 8.5% × 4, 12-3 monthly, no take-profit. Kelly sizing active.
- `gap_scanner` — 8%

### BULL / CHOP
- `quality_momentum` — 6% × 3, monthly
- `conviction_long` — 12% × 4, weekly scan
- `52wh_vol` — 8%
- `trend_pullback` — 3%
- `momentum` — 4%
- `breakout` — 8%
- `sector_rotation` — 6% × 2

### All regimes
- `dual_momentum` — 12% × 2, monthly (Antonacci)
- `multi_tf_rsi` — 4%
- `mean_reversion` — 2.5% × 4
- `vwap_reclaim` — 4%
- `earnings_drift` — 4%
- `spy_dip` — 4%
- `ai_research`
- `earnings_prediction`

### BEAR / CHOP
- `short_hedge` — 3%
- `vix_reversal` — 4%

---

## Portfolio constraints

- `MAX_PORTFOLIO_EXPOSURE = 0.80`
- `MIN_ENTRY_NOTIONAL = $500`
- **48h cross-strategy cooldown** per symbol after a stop-out.
- **Kelly sizing** active for `cs_momentum`.

---

## Conviction long tracking

Conviction-long slots are tracked in `/app/conviction_positions.json`, **not** in Alpaca — Alpaca does not support custom position tags, so slot ownership is maintained locally.

- 4 slots max, **12% equity each**.
- Populated by the weekly scan (Sunday 10 PM BST) via a scheduled Perplexity cron.
- The scanner scores the universe and opens the top 4 symbols scoring **≥ 55/100**.

---

## Scheduled tasks

These run as **Perplexity Computer crons** and are **not part of this repo**:

| Task | Schedule | Action |
|------|----------|--------|
| Morning watchdog | Mon–Fri 9:35 AM ET | Audits stops, auto-fixes unprotected positions. |
| Afternoon watchdog | Mon–Fri 3:30 PM ET | Same audit. |
| EOD P&L summary | Mon–Fri 4:30 PM ET | Pushes to `ntfy.sh/perplexitybotnr1foa_goat`. |
| Weekly conviction scanner | Sunday 10 PM BST | Scores universe, opens top 4 scoring ≥ 55/100. |

---

## Adding a new strategy

1. Create `bot/strategies/your_strategy.py` exposing a `run(broker, db_conn)` function.
2. Import and call it from `run_all_strategies()` in `bot/main.py`.
3. Add any thresholds or sizing constants to `bot/config.py`.
4. The dashboard picks up new strategies automatically in all charts.

Use an existing file in `bot/strategies/` as a template — `mean_reversion.py` and `momentum.py` are good starting points for reversion and trend patterns respectively.

---

## Version history

| Versions | Changes |
|----------|---------|
| v85–v94 | Regime exit logic, bug fixes, rationale-gated entries, cooldown system. |
| v95–v96 | Composite regime score, transition-band gate, CS momentum + Kelly sizing. |
| v97 | IVTS pre-alert, 5% tight stop, GLD/SH hedge rotation on transition. |
| v98 | Strategy firing fixes — `spy_dip` bug, `short_hedge` redesign, gate widening across 6 strategies. |
| v99 | Conviction scanner local JSON tracking, slot counting fix, MU wash-trade fix. |

---

## Backtested performance

Validated on v96/v97:

| Metric | AlphaBot | SPY |
|--------|----------|-----|
| 10yr CAGR | 12.11% | 13.60% |
| Sharpe | 0.828 | 0.813 |
| Max drawdown | -23.75% | -33.72% |

**v97 transition fix:** average transition P&L **+1.202%**; worst event **-1.84%** (down from -2.60%).

> Past performance does not guarantee future results. This bot runs on a paper account.

---

## Outstanding items

1. **`PERPLEXITY_API_KEY` not yet added to Railway.** The conviction scanner's dimension 4 (research signal) falls back to a quantitative proxy until it is set. Add it in Railway dashboard → Variables.
2. **Railway project name/ID is not documented** here.
