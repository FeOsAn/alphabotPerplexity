# Weekly reconciliation — runbook

Fires Mondays. Steps 1, 2 and 4 read the **live Alpaca account**; steps 3 and 5
work from the repo alone.

## Required inputs

| Need | Why | How to supply |
|---|---|---|
| `ALPACA_API_KEY`, `ALPACA_SECRET_KEY` | P&L attribution, stop-coverage audit, churn/drift checks | **Session environment variables** |
| Railway public URL (optional) | `GET /diag` reports deployed VERSION, `recap_enabled`, ntfy send status | paste the URL |

`backtests/decay_check.py` already reads both keys from the environment — that
is the supported path.

## Do NOT reintroduce the handoff-doc parse

An earlier scratchpad helper (`alpaca_env.py`) recovered the keys by regexing
the uploaded handoff document. That existed for one good reason — it kept the
secret out of the shell command line after the tool classifier correctly blocked
a command with the key inlined — but it has two failure modes:

1. **It dies with the container.** Sessions run in ephemeral containers. When
   one is reclaimed the scratchpad *and* the uploaded document go with it, and
   the credentials cannot be reconstructed from session context, because they
   were deliberately never printed. On 2026-08-03 the weekly job fired into a
   fresh container and could complete only the repo-side steps.
2. **It depends on a document that holds a live secret in plaintext.** Those
   keys were exposed in chat and are still pending rotation.

Environment variables solve both. Never inline a key into a shell command.

## Repo-side steps (always available)

- Latest VERSION on `main`: `git show origin/main:bot/main.py | grep -m1 '^VERSION'`
- Gate: `python -m pytest tests/ -q` and the pyflakes undefined-name check
- Revalidation calendar: `research/strategy_registry.json` (`revalidate_by`)

## Reporting rule

If nothing is wrong, a few lines. Never present a step as done when its data
source was unavailable — say which steps ran and which were blocked.
