"""
v95 — Regime State Machine (Idea 2).

The regime only CHANGES after N consecutive daily closes confirming the new
state. A single day on the other side of MA50 does NOT flip the regime — this
prevents the whipsaw exits (close profitable position on a 1-day dip → market
recovers → re-enter higher) seen in the MRVL/NKE churn.

State persists in the `regime_state` table so it survives bot restarts.

`update()` is date-guarded: the bot loop calls it many times per day, but the
confirmation counter only advances on the first call of each new calendar day.
"""
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Optimised via 432-combination backtest grid, Jun 2024-Jun 2026. Sharpe 1.330, MaxDD -17.4%
CONFIRMATION_DAYS_REQUIRED = 2      # was 3 — faster regime confirmation (2-day)
TRANSITION_CONFIDENCE_STEP = 1.0 / CONFIRMATION_DAYS_REQUIRED  # 0.50 per day


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


class RegimeStateMachine:
    def __init__(self, db_conn):
        self.db = db_conn
        self._ensure_table()

    def _ensure_table(self):
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS regime_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                current_regime    TEXT NOT NULL,
                candidate_regime  TEXT,
                confirmation_count INTEGER NOT NULL DEFAULT 0,
                last_processed_date TEXT,
                last_updated TEXT
            )
            """
        )
        self.db.commit()

    def _load(self) -> dict:
        row = self.db.execute(
            "SELECT current_regime, candidate_regime, confirmation_count, "
            "last_processed_date FROM regime_state WHERE id = 1"
        ).fetchone()
        if row is None:
            self.db.execute(
                "INSERT INTO regime_state (id, current_regime, candidate_regime, "
                "confirmation_count, last_processed_date, last_updated) "
                "VALUES (1, 'bull', NULL, 0, NULL, ?)",
                (_today(),),
            )
            self.db.commit()
            return {"current_regime": "bull", "candidate_regime": None,
                    "confirmation_count": 0, "last_processed_date": None}
        return {
            "current_regime": row["current_regime"],
            "candidate_regime": row["candidate_regime"],
            "confirmation_count": row["confirmation_count"],
            "last_processed_date": row["last_processed_date"],
        }

    def _save(self, current, candidate, count, processed_date):
        self.db.execute(
            "UPDATE regime_state SET current_regime=?, candidate_regime=?, "
            "confirmation_count=?, last_processed_date=?, last_updated=? WHERE id=1",
            (current, candidate, count, processed_date,
             datetime.now(timezone.utc).isoformat()),
        )
        self.db.commit()

    def _result(self, current, raw, candidate, count) -> dict:
        in_transition = (raw != current)
        confidence = min(1.0, count * TRANSITION_CONFIDENCE_STEP) if in_transition else 1.0
        return {
            "confirmed_regime": current,
            "raw_regime": raw,
            "in_transition": in_transition,
            "transition_confidence": round(confidence, 3),
            "days_confirming": count,
            "candidate_regime": candidate,
        }

    def update(self, raw_regime: str) -> dict:
        """
        Feed the raw (composite-score) regime signal in. Returns the confirmed
        regime, which only flips after CONFIRMATION_DAYS_REQUIRED consecutive
        confirming closes.

        Date-guarded: the confirmation counter advances at most once per calendar
        day, so repeated intraday calls are idempotent.
        """
        state = self._load()
        current = state["current_regime"]
        candidate = state["candidate_regime"]
        count = state["confirmation_count"]
        today = _today()

        # Already processed a close today — return current state without advancing.
        if state["last_processed_date"] == today:
            return self._result(current, raw_regime, candidate, count)

        if raw_regime == current:
            # Confirms the existing regime — reset any pending candidate.
            candidate, count = None, 0
        else:
            if raw_regime == candidate:
                count += 1
            else:
                candidate, count = raw_regime, 1

            if count >= CONFIRMATION_DAYS_REQUIRED:
                logger.info(
                    f"[RegimeSM] CONFIRMED flip {current} → {raw_regime} "
                    f"after {count} closes"
                )
                current, candidate, count = raw_regime, None, 0
            else:
                logger.info(
                    f"[RegimeSM] {current} holding — candidate={raw_regime} "
                    f"day {count}/{CONFIRMATION_DAYS_REQUIRED}"
                )

        self._save(current, candidate, count, today)
        return self._result(current, raw_regime, candidate, count)
