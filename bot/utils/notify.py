"""
notify.py — Centralised ntfy notifications for AlphaBot.

Rules:
  - Daily recap fires once at 21:00 BST.
  - Emergency alerts (cash < 0, circuit breaker) fire ONCE per day max.
  - Cash floor breached = bot handles it silently, NO alert to user.
  - Everything else = logged only, no ntfy.
"""
import logging
import requests
import os
from datetime import timezone
from utils.clock import now_utc, today_utc

logger = logging.getLogger(__name__)
NTFY_URL = os.getenv("NTFY_URL", f"https://ntfy.sh/{os.getenv('NTFY_TOPIC', 'alphabot')}")

# Dedup: track which alert keys have fired today
_fired_today: dict[str, str] = {}  # key -> date_str


def send(title: str, body: str, priority: str = "default", tags: str = "") -> bool:
    """
    Send ntfy notification. Only fires for genuine emergencies.
    priority: "min" | "low" | "default" | "high" | "urgent"
    """
    try:
        payload = {
            "topic": NTFY_URL.rstrip("/").split("/")[-1],
            "title": title,
            "message": body,
            "priority": {"min": 1, "low": 2, "default": 3, "high": 4, "urgent": 5}.get(priority, 3),
        }
        if tags:
            payload["tags"] = [t.strip() for t in tags.split(",")]
        base_url = "/".join(NTFY_URL.rstrip("/").split("/")[:-1])
        r = requests.post(base_url, json=payload, timeout=5)
        return r.status_code == 200
    except Exception as e:
        logger.warning(f"[ntfy] Failed to send notification: {e}")
        return False


def emergency(title: str, body: str, key: str, priority: str = "urgent") -> bool:
    """
    Send an emergency alert — fires AT MOST ONCE PER DAY per key.
    Use this for: cash < 0, circuit breaker triggered.
    Do NOT use for cash floor breached (bot handles that silently).
    
    key: unique string identifying this alert type e.g. "negative_cash_momentum"
    """
    today = today_utc()
    if _fired_today.get(key) == today:
        logger.debug(f"[ntfy] Emergency '{key}' already fired today — suppressed")
        return False
    _fired_today[key] = today
    logger.critical(f"[EMERGENCY] {title}: {body}")
    return send(title, body, priority=priority)


def recap(title: str, body: str) -> bool:
    """Send the daily recap. No dedup — fires once at scheduled time."""
    return send(title, body, priority="default")
