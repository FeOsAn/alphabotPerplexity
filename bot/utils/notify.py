"""
notify.py — Centralised ntfy notifications for AlphaBot.

Rules:
  - Daily recap fires once at 21:00 BST.
  - Emergency alerts (cash < 0, circuit breaker) fire ONCE per day max.
  - Cash floor breached = bot handles it silently, NO alert to user.
  - Everything else = logged only, no ntfy.

v101.2 — the notification path is now OBSERVABLE. On 2026-07-30 the account was
trading normally but the ntfy topic had ZERO messages across its whole 12h
cache, and there was no way to tell from outside whether the bot was disabled,
misconfigured, or erroring: send() swallowed the response body, the resolved
topic was never logged, and an unset NTFY_TOPIC silently published a private
portfolio to the generic public topic "alphabot". All three are fixed here, and
last_status() feeds the /health endpoint so diagnosis is one curl, not a
Railway log dig.
"""
import logging
import requests
import os
from datetime import timezone
from utils.clock import now_utc, today_utc

logger = logging.getLogger(__name__)

# ── Topic resolution (explicit, logged, fail-loud) ────────────────────────────
# Precedence: NTFY_URL (full URL) > NTFY_TOPIC (topic name) > unconfigured.
_FALLBACK_TOPIC = "alphabot"          # generic + PUBLIC — never silently use it
_env_url = os.getenv("NTFY_URL", "").strip()
_env_topic = os.getenv("NTFY_TOPIC", "").strip()
CONFIGURED = bool(_env_url or _env_topic)

if _env_url:
    NTFY_URL = _env_url
elif _env_topic:
    NTFY_URL = f"https://ntfy.sh/{_env_topic}"
else:
    NTFY_URL = f"https://ntfy.sh/{_FALLBACK_TOPIC}"

_parts = NTFY_URL.rstrip("/").split("/")
TOPIC = _parts[-1] if _parts else _FALLBACK_TOPIC
SERVER = "/".join(_parts[:-1]) or "https://ntfy.sh"

# Last-send observability (served by /health)
_last_status: int | None = None
_last_error: str = ""
_last_ok_iso: str = ""
_last_attempt_iso: str = ""

# Dedup: track which alert keys have fired today
_fired_today: dict[str, str] = {}  # key -> date_str


def log_config() -> None:
    """Log the resolved notification target at startup. Call once from main()."""
    if not CONFIGURED:
        logger.critical(
            "[ntfy] MISCONFIGURED — neither NTFY_URL nor NTFY_TOPIC is set. "
            "Falling back to the PUBLIC topic '%s', which is almost certainly "
            "not yours: your recaps are going nowhere you can see. Set NTFY_TOPIC "
            "on Railway.", _FALLBACK_TOPIC,
        )
    else:
        logger.info("[ntfy] target resolved: server=%s topic=%s", SERVER, TOPIC)


def _mask(topic: str) -> str:
    """
    Mask the topic for any output that could leave the container.

    An ntfy.sh topic is an unauthenticated bearer secret: anyone who learns it
    can both read your portfolio notifications and publish fake ones. /diag is
    reachable on Railway's public domain, so it gets a fingerprint — enough to
    confirm "yes, that's the topic I configured" — never the value. The full
    topic goes to the logs, which are private.
    """
    if not topic:
        return "(none)"
    if len(topic) <= 6:
        return f"{topic[0]}***({len(topic)})"
    return f"{topic[:3]}***{topic[-2:]}({len(topic)})"


def last_status() -> dict:
    """Notification-path diagnostics for the /diag endpoint (topic masked)."""
    return {
        "configured": CONFIGURED,
        "server": SERVER,
        "topic_fingerprint": _mask(TOPIC),
        "last_attempt": _last_attempt_iso,
        "last_ok": _last_ok_iso,
        "last_http_status": _last_status,
        "last_error": _last_error,
    }


def send(title: str, body: str, priority: str = "default", tags: str = "") -> bool:
    """
    Send ntfy notification. Only fires for genuine emergencies.
    priority: "min" | "low" | "default" | "high" | "urgent"
    """
    global _last_status, _last_error, _last_ok_iso, _last_attempt_iso
    _last_attempt_iso = now_utc().isoformat(timespec="seconds")
    try:
        payload = {
            "topic": TOPIC,
            "title": title,
            "message": body,
            "priority": {"min": 1, "low": 2, "default": 3, "high": 4, "urgent": 5}.get(priority, 3),
        }
        if tags:
            payload["tags"] = [t.strip() for t in tags.split(",")]
        r = requests.post(SERVER, json=payload, timeout=5)
        _last_status = r.status_code
        if r.status_code == 200:
            _last_error = ""
            _last_ok_iso = _last_attempt_iso
            return True
        # Log the body — a 4xx from ntfy explains itself, and swallowing it is
        # how a silent-notification outage stayed invisible for days.
        _last_error = f"HTTP {r.status_code}: {r.text[:200]}"
        logger.error("[ntfy] publish to topic '%s' failed — %s", TOPIC, _last_error)
        return False
    except Exception as e:
        _last_status = None
        _last_error = f"{type(e).__name__}: {e}"
        logger.warning(f"[ntfy] Failed to send notification: {e}")
        return False


def startup_ping(version: str, recap_enabled: bool) -> bool:
    """
    Boot-time proof-of-life on the notification channel itself.

    This is the whole point: if you get this on your phone after a deploy, the
    topic, the subscription and the send path all work — so any later silence is
    a scheduling/gating problem, not a plumbing one. If you DON'T get it, the
    channel is broken and no recap was ever going to arrive.

    Caller throttles to once per day (see main._startup_ping_once) so a Railway
    restart loop cannot spam.
    """
    return send(
        title=f"AlphaBot {version} online",
        body=(f"Notifications are wired up.\n"
              f"Topic: {TOPIC}\n"
              f"Daily recap: {'ENABLED' if recap_enabled else 'DISABLED (DAILY_RECAP_ENABLED=0)'}"),
        priority="low",
        tags="rocket",
    )


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
