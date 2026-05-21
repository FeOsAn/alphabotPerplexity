"""
Unified ntfy notification helper.
Wraps requests.post(NTFY_URL, ...) with consistent headers and timeouts.
All bot ntfy alerts should route through send() for uniformity.
"""
import os
import logging
import requests

logger = logging.getLogger(__name__)
NTFY_URL = f"https://ntfy.sh/{os.getenv('NTFY_TOPIC', 'perplexitybotnr1foa_goat')}"


def send(title: str, body: str = "", priority: str = "default", tags: str = "") -> bool:
    """
    Send an ntfy notification. Returns True on HTTP 200.
    priority: "min" | "low" | "default" | "high" | "urgent"
    tags: comma-separated ntfy tag names (e.g. "warning,rotating_light")
    """
    try:
        # Use requests with json payload so UTF-8 emojis work in title
        payload = {
            "topic": NTFY_URL.rstrip("/").split("/")[-1],
            "title": title,
            "message": body,
            "priority": {"min":1,"low":2,"default":3,"high":4,"urgent":5}.get(priority, 3),
        }
        if tags:
            payload["tags"] = [t.strip() for t in tags.split(",")]
        base_url = "/".join(NTFY_URL.rstrip("/").split("/")[:-1])
        r = requests.post(base_url, json=payload, timeout=5)
        return r.status_code == 200
    except Exception as e:
        logger.warning(f"[ntfy] Failed to send notification: {e}")
        return False
