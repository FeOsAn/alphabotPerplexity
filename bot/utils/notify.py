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
        headers = {"Title": title, "Priority": priority}
        if tags:
            headers["Tags"] = tags
        r = requests.post(NTFY_URL, data=body.encode("utf-8"), headers=headers, timeout=5)
        return r.status_code == 200
    except Exception as e:
        logger.warning(f"[ntfy] Failed to send notification: {e}")
        return False
