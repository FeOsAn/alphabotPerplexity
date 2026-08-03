"""
Regression tests for the notification path (v101.2).

Origin: on 2026-07-30 the bot traded normally all week while the ntfy topic held
ZERO messages. Nothing in the code could tell you why — the resolved topic was
never logged, a missing NTFY_TOPIC silently published to a generic PUBLIC topic,
and send() discarded the HTTP body on failure. These tests pin the contract so
a silent-notification outage can't recur unnoticed.
"""
import sys
import pathlib
import importlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bot"))


def _reload_notify(monkeypatch, **env):
    """Re-import notify with a specific NTFY_* environment."""
    for k in ("NTFY_URL", "NTFY_TOPIC"):
        monkeypatch.delenv(k, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    import utils.notify as n
    return importlib.reload(n)


def test_topic_from_ntfy_topic(monkeypatch):
    n = _reload_notify(monkeypatch, NTFY_TOPIC="perplexitybotnr1foa_goat")
    assert n.TOPIC == "perplexitybotnr1foa_goat"
    assert n.SERVER == "https://ntfy.sh"
    assert n.CONFIGURED is True


def test_topic_from_full_url(monkeypatch):
    n = _reload_notify(monkeypatch, NTFY_URL="https://ntfy.sh/my_topic")
    assert n.TOPIC == "my_topic"
    assert n.SERVER == "https://ntfy.sh"
    assert n.CONFIGURED is True


def test_url_takes_precedence_over_topic(monkeypatch):
    n = _reload_notify(monkeypatch, NTFY_URL="https://push.example.com/a",
                       NTFY_TOPIC="ignored")
    assert n.TOPIC == "a"
    assert n.SERVER == "https://push.example.com"


def test_unconfigured_is_flagged_not_silent(monkeypatch):
    """An unset topic must be detectable — it publishes to a PUBLIC fallback."""
    n = _reload_notify(monkeypatch)
    assert n.CONFIGURED is False, "missing NTFY_* must be reported, not silently defaulted"
    n.log_config()  # must not raise; logs CRITICAL


def test_send_reports_failure_status(monkeypatch):
    """A non-200 must return False AND be visible via last_status()."""
    n = _reload_notify(monkeypatch, NTFY_TOPIC="t")

    class FakeResp:
        status_code = 403
        text = "forbidden"

    monkeypatch.setattr(n.requests, "post", lambda *a, **k: FakeResp())
    assert n.send("t", "b") is False
    st = n.last_status()
    assert st["last_http_status"] == 403
    assert "403" in st["last_error"]
    assert st["configured"] is True


def test_diag_never_leaks_the_topic(monkeypatch):
    """
    /diag is reachable on Railway's public domain and an ntfy topic is an
    unauthenticated bearer secret — publishing it would let anyone read the
    portfolio recaps and forge alerts. Only a fingerprint may escape.
    """
    secret = "perplexitybotnr1foa_goat"
    n = _reload_notify(monkeypatch, NTFY_TOPIC=secret)
    st = n.last_status()
    assert "topic" not in st, "raw topic key must not be served"
    blob = repr(st)
    assert secret not in blob, "full topic leaked into diagnostics"
    fp = st["topic_fingerprint"]
    assert fp.startswith("per") and fp.endswith(f"({len(secret)})")
    assert "exitybotnr1foa" not in fp


def test_send_success_records_ok(monkeypatch):
    n = _reload_notify(monkeypatch, NTFY_TOPIC="t")

    class FakeResp:
        status_code = 200
        text = "{}"

    sent = {}

    def fake_post(url, json=None, timeout=None):
        sent["url"] = url
        sent["json"] = json
        return FakeResp()

    monkeypatch.setattr(n.requests, "post", fake_post)
    assert n.send("title", "body", priority="urgent", tags="rocket") is True
    # ntfy JSON publish = POST to server root with topic in the body
    assert sent["url"] == "https://ntfy.sh"
    assert sent["json"]["topic"] == "t"
    assert sent["json"]["priority"] == 5
    assert sent["json"]["tags"] == ["rocket"]
    assert n.last_status()["last_ok"]


def test_startup_ping_states_recap_status(monkeypatch):
    n = _reload_notify(monkeypatch, NTFY_TOPIC="t")
    bodies = []

    class FakeResp:
        status_code = 200
        text = "{}"

    monkeypatch.setattr(n.requests, "post",
                        lambda url, json=None, timeout=None: (bodies.append(json), FakeResp())[1])
    n.startup_ping("v101.2", recap_enabled=False)
    assert "DISABLED" in bodies[-1]["message"]
    n.startup_ping("v101.2", recap_enabled=True)
    assert "ENABLED" in bodies[-1]["message"]


# ── fatal-startup paging (v101.3) ────────────────────────────────────────────
# v101.2 put the deploy proof-of-life ping AFTER both fatal startup gates, so a
# bad-credentials exit was completely silent — the exact failure that left the
# bot dead and unnoticed over the 2026-08-01 weekend. These pin the fix.

def _import_main(monkeypatch):
    monkeypatch.setenv("NTFY_TOPIC", "t")
    monkeypatch.setenv("ALPACA_API_KEY", "dummy")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "dummy")
    import importlib
    return importlib.import_module("main")


def test_fatal_startup_pages_then_exits(monkeypatch):
    import pytest
    m = _import_main(monkeypatch)
    sent = []
    monkeypatch.setattr(m.notify, "send",
                        lambda **kw: (sent.append(kw), True)[1])
    with pytest.raises(SystemExit) as ex:
        m._fatal_startup("creds bad", "Unauthorized")
    assert ex.value.code == 1
    assert len(sent) == 1, "a fatal startup must push before exiting"
    assert sent[0]["priority"] == "urgent"
    assert "creds bad" in sent[0]["body"]
    assert "not running" in sent[0]["body"].lower()


def test_fatal_startup_exits_even_if_push_fails(monkeypatch):
    """The alert is best-effort — it must never mask the real failure."""
    import pytest
    m = _import_main(monkeypatch)

    def boom(**kw):
        raise RuntimeError("ntfy down")

    monkeypatch.setattr(m.notify, "send", boom)
    with pytest.raises(SystemExit):
        m._fatal_startup("creds bad", "Unauthorized")


def test_startup_gates_do_not_exit_silently():
    """
    Source guard: every sys.exit in main()'s startup sequence must go through
    _fatal_startup, which pages first. A bare sys.exit there is the regression.
    """
    import pathlib
    src = (pathlib.Path(__file__).resolve().parents[1]
           / "bot" / "main.py").read_text()
    body = src[src.index("def main():"):]
    head = body[:body.index("logger.info(f\"Connected")]
    assert "sys.exit(" not in head, (
        "bare sys.exit in the startup gates — use _fatal_startup so the "
        "failure is announced instead of silent"
    )
