"""
Source-level regression guards for defect classes found live (Jul 2026).
These read the SOURCE, so they need no alpaca/pandas_ta installed and can
never be skipped for missing deps.
"""
import re
import pathlib

BOT = pathlib.Path(__file__).resolve().parents[1] / "bot"


def _read(rel):
    return (BOT / rel).read_text()


def test_oco_orders_never_carry_take_profit_leg():
    """DEFECT CLASS 1 (v81..v100.5): OCO built with BOTH parent limit_price and
    a take_profit leg 422s at Alpaca -> every OCO in account history silently
    failed; protection degraded to plain stops or nothing. Guard: no OCO
    construction block may mention take_profit."""
    offenders = []
    for py in BOT.rglob("*.py"):
        src = py.read_text()
        for m in re.finditer(r"OrderClass\.OCO", src):
            block = src[m.start(): m.start() + 600]  # the request constructor body
            # trim at the closing submit to keep the window tight
            cut = block.find("submit_order")
            if cut != -1:
                block = block[:cut]
            if "take_profit=" in block:
                offenders.append(str(py))
    assert not offenders, f"OCO blocks with take_profit leg (422s at Alpaca): {offenders}"


def test_no_undefined_names_anywhere():
    """DEFECT CLASS 2 (TakeProfitRequest NameError, earnings_nlp 'side'):
    statically detectable undefined names must never ship. Runs pyflakes over
    bot/ and fails on any 'undefined name'."""
    import subprocess, sys
    out = subprocess.run([sys.executable, "-m", "pyflakes", str(BOT)],
                         capture_output=True, text=True)
    bad = [l for l in out.stdout.splitlines() if "undefined name" in l]
    assert not bad, f"undefined names in bot/: {bad}"


def test_version_is_bumped_and_logged():
    """DEFECT CLASS 3 (deployment ambiguity): VERSION stayed 'v99' through five
    releases, making 'what is Railway running' unverifiable. Guard: VERSION
    exists, is not v99, and is logged at startup."""
    src = _read("main.py")
    m = re.search(r'^VERSION\s*=\s*"(v[\d.]+)"', src, re.M)
    assert m, "VERSION constant missing from main.py"
    assert m.group(1) != "v99", "VERSION was never bumped past v99"
    assert "AlphaBot Starting ({VERSION})" in src or "AlphaBot Starting ({" in src, \
        "startup log must include VERSION"


def test_entry_gate_contains_all_safety_layers():
    """The centralised entry gate must keep every safety layer added after the
    live failures: circuit breaker, cooldown, cluster cap, exposure cap."""
    src = _read("broker.py")
    gate = src[src.index("def _entry_blocked"):]
    gate = gate[:gate.index("\n    def ", 10)]  # this method only
    for needle, why in [
        ("_daily_loss_tripped", "daily-loss circuit breaker"),
        ("is_on_cooldown", "cross-strategy cooldown"),
        ("INDEX_ETF_CLUSTER_CAP", "index/sector-ETF cluster cap"),
        ("effective_exposure_cap", "vol-target/200DMA exposure overlay"),
    ]:
        assert needle in gate, f"entry gate lost its {why} ({needle})"


def test_watchdog_has_plain_stop_fallback():
    """If OCO placement fails, the watchdog must place a plain stop in the SAME
    pass — never leave a position naked until the next cycle."""
    src = _read("utils/stop_watchdog.py")
    assert "plain-stop fallback" in src or "if not placed" in src, \
        "stop_watchdog lost its plain-stop fallback after OCO failure"


def test_watchdog_registered_in_main_loop():
    src = _read("main.py")
    assert "ensure_stops(broker" in src, "stop watchdog no longer called from main loop"
