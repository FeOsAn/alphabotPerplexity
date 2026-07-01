"""
Backtest data layer.
--------------------
Fetches daily OHLCV from Yahoo's public chart API via `requests`.

Why not yfinance directly? In several sandboxed / proxied environments yfinance's
curl_cffi transport fails the TLS CONNECT handshake, while plain `requests`
(which honours REQUESTS_CA_BUNDLE / the system trust store) works fine. This
module therefore talks to the same Yahoo endpoint yfinance uses, but over
`requests`, and caches every series to disk so a 10-year multi-symbol backtest
only hits the network once.

Returned frame columns: Open, High, Low, Close, Volume  (auto-adjusted close).
Index: tz-naive DatetimeIndex (US/Eastern trading days).
"""

from __future__ import annotations

import os
import time
import json
import pathlib
import datetime as dt
from typing import Iterable

import pandas as pd
import requests

# Honour the agent-proxy CA bundle if present (no-op on a normal machine).
_CA = "/root/.ccr/ca-bundle.crt"
if os.path.exists(_CA):
    os.environ.setdefault("REQUESTS_CA_BUNDLE", _CA)
    os.environ.setdefault("SSL_CERT_FILE", _CA)

_CACHE_DIR = pathlib.Path(__file__).parent / ".cache"
_CACHE_DIR.mkdir(exist_ok=True)

_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "Mozilla/5.0 (backtest)"})

_BASE = "https://query1.finance.yahoo.com/v8/finance/chart/{sym}"


def _to_epoch(d: str | dt.date) -> int:
    if isinstance(d, str):
        d = dt.date.fromisoformat(d)
    return int(time.mktime(dt.datetime(d.year, d.month, d.day).timetuple()))


def _cache_path(sym: str, start: str, end: str) -> pathlib.Path:
    return _CACHE_DIR / f"{sym.replace('^','_')}_{start}_{end}.pkl"


def get_history(sym: str, start: str, end: str, *, retries: int = 4) -> pd.DataFrame:
    """Daily OHLCV for `sym` over [start, end). Cached to disk (pickle, no extra deps)."""
    cp = _cache_path(sym, start, end)
    if cp.exists():
        try:
            return pd.read_pickle(cp)
        except Exception:
            cp.unlink(missing_ok=True)

    url = _BASE.format(sym=sym)
    params = {
        "period1": _to_epoch(start),
        "period2": _to_epoch(end),
        "interval": "1d",
        "events": "div,splits",
    }

    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            r = _SESSION.get(url, params=params, timeout=30)
            if r.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            j = r.json()
            res = j["chart"]["result"]
            if not res:
                return pd.DataFrame()
            res = res[0]
            ts = res.get("timestamp")
            if not ts:
                return pd.DataFrame()
            quote = res["indicators"]["quote"][0]
            idx = pd.to_datetime(ts, unit="s").tz_localize("UTC").tz_convert("US/Eastern").tz_localize(None).normalize()
            df = pd.DataFrame(
                {
                    "Open": quote.get("open"),
                    "High": quote.get("high"),
                    "Low": quote.get("low"),
                    "Close": quote.get("close"),
                    "Volume": quote.get("volume"),
                },
                index=idx,
            )
            # Prefer adjusted close when available (splits + dividends).
            adj = res.get("indicators", {}).get("adjclose")
            if adj and adj[0].get("adjclose"):
                adj_close = pd.Series(adj[0]["adjclose"], index=idx)
                # Scale OHLV by the adj/close ratio so the whole bar is consistent.
                ratio = adj_close / df["Close"]
                for c in ("Open", "High", "Low"):
                    df[c] = df[c] * ratio
                df["Close"] = adj_close
            df = df.dropna(subset=["Close"])
            df = df[~df.index.duplicated(keep="last")]
            if not df.empty:
                df.to_pickle(cp)
            return df
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    print(f"[data] {sym} failed after {retries} tries: {last_err}")
    return pd.DataFrame()


def get_panel(symbols: Iterable[str], start: str, end: str) -> dict[str, pd.DataFrame]:
    """Fetch many symbols. Silently drops those that fail."""
    out: dict[str, pd.DataFrame] = {}
    for s in symbols:
        df = get_history(s, start, end)
        if not df.empty and len(df) > 20:
            out[s] = df
        else:
            print(f"[data] dropping {s} (empty/short)")
    return out


def close_panel(symbols: Iterable[str], start: str, end: str) -> pd.DataFrame:
    """Aligned Close-price panel (one column per symbol, inner-joined on dates)."""
    frames = {}
    for s in symbols:
        df = get_history(s, start, end)
        if not df.empty:
            frames[s] = df["Close"]
    if not frames:
        return pd.DataFrame()
    return pd.DataFrame(frames).dropna(how="all")


if __name__ == "__main__":
    spy = get_history("SPY", "2015-01-01", "2026-06-30")
    print("SPY rows:", len(spy))
    print(spy.tail(3))
