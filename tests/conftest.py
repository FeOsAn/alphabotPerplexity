"""Stub heavyweight/broker-only deps when absent so pure-logic tests run
anywhere (sandboxes without pandas_ta/alpaca). CI installs the real ones;
the stubs only fill genuine gaps."""
import sys
import types


def _stub(name):
    if name not in sys.modules:
        try:
            __import__(name)
        except Exception:
            mod = types.ModuleType(name)
            sys.modules[name] = mod
            return mod
    return None


_stub("pandas_ta")
_anth = _stub("anthropic")   # ai_research instantiates a client at module level
if _anth is not None:
    _anth.Anthropic = lambda *a, **k: types.SimpleNamespace()
_stub("openai")
_stub("schedule")

alp = _stub("alpaca")
if alp is not None:
    for sub in ("alpaca.trading", "alpaca.trading.client",
                "alpaca.trading.requests", "alpaca.trading.enums",
                "alpaca.data", "alpaca.data.historical", "alpaca.data.requests",
                "alpaca.data.timeframe", "alpaca.common", "alpaca.common.exceptions"):
        m = types.ModuleType(sub)
        sys.modules[sub] = m
    # minimal names used at import time
    sys.modules["alpaca.trading.client"].TradingClient = object
    req = sys.modules["alpaca.trading.requests"]
    for cls in ("MarketOrderRequest", "LimitOrderRequest", "StopOrderRequest",
                "StopLossRequest", "TakeProfitRequest", "GetOrdersRequest",
                "OrderClass", "ClosePositionRequest", "TrailingStopOrderRequest"):
        setattr(req, cls, type(cls, (), {}))
    en = sys.modules["alpaca.trading.enums"]
    for cls in ("OrderSide", "TimeInForce", "QueryOrderStatus", "OrderClass",
                "OrderStatus", "AssetClass"):
        setattr(en, cls, type(cls, (), {"BUY": "buy", "SELL": "sell",
                                        "GTC": "gtc", "DAY": "day",
                                        "OPEN": "open", "CLOSED": "closed",
                                        "OCO": "oco"}))
    sys.modules["alpaca.data.historical"].StockHistoricalDataClient = object
    sys.modules["alpaca.data.requests"].StockBarsRequest = type("StockBarsRequest", (), {})
    sys.modules["alpaca.data.timeframe"].TimeFrame = type("TimeFrame", (), {"Day": "day", "Minute": "min"})
    sys.modules["alpaca.common.exceptions"].APIError = type("APIError", (Exception,), {})
