"""
Intraday VWAP Reclaim Strategy — AlphaBot
Entry signal: stock gaps down >2% at open, then reclaims VWAP by 10:30 AM ET.
This is an institutional buying signal — smart money absorbing the gap-down.
Exits: +4% take profit, -2% stop loss, or market close (same day).
Max 5 concurrent positions, 3% portfolio per trade.
Only runs 9:35–10:30 AM ET for entries. Exits run all day.
"""
import gc
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

MAX_POSITIONS = 5
POSITION_PCT = 0.03
TAKE_PROFIT = 0.04
STOP_LOSS = -0.02
GAP_DOWN_MIN = 0.02
ENTRY_WINDOW_START = (13, 35)   # 9:35 AM ET = 13:35 UTC
ENTRY_WINDOW_END = (14, 30)     # 10:30 AM ET = 14:30 UTC

VWAP_UNIVERSE = [
    "AAPL", "MSFT", "NVDA", "AMD", "META", "GOOGL", "AMZN", "TSLA",
    "AVGO", "CRM", "PANW", "CRWD", "NET", "DDOG", "SNOW",
    "JPM", "GS", "MS", "V", "MA",
    "SPY", "QQQ", "IWM",
    "NFLX", "UBER", "COIN", "PLTR",
]

_active_positions: dict = {}
_scanned_today: set = set()
_scan_date: str = ""


def _get_intraday_data(symbol: str) -> dict:
    """
    Fetch today's intraday bars and compute VWAP.
    Returns dict with keys: current_price, vwap, prev_close, gap_pct, above_vwap
    """
    import yfinance as yf
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="2d", interval="5m", auto_adjust=True)
        if hist is None or len(hist) < 4:
            return {}

        now_utc = datetime.now(timezone.utc)
        today_str = now_utc.strftime("%Y-%m-%d")

        try:
            mask_today = hist.index.strftime("%Y-%m-%d") == today_str
            today_bars = hist[mask_today]
        except Exception:
            today_bars = hist.tail(20)

        if today_bars.empty or len(today_bars) < 2:
            return {}

        typical = (today_bars["High"] + today_bars["Low"] + today_bars["Close"]) / 3
        vol = today_bars["Volume"]
        vol_sum = float(vol.sum())
        vwap = float((typical * vol).sum() / vol_sum) if vol_sum > 0 else float(today_bars["Close"].mean())

        current = float(today_bars["Close"].iloc[-1])

        try:
            mask_prev = hist.index.strftime("%Y-%m-%d") != today_str
            prev_bars = hist[mask_prev]
        except Exception:
            prev_bars = hist.head(max(0, len(hist) - len(today_bars)))

        prev_close = float(prev_bars["Close"].iloc[-1]) if not prev_bars.empty else current

        gap_pct = (float(today_bars["Open"].iloc[0]) - prev_close) / prev_close if prev_close > 0 else 0.0

        return {
            "current_price": current,
            "vwap": vwap,
            "prev_close": prev_close,
            "gap_pct": gap_pct,
            "above_vwap": current >= vwap,
        }
    except Exception as e:
        logger.debug(f"[VWAPReclaim] Data error {symbol}: {e}")
        return {}
    finally:
        gc.collect()


def _is_entry_window() -> bool:
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5:
        return False
    start_min = ENTRY_WINDOW_START[0] * 60 + ENTRY_WINDOW_START[1]
    end_min = ENTRY_WINDOW_END[0] * 60 + ENTRY_WINDOW_END[1]
    now_min = now.hour * 60 + now.minute
    return start_min <= now_min <= end_min


def _manage_positions(broker):
    """Check take profit, stop loss, EOD exit for VWAP positions."""
    import yfinance as yf
    now = datetime.now(timezone.utc)
    eod_min = 19 * 60 + 55  # 3:55 PM ET = 19:55 UTC
    now_min = now.hour * 60 + now.minute

    for sym, pos in list(_active_positions.items()):
        try:
            fi = yf.Ticker(sym).fast_info
            current = getattr(fi, "last_price", None)
            if not current:
                continue
            current = float(current)
            entry = pos["entry_price"]
            pnl_pct = (current - entry) / entry if entry else 0.0
            reason = None
            if pnl_pct >= TAKE_PROFIT:
                reason = f"TAKE_PROFIT {pnl_pct:+.2%}"
            elif pnl_pct <= STOP_LOSS:
                reason = f"STOP_LOSS {pnl_pct:+.2%}"
            elif now_min >= eod_min:
                reason = f"EOD_EXIT {pnl_pct:+.2%}"
            if reason:
                try:
                    broker.submit_order(
                        symbol=sym, qty=pos["qty"],
                        side="sell", type="market", time_in_force="day"
                    )
                except Exception as e:
                    logger.error(f"[VWAPReclaim] Close order failed {sym}: {e}")
                    continue
                logger.info(f"[VWAPReclaim] Closed {sym} — {reason}")
                del _active_positions[sym]
        except Exception as e:
            logger.debug(f"[VWAPReclaim] Position check error {sym}: {e}")
        finally:
            gc.collect()


def _get_equity(broker) -> float:
    """Robust equity getter across broker API styles."""
    try:
        if hasattr(broker, "get_account"):
            acct = broker.get_account()
            if isinstance(acct, dict):
                return float(acct.get("equity") or acct.get("portfolio_value") or 0.0)
            return float(getattr(acct, "equity", 0.0))
        if hasattr(broker, "trading"):
            acct = broker.trading.get_account()
            return float(getattr(acct, "equity", 0.0))
    except Exception:
        pass
    return 0.0


def run(broker, db_conn=None):
    """Main entry — scan for VWAP reclaim setups and manage existing positions."""
    global _scan_date, _scanned_today

    if _active_positions:
        _manage_positions(broker)

    if not _is_entry_window():
        return

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _scan_date != today:
        _scanned_today = set()
        _scan_date = today

    if len(_active_positions) >= MAX_POSITIONS:
        return

    try:
        equity = _get_equity(broker)
        if equity <= 0:
            logger.warning("[VWAPReclaim] Equity unavailable — skipping cycle")
            return
    except Exception as e:
        logger.error(f"[VWAPReclaim] Account error: {e}")
        return

    for sym in VWAP_UNIVERSE:
        if sym in _scanned_today:
            continue
        if sym in _active_positions:
            continue
        if len(_active_positions) >= MAX_POSITIONS:
            break

        data = _get_intraday_data(sym)
        if not data:
            continue

        gap_pct = data.get("gap_pct", 0.0)
        above_vwap = data.get("above_vwap", False)
        current = data.get("current_price", 0.0)
        vwap = data.get("vwap", 0.0)

        if gap_pct <= -GAP_DOWN_MIN and above_vwap and current > 0:
            _scanned_today.add(sym)
            logger.info(
                f"[VWAPReclaim] SIGNAL: {sym} gapped {gap_pct:.2%} at open, "
                f"now ${current:.2f} above VWAP ${vwap:.2f} — entering"
            )
            try:
                trade_value = equity * POSITION_PCT
                qty = int(trade_value / current)
                if qty < 1:
                    continue
                broker.submit_order(
                    symbol=sym, qty=qty, side="buy",
                    type="market", time_in_force="day"
                )
                _active_positions[sym] = {
                    "entry_price": current,
                    "vwap_at_entry": vwap,
                    "qty": qty,
                }
                logger.info(f"[VWAPReclaim] BUY {qty} {sym} @ ${current:.2f} (VWAP ${vwap:.2f})")
            except Exception as e:
                logger.error(f"[VWAPReclaim] Order error {sym}: {e}")
        else:
            _scanned_today.add(sym)
