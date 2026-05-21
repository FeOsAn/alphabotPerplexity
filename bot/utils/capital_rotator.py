"""
Capital Rotation Engine
-----------------------
When a high-conviction opportunity (score >= ROTATION_MIN_SCORE) appears but
the portfolio has insufficient cash, this module:
1. Scores all current positions by "stay score" (reason to keep holding)
2. If the new opportunity's conviction score exceeds the weakest position's
   stay score by ROTATION_EDGE_THRESHOLD, sell the weakest position first
3. Then enter the new opportunity with the freed capital

Guiding principle: always be in the highest expected-value positions available.
"""
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# --- Config ---
ROTATION_MIN_SCORE       = 0.25   # new opportunity must score >= this to trigger rotation
ROTATION_EDGE_THRESHOLD  = 0.15   # new opp score must beat weakest position's stay_score by this margin
ROTATION_MIN_GAIN_TO_KEEP = 0.12  # never rotate out of a position up >12% (let winners run)
ROTATION_MAX_LOSS_TO_KEEP = -0.05 # always rotate out of positions down >5% if better opp exists
ROTATION_PROTECTED_TAGS  = {"pairs_trading", "short_hedge"}  # never rotate out of these


def _compute_stay_score(pos: dict, rsi: float, gain_pct: float, days_held: int,
                        momentum_score: float) -> float:
    """
    Score for keeping a position (0.0 = definitely exit, 1.0 = definitely keep).

    Factors:
    - gain_pct: big winners get bonus (let winners run)
    - rsi: overbought positions get penalty (likely to revert)
    - days_held: fresh positions get benefit of the doubt
    - momentum_score: current 3m momentum score of the symbol
    """
    score = 0.5  # neutral base

    # Gain factor: +0.3 for big winners, -0.2 for losers
    if gain_pct >= 0.12:
        score += 0.30
    elif gain_pct >= 0.05:
        score += 0.15
    elif gain_pct >= 0:
        score += 0.05
    elif gain_pct >= -0.03:
        score -= 0.10
    else:
        score -= 0.20  # sitting in a loser

    # RSI factor: overbought positions are closer to reversal
    if rsi >= 85:
        score -= 0.20
    elif rsi >= 75:
        score -= 0.10
    elif rsi <= 40:
        score -= 0.10  # downtrend
    elif 50 <= rsi <= 70:
        score += 0.05  # sweet spot

    # Momentum score factor
    if momentum_score >= 0.25:
        score += 0.15
    elif momentum_score >= 0.10:
        score += 0.05
    elif momentum_score < 0.03:
        score -= 0.15  # momentum fading

    # Days held: give fresh positions (< 3 days) benefit of the doubt
    if days_held <= 3:
        score += 0.10

    return max(0.0, min(1.0, score))


def find_rotation_candidate(
    new_symbol: str,
    new_score: float,
    new_notional: float,
    current_positions: list,
    broker,
    db_conn,
) -> Optional[str]:
    """
    Given a new opportunity, find the weakest current position to rotate out of.

    Returns the symbol to sell, or None if rotation is not warranted.
    """
    from utils import yf_cache
    from db import get_trades_for_symbol
    from config import MIN_CASH_RESERVE_PCT

    # Gate 1: new opportunity must meet minimum conviction
    if new_score < ROTATION_MIN_SCORE:
        return None

    # Gate 2: must actually need cash (if we have enough cash, no rotation needed)
    try:
        acc = broker.get_account()
        cash = float(acc["cash"])
        pv = float(acc["portfolio_value"])
        available = cash - pv * MIN_CASH_RESERVE_PCT
        if available >= new_notional:
            return None  # have enough cash, no rotation needed
    except Exception as e:
        logger.warning(f"[Rotator] Account check failed: {e}")
        return None

    # Score every current position
    candidates = []

    for pos in current_positions:
        sym = pos["symbol"]
        strategy_tag = pos.get("strategy", "")

        # Never rotate out of protected strategies
        if strategy_tag in ROTATION_PROTECTED_TAGS:
            continue

        # Never rotate into a symbol we already hold
        if sym == new_symbol:
            continue

        # broker.get_positions() always returns unrealized_pnl_pct already * 100
        # (e.g. +17.5 for +17.5%). Divide by 100 to get fraction for threshold comparisons.
        raw_pnl_pct = pos.get("unrealized_pnl_pct", pos.get("unrealized_plpc", 0.0))
        gain_pct = float(raw_pnl_pct) / 100.0

        # Never rotate out of strong winners
        if gain_pct >= ROTATION_MIN_GAIN_TO_KEEP:
            continue

        # Compute RSI and momentum score
        try:
            hist = yf_cache.get_history(sym, period="60d", interval="1d")
            if hist is None or hist.empty or len(hist) < 20:
                rsi = 50.0
                momentum_score = 0.0
            else:
                close = hist["Close"].dropna()
                # RSI(14)
                delta = close.diff()
                gain_s = delta.clip(lower=0).rolling(14).mean()
                loss_s = (-delta.clip(upper=0)).rolling(14).mean()
                last_gain = gain_s.iloc[-1]
                last_loss = loss_s.iloc[-1]
                if last_loss and last_loss > 0:
                    rs = last_gain / last_loss
                    rsi = float(100 - 100 / (1 + rs))
                else:
                    rsi = 50.0
                # ~3m momentum score (lookback up to 63 trading days)
                if len(close) >= 5:
                    lookback = min(63, len(close) - 1)
                    momentum_score = float(close.iloc[-1] / close.iloc[-lookback] - 1)
                else:
                    momentum_score = 0.0
        except Exception:
            rsi = 50.0
            momentum_score = 0.0

        # Days held — try DB first, fallback to 0
        days_held = 0
        try:
            trades = get_trades_for_symbol(db_conn, sym)
            if trades:
                buys = [t for t in trades if str(t.get("side", "")).startswith("buy")]
                if buys:
                    last_buy = max(buys, key=lambda t: t.get("created_at", ""))
                    created_at = last_buy.get("created_at", "")
                    if created_at:
                        # sqlite default created_at is "YYYY-MM-DD HH:MM:SS" (UTC, no tz)
                        try:
                            entry_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                        except ValueError:
                            entry_dt = datetime.strptime(created_at[:19], "%Y-%m-%d %H:%M:%S")
                        if entry_dt.tzinfo is None:
                            entry_dt = entry_dt.replace(tzinfo=timezone.utc)
                        days_held = (datetime.now(timezone.utc) - entry_dt).days
        except Exception:
            pass

        stay_score = _compute_stay_score(pos, rsi, gain_pct, days_held, momentum_score)

        candidates.append({
            "symbol": sym,
            "stay_score": stay_score,
            "gain_pct": gain_pct,
            "rsi": rsi,
            "momentum_score": momentum_score,
            "days_held": days_held,
        })

        logger.debug(f"[Rotator] {sym}: stay_score={stay_score:.2f} gain={gain_pct:+.1%} rsi={rsi:.0f}")

    if not candidates:
        return None

    # Sort by stay_score ascending — weakest position first
    candidates.sort(key=lambda x: x["stay_score"])
    weakest = candidates[0]

    # Gate 3: new opportunity must beat weakest position by ROTATION_EDGE_THRESHOLD
    edge = new_score - weakest["stay_score"]
    if edge < ROTATION_EDGE_THRESHOLD:
        logger.debug(
            f"[Rotator] {new_symbol} (score={new_score:.2f}) not enough edge over "
            f"{weakest['symbol']} (stay={weakest['stay_score']:.2f}, "
            f"edge={edge:.2f} < {ROTATION_EDGE_THRESHOLD})"
        )
        return None

    logger.info(
        f"[Rotator] ROTATE: sell {weakest['symbol']} "
        f"(stay={weakest['stay_score']:.2f}, gain={weakest['gain_pct']:+.1%}) → "
        f"buy {new_symbol} (score={new_score:.2f}, edge={edge:.2f})"
    )
    return weakest["symbol"]


def execute_rotation(
    sell_symbol: str,
    buy_symbol: str,
    buy_notional: float,
    buy_score: float,
    broker,
    db_conn,
    strategy_name: str,
) -> bool:
    """
    Execute the rotation: close sell_symbol. The caller is responsible for
    placing the buy order for buy_symbol after this returns True.
    Returns True if the sell completed successfully.
    """
    from utils import notify
    from db import log_trade

    try:
        sell_pos = next(
            (p for p in broker.get_positions() if p["symbol"] == sell_symbol),
            None,
        )
        if not sell_pos:
            logger.warning(f"[Rotator] {sell_symbol} no longer in positions — aborting rotation")
            return False

        sell_price = float(sell_pos.get("current_price", 0))
        sell_qty = abs(float(sell_pos.get("qty", 0)))
        sell_gain_pct_raw = sell_pos.get("unrealized_pnl_pct", 0.0)
        sell_gain = float(sell_gain_pct_raw) / 100.0  # broker stores pct*100
        realized_pnl = float(sell_pos.get("unrealized_pnl") or 0.0)

        logger.info(
            f"[Rotator] Closing {sell_symbol} (gain={sell_gain:+.1%}) to fund {buy_symbol}"
        )
        broker.close_position(sell_symbol, f"{strategy_name}_rotation")
        log_trade(
            db_conn, f"{strategy_name}_rotation", sell_symbol, "sell_rotation",
            sell_qty, sell_price, realized_pnl,
        )

        try:
            pass  # [ntfy silenced — logged only]
        except Exception as _ne:
            logger.debug(f"[Rotator] notify failed: {_ne}")

        return True

    except Exception as e:
        logger.error(f"[Rotator] execute_rotation failed: {e}")
        return False
