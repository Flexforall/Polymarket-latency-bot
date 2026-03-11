"""
main.py
-------
Bot orchestrator — ties together price feed, market scanner, fair value
model, and trader into a single async event loop.

Replicates the observed wallet strategy:
  1. Listen to Binance BTC/USDT price in real-time
  2. On fast price move (velocity > threshold), scan Polymarket BTC markets
  3. Compute fair value (log-normal) for each candidate market
  4. Buy mispriced YES/NO tokens if |edge| > threshold
  5. Exit when Polymarket reprices (edge closes) or deep ITM → hold to resolution

Usage:
  python main.py                    # paper trading (default)
  PAPER_TRADING=false python main.py  # live trading (set after review!)
"""

import asyncio
import signal
import sys
import time
import logging
from typing import Optional

from config import STRATEGY, PAPER_TRADING, LOG_DIR, LOG_LEVEL, LOG_TO_FILE, LOG_TRADE_CSV
from logger import setup_logging, TradeCSVLogger
from price_feed import BinancePriceFeed
from market_scanner import MarketScanner, PolyMarket
from fair_value import fair_value, compute_edge, edge_to_signal, estimate_realised_vol
from trader import Trader

# ─────────────────────────────────────────────
#  SETUP
# ─────────────────────────────────────────────
setup_logging(LOG_DIR, LOG_LEVEL)
logger = logging.getLogger("main")
trade_csv = TradeCSVLogger() if LOG_TRADE_CSV else None

# ─────────────────────────────────────────────
#  GLOBAL STATE
# ─────────────────────────────────────────────
price_feed   = BinancePriceFeed()
scanner      = MarketScanner()
trader       = Trader()

_last_scan_time: float = 0.0
_burst_window_start: Optional[float] = None
_burst_entries_this_window: int = 0
_last_status_print: float = 0.0


# ─────────────────────────────────────────────
#  CORE ARB LOGIC
# ─────────────────────────────────────────────

async def scan_and_trade(velocity: float, spot: float) -> None:
    """
    Called when Binance price velocity exceeds threshold.
    Scans all candidate Polymarket markets for mispricing and enters.
    This is the hot path — keep it fast.
    """
    global _burst_window_start, _burst_entries_this_window

    now = time.time()

    # Track burst window
    if _burst_window_start is None or now - _burst_window_start > STRATEGY.burst_window_secs:
        _burst_window_start = now
        _burst_entries_this_window = 0

    # Don't exceed max burst entries
    if _burst_entries_this_window >= STRATEGY.max_burst_markets:
        logger.debug("Max burst entries reached for this window")
        return

    # Get candidate markets near current spot
    candidates = scanner.get_candidate_markets(spot)
    if not candidates:
        logger.debug(f"No candidate markets for spot=${spot:,.0f}")
        return

    # Use live realised vol if we have enough data, else fallback to config
    vol = price_feed.implied_vol if len(price_feed.recent_prices) > 30 else STRATEGY.implied_vol
    vol = max(0.30, min(vol, 1.50))   # Clamp to sane range

    logger.info(
        f"⚡ Scanning {len(candidates)} markets | "
        f"spot=${spot:,.2f} | vel={velocity:+.1f}$/s | vol={vol:.0%}"
    )

    entries_this_scan = 0
    for market in candidates:
        if _burst_entries_this_window + entries_this_scan >= STRATEGY.max_burst_markets:
            break

        try:
            result = await evaluate_market(market, spot, vol, velocity)
            if result:
                entries_this_scan += 1
        except Exception as e:
            logger.warning(f"Error evaluating {market.title[:30]}: {e}")

    _burst_entries_this_window += entries_this_scan


async def evaluate_market(
    market: PolyMarket,
    spot: float,
    vol: float,
    velocity: float,
) -> bool:
    """
    Evaluate a single market and execute if edge is sufficient.
    Returns True if a trade was entered.
    """
    fv = fair_value(
        spot=spot,
        strike=market.strike,
        days_to_expiry=market.days_to_expiry,
        annual_vol=vol,
        direction=market.direction,
        vol_smile=STRATEGY.vol_smile_adjustment,
    )

    edge   = compute_edge(fv, market.yes_price)
    signal = edge_to_signal(edge, STRATEGY.min_edge_to_enter)

    if signal == "NO_SIGNAL":
        logger.debug(
            f"  {market.title[:40]} | fv={fv.probability:.3f} poly={market.yes_price:.3f} edge={edge:+.3f} → no signal"
        )
        return False

    logger.info(
        f"  🎯 SIGNAL: {signal} | {market.title[:40]} | "
        f"fv={fv.probability:.3f} poly={market.yes_price:.3f} "
        f"edge={edge:+.3f} | {market.expiry_label} ({market.days_to_expiry:.1f}d)"
    )

    order = await trader.enter(
        market=market,
        signal=signal,
        edge=edge,
        fair_value=fv.probability,
        current_spot=spot,
    )

    if order and trade_csv:
        trade_csv.log_entry(order)

    return order is not None


# ─────────────────────────────────────────────
#  EVENT HANDLERS (callbacks from price feed)
# ─────────────────────────────────────────────

async def on_velocity(velocity: float, price: float) -> None:
    """Called by price_feed when velocity threshold is crossed."""
    global _last_scan_time
    now = time.time()

    # Throttle: don't scan more than once per 2 seconds
    if now - _last_scan_time < 2.0:
        return
    _last_scan_time = now

    await scan_and_trade(velocity, price)


async def on_tick(price: float, ts: float) -> None:
    """Called on every Binance price tick."""
    global _last_status_print

    # Print status every 60 seconds
    if time.time() - _last_status_print > 60:
        _last_status_print = time.time()
        _print_status(price)


# ─────────────────────────────────────────────
#  STATUS / HEARTBEAT
# ─────────────────────────────────────────────

def _print_status(spot: Optional[float] = None) -> None:
    stats = trader.stats_summary()
    feed  = price_feed.summary()

    logger.info("─" * 70)
    logger.info(
        f"STATUS | BTC=${spot or feed['price'] or 0:,.2f} | "
        f"vel={feed['velocity']:+.1f}$/s | "
        f"vol={feed['implied_vol']:.0%} | "
        f"markets={scanner.total_markets_seen}"
    )
    logger.info(
        f"TRADES | open={stats['open_positions']} | "
        f"closed={stats['closed_trades']} | "
        f"win_rate={stats['win_rate']:.0%} | "
        f"PnL={'+' if stats['total_pnl']>=0 else ''}${stats['total_pnl']:.2f} | "
        f"daily={'+' if stats['daily_pnl']>=0 else ''}${stats['daily_pnl']:.2f}"
    )
    if PAPER_TRADING:
        logger.info(f"PAPER  | balance=${stats['paper_balance']:,.2f}")
    logger.info("─" * 70)


# ─────────────────────────────────────────────
#  GRACEFUL SHUTDOWN
# ─────────────────────────────────────────────

async def shutdown() -> None:
    logger.info("Shutting down gracefully…")
    await price_feed.stop()
    await scanner.stop()
    await trader.stop()

    # Log final open positions
    if trader.open_positions:
        logger.info(f"  {len(trader.open_positions)} positions still open at shutdown:")
        for order in trader.open_positions.values():
            logger.info(f"    {order.summary()}")

    _print_status()
    logger.info("Bot stopped.")


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────

async def main() -> None:
    mode = "📄 PAPER TRADING" if PAPER_TRADING else "🔴 LIVE TRADING"
    logger.info("=" * 70)
    logger.info(f"  BTC Latency Arb Bot — Polymarket × Binance")
    logger.info(f"  Mode: {mode}")
    logger.info(f"  Edge threshold: {STRATEGY.min_edge_to_enter:.0%}")
    logger.info(f"  Velocity trigger: {STRATEGY.price_velocity_threshold:.0f} USD/s")
    logger.info(f"  Max position size: ${STRATEGY.max_bet_usdc:.0f}")
    logger.info(f"  Max concurrent: {STRATEGY.max_concurrent_positions}")
    logger.info(f"  Max daily loss: ${STRATEGY.max_daily_loss_usdc:.0f}")
    logger.info("=" * 70)

    if not PAPER_TRADING and not all([
        __import__("config").POLYMARKET_PRIVATE_KEY,
        __import__("config").POLYMARKET_API_KEY,
    ]):
        logger.error(
            "Live trading requires POLY_PRIVATE_KEY and POLY_API_KEY env vars. "
            "Set them or switch to PAPER_TRADING=true"
        )
        sys.exit(1)

    # Register callbacks
    price_feed.on_velocity(on_velocity)
    price_feed.on_tick(on_tick)

    # Start components
    await asyncio.gather(
        price_feed.start(),
        scanner.start(),
        trader.start(),
    )


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Handle Ctrl+C
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig,
            lambda: asyncio.create_task(shutdown())
        )

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        loop.run_until_complete(shutdown())
    finally:
        loop.close()
