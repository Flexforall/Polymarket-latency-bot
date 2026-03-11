"""
price_feed.py
-------------
Real-time BTC/USDT price feed via Binance WebSocket aggTrade stream.
Maintains a rolling price + timestamp history and exposes velocity metrics.
Calls registered callbacks when price updates or velocity threshold is crossed.
"""

import asyncio
import json
import time
import logging
from collections import deque
from typing import Callable, Awaitable, Optional
import websockets
from websockets.exceptions import ConnectionClosed

from config import BINANCE_WS_URL, STRATEGY
from fair_value import price_velocity, estimate_realised_vol

logger = logging.getLogger("price_feed")


class BinancePriceFeed:
    """
    Connects to Binance aggTrade WebSocket and maintains a rolling price history.

    Emits events:
      - on_tick(price, timestamp)         → called on every price update
      - on_velocity(velocity, price)      → called when abs(velocity) > threshold
    """

    def __init__(self):
        self.price: Optional[float] = None
        self.prev_price: Optional[float] = None

        # Rolling history: deque keeps last N=300 ticks (~5 minutes at ~1 tick/sec)
        self._prices:     deque[float] = deque(maxlen=300)
        self._timestamps: deque[float] = deque(maxlen=300)

        self._tick_callbacks:     list[Callable] = []
        self._velocity_callbacks: list[Callable] = []

        self._ws = None
        self._running = False
        self._reconnect_delay = 1.0

        # Stats
        self.total_ticks = 0
        self.velocity_triggers = 0
        self.connected_at: Optional[float] = None

    # ─────────────────────────────────────────────
    #  PUBLIC API
    # ─────────────────────────────────────────────

    def on_tick(self, fn: Callable[[float, float], Awaitable]) -> None:
        """Register async callback: fn(price, timestamp)"""
        self._tick_callbacks.append(fn)

    def on_velocity(self, fn: Callable[[float, float], Awaitable]) -> None:
        """Register async callback: fn(velocity_usd_per_sec, current_price)"""
        self._velocity_callbacks.append(fn)

    @property
    def current_velocity(self) -> float:
        """Current USD/sec price velocity over rolling window."""
        if len(self._prices) < 2:
            return 0.0
        return price_velocity(
            list(self._prices),
            list(self._timestamps),
            window_secs=STRATEGY.velocity_window_secs,
        )

    @property
    def implied_vol(self) -> float:
        """Estimated annualised realised vol from recent ticks."""
        return estimate_realised_vol(
            list(self._prices),
            window_secs=STRATEGY.velocity_window_secs * 30,
        )

    @property
    def recent_prices(self) -> list[float]:
        return list(self._prices)

    @property
    def recent_timestamps(self) -> list[float]:
        return list(self._timestamps)

    # ─────────────────────────────────────────────
    #  INTERNAL
    # ─────────────────────────────────────────────

    async def start(self) -> None:
        """Start the WebSocket feed. Reconnects automatically on disconnect."""
        self._running = True
        while self._running:
            try:
                await self._connect()
            except Exception as e:
                logger.warning(f"Price feed error: {e}. Reconnecting in {self._reconnect_delay:.1f}s…")
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 1.5, 30.0)

    async def stop(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()

    async def _connect(self) -> None:
        logger.info(f"Connecting to Binance: {BINANCE_WS_URL}")
        async with websockets.connect(
            BINANCE_WS_URL,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            self._ws = ws
            self._reconnect_delay = 1.0
            self.connected_at = time.time()
            logger.info("Binance WebSocket connected ✓")

            async for raw in ws:
                if not self._running:
                    break
                await self._handle_message(raw)

    async def _handle_message(self, raw: str) -> None:
        try:
            data = json.loads(raw)
            price = float(data["p"])
            ts    = float(data["T"]) / 1000.0   # Binance sends ms

            self.prev_price = self.price
            self.price = price
            self._prices.append(price)
            self._timestamps.append(ts)
            self.total_ticks += 1

            # Emit tick to all listeners
            for cb in self._tick_callbacks:
                asyncio.create_task(cb(price, ts))

            # Check velocity threshold
            if len(self._prices) >= 3:
                vel = self.current_velocity
                if abs(vel) >= STRATEGY.price_velocity_threshold:
                    self.velocity_triggers += 1
                    logger.debug(
                        f"⚡ Velocity trigger: {vel:+.1f} USD/s @ ${price:,.2f}"
                    )
                    for cb in self._velocity_callbacks:
                        asyncio.create_task(cb(vel, price))

        except (KeyError, ValueError, json.JSONDecodeError) as e:
            logger.debug(f"Message parse error: {e}")

    def summary(self) -> dict:
        return {
            "price":           self.price,
            "velocity":        self.current_velocity,
            "implied_vol":     self.implied_vol,
            "total_ticks":     self.total_ticks,
            "velocity_triggers": self.velocity_triggers,
            "uptime_secs":     time.time() - (self.connected_at or time.time()),
        }
