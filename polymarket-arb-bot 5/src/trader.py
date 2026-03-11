"""
trader.py
---------
Handles order execution on Polymarket via the CLOB API.

In PAPER_TRADING=True mode: simulates fills, tracks virtual PnL.
In live mode: signs and submits real limit/market orders via py-clob-client.

The original wallet places MARKET orders (takes liquidity) to guarantee fill
during the brief latency window. We replicate this behavior.
"""

import asyncio
import time
import uuid
import logging
from dataclasses import dataclass, field
from typing import Optional, Literal
import aiohttp

from config import (
    STRATEGY, PAPER_TRADING, PAPER_STARTING_BALANCE,
    POLYMARKET_PRIVATE_KEY, POLYMARKET_API_KEY,
    POLYMARKET_API_SECRET, POLYMARKET_API_PASSPHRASE,
    CHAIN_ID, POLY_CLOB_API,
)
from market_scanner import PolyMarket

logger = logging.getLogger("trader")


# ─────────────────────────────────────────────
#  ORDER / POSITION DATA CLASSES
# ─────────────────────────────────────────────

@dataclass
class Order:
    id: str
    condition_id: str
    market_title: str
    side: Literal["BUY_YES", "BUY_NO"]
    size_usdc: float
    entry_price: float          # YES price at entry (0–1)
    fair_value: float           # Our model's fair value at entry
    edge_at_entry: float        # Fair value − market price at entry
    timestamp: float
    token_id: str               # YES or NO token id
    status: Literal["OPEN", "CLOSED", "EXPIRED"] = "OPEN"
    exit_price: Optional[float] = None
    exit_timestamp: Optional[float] = None
    pnl_usdc: Optional[float] = None
    shares: float = 0.0         # Number of outcome shares bought

    @property
    def hold_seconds(self) -> float:
        end = self.exit_timestamp or time.time()
        return end - self.timestamp

    @property
    def is_deep_itm(self) -> bool:
        if self.exit_price is None:
            return False
        return self.exit_price >= STRATEGY.deep_itm_threshold

    def summary(self) -> str:
        sign = "+" if (self.pnl_usdc or 0) >= 0 else ""
        return (
            f"[{self.side}] {self.market_title[:45]} | "
            f"entry={self.entry_price:.3f} exit={self.exit_price or '?'} | "
            f"hold={self.hold_seconds:.0f}s | "
            f"PnL={sign}{self.pnl_usdc:.2f}" if self.pnl_usdc is not None
            else f"[{self.side}] {self.market_title[:45]} | entry={self.entry_price:.3f} | OPEN"
        )


@dataclass
class PaperAccount:
    balance: float = PAPER_STARTING_BALANCE
    total_pnl: float = 0.0
    total_trades: int = 0
    winning_trades: int = 0
    daily_pnl: float = 0.0
    day_start: float = field(default_factory=time.time)

    def reset_daily(self) -> None:
        if time.time() - self.day_start > 86400:
            self.daily_pnl = 0.0
            self.day_start = time.time()


# ─────────────────────────────────────────────
#  TRADER
# ─────────────────────────────────────────────

class Trader:
    """
    Executes trades on Polymarket.
    Supports both paper trading (default) and live trading.
    """

    def __init__(self):
        self.open_positions: dict[str, Order] = {}    # order_id → Order
        self.closed_positions: list[Order] = []
        self.paper = PaperAccount()
        self._session: Optional[aiohttp.ClientSession] = None
        self._clob_client = None    # py-clob-client instance (live only)
        self._running = False

        # Risk: track daily spend
        self._daily_spent: float = 0.0
        self._day_start: float = time.time()

    async def start(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20)
        )
        self._running = True

        if not PAPER_TRADING:
            await self._init_live_client()
            logger.warning("🔴 LIVE TRADING MODE ACTIVE — real money at risk")
        else:
            logger.info(f"📄 Paper trading mode | starting balance: ${self.paper.balance:,.2f}")

        # Start position monitor loop
        asyncio.create_task(self._position_monitor_loop())

    async def stop(self) -> None:
        self._running = False
        if self._session:
            await self._session.close()

    # ─────────────────────────────────────────────
    #  ENTRY
    # ─────────────────────────────────────────────

    async def enter(
        self,
        market: PolyMarket,
        signal: Literal["BUY_YES", "BUY_NO"],
        edge: float,
        fair_value: float,
        current_spot: float,
    ) -> Optional[Order]:
        """
        Execute entry order for an arb signal.
        Returns the Order if successful, None if skipped/failed.
        """
        # ── Risk checks ──
        if not self._passes_risk_checks(market, signal, edge):
            return None

        size_usdc = self._size_position(edge)
        if size_usdc < STRATEGY.min_bet_usdc:
            logger.debug(f"Skipping {market.title[:30]} — size too small: ${size_usdc:.2f}")
            return None

        # Determine execution price and token
        if signal == "BUY_YES":
            exec_price = market.best_ask or market.yes_price   # Market order: take ask
            token_id   = market.yes_token_id
        else:
            exec_price = 1.0 - (market.best_bid or market.yes_price)  # NO price = 1 - YES bid
            token_id   = market.no_token_id

        # Slippage check
        mid_price = market.yes_price if signal == "BUY_YES" else (1 - market.yes_price)
        slippage   = abs(exec_price - mid_price)
        if slippage > STRATEGY.max_slippage_pct:
            logger.info(
                f"Skipping {market.title[:30]} — slippage too high: {slippage:.3f}"
            )
            return None

        shares = size_usdc / exec_price if exec_price > 0 else 0

        order = Order(
            id=str(uuid.uuid4()),
            condition_id=market.condition_id,
            market_title=market.title,
            side=signal,
            size_usdc=size_usdc,
            entry_price=exec_price,
            fair_value=fair_value,
            edge_at_entry=edge,
            timestamp=time.time(),
            token_id=token_id,
            shares=shares,
        )

        if PAPER_TRADING:
            success = await self._paper_enter(order)
        else:
            success = await self._live_enter(order, market, signal, size_usdc, exec_price)

        if success:
            self.open_positions[order.id] = order
            logger.info(
                f"✅ ENTERED | {signal} | {market.title[:40]} | "
                f"price={exec_price:.3f} | size=${size_usdc:.2f} | edge={edge:+.3f}"
            )
            return order

        return None

    # ─────────────────────────────────────────────
    #  EXIT
    # ─────────────────────────────────────────────

    async def exit(self, order: Order, current_yes_price: float, reason: str) -> None:
        """
        Close an open position.
        current_yes_price = current Polymarket YES mid price.
        """
        if order.id not in self.open_positions:
            return

        # Determine exit price for our side
        if order.side == "BUY_YES":
            exit_price = current_yes_price   # We sell YES shares back
        else:
            exit_price = 1.0 - current_yes_price   # We sell NO shares back

        if PAPER_TRADING:
            await self._paper_exit(order, exit_price)
        else:
            await self._live_exit(order, exit_price)

        # Calculate PnL
        pnl = (exit_price - order.entry_price) * order.shares
        order.exit_price = exit_price
        order.exit_timestamp = time.time()
        order.pnl_usdc = pnl
        order.status = "CLOSED"

        self.paper.total_pnl += pnl
        self.paper.daily_pnl += pnl
        self.paper.total_trades += 1
        if pnl > 0:
            self.paper.winning_trades += 1

        del self.open_positions[order.id]
        self.closed_positions.append(order)

        sign = "+" if pnl >= 0 else ""
        logger.info(
            f"{'✅' if pnl >= 0 else '❌'} EXITED  | {order.side} | "
            f"{order.market_title[:40]} | "
            f"entry={order.entry_price:.3f} exit={exit_price:.3f} | "
            f"hold={order.hold_seconds:.0f}s | "
            f"PnL={sign}${pnl:.2f} | reason={reason}"
        )

    # ─────────────────────────────────────────────
    #  POSITION MONITOR (exit logic)
    # ─────────────────────────────────────────────

    async def _position_monitor_loop(self) -> None:
        """
        Continuously check open positions and exit when:
        1. Edge has closed (Polymarket repriced to fair value)
        2. Position is deep ITM → hold to resolution
        3. Max hold time exceeded → cut loss
        4. Price moved against us → stop loss
        """
        while self._running:
            await asyncio.sleep(3)
            await self._check_positions()

    async def _check_positions(self) -> None:
        from market_scanner import PolyMarket
        for order_id, order in list(self.open_positions.items()):
            try:
                await self._evaluate_exit(order)
            except Exception as e:
                logger.debug(f"Position eval error for {order_id}: {e}")

    async def _evaluate_exit(self, order: Order) -> None:
        """Decide whether to exit this position now."""
        # Need current price — fetch from CLOB
        current_yes = await self._fetch_current_price(order.token_id)
        if current_yes is None:
            return

        hold_secs = order.hold_seconds

        # Current edge (from our entry fair value perspective)
        if order.side == "BUY_YES":
            current_price = current_yes
            # Edge has closed when poly price converged toward our fair value entry
            remaining_edge = order.fair_value - current_yes
        else:
            current_price = 1.0 - current_yes
            remaining_edge = (1.0 - order.fair_value) - (1.0 - current_yes)

        # ── HOLD TO RESOLUTION: deep ITM ──
        if current_price >= STRATEGY.deep_itm_threshold and STRATEGY.hold_to_resolution:
            logger.debug(
                f"Deep ITM ({current_price:.3f}) — holding to resolution: {order.market_title[:30]}"
            )
            return

        # ── TAKE PROFIT: edge has closed ──
        if remaining_edge <= STRATEGY.min_edge_to_hold:
            await self.exit(order, current_yes, reason="edge_closed")
            return

        # ── STOP LOSS: edge flipped against us ──
        if remaining_edge <= STRATEGY.take_profit_edge:
            await self.exit(order, current_yes, reason="stop_loss")
            return

        # ── MAX HOLD TIME ──
        if hold_secs >= STRATEGY.max_hold_seconds:
            await self.exit(order, current_yes, reason="max_hold_time")
            return

    async def _fetch_current_price(self, token_id: str) -> Optional[float]:
        """Fetch current YES mid price from CLOB."""
        if not token_id or not self._session:
            return None
        try:
            url = f"{POLY_CLOB_API}/book?token_id={token_id}"
            async with self._session.get(url) as resp:
                if not resp.ok:
                    return None
                book = await resp.json()
                bids = book.get("bids", [])
                asks = book.get("asks", [])
                bid = float(bids[0]["price"]) if bids else 0.0
                ask = float(asks[0]["price"]) if asks else 1.0
                return (bid + ask) / 2
        except Exception:
            return None

    # ─────────────────────────────────────────────
    #  PAPER TRADING EXECUTION
    # ─────────────────────────────────────────────

    async def _paper_enter(self, order: Order) -> bool:
        if self.paper.balance < order.size_usdc:
            logger.warning("Paper account: insufficient balance")
            return False
        self.paper.balance -= order.size_usdc
        logger.debug(f"[PAPER] Entered ${order.size_usdc:.2f} | balance: ${self.paper.balance:.2f}")
        return True

    async def _paper_exit(self, order: Order, exit_price: float) -> None:
        proceeds = exit_price * order.shares
        self.paper.balance += proceeds
        logger.debug(f"[PAPER] Exited ${proceeds:.2f} | balance: ${self.paper.balance:.2f}")

    # ─────────────────────────────────────────────
    #  LIVE TRADING EXECUTION (via py-clob-client)
    # ─────────────────────────────────────────────

    async def _init_live_client(self) -> None:
        """
        Initialise the Polymarket CLOB client.
        Requires py-clob-client: pip install py-clob-client
        """
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds

            creds = ApiCreds(
                api_key=POLYMARKET_API_KEY,
                api_secret=POLYMARKET_API_SECRET,
                api_passphrase=POLYMARKET_API_PASSPHRASE,
            )
            self._clob_client = ClobClient(
                host=POLY_CLOB_API,
                key=POLYMARKET_PRIVATE_KEY,
                chain_id=CHAIN_ID,
                creds=creds,
            )
            logger.info("CLOB client initialised ✓")
        except ImportError:
            raise RuntimeError(
                "py-clob-client not installed. Run: pip install py-clob-client"
            )
        except Exception as e:
            raise RuntimeError(f"CLOB client init failed: {e}")

    async def _live_enter(
        self,
        order: Order,
        market: PolyMarket,
        signal: Literal["BUY_YES", "BUY_NO"],
        size_usdc: float,
        price: float,
    ) -> bool:
        """
        Submit a real order to Polymarket CLOB.
        Uses MARKET order (FOK) to guarantee fill in the latency window.
        """
        if not self._clob_client:
            logger.error("CLOB client not initialised")
            return False

        try:
            from py_clob_client.clob_types import MarketOrderArgs, OrderType

            loop = asyncio.get_event_loop()
            args = MarketOrderArgs(
                token_id=order.token_id,
                amount=size_usdc,    # USDC amount to spend
            )

            # Submit in thread pool (py-clob-client is sync)
            result = await loop.run_in_executor(
                None,
                lambda: self._clob_client.create_and_post_market_order(args)
            )

            if result and result.get("status") in ("MATCHED", "PLACED"):
                logger.info(f"Live order placed: {result.get('orderID', '?')}")
                return True
            else:
                logger.warning(f"Live order failed: {result}")
                return False

        except Exception as e:
            logger.error(f"Live order error: {e}")
            return False

    async def _live_exit(self, order: Order, exit_price: float) -> None:
        """
        Sell shares back on the CLOB.
        """
        if not self._clob_client:
            return
        try:
            from py_clob_client.clob_types import MarketOrderArgs

            loop = asyncio.get_event_loop()
            args = MarketOrderArgs(
                token_id=order.token_id,
                amount=order.shares,   # Shares to sell
                side="SELL",
            )
            result = await loop.run_in_executor(
                None,
                lambda: self._clob_client.create_and_post_market_order(args)
            )
            logger.info(f"Live exit placed: {result}")
        except Exception as e:
            logger.error(f"Live exit error: {e}")

    # ─────────────────────────────────────────────
    #  RISK
    # ─────────────────────────────────────────────

    def _passes_risk_checks(
        self,
        market: PolyMarket,
        signal: str,
        edge: float,
    ) -> bool:
        """Return True if we're allowed to take this trade."""
        # Reset daily tracker if new day
        if time.time() - self._day_start > 86400:
            self._daily_spent = 0.0
            self._day_start = time.time()

        # Max concurrent positions
        if len(self.open_positions) >= STRATEGY.max_concurrent_positions:
            logger.debug("Max concurrent positions reached — skipping")
            return False

        # Daily loss kill switch
        if self.paper.daily_pnl <= -STRATEGY.max_daily_loss_usdc:
            logger.warning(
                f"Daily loss limit hit (${self.paper.daily_pnl:.2f}) — no new trades today"
            )
            return False

        # Max total exposure
        total_exposure = sum(
            p.size_usdc for p in self.open_positions.values()
        )
        if total_exposure >= STRATEGY.max_total_exposure_usdc:
            logger.debug("Max exposure reached — skipping")
            return False

        # Don't double-up on same market
        for p in self.open_positions.values():
            if p.condition_id == market.condition_id:
                logger.debug(f"Already in position for {market.title[:30]}")
                return False

        # Signal direction filter
        if signal == "BUY_YES" and not STRATEGY.allow_buy_yes:
            return False
        if signal == "BUY_NO" and not STRATEGY.allow_buy_no:
            return False

        return True

    def _size_position(self, edge: float) -> float:
        from fair_value import size_bet
        base = STRATEGY.base_bet_usdc
        if abs(edge) / STRATEGY.min_edge_to_enter > 1.5:
            # Strong signal — apply YES bias multiplier if relevant
            base *= STRATEGY.yes_bias_multiplier

        return size_bet(
            edge=edge,
            base_usdc=base,
            min_edge=STRATEGY.min_edge_to_enter,
            max_usdc=STRATEGY.max_bet_usdc,
            scaling_factor=STRATEGY.bet_scaling_factor,
        )

    # ─────────────────────────────────────────────
    #  STATS
    # ─────────────────────────────────────────────

    @property
    def win_rate(self) -> float:
        if not self.paper.total_trades:
            return 0.0
        return self.paper.winning_trades / self.paper.total_trades

    @property
    def total_pnl(self) -> float:
        return self.paper.total_pnl

    def stats_summary(self) -> dict:
        return {
            "open_positions":   len(self.open_positions),
            "closed_trades":    len(self.closed_positions),
            "total_pnl":        self.paper.total_pnl,
            "daily_pnl":        self.paper.daily_pnl,
            "win_rate":         self.win_rate,
            "paper_balance":    self.paper.balance,
        }
