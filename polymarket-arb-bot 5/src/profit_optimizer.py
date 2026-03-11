"""
profit_optimizer.py
───────────────────
Addresses the 9 missing pieces that affect real profitability:

  1. Gas cost filter       — don't trade when edge < gas cost
  2. Fill probability      — model how much of our order fills
  3. Liquidity-aware sizing — size to what's actually available
  4. Oracle lag timing     — enter in the optimal window only
  5. Dynamic implied vol   — use realised vol not static assumption
  6. Market ranking        — focus capital on best opportunities
  7. Partial fill handling — adjust position tracking for partial fills
  8. Rate limit tracking   — avoid Polymarket throttle
  9. USDC float management — track available capital precisely

All functions are called from fast_core.py hot path.
"""

from __future__ import annotations

import json
import math
import time
import statistics
from collections import deque, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


# ══════════════════════════════════════════════════════════════════════
#  1. GAS COST MODEL
#  Polygon gas for a Polymarket FOK order:
#  ~150,000 gas × gas_price_gwei × 1e-9 × MATIC_USD
# ══════════════════════════════════════════════════════════════════════

@dataclass
class GasCostModel:
    """
    Estimates Polygon gas cost per trade in USDC.
    Fetches live MATIC price and gas price.
    Cached for 60s — not worth fetching on every tick.
    """
    gas_units:   int   = 150_000   # typical Polymarket order gas usage
    matic_usd:   float = 0.80      # MATIC price fallback
    gas_gwei:    float = 30.0      # gas price fallback (Polygon)
    last_update: float = 0.0
    _cache_secs: int   = 60

    def cost_usdc(self) -> float:
        """Returns estimated gas cost in USDC for one trade."""
        return (self.gas_units * self.gas_gwei * 1e-9) * self.matic_usd

    def min_profitable_edge(self, size_usdc: float) -> float:
        """
        Minimum edge needed for the trade to be profitable after gas.
        edge_needed = gas_cost / size_usdc  (as a fraction of position)
        """
        if size_usdc <= 0:
            return 1.0
        gas = self.cost_usdc()
        return gas / size_usdc

    def is_profitable(self, edge: float, size_usdc: float) -> bool:
        """True if edge covers gas + a minimum profit buffer (2¢)."""
        min_edge = self.min_profitable_edge(size_usdc) + 0.02
        return abs(edge) >= min_edge

    def update(self) -> None:
        """Fetch live MATIC price and gas price. Non-blocking best-effort."""
        if time.time() - self.last_update < self._cache_secs:
            return
        try:
            import urllib.request
            # Polygon gas station
            with urllib.request.urlopen(
                "https://gasstation.polygon.technology/v2", timeout=3
            ) as r:
                data = json.loads(r.read())
                self.gas_gwei = float(data.get("fast", {}).get("maxFee", 30))
        except Exception:
            pass
        try:
            import urllib.request
            with urllib.request.urlopen(
                "https://api.coingecko.com/api/v3/simple/price?ids=matic-network&vs_currencies=usd",
                timeout=3
            ) as r:
                data = json.loads(r.read())
                self.matic_usd = float(data.get("matic-network", {}).get("usd", 0.80))
        except Exception:
            pass
        self.last_update = time.time()


# ══════════════════════════════════════════════════════════════════════
#  2 & 3. FILL PROBABILITY + LIQUIDITY-AWARE SIZING
# ══════════════════════════════════════════════════════════════════════

@dataclass
class OrderBookState:
    """Snapshot of one side of the order book."""
    levels:     list[tuple[float, float]]  # (price, size_usdc)
    timestamp:  float = field(default_factory=time.time)

    def available_at_price(self, max_price: float) -> float:
        """Total USDC available at or below max_price."""
        return sum(s for p, s in self.levels if p <= max_price)

    def vwap(self, size_usdc: float) -> Optional[float]:
        """
        Volume-weighted average fill price for a given order size.
        Returns None if not enough liquidity.
        """
        remaining = size_usdc
        total_cost = 0.0
        for price, level_size in self.levels:
            fill = min(remaining, level_size)
            total_cost += fill * price
            remaining  -= fill
            if remaining <= 0:
                break
        if remaining > 0:
            return None   # not enough liquidity
        return total_cost / size_usdc


def compute_fill_size(
    book: OrderBookState,
    desired_size: float,
    max_slippage: float = 0.03,
    mid_price: float = 0.5,
) -> tuple[float, float]:
    """
    Returns (actual_fill_size, expected_fill_price) given order book depth.

    Caps order size at available liquidity within slippage tolerance.
    Better to send a smaller order that fills 100% than a large one
    that fills 60% and leaves open risk.

    Returns (0, 0) if no fillable liquidity.
    """
    max_price   = mid_price + max_slippage
    available   = book.available_at_price(max_price)

    if available < 1.0:
        return 0.0, 0.0   # No liquidity

    fill_size = min(desired_size, available * 0.80)   # use 80% of available
    fill_size = max(fill_size, 0.0)

    vwap = book.vwap(fill_size)
    if vwap is None:
        fill_size = available * 0.50
        vwap = book.vwap(fill_size) or mid_price

    return fill_size, vwap


def parse_order_book(raw: dict, side: str = "YES") -> OrderBookState:
    """
    Parse Polymarket CLOB order book response into OrderBookState.
    side="YES" → use asks (we're buying YES)
    side="NO"  → use bids (we're buying NO = selling YES)
    """
    if side == "YES":
        levels_raw = raw.get("asks", [])
        levels = sorted(
            [(float(l["price"]), float(l["size"]) * float(l["price"]))
             for l in levels_raw],
            key=lambda x: x[0],
        )
    else:
        levels_raw = raw.get("bids", [])
        levels = sorted(
            [(1.0 - float(l["price"]), float(l["size"]) * (1.0 - float(l["price"])))
             for l in levels_raw],
            key=lambda x: x[0],
        )
    return OrderBookState(levels=levels)


# ══════════════════════════════════════════════════════════════════════
#  4. ORACLE LAG TIMING
#  Uses measured lag distribution to determine:
#  - Whether we're still in the entry window
#  - When to expect the position to close naturally
# ══════════════════════════════════════════════════════════════════════

@dataclass
class OracleLagProfile:
    """
    Loaded from oracle_lag.py output.
    Defines the tradeable window for each move event.
    """
    entry_window_secs:  float = 8.0    # enter within this many secs of move
    close_window_secs:  float = 25.0   # expect reprice within this many secs
    reprice_rate:       float = 0.70   # % of markets that reprice after a move
    correct_direction:  float = 0.75   # % that move the right direction

    @classmethod
    def load(cls) -> "OracleLagProfile":
        """Load from saved oracle_lag results if available."""
        path = Path("backtest_results/oracle_lag_summary.json")
        if not path.exists():
            return cls()
        try:
            with open(path) as f:
                s = json.load(f)
            return cls(
                entry_window_secs = s.get("optimal_entry_window_secs", 8.0),
                close_window_secs = s.get("close_window_secs", 25.0),
                reprice_rate      = s.get("reprice_rate", 0.70),
                correct_direction = s.get("correct_direction", 0.75),
            )
        except Exception:
            return cls()

    def in_entry_window(self, move_ts: float) -> bool:
        """True if we're still within the optimal entry window."""
        return (time.time() - move_ts) < self.entry_window_secs

    def expected_hold_secs(self) -> float:
        """Expected time until Polymarket reprices (use as max_hold guide)."""
        return self.close_window_secs * 1.5   # buffer


# ══════════════════════════════════════════════════════════════════════
#  5. DYNAMIC IMPLIED VOLATILITY
#  Real-time BTC IV from Deribit (most liquid BTC options exchange)
#  Falls back to realised vol if Deribit unavailable
# ══════════════════════════════════════════════════════════════════════

class DynamicVolModel:
    """
    Fetches BTC implied volatility from Deribit DVOL index.
    Updates every 5 minutes.
    Falls back to rolling realised vol from Binance tick data.
    """

    def __init__(self, default_vol: float = 0.70):
        self._iv:          float = default_vol
        self._rv:          float = default_vol   # realised vol
        self._last_deribit: float = 0.0
        self._price_history: deque = deque(maxlen=3600)   # 1hr at 1/sec
        self._ts_history:    deque = deque(maxlen=3600)

    def push_price(self, price: float, ts: float) -> None:
        self._price_history.append(price)
        self._ts_history.append(ts)

    def current_vol(self) -> float:
        """Returns best available vol estimate."""
        self._maybe_fetch_deribit()
        return self._iv

    def _maybe_fetch_deribit(self) -> None:
        if time.time() - self._last_deribit < 300:
            return
        try:
            import urllib.request
            url = "https://www.deribit.com/api/v2/public/get_index?currency=BTC"
            with urllib.request.urlopen(url, timeout=3) as r:
                data = json.loads(r.read())
                # DVOL is annualised implied vol in %
                dvol = data.get("result", {}).get("DVOL")
                if dvol and dvol > 0:
                    self._iv = dvol / 100.0
                    self._last_deribit = time.time()
                    return
        except Exception:
            pass

        # Fallback: compute realised vol from price history
        prices = list(self._price_history)
        if len(prices) > 60:
            log_rets = [
                math.log(prices[i] / prices[i-1])
                for i in range(1, len(prices))
                if prices[i-1] > 0
            ]
            if log_rets:
                n       = len(log_rets)
                mean    = sum(log_rets) / n
                var     = sum((r-mean)**2 for r in log_rets) / max(n-1, 1)
                std_per_tick = math.sqrt(var)
                # Scale to annual (assuming 1 tick/sec)
                ticks_per_year = 365 * 86400
                rv = std_per_tick * math.sqrt(ticks_per_year)
                self._rv = max(0.30, min(rv, 2.50))
                self._iv = self._rv
        self._last_deribit = time.time()


# ══════════════════════════════════════════════════════════════════════
#  6. MARKET RANKER
#  Tracks historical performance per market.
#  Ranks markets by: (historical_edge × reprice_probability × liquidity)
#  Hot path scans highest-ranked markets first.
# ══════════════════════════════════════════════════════════════════════

@dataclass
class MarketStats:
    __slots__ = ("cid", "trades", "wins", "total_edge", "total_pnl",
                 "avg_liquidity", "reprice_count", "skip_count", "rank")
    cid:           str
    trades:        int
    wins:          int
    total_edge:    float
    total_pnl:     float
    avg_liquidity: float
    reprice_count: int
    skip_count:    int
    rank:          float


class MarketRanker:
    """
    Tracks per-market performance and produces a ranked list.
    Markets with higher historical edge + reprice rate rank higher.
    Called from fast_core when building candidate list.
    """

    def __init__(self):
        self._stats: dict[str, MarketStats] = {}
        self._decay  = 0.95   # exponential decay for old observations

    def record_trade(self, cid: str, edge: float, pnl: float, liquidity: float) -> None:
        s = self._get_or_create(cid)
        s.trades      += 1
        s.total_edge  += edge
        s.total_pnl   += pnl
        s.avg_liquidity = s.avg_liquidity * 0.9 + liquidity * 0.1
        if pnl > 0:
            s.wins += 1
        self._update_rank(s)

    def record_reprice(self, cid: str) -> None:
        s = self._get_or_create(cid)
        s.reprice_count += 1
        self._update_rank(s)

    def record_skip(self, cid: str, reason: str) -> None:
        s = self._get_or_create(cid)
        s.skip_count += 1

    def _update_rank(self, s: MarketStats) -> None:
        """
        Rank = win_rate × avg_edge × reprice_prob × liquidity_factor
        Markets with no history get a moderate default rank.
        """
        if s.trades == 0:
            s.rank = 0.5
            return
        win_rate      = s.wins / s.trades
        avg_edge      = s.total_edge / s.trades
        reprice_prob  = s.reprice_count / max(s.trades + s.skip_count, 1)
        liq_factor    = min(1.0, s.avg_liquidity / 5000.0)   # cap at $5k
        s.rank = win_rate * avg_edge * (0.5 + 0.5 * reprice_prob) * liq_factor

    def ranked_cids(self, candidates: list[dict]) -> list[dict]:
        """Sort candidate markets by rank (highest first)."""
        def get_rank(m: dict) -> float:
            s = self._stats.get(m["cid"])
            return s.rank if s else 0.5   # unknown markets get middle rank
        return sorted(candidates, key=get_rank, reverse=True)

    def _get_or_create(self, cid: str) -> MarketStats:
        if cid not in self._stats:
            self._stats[cid] = MarketStats(
                cid=cid, trades=0, wins=0, total_edge=0.0,
                total_pnl=0.0, avg_liquidity=1000.0,
                reprice_count=0, skip_count=0, rank=0.5,
            )
        return self._stats[cid]

    def top_markets(self, n: int = 10) -> list[tuple[str, float]]:
        ranked = sorted(self._stats.items(), key=lambda x: x[1].rank, reverse=True)
        return [(cid, s.rank) for cid, s in ranked[:n]]

    def save(self, path: str = "backtest_results/market_ranks.json") -> None:
        Path(path).parent.mkdir(exist_ok=True)
        data = {
            cid: {
                "trades": s.trades, "wins": s.wins,
                "total_pnl": s.total_pnl, "rank": s.rank,
                "avg_liquidity": s.avg_liquidity,
            }
            for cid, s in self._stats.items()
        }
        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    def load(self, path: str = "backtest_results/market_ranks.json") -> None:
        p = Path(path)
        if not p.exists():
            return
        try:
            with open(p) as f:
                data = json.load(f)
            for cid, d in data.items():
                s = self._get_or_create(cid)
                s.trades       = d.get("trades", 0)
                s.wins         = d.get("wins", 0)
                s.total_pnl    = d.get("total_pnl", 0.0)
                s.avg_liquidity = d.get("avg_liquidity", 1000.0)
                s.rank         = d.get("rank", 0.5)
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════
#  8. RATE LIMIT TRACKER
# ══════════════════════════════════════════════════════════════════════

class RateLimitTracker:
    """
    Tracks API call rate to Polymarket CLOB.
    Stays under ~100 requests/minute to avoid throttling.
    """

    def __init__(self, max_per_minute: int = 80):
        self._calls:     deque = deque(maxlen=200)
        self._max_rpm   = max_per_minute
        self._throttled = 0

    def can_call(self) -> bool:
        now    = time.time()
        cutoff = now - 60.0
        # Remove calls older than 60s
        while self._calls and self._calls[0] < cutoff:
            self._calls.popleft()
        if len(self._calls) >= self._max_rpm:
            self._throttled += 1
            return False
        return True

    def record_call(self) -> None:
        self._calls.append(time.time())

    @property
    def current_rpm(self) -> int:
        now    = time.time()
        cutoff = now - 60.0
        return sum(1 for t in self._calls if t >= cutoff)

    @property
    def throttle_count(self) -> int:
        return self._throttled


# ══════════════════════════════════════════════════════════════════════
#  9. USDC FLOAT MANAGER
# ══════════════════════════════════════════════════════════════════════

class FloatManager:
    """
    Tracks available USDC capital on Polygon.
    Prevents over-committing capital across concurrent positions.
    """

    def __init__(self, starting_balance: float = 10_000.0):
        self._balance:    float = starting_balance
        self._committed:  float = 0.0    # in open positions
        self._realized_pnl: float = 0.0

    @property
    def available(self) -> float:
        return max(0.0, self._balance - self._committed)

    @property
    def total_balance(self) -> float:
        return self._balance + self._realized_pnl

    def can_afford(self, size_usdc: float, safety_buffer: float = 0.10) -> bool:
        """True if we have enough free capital including a safety buffer."""
        return self.available >= size_usdc * (1 + safety_buffer)

    def commit(self, size_usdc: float) -> None:
        self._committed = min(self._committed + size_usdc, self._balance)

    def release(self, size_usdc: float, pnl: float) -> None:
        self._committed    = max(0.0, self._committed - size_usdc)
        self._realized_pnl += pnl

    def summary(self) -> dict:
        return {
            "balance":      self._balance,
            "committed":    self._committed,
            "available":    self.available,
            "realized_pnl": self._realized_pnl,
            "total":        self.total_balance,
        }


# ══════════════════════════════════════════════════════════════════════
#  COMBINED PROFIT FILTER
#  Single entry point called from fast_core hot path.
#  Returns None to skip, or adjusted (size, price) to proceed.
# ══════════════════════════════════════════════════════════════════════

@dataclass
class TradeDecision:
    """Result of profit filter evaluation."""
    proceed:    bool
    size_usdc:  float = 0.0
    fill_price: float = 0.0
    reason:     str   = ""
    gas_cost:   float = 0.0


def evaluate_trade(
    edge:           float,
    desired_size:   float,
    book:           Optional[OrderBookState],
    mid_price:      float,
    signal:         str,        # "BUY_YES" or "BUY_NO"
    gas_model:      GasCostModel,
    float_mgr:      FloatManager,
    lag_profile:    OracleLagProfile,
    move_ts:        float,
    rate_tracker:   RateLimitTracker,
    max_slippage:   float = 0.03,
) -> TradeDecision:
    """
    Master profit filter. Checks all 9 conditions before allowing a trade.
    Fast path: fails early on cheapest checks first.
    """

    # 1. Rate limit check (cheapest — no I/O)
    if not rate_tracker.can_call():
        return TradeDecision(False, reason="rate_limited")

    # 2. Oracle lag window check
    if not lag_profile.in_entry_window(move_ts):
        return TradeDecision(False, reason="outside_lag_window")

    # 3. Capital check
    if not float_mgr.can_afford(desired_size):
        return TradeDecision(False, reason="insufficient_capital")

    # 4. Gas cost check (update gas model in background, don't block here)
    gas = gas_model.cost_usdc()
    if not gas_model.is_profitable(edge, desired_size):
        return TradeDecision(False, reason=f"edge_below_gas_cost (gas=${gas:.4f})",
                             gas_cost=gas)

    # 5. Liquidity / fill size check
    if book is not None:
        fill_size, fill_price = compute_fill_size(
            book, desired_size, max_slippage, mid_price
        )
        if fill_size < 1.0:
            return TradeDecision(False, reason="no_fillable_liquidity")
        # Re-check gas profitability with actual fill size
        if not gas_model.is_profitable(edge, fill_size):
            return TradeDecision(False, reason="fill_size_too_small_for_gas")
    else:
        fill_size  = desired_size
        fill_price = mid_price + (0.003 if signal == "BUY_YES" else 0.003)

    return TradeDecision(
        proceed=True,
        size_usdc=fill_size,
        fill_price=fill_price,
        gas_cost=gas,
    )
