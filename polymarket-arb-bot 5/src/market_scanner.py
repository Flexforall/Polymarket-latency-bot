"""
market_scanner.py
-----------------
Fetches and maintains a live list of active BTC price markets on Polymarket.
Parses strike prices and directions from market titles.
Fetches order book mid-prices to get current Polymarket implied probabilities.
"""

import asyncio
import re
import time
import logging
from dataclasses import dataclass, field
from typing import Optional, Literal
import aiohttp

from config import POLY_GAMMA_API, POLY_CLOB_API, STRATEGY

logger = logging.getLogger("market_scanner")


# ─────────────────────────────────────────────
#  DATA CLASSES
# ─────────────────────────────────────────────

@dataclass
class PolyMarket:
    """A single Polymarket BTC price market."""
    id: str
    condition_id: str
    slug: str
    title: str

    direction: Literal["above", "below"]   # YES = BTC above/below strike
    strike: float                           # Strike price in USD

    yes_token_id: str = ""
    no_token_id:  str = ""

    # Live pricing (updated by scanner)
    yes_price: float = 0.0       # Current Polymarket YES mid price (0–1)
    best_bid:  float = 0.0
    best_ask:  float = 0.0
    spread:    float = 0.0

    # Expiry
    end_date_iso: str = ""
    days_to_expiry: float = 7.0
    expiry_label: str = ""

    # Liquidity
    volume_24h: float = 0.0
    liquidity:  float = 0.0

    # Metadata
    last_price_update: float = field(default_factory=time.time)
    active: bool = True

    def is_liquid_enough(self) -> bool:
        return self.liquidity >= STRATEGY.min_liquidity_usdc

    def is_in_strike_range(self, spot: float) -> bool:
        dist = abs(spot - self.strike) / spot
        return (
            STRATEGY.min_strike_pct_from_spot <= dist <= STRATEGY.max_strike_pct_from_spot
        )

    def is_valid_expiry(self) -> bool:
        return (
            STRATEGY.min_days_to_expiry <= self.days_to_expiry <= STRATEGY.max_days_to_expiry
        )


# ─────────────────────────────────────────────
#  TITLE PARSER
# ─────────────────────────────────────────────

_ABOVE_KEYWORDS = r"above|over|exceed|reach|hit|higher|more than|at least|break above|pass"
_BELOW_KEYWORDS = r"below|under|lower|less than|drop|fall below"

def parse_btc_market(title: str) -> Optional[tuple[float, Literal["above", "below"]]]:
    """
    Extract (strike_price, direction) from a Polymarket market title.

    Examples:
      "Will BTC exceed $100,000 by March?" → (100000, "above")
      "Will Bitcoin fall below $80k?"      → (80000,  "below")

    Returns None if not a BTC price market or can't parse.
    """
    t = title.lower()

    if not re.search(r"bitcoin|btc", t):
        return None

    # Determine direction
    direction: Optional[Literal["above", "below"]] = None
    if re.search(_ABOVE_KEYWORDS, t):
        direction = "above"
    elif re.search(_BELOW_KEYWORDS, t):
        direction = "below"
    if direction is None:
        return None

    # Extract all dollar amounts from original title (case-sensitive to catch 'k'/'K')
    amounts = re.findall(r"\$[\d,]+(?:[kK])?(?:\.\d+)?", title)
    if not amounts:
        return None

    strike = None
    for amt in amounts:
        raw = amt.replace("$", "").replace(",", "")
        if raw.lower().endswith("k"):
            val = float(raw[:-1]) * 1000
        else:
            val = float(raw)
        if 10_000 < val < 10_000_000:   # Sane BTC strike range
            strike = val
            break

    if strike is None:
        return None

    return strike, direction


def parse_expiry(end_date_iso: str) -> tuple[float, str]:
    """Returns (days_to_expiry, human_label)."""
    if not end_date_iso:
        return 14.0, "~14d"
    try:
        from datetime import datetime, timezone
        dt = datetime.fromisoformat(end_date_iso.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        days = (dt - now).total_seconds() / 86400.0
        label = dt.strftime("%b %d")
        return max(days, 0.0), label
    except Exception:
        return 14.0, "~14d"


# ─────────────────────────────────────────────
#  SCANNER
# ─────────────────────────────────────────────

class MarketScanner:
    """
    Maintains a live list of watchable BTC price markets.

    - Refreshes market list every MARKET_REFRESH_INTERVAL_SECS
    - Refreshes order book pricing every ORDERBOOK_REFRESH_SECS
    """

    def __init__(self):
        self.markets: dict[str, PolyMarket] = {}   # condition_id → PolyMarket
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self.last_refresh: float = 0.0
        self.total_markets_seen: int = 0

    async def start(self) -> None:
        self._running = True
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15),
            headers={"User-Agent": "polymarket-arb-bot/1.0"},
        )
        logger.info("Market scanner started")
        await asyncio.gather(
            self._market_refresh_loop(),
            self._price_refresh_loop(),
        )

    async def stop(self) -> None:
        self._running = False
        if self._session:
            await self._session.close()

    # ── Market list refresh ──────────────────────────────────────────

    async def _market_refresh_loop(self) -> None:
        from config import MARKET_REFRESH_INTERVAL_SECS
        while self._running:
            try:
                await self._fetch_markets()
            except Exception as e:
                logger.warning(f"Market refresh error: {e}")
            await asyncio.sleep(MARKET_REFRESH_INTERVAL_SECS)

    async def _fetch_markets(self) -> None:
        """Fetch active BTC price markets from Polymarket Gamma API."""
        new_markets: dict[str, PolyMarket] = {}

        tag_slugs = ["bitcoin", "crypto", "btc-price"]
        seen_ids: set[str] = set()

        for slug in tag_slugs:
            try:
                url = f"{POLY_GAMMA_API}/markets?active=true&closed=false&tag_slug={slug}&limit=100"
                async with self._session.get(url) as resp:
                    if not resp.ok:
                        continue
                    raw = await resp.json()
                    items = raw if isinstance(raw, list) else raw.get("markets", raw.get("data", []))

                    for m in items:
                        mid = m.get("id", "")
                        if mid in seen_ids:
                            continue
                        seen_ids.add(mid)

                        title = m.get("question") or m.get("title") or ""
                        parsed = parse_btc_market(title)
                        if not parsed:
                            continue

                        strike, direction = parsed
                        cid = m.get("conditionId", mid)

                        days, label = parse_expiry(
                            m.get("end_date_iso") or m.get("endDateIso") or ""
                        )

                        # Extract token IDs for CLOB trading
                        yes_tok = no_tok = ""
                        tokens = m.get("tokens") or m.get("clobTokenIds") or []
                        if isinstance(tokens, list) and len(tokens) >= 2:
                            yes_tok = tokens[0].get("token_id", tokens[0]) if isinstance(tokens[0], dict) else tokens[0]
                            no_tok  = tokens[1].get("token_id", tokens[1]) if isinstance(tokens[1], dict) else tokens[1]

                        # Get current price from outcome prices
                        yes_price = 0.5
                        op = m.get("outcomePrices")
                        if op:
                            try:
                                prices = op if isinstance(op, list) else __import__("json").loads(op)
                                yes_price = float(prices[0])
                                if yes_price > 1:
                                    yes_price /= 100
                            except Exception:
                                pass

                        market = PolyMarket(
                            id=mid,
                            condition_id=cid,
                            slug=m.get("slug", ""),
                            title=title,
                            direction=direction,
                            strike=strike,
                            yes_token_id=yes_tok,
                            no_token_id=no_tok,
                            yes_price=yes_price,
                            end_date_iso=m.get("end_date_iso") or m.get("endDateIso") or "",
                            days_to_expiry=days,
                            expiry_label=label,
                            volume_24h=float(m.get("volume24hr", 0) or 0),
                            liquidity=float(m.get("liquidity", 0) or 0),
                            active=True,
                        )
                        new_markets[cid] = market

            except aiohttp.ClientError as e:
                logger.warning(f"Gamma API error for slug={slug}: {e}")

        if new_markets:
            self.markets = new_markets
            self.total_markets_seen = len(new_markets)
            self.last_refresh = time.time()
            logger.info(f"Refreshed {len(new_markets)} BTC markets from Polymarket")
        else:
            logger.warning("Market refresh returned 0 markets — keeping previous list")

    # ── Order book / price refresh ───────────────────────────────────

    async def _price_refresh_loop(self) -> None:
        from config import ORDERBOOK_REFRESH_SECS
        await asyncio.sleep(5)   # Wait for initial market load
        while self._running:
            try:
                await self._refresh_prices()
            except Exception as e:
                logger.debug(f"Price refresh error: {e}")
            await asyncio.sleep(ORDERBOOK_REFRESH_SECS)

    async def _refresh_prices(self) -> None:
        """Fetch order book mid prices for all tracked markets."""
        tasks = [
            self._fetch_market_price(cid, m)
            for cid, m in list(self.markets.items())
            if m.yes_token_id
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _fetch_market_price(self, cid: str, market: PolyMarket) -> None:
        """Fetch CLOB order book for a single market."""
        try:
            url = f"{POLY_CLOB_API}/book?token_id={market.yes_token_id}"
            async with self._session.get(url) as resp:
                if not resp.ok:
                    return
                book = await resp.json()

                bids = book.get("bids", [])
                asks = book.get("asks", [])

                best_bid = float(bids[0]["price"]) if bids else 0.0
                best_ask = float(asks[0]["price"]) if asks else 1.0

                market.best_bid  = best_bid
                market.best_ask  = best_ask
                market.yes_price = (best_bid + best_ask) / 2
                market.spread    = best_ask - best_bid
                market.last_price_update = time.time()

        except Exception as e:
            logger.debug(f"CLOB price fetch error for {cid}: {e}")

    # ── Public query ─────────────────────────────────────────────────

    def get_candidate_markets(self, spot: float) -> list[PolyMarket]:
        """
        Returns markets worth scanning for arb, filtered by:
          - BTC strike within configured range of spot
          - Valid expiry window
          - Sufficient liquidity
          - Price data is fresh
        """
        now = time.time()
        candidates = []
        for m in self.markets.values():
            if not m.active:
                continue
            if not m.is_in_strike_range(spot):
                continue
            if not m.is_valid_expiry():
                continue
            if not m.is_liquid_enough():
                continue
            if now - m.last_price_update > 60:
                continue    # Stale price — skip
            candidates.append(m)

        # Sort by proximity to spot (closest strikes first)
        candidates.sort(key=lambda m: abs(m.strike - spot))
        return candidates
