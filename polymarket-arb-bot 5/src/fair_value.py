"""
fair_value.py
-------------
Log-normal (Black-Scholes) probability model for BTC price markets.

Given:
  S  = current spot price (Binance)
  K  = strike price (from Polymarket market title)
  T  = time to expiry in years
  σ  = annualised volatility

P(BTC > K at expiry) = N(d1)   where d1 = (ln(S/K) + 0.5σ²T) / (σ√T)
P(BTC < K at expiry) = 1 - N(d1)

This is the exact same model a latency arb bot would use to determine
whether Polymarket's implied probability is mispriced vs spot.
"""

import math
from typing import Literal
from dataclasses import dataclass


# ─────────────────────────────────────────────────────────────────────
#  NORMAL CDF (Abramowitz & Stegun approximation — accurate to 1.5e-7)
# ─────────────────────────────────────────────────────────────────────
def _norm_cdf(x: float) -> float:
    """Cumulative distribution function for standard normal."""
    a1, a2, a3, a4, a5 = (
        0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429
    )
    p = 0.3275911
    sign = 1 if x >= 0 else -1
    x = abs(x) / math.sqrt(2)
    t = 1.0 / (1.0 + p * x)
    y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * math.exp(-x * x)
    return 0.5 * (1.0 + sign * y)


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)


# ─────────────────────────────────────────────────────────────────────
#  CORE FAIR VALUE FUNCTIONS
# ─────────────────────────────────────────────────────────────────────
@dataclass
class FairValueResult:
    probability: float          # 0–1 probability of outcome resolving YES
    d1: float                   # Black-Scholes d1
    intrinsic: float            # Pure intrinsic value (ignoring time)
    time_value: float           # Extra probability from time remaining
    spot: float
    strike: float
    days_to_expiry: float
    implied_vol: float
    direction: str              # "above" or "below"


def fair_value(
    spot: float,
    strike: float,
    days_to_expiry: float,
    annual_vol: float,
    direction: Literal["above", "below"],
    vol_smile: bool = True,
) -> FairValueResult:
    """
    Compute fair probability for a binary BTC price market.

    Args:
        spot:            Current BTC spot price from Binance
        strike:          Market's target strike price
        days_to_expiry:  Calendar days until market closes
        annual_vol:      Annualised implied volatility (e.g. 0.70 = 70%)
        direction:       "above" → P(BTC > strike), "below" → P(BTC < strike)
        vol_smile:       Apply vol smile skew for OTM/ITM options

    Returns:
        FairValueResult with probability and diagnostics
    """
    if days_to_expiry <= 0:
        # Expired — use intrinsic
        prob = 1.0 if (
            (direction == "above" and spot > strike) or
            (direction == "below" and spot < strike)
        ) else 0.0
        return FairValueResult(
            probability=prob, d1=0, intrinsic=prob,
            time_value=0, spot=spot, strike=strike,
            days_to_expiry=0, implied_vol=annual_vol, direction=direction,
        )

    T = days_to_expiry / 365.0

    # Vol smile adjustment: OTM options have higher implied vol in practice
    # Calibrated to typical BTC skew (~2-5% vol points per 10% moneyness)
    adj_vol = annual_vol
    if vol_smile:
        moneyness = math.log(spot / strike)
        # Simple quadratic smile: adds vol for OTM strikes
        smile_bump = 0.20 * moneyness ** 2 + 0.03 * abs(moneyness)
        adj_vol = annual_vol + smile_bump
        adj_vol = max(0.20, min(adj_vol, 2.0))  # clamp

    d1 = (math.log(spot / strike) + 0.5 * adj_vol ** 2 * T) / (adj_vol * math.sqrt(T))

    prob_above = _norm_cdf(d1)
    intrinsic  = 1.0 if spot > strike else 0.0

    if direction == "above":
        probability = prob_above
    else:
        probability = 1.0 - prob_above

    time_value = abs(probability - (1.0 if (
        (direction == "above" and spot > strike) or
        (direction == "below" and spot < strike)
    ) else 0.0))

    return FairValueResult(
        probability=probability,
        d1=d1,
        intrinsic=intrinsic,
        time_value=time_value,
        spot=spot,
        strike=strike,
        days_to_expiry=days_to_expiry,
        implied_vol=adj_vol,
        direction=direction,
    )


def compute_edge(
    fair: FairValueResult,
    poly_yes_price: float,
) -> float:
    """
    Edge = fair probability − Polymarket YES price.

    Positive  → BUY YES (market underpricing YES)
    Negative  → BUY NO  (market overpricing YES, i.e. underpricing NO)
    """
    return fair.probability - poly_yes_price


def edge_to_signal(
    edge: float,
    min_edge: float,
) -> Literal["BUY_YES", "BUY_NO", "NO_SIGNAL"]:
    if edge >= min_edge:
        return "BUY_YES"
    if edge <= -min_edge:
        return "BUY_NO"
    return "NO_SIGNAL"


# ─────────────────────────────────────────────────────────────────────
#  POSITION SIZING: scale bet with edge magnitude
# ─────────────────────────────────────────────────────────────────────
def size_bet(
    edge: float,
    base_usdc: float,
    min_edge: float,
    max_usdc: float,
    scaling_factor: float = 2.0,
) -> float:
    """
    Bet size scales with conviction (edge size).
    Uses power scaling: bet = base * (edge / min_edge) ^ scaling_factor

    At min_edge:  bet = base_usdc
    At 2x edge:   bet = base * 2^scaling = base * 4 (scaling=2)
    Capped at max_usdc.
    """
    if abs(edge) < min_edge:
        return 0.0
    ratio = abs(edge) / min_edge
    size = base_usdc * (ratio ** scaling_factor)
    return round(min(size, max_usdc), 2)


# ─────────────────────────────────────────────────────────────────────
#  VOL CALIBRATION: estimate implied vol from recent price history
# ─────────────────────────────────────────────────────────────────────
def estimate_realised_vol(prices: list[float], window_secs: int = 300) -> float:
    """
    Estimate annualised realised vol from recent price ticks.
    Uses log-return standard deviation scaled to annual.

    Args:
        prices:      List of recent prices (oldest → newest)
        window_secs: How many seconds the price history covers

    Returns:
        Annualised volatility estimate (e.g. 0.75 = 75%)
    """
    if len(prices) < 5:
        return 0.70  # fallback

    log_returns = [
        math.log(prices[i] / prices[i - 1])
        for i in range(1, len(prices))
        if prices[i - 1] > 0
    ]
    if not log_returns:
        return 0.70

    n = len(log_returns)
    mean = sum(log_returns) / n
    variance = sum((r - mean) ** 2 for r in log_returns) / max(n - 1, 1)
    std_per_tick = math.sqrt(variance)

    # Scale to annual: ticks per year = (365 * 86400) / secs_per_tick
    secs_per_tick = window_secs / max(n, 1)
    ticks_per_year = (365 * 86400) / max(secs_per_tick, 0.1)
    annual_vol = std_per_tick * math.sqrt(ticks_per_year)

    # Clamp to sane range
    return max(0.30, min(annual_vol, 2.50))


# ─────────────────────────────────────────────────────────────────────
#  PRICE VELOCITY: detect fast moves that trigger the arb window
# ─────────────────────────────────────────────────────────────────────
def price_velocity(
    prices: list[float],
    timestamps: list[float],
    window_secs: float = 10.0,
) -> float:
    """
    Returns price velocity in USD/second over the last `window_secs`.
    Positive = upward move, negative = downward.

    This is the key trigger: when velocity exceeds threshold,
    Polymarket markets are likely to lag behind.
    """
    now = timestamps[-1] if timestamps else 0
    cutoff = now - window_secs

    # Find prices within window
    recent = [
        (p, t) for p, t in zip(prices, timestamps)
        if t >= cutoff
    ]

    if len(recent) < 2:
        return 0.0

    oldest_price = recent[0][0]
    newest_price = recent[-1][0]
    elapsed = recent[-1][1] - recent[0][1]

    if elapsed < 0.1:
        return 0.0

    return (newest_price - oldest_price) / elapsed
