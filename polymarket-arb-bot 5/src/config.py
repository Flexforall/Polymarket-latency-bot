"""
config.py
---------
All bot parameters. Edit this file before running.
Your IT guy should review API key handling and risk limits first.
"""

import os
from dataclasses import dataclass, field
from typing import Optional

# ─────────────────────────────────────────────
#  WALLET / AUTH  (set via environment variables — never hardcode keys)
# ─────────────────────────────────────────────
POLYMARKET_PRIVATE_KEY: str = os.environ.get("POLY_PRIVATE_KEY", "")
POLYMARKET_API_KEY: str     = os.environ.get("POLY_API_KEY", "")
POLYMARKET_API_SECRET: str  = os.environ.get("POLY_API_SECRET", "")
POLYMARKET_API_PASSPHRASE: str = os.environ.get("POLY_API_PASSPHRASE", "")

# Chain ID: 137 = Polygon mainnet, 80001 = Mumbai testnet
CHAIN_ID: int = int(os.environ.get("CHAIN_ID", "137"))

# ─────────────────────────────────────────────
#  API ENDPOINTS
# ─────────────────────────────────────────────
BINANCE_WS_URL    = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
POLY_GAMMA_API    = "https://gamma-api.polymarket.com"
POLY_CLOB_API     = "https://clob.polymarket.com"
POLY_DATA_API     = "https://data-api.polymarket.com"

# ─────────────────────────────────────────────
#  STRATEGY PARAMETERS  (reverse-engineered from wallet analysis)
# ─────────────────────────────────────────────
@dataclass
class StrategyConfig:
    # ── Edge detection ──
    min_edge_to_enter: float = 0.05        # Min fair-value gap to trigger entry (5¢)
    min_edge_to_hold:  float = 0.02        # Exit if edge falls below this (2¢)

    # ── Price velocity trigger ──
    # From burst analysis: bot fires when BTC moves fast, not just far
    price_velocity_threshold: float = 80.0   # USD/sec — min move speed to scan for arb
    velocity_window_secs: int = 10           # Rolling window to measure velocity

    # ── Strike proximity filter ──
    # Only watch markets where strike is within X% of spot
    max_strike_pct_from_spot: float = 0.08   # 8% max distance (e.g. BTC @ 95k → $87k–103k)
    min_strike_pct_from_spot: float = 0.00   # Include ATM markets

    # ── Expiry filter ──
    min_days_to_expiry: float = 0.5          # Don't trade same-day expiry (too noisy)
    max_days_to_expiry: float = 45.0         # Max expiry window to arb

    # ── Volatility model ──
    # Annualised BTC vol assumption for Black-Scholes fair value
    # Calibrated from historical BTC IV (~65-75% annualised)
    implied_vol: float = 0.70               # 70% annualised vol (tune based on VIX/BVIV)
    vol_smile_adjustment: bool = True       # Add vol smile skew for OTM strikes

    # ── Position sizing (from wallet analysis) ──
    base_bet_usdc: float    = 50.0          # Base trade size in USDC
    max_bet_usdc: float     = 500.0         # Max single trade
    min_bet_usdc: float     = 10.0          # Min trade (avoid dust)
    # Scale bet by edge: larger edge = larger bet
    bet_scaling_factor: float = 2.0        # bet = base * (edge / min_edge) ^ scaling
    max_concurrent_positions: int = 8      # Max open positions at once

    # ── Exit rules ──
    max_hold_seconds: int   = 1800         # Force-exit after 30min (stop holding losers)
    take_profit_edge: float = -0.01        # Exit when edge flips against you by 1¢
    hold_to_resolution: bool = True        # If deep ITM (>85¢), hold to expiry
    deep_itm_threshold: float = 0.85       # What counts as "deep ITM"

    # ── YES/NO bias (from wallet: slight YES bias on BTC upside) ──
    allow_buy_yes: bool = True
    allow_buy_no:  bool = True
    yes_bias_multiplier: float = 1.1       # Slightly prefer YES signals (1.0 = neutral)

    # ── Burst timing ──
    burst_window_secs: int = 30            # Time window to batch burst entries
    max_burst_markets: int = 5             # Max markets to enter in one burst

    # ── Risk management ──
    max_daily_loss_usdc: float = 200.0     # Kill switch: stop trading if daily loss > this
    max_total_exposure_usdc: float = 2000.0 # Max total $ at risk simultaneously
    min_liquidity_usdc: float = 500.0      # Skip market if order book too thin

    # ── Slippage tolerance ──
    max_slippage_pct: float = 0.03         # Max 3¢ slippage vs mid price


STRATEGY = StrategyConfig()

# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────
LOG_DIR          = "logs"
LOG_LEVEL        = "INFO"              # DEBUG | INFO | WARNING | ERROR
LOG_TO_FILE      = True
LOG_TRADE_CSV    = True                # Write every trade to CSV

# ─────────────────────────────────────────────
#  PAPER TRADING (dry run — no real orders sent)
# ─────────────────────────────────────────────
PAPER_TRADING: bool = True             # ← SET TO FALSE ONLY AFTER FULL REVIEW
PAPER_STARTING_BALANCE: float = 10000.0

# ─────────────────────────────────────────────
#  MARKET REFRESH
# ─────────────────────────────────────────────
MARKET_REFRESH_INTERVAL_SECS: int = 30   # Re-fetch Polymarket market list every N secs
ORDERBOOK_REFRESH_SECS: int = 5          # Re-fetch order books for watched markets
