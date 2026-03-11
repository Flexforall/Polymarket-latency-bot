"""
calibrate.py
------------
Fetches the target wallet's full trade history from Polymarket,
extracts every strategy parameter from real behaviour,
and writes a calibrated config.py ready to drop into the bot.

Usage:
    python calibrate.py                                        # uses default wallet
    python calibrate.py 0xABCDEF...                           # custom wallet
    python calibrate.py --out calibrated_config.py            # custom output path
"""

import asyncio
import sys
import json
import math
import statistics
import time
import re
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional
import aiohttp

# ─────────────────────────────────────────────
DEFAULT_WALLET = "0xde17f7144fbd0eddb2679132c10ff5e74b120988"
DATA_API       = "https://data-api.polymarket.com"
OUTPUT_PATH    = "src/config.py"
# ─────────────────────────────────────────────


# ══════════════════════════════════════════════════════════════════════
#  DATA FETCH
# ══════════════════════════════════════════════════════════════════════

async def fetch_all(session: aiohttp.ClientSession, url: str, max_pages: int = 30) -> list:
    results = []
    for page in range(max_pages):
        sep = "&" if "?" in url else "?"
        try:
            async with session.get(f"{url}{sep}limit=500&offset={page*500}") as r:
                if not r.ok:
                    break
                data = await r.json()
                items = data if isinstance(data, list) else data.get("data", [])
                results.extend(items)
                if len(items) < 500:
                    break
        except Exception as e:
            print(f"  Fetch error (page {page}): {e}")
            break
    return results


async def fetch_wallet(wallet: str) -> tuple[list, list]:
    print(f"\n{'─'*60}")
    print(f"  Fetching wallet: {wallet}")
    print(f"{'─'*60}")

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30),
        headers={"User-Agent": "polymarket-calibrator/1.0"},
    ) as session:
        print("  [1/2] Fetching trade history…")
        activity = await fetch_all(
            session,
            f"{DATA_API}/activity?user={wallet}&type=TRADE,REDEEM"
        )
        print(f"        → {len(activity)} records")

        print("  [2/2] Fetching open positions…")
        positions = await fetch_all(
            session,
            f"{DATA_API}/positions?user={wallet}"
        )
        print(f"        → {len(positions)} positions")

    return activity, positions


# ══════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════

def is_btc(t: dict) -> bool:
    return bool(re.search(r"bitcoin|btc", (t.get("title") or ""), re.I))


def parse_strike(title: str) -> Optional[float]:
    if not title:
        return None
    amounts = re.findall(r"\$[\d,]+(?:[kK])?(?:\.\d+)?", title)
    for amt in amounts:
        raw = amt.replace("$", "").replace(",", "")
        v = float(raw[:-1]) * 1000 if raw.lower().endswith("k") else float(raw)
        if 10_000 < v < 10_000_000:
            return v
    return None


def parse_direction(title: str) -> Optional[str]:
    t = title.lower()
    if re.search(r"above|over|exceed|reach|hit|higher|more than|at least|break", t):
        return "above"
    if re.search(r"below|under|lower|less than|drop|fall", t):
        return "below"
    return None


def median(vals: list) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    n = len(s)
    return (s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2)


def percentile(vals: list, p: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    idx = int(len(s) * p / 100)
    return s[min(idx, len(s) - 1)]


# ══════════════════════════════════════════════════════════════════════
#  CORE ANALYSIS
# ══════════════════════════════════════════════════════════════════════

@dataclass
class CalibrationResult:
    wallet: str

    # ── Trade counts ──
    total_trades:      int = 0
    total_buys:        int = 0
    total_sells:       int = 0
    total_redeems:     int = 0
    btc_buy_pct:       float = 0.0

    # ── Edge / entry ──
    avg_entry_price:   float = 0.0
    btc_avg_entry:     float = 0.0
    avg_exit_price:    float = 0.0
    avg_edge_proxy:    float = 0.0   # exit_price - entry_price on round trips
    p25_edge:          float = 0.0
    p75_edge:          float = 0.0
    min_edge_estimate: float = 0.0   # conservative edge threshold

    # ── Hold times (seconds) ──
    median_hold_secs:  float = 0.0
    avg_hold_secs:     float = 0.0
    p10_hold_secs:     float = 0.0
    p90_hold_secs:     float = 0.0
    pct_quick_exits:   float = 0.0   # exits < 5 min
    pct_held_resolution: float = 0.0

    # ── Bursts ──
    burst_count:       int   = 0
    avg_burst_size:    float = 0.0
    max_burst_size:    int   = 0
    burst_window_secs: int   = 30    # window used to detect bursts
    avg_inter_burst_secs: float = 0.0

    # ── Bet sizing ──
    avg_bet_usdc:      float = 0.0
    median_bet_usdc:   float = 0.0
    p25_bet_usdc:      float = 0.0
    p75_bet_usdc:      float = 0.0
    p90_bet_usdc:      float = 0.0
    max_bet_usdc:      float = 0.0
    base_bet_estimate: float = 0.0
    max_bet_estimate:  float = 0.0

    # ── YES/NO bias ──
    yes_count:         int   = 0
    no_count:          int   = 0
    yes_bias:          float = 1.0   # >1 = yes-biased

    # ── Financials ──
    total_in_usdc:     float = 0.0
    total_out_usdc:    float = 0.0
    total_redeemed:    float = 0.0
    est_pnl:           float = 0.0
    roi_pct:           float = 0.0
    win_rate:          float = 0.0

    # ── Concurrency ──
    avg_concurrent:    float = 0.0
    max_concurrent:    int   = 0

    # ── Derived config values ──
    cfg_min_edge:              float = 0.05
    cfg_min_edge_to_hold:      float = 0.02
    cfg_velocity_threshold:    float = 80.0
    cfg_velocity_window_secs:  int   = 10
    cfg_max_hold_seconds:      int   = 1800
    cfg_base_bet_usdc:         float = 50.0
    cfg_max_bet_usdc:          float = 500.0
    cfg_bet_scaling_factor:    float = 2.0
    cfg_yes_bias_multiplier:   float = 1.1
    cfg_max_concurrent:        int   = 8
    cfg_max_days_expiry:       float = 45.0
    cfg_implied_vol:           float = 0.70
    cfg_deep_itm_threshold:    float = 0.85
    cfg_max_daily_loss:        float = 200.0
    cfg_max_total_exposure:    float = 2000.0


def analyse(activity: list, positions: list, wallet: str) -> CalibrationResult:
    r = CalibrationResult(wallet=wallet)

    trades  = [t for t in activity if t.get("type") == "TRADE"]
    redeems = [t for t in activity if t.get("type") == "REDEEM"]
    buys    = [t for t in trades if t.get("side") == "BUY"]
    sells   = [t for t in trades if t.get("side") == "SELL"]

    r.total_trades  = len(trades)
    r.total_buys    = len(buys)
    r.total_sells   = len(sells)
    r.total_redeems = len(redeems)

    if not buys:
        print("  ⚠️  No buy trades found — cannot calibrate.")
        return r

    btc_buys  = [t for t in buys  if is_btc(t)]
    btc_sells = [t for t in sells if is_btc(t)]
    r.btc_buy_pct = len(btc_buys) / len(buys)

    # ── Entry / exit prices ──────────────────────────────────────────
    r.avg_entry_price = statistics.mean(t["price"] for t in buys)
    r.btc_avg_entry   = statistics.mean(t["price"] for t in btc_buys) if btc_buys else 0
    r.avg_exit_price  = statistics.mean(t["price"] for t in sells) if sells else 0

    # ── YES / NO bias ────────────────────────────────────────────────
    r.yes_count = sum(1 for t in btc_buys if re.search(r"\byes\b", (t.get("outcome") or ""), re.I))
    r.no_count  = sum(1 for t in btc_buys if re.search(r"\bno\b",  (t.get("outcome") or ""), re.I))
    r.yes_bias  = (r.yes_count / r.no_count) if r.no_count > 0 else 1.5

    # ── Financials ───────────────────────────────────────────────────
    r.total_in_usdc  = sum(t.get("usdcSize") or 0 for t in buys)
    r.total_out_usdc = sum(t.get("usdcSize") or 0 for t in sells)
    r.total_redeemed = sum(t.get("usdcSize") or 0 for t in redeems)
    r.est_pnl        = r.total_out_usdc + r.total_redeemed - r.total_in_usdc
    r.roi_pct        = (r.est_pnl / r.total_in_usdc * 100) if r.total_in_usdc > 0 else 0

    # ── Bet sizing ───────────────────────────────────────────────────
    sizes = sorted(t.get("usdcSize") or 0 for t in buys if (t.get("usdcSize") or 0) > 0)
    if sizes:
        r.avg_bet_usdc    = statistics.mean(sizes)
        r.median_bet_usdc = median(sizes)
        r.p25_bet_usdc    = percentile(sizes, 25)
        r.p75_bet_usdc    = percentile(sizes, 75)
        r.p90_bet_usdc    = percentile(sizes, 90)
        r.max_bet_usdc    = max(sizes)

    # ── Round trips (buy → sell on same conditionId) ─────────────────
    by_condition: dict[str, dict] = defaultdict(lambda: {"buys": [], "sells": []})
    for t in trades:
        cid = t.get("conditionId") or t.get("condition_id") or ""
        if t["side"] == "BUY":
            by_condition[cid]["buys"].append(t)
        else:
            by_condition[cid]["sells"].append(t)

    hold_times:   list[float] = []
    round_trip_edges: list[float] = []
    wins = 0
    total_rts = 0
    quick_exits = 0
    held_to_res = 0

    for cid, data in by_condition.items():
        b_list = sorted(data["buys"],  key=lambda t: t["timestamp"])
        s_list = sorted(data["sells"], key=lambda t: t["timestamp"])
        if not b_list or not s_list:
            continue

        first_buy  = b_list[0]
        first_sell = next((s for s in s_list if s["timestamp"] > first_buy["timestamp"]), None)
        if not first_sell:
            # Never sold — held to resolution or still open
            held_to_res += 1
            continue

        hold_secs = first_sell["timestamp"] - first_buy["timestamp"]
        if hold_secs < 0:
            continue

        hold_times.append(hold_secs)
        edge = first_sell["price"] - first_buy["price"]
        round_trip_edges.append(edge)

        total_rts += 1
        if edge > 0:
            wins += 1
        if hold_secs < 300:
            quick_exits += 1

    if hold_times:
        r.median_hold_secs = median(hold_times)
        r.avg_hold_secs    = statistics.mean(hold_times)
        r.p10_hold_secs    = percentile(hold_times, 10)
        r.p90_hold_secs    = percentile(hold_times, 90)
        r.pct_quick_exits  = quick_exits / len(hold_times)

    if total_rts > 0:
        r.win_rate = wins / total_rts
        r.pct_held_resolution = held_to_res / max(len(buys), 1)

    if round_trip_edges:
        r.avg_edge_proxy = statistics.mean(round_trip_edges)
        r.p25_edge       = percentile(round_trip_edges, 25)
        r.p75_edge       = percentile(round_trip_edges, 75)
        # Min edge estimate: use 25th percentile of positive exits
        pos_edges = [e for e in round_trip_edges if e > 0]
        r.min_edge_estimate = percentile(pos_edges, 20) if pos_edges else 0.05

    # ── Burst detection ──────────────────────────────────────────────
    sorted_buys = sorted(buys, key=lambda t: t["timestamp"])
    bursts: list[list] = []
    i = 0
    while i < len(sorted_buys):
        window = [sorted_buys[i]]
        j = i + 1
        while j < len(sorted_buys) and sorted_buys[j]["timestamp"] - sorted_buys[i]["timestamp"] <= 30:
            window.append(sorted_buys[j])
            j += 1
        if len(window) >= 2:
            bursts.append(window)
        i = j

    r.burst_count = len(bursts)
    if bursts:
        burst_sizes = [len(b) for b in bursts]
        r.avg_burst_size = statistics.mean(burst_sizes)
        r.max_burst_size = max(burst_sizes)

        # Inter-burst spacing → hints at cooldown between price triggers
        burst_starts = [b[0]["timestamp"] for b in bursts]
        if len(burst_starts) > 1:
            gaps = [burst_starts[k+1] - burst_starts[k] for k in range(len(burst_starts)-1)]
            r.avg_inter_burst_secs = statistics.mean(gaps)

    # ── Max concurrent positions (snapshot from timestamps) ──────────
    events = []
    for t in buys:
        events.append((t["timestamp"], +1))
    for t in sells:
        events.append((t["timestamp"], -1))
    events.sort()
    concurrent = peak = 0
    for _, delta in events:
        concurrent = max(0, concurrent + delta)
        peak = max(peak, concurrent)
    r.max_concurrent = peak
    r.avg_concurrent = len(buys) / max(1, len(bursts)) if bursts else r.avg_concurrent

    # ── Expiry range from traded markets ────────────────────────────
    expiry_days = []
    for t in btc_buys:
        title = t.get("title") or ""
        # Rough expiry estimation not available from trade data alone
        # Use a default based on what Polymarket typically offers
    r.cfg_max_days_expiry = 45.0   # conservative default

    # ══════════════════════════════════════════════════════════════════
    #  DERIVE CONFIG VALUES FROM REAL DATA
    # ══════════════════════════════════════════════════════════════════

    # min_edge_to_enter: use 20th percentile of positive round trips
    # but floor at 2¢ (below that it's noise/fees)
    r.cfg_min_edge = max(0.02, round(r.min_edge_estimate, 3)) if r.min_edge_estimate > 0 else 0.05

    # min_edge_to_hold: half of entry edge (exit when half the profit captured)
    r.cfg_min_edge_to_hold = round(r.cfg_min_edge * 0.4, 3)

    # velocity threshold: we can't directly measure this from trade data alone
    # but inter-burst spacing + burst frequency gives a proxy
    # Bursts every ~avg_inter_burst_secs → velocity must spike at that rate
    # We use a heuristic: if lots of bursts, lower threshold; fewer = higher
    if r.burst_count > 50:
        r.cfg_velocity_threshold = 50.0
    elif r.burst_count > 20:
        r.cfg_velocity_threshold = 80.0
    elif r.burst_count > 5:
        r.cfg_velocity_threshold = 120.0
    else:
        r.cfg_velocity_threshold = 150.0

    # velocity window: use p10 hold time as proxy (reaction window)
    r.cfg_velocity_window_secs = max(5, min(int(r.p10_hold_secs or 10), 30))

    # max_hold_seconds: use p90 hold time + 20% buffer
    r.cfg_max_hold_seconds = max(300, int((r.p90_hold_secs or 1800) * 1.2))

    # base_bet_usdc: use 25th percentile of bet sizes (typical small bet)
    r.base_bet_estimate = round(r.p25_bet_usdc or 20.0, 2)
    r.cfg_base_bet_usdc = max(5.0, r.base_bet_estimate)

    # max_bet_usdc: use 90th percentile (don't use absolute max — too risky)
    r.max_bet_estimate = round(r.p90_bet_usdc or 200.0, 2)
    r.cfg_max_bet_usdc = max(r.cfg_base_bet_usdc * 2, r.max_bet_estimate)

    # bet scaling: reverse-engineer from size distribution spread
    # scaling_factor = log(max/base) / log(p75_edge/min_edge) if edges available
    if r.avg_edge_proxy > r.cfg_min_edge and r.cfg_max_bet_usdc > r.cfg_base_bet_usdc:
        try:
            r.cfg_bet_scaling_factor = round(
                math.log(r.cfg_max_bet_usdc / r.cfg_base_bet_usdc) /
                math.log(max(1.01, r.p75_edge / r.cfg_min_edge)),
                2,
            )
            r.cfg_bet_scaling_factor = max(0.5, min(r.cfg_bet_scaling_factor, 4.0))
        except (ValueError, ZeroDivisionError):
            r.cfg_bet_scaling_factor = 2.0
    else:
        r.cfg_bet_scaling_factor = 2.0

    # yes bias multiplier (1.0 = neutral, >1 = prefer YES)
    r.cfg_yes_bias_multiplier = round(min(2.0, max(0.5, r.yes_bias ** 0.3)), 3)

    # max_concurrent_positions: use peak + small buffer, cap at 15
    r.cfg_max_concurrent = min(15, max(3, r.max_concurrent + 2))

    # implied_vol: we default to 0.70 (70% BTC IV is typical)
    # This can't be derived from trade prices alone without a market oracle
    r.cfg_implied_vol = 0.70

    # deep_itm threshold: if win rate is high and pct_held_resolution is high,
    # they're confident holding at high prices
    if r.pct_held_resolution > 0.5:
        r.cfg_deep_itm_threshold = 0.80
    elif r.pct_held_resolution > 0.3:
        r.cfg_deep_itm_threshold = 0.85
    else:
        r.cfg_deep_itm_threshold = 0.90

    # daily loss limit: ~10% of avg daily volume
    daily_vol = r.total_in_usdc / max(1, 365)
    r.cfg_max_daily_loss = round(max(50.0, daily_vol * 0.10), 2)

    # max total exposure: ~5× avg concurrent × avg bet
    r.cfg_max_total_exposure = round(
        max(200.0, r.cfg_max_concurrent * r.cfg_base_bet_usdc * 3), 2
    )

    return r


# ══════════════════════════════════════════════════════════════════════
#  PRINT REPORT
# ══════════════════════════════════════════════════════════════════════

def print_report(r: CalibrationResult) -> None:
    def row(label, value, note=""):
        note_str = f"  ← {note}" if note else ""
        print(f"  {label:<36} {str(value):<20}{note_str}")

    print(f"\n{'═'*65}")
    print(f"  CALIBRATION REPORT — {r.wallet[:20]}…")
    print(f"{'═'*65}")

    print("\n  TRADE HISTORY")
    row("Total buys",          r.total_buys)
    row("Total sells",         r.total_sells)
    row("BTC market focus",    f"{r.btc_buy_pct:.0%}")
    row("YES buys",            r.yes_count,  f"vs {r.no_count} NO → bias {r.yes_bias:.2f}x")
    row("Win rate (exits)",    f"{r.win_rate:.0%}")
    row("Est. ROI",            f"{r.roi_pct:+.1f}%")

    print("\n  HOLD TIME")
    row("Median hold",         f"{r.median_hold_secs:.0f}s")
    row("P10 / P90 hold",      f"{r.p10_hold_secs:.0f}s / {r.p90_hold_secs:.0f}s")
    row("Quick exits (<5min)", f"{r.pct_quick_exits:.0%}")
    row("Held to resolution",  f"{r.pct_held_resolution:.0%}")

    print("\n  BURST BEHAVIOUR (automation signal)")
    row("Burst clusters found", r.burst_count)
    row("Avg burst size",       f"{r.avg_burst_size:.1f} trades/burst")
    row("Max burst size",       r.max_burst_size)
    row("Avg inter-burst gap",  f"{r.avg_inter_burst_secs:.0f}s")

    print("\n  BET SIZING")
    row("Avg bet",              f"${r.avg_bet_usdc:.2f}")
    row("Median bet",           f"${r.median_bet_usdc:.2f}")
    row("P25 / P75 / P90 bet",  f"${r.p25_bet_usdc:.0f} / ${r.p75_bet_usdc:.0f} / ${r.p90_bet_usdc:.0f}")
    row("Max single bet",       f"${r.max_bet_usdc:.2f}")

    print("\n  EDGE ESTIMATES (from round-trip exit prices)")
    row("Avg price gain on exit", f"{r.avg_edge_proxy:+.3f}")
    row("P25 / P75 edge",         f"{r.p25_edge:+.3f} / {r.p75_edge:+.3f}")
    row("Min edge estimate",      f"{r.min_edge_estimate:.3f}")

    print(f"\n{'─'*65}")
    print("  CALIBRATED CONFIG VALUES")
    print(f"{'─'*65}")
    row("min_edge_to_enter",      r.cfg_min_edge)
    row("min_edge_to_hold",       r.cfg_min_edge_to_hold)
    row("velocity_threshold",     f"{r.cfg_velocity_threshold:.0f} USD/s")
    row("velocity_window_secs",   r.cfg_velocity_window_secs)
    row("max_hold_seconds",       r.cfg_max_hold_seconds)
    row("base_bet_usdc",          f"${r.cfg_base_bet_usdc:.2f}")
    row("max_bet_usdc",           f"${r.cfg_max_bet_usdc:.2f}")
    row("bet_scaling_factor",     r.cfg_bet_scaling_factor)
    row("yes_bias_multiplier",    r.cfg_yes_bias_multiplier)
    row("max_concurrent",         r.cfg_max_concurrent)
    row("deep_itm_threshold",     r.cfg_deep_itm_threshold)
    row("max_daily_loss_usdc",    f"${r.cfg_max_daily_loss:.2f}")
    row("max_total_exposure",     f"${r.cfg_max_total_exposure:.2f}")
    print(f"{'═'*65}\n")


# ══════════════════════════════════════════════════════════════════════
#  WRITE CALIBRATED CONFIG
# ══════════════════════════════════════════════════════════════════════

def write_config(r: CalibrationResult, out_path: str) -> None:
    content = f'''"""
config.py  ──  AUTO-CALIBRATED
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Generated by calibrate.py from live wallet data.
Source wallet : {r.wallet}
Calibrated at : {time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())}

Wallet stats used:
  Total buys        : {r.total_buys}
  BTC market focus  : {r.btc_buy_pct:.0%}
  Win rate          : {r.win_rate:.0%}
  Est. ROI          : {r.roi_pct:+.1f}%
  Burst clusters    : {r.burst_count}
  Median hold       : {r.median_hold_secs:.0f}s
  Avg bet size      : ${r.avg_bet_usdc:.2f}

Do not edit the StrategyConfig values manually — re-run calibrate.py
to update from fresh wallet data.
"""

import os
from dataclasses import dataclass

# ─────────────────────────────────────────────
#  WALLET / AUTH
# ─────────────────────────────────────────────
POLYMARKET_PRIVATE_KEY : str = os.environ.get("POLY_PRIVATE_KEY",    "")
POLYMARKET_API_KEY     : str = os.environ.get("POLY_API_KEY",        "")
POLYMARKET_API_SECRET  : str = os.environ.get("POLY_API_SECRET",     "")
POLYMARKET_API_PASSPHRASE: str = os.environ.get("POLY_API_PASSPHRASE","")
CHAIN_ID               : int = int(os.environ.get("CHAIN_ID",        "137"))

# ─────────────────────────────────────────────
#  API ENDPOINTS
# ─────────────────────────────────────────────
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
POLY_GAMMA_API = "https://gamma-api.polymarket.com"
POLY_CLOB_API  = "https://clob.polymarket.com"
POLY_DATA_API  = "https://data-api.polymarket.com"

# ─────────────────────────────────────────────
#  STRATEGY  (calibrated from wallet {r.wallet[:12]}…)
# ─────────────────────────────────────────────
@dataclass
class StrategyConfig:

    # ── Edge detection (derived from round-trip exit-price analysis) ──
    min_edge_to_enter : float = {r.cfg_min_edge}
    # Wallet\'s 20th-pctile positive exit = {r.min_edge_estimate:.4f}
    min_edge_to_hold  : float = {r.cfg_min_edge_to_hold}
    # = {round(r.cfg_min_edge_to_hold / r.cfg_min_edge * 100):.0f}% of entry edge — exit when spread mostly captured

    # ── Price velocity trigger ──
    price_velocity_threshold : float = {r.cfg_velocity_threshold}
    # Estimated from burst frequency ({r.burst_count} clusters detected)
    velocity_window_secs     : int   = {r.cfg_velocity_window_secs}
    # Based on P10 hold time ({r.p10_hold_secs:.0f}s) — reaction window

    # ── Strike proximity filter ──
    max_strike_pct_from_spot : float = 0.08
    min_strike_pct_from_spot : float = 0.00

    # ── Expiry filter ──
    min_days_to_expiry : float = 0.5
    max_days_to_expiry : float = {r.cfg_max_days_expiry}

    # ── Volatility ──
    implied_vol            : float = {r.cfg_implied_vol}
    vol_smile_adjustment   : bool  = True

    # ── Position sizing (from wallet bet-size distribution) ──
    base_bet_usdc      : float = {r.cfg_base_bet_usdc}
    # Wallet P25 bet = ${r.p25_bet_usdc:.2f}
    max_bet_usdc       : float = {r.cfg_max_bet_usdc}
    # Wallet P90 bet = ${r.p90_bet_usdc:.2f}
    min_bet_usdc       : float = {max(1.0, round(r.cfg_base_bet_usdc * 0.1, 2))}
    bet_scaling_factor : float = {r.cfg_bet_scaling_factor}
    max_concurrent_positions : int = {r.cfg_max_concurrent}
    # Wallet peak concurrent = {r.max_concurrent}

    # ── Exit rules ──
    max_hold_seconds     : int   = {r.cfg_max_hold_seconds}
    # Wallet P90 hold = {r.p90_hold_secs:.0f}s (×1.2 buffer)
    take_profit_edge     : float = {round(-r.cfg_min_edge_to_hold, 3)}
    hold_to_resolution   : bool  = {str(r.pct_held_resolution > 0.2)}
    # Wallet held-to-res rate = {r.pct_held_resolution:.0%}
    deep_itm_threshold   : float = {r.cfg_deep_itm_threshold}

    # ── YES/NO bias (wallet: {r.yes_count} YES vs {r.no_count} NO) ──
    allow_buy_yes          : bool  = True
    allow_buy_no           : bool  = True
    yes_bias_multiplier    : float = {r.cfg_yes_bias_multiplier}
    # Raw YES/NO ratio = {r.yes_bias:.2f}x (dampened with ^0.3)

    # ── Burst config ──
    burst_window_secs  : int = 30
    max_burst_markets  : int = {max(2, min(int(r.avg_burst_size + 1), 8))}
    # Wallet avg burst = {r.avg_burst_size:.1f} markets/burst

    # ── Risk ──
    max_daily_loss_usdc     : float = {r.cfg_max_daily_loss}
    max_total_exposure_usdc : float = {r.cfg_max_total_exposure}
    min_liquidity_usdc      : float = 500.0
    max_slippage_pct        : float = 0.03


STRATEGY = StrategyConfig()

# ─────────────────────────────────────────────
#  LOGGING / PAPER TRADING
# ─────────────────────────────────────────────
LOG_DIR       = "logs"
LOG_LEVEL     = "INFO"
LOG_TO_FILE   = True
LOG_TRADE_CSV = True

PAPER_TRADING           : bool  = True   # ← Set False only after IT review
PAPER_STARTING_BALANCE  : float = 10000.0

MARKET_REFRESH_INTERVAL_SECS : int = 30
ORDERBOOK_REFRESH_SECS       : int = 5
'''

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    # Back up existing config
    existing = Path(out_path)
    if existing.exists():
        backup = out_path.replace(".py", f"_backup_{int(time.time())}.py")
        existing.rename(backup)
        print(f"  ⚠  Backed up old config → {backup}")

    with open(out_path, "w") as f:
        f.write(content)

    print(f"  ✅ Calibrated config written → {out_path}")


# ══════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════

async def main():
    # Parse args
    wallet = DEFAULT_WALLET
    out    = OUTPUT_PATH

    args = sys.argv[1:]
    for i, arg in enumerate(args):
        if arg == "--out" and i + 1 < len(args):
            out = args[i + 1]
        elif arg.startswith("0x"):
            wallet = arg

    print(f"\n  Polymarket Arb Bot — Strategy Calibrator")
    print(f"  Target wallet : {wallet}")
    print(f"  Output        : {out}")

    activity, positions = await fetch_wallet(wallet)

    if not activity:
        print("\n  ❌ No trade data found for this wallet.")
        sys.exit(1)

    print("\n  Analysing trade history…")
    result = analyse(activity, positions, wallet)

    print_report(result)
    write_config(result, out)

    print(f"\n  Done. Now run:  python main.py")
    print(f"  The bot is calibrated to replicate {wallet[:16]}…\n")


if __name__ == "__main__":
    asyncio.run(main())
