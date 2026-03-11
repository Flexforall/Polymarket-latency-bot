"""
fast_main.py
────────────
High-speed entry point. Replaces main.py.

Architecture:
  ┌─────────────────────────────────────────────────────────┐
  │  Thread 1: price_feed  (Binance raw WS)                 │
  │    recv bytes → parse price → velocity → FV lookup      │
  │    → push OrderSignal to queue  [target: <1ms]          │
  │                    │                                     │
  │                    ▼  (lock-free queue)                  │
  │  Thread 2: order_dispatch                               │
  │    sign → POST /order (persistent TCP)  [target: <3ms]  │
  │                                                         │
  │  Thread 3: market_refresh  (background, every 5s)       │
  │    updates MarketCache prices from CLOB order books     │
  │                                                         │
  │  Thread 4: stats_printer (every 30s)                    │
  └─────────────────────────────────────────────────────────┘

Usage:
    python fast_main.py                  # paper mode
    PAPER_TRADING=false python fast_main.py  # live
"""

import asyncio
import json
import math
import os
import signal
import sys
import threading
import time
import ssl
import http.client
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── Try uvloop ───────────────────────────────────────────────────────
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    print("✓ uvloop active")
except ImportError:
    print("  uvloop not installed (pip install uvloop for 2-4× speedup)")

from fast_core import HotPathEngine, MarketCache
import re

# ─────────────────────────────────────────────
#  LOAD CONFIG
# ─────────────────────────────────────────────

def load_config() -> dict:
    """Load from calibrated src/config.py if available, else defaults."""
    cfg = {
        "velocity_threshold": 80.0,
        "velocity_window":    10,
        "min_edge":           0.05,
        "min_edge_to_hold":   0.02,
        "implied_vol":        0.70,
        "max_hold_secs":      1800,
        "base_bet":           50.0,
        "max_bet":            500.0,
        "scaling_factor":     2.0,
        "max_concurrent":     8,
        "deep_itm":           0.85,
        "paper_trading":      os.environ.get("PAPER_TRADING", "true").lower() != "false",
        "clob_host":          "https://clob.polymarket.com",
        "api_key":            os.environ.get("POLY_API_KEY", ""),
        "api_secret":         os.environ.get("POLY_API_SECRET", ""),
        "api_pass":           os.environ.get("POLY_API_PASSPHRASE", ""),
        "private_key":        os.environ.get("POLY_PRIVATE_KEY", ""),
    }

    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location("config", "src/config.py")
        mod  = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        s = mod.STRATEGY
        cfg.update({
            "velocity_threshold": s.price_velocity_threshold,
            "velocity_window":    s.velocity_window_secs,
            "min_edge":           s.min_edge_to_enter,
            "min_edge_to_hold":   s.min_edge_to_hold,
            "implied_vol":        s.implied_vol,
            "max_hold_secs":      s.max_hold_seconds,
            "base_bet":           s.base_bet_usdc,
            "max_bet":            s.max_bet_usdc,
            "scaling_factor":     s.bet_scaling_factor,
            "max_concurrent":     s.max_concurrent_positions,
            "deep_itm":           s.deep_itm_threshold,
            "paper_trading":      mod.PAPER_TRADING,
        })
        print("✓ Loaded calibrated config from src/config.py")
    except Exception as e:
        print(f"  Using default config ({e})")

    return cfg


# ─────────────────────────────────────────────
#  MARKET BOOTSTRAP
#  Fetch all BTC markets and populate cache
# ─────────────────────────────────────────────

def _parse_btc(title: str):
    if not re.search(r"bitcoin|btc", title, re.I):
        return None
    t = title.lower()
    if re.search(r"above|over|exceed|reach|hit|higher|more than|at least|break", t):
        direction = "above"
    elif re.search(r"below|under|lower|less than|drop|fall", t):
        direction = "below"
    else:
        return None
    amounts = re.findall(r"\$[\d,]+(?:[kK])?(?:\.\d+)?", title)
    for amt in amounts:
        raw = amt.replace("$","").replace(",","")
        v = float(raw[:-1])*1000 if raw.lower().endswith("k") else float(raw)
        if 10_000 < v < 10_000_000:
            return v, direction
    return None


def _days_to_expiry(end_iso: str) -> float:
    if not end_iso:
        return 14.0
    try:
        dt = datetime.fromisoformat(end_iso.replace("Z","+00:00"))
        return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds() / 86400)
    except Exception:
        return 14.0


def bootstrap_markets(cache: MarketCache) -> int:
    """
    Synchronously fetch all BTC markets from Polymarket Gamma API.
    Called once at startup before the hot path begins.
    """
    import urllib.request
    count = 0

    for slug in ["bitcoin", "crypto"]:
        url = f"https://gamma-api.polymarket.com/markets?active=true&closed=false&tag_slug={slug}&limit=200"
        try:
            with urllib.request.urlopen(url, timeout=15) as r:
                raw  = json.loads(r.read())
                items = raw if isinstance(raw, list) else raw.get("markets", [])
                for m in items:
                    title = m.get("question") or m.get("title") or ""
                    parsed = _parse_btc(title)
                    if not parsed:
                        continue
                    strike, direction = parsed
                    cid = m.get("conditionId") or m.get("id") or ""
                    if not cid:
                        continue

                    # Extract token IDs
                    yes_tok = no_tok = ""
                    tokens = m.get("tokens") or []
                    if isinstance(tokens, list) and len(tokens) >= 2:
                        t0 = tokens[0]
                        t1 = tokens[1]
                        yes_tok = t0.get("token_id", t0) if isinstance(t0, dict) else t0
                        no_tok  = t1.get("token_id", t1) if isinstance(t1, dict) else t1

                    # Current YES price
                    yes_price = 0.5
                    op = m.get("outcomePrices")
                    if op:
                        try:
                            prices = op if isinstance(op, list) else json.loads(op)
                            yes_price = float(prices[0])
                            if yes_price > 1:
                                yes_price /= 100
                        except Exception:
                            pass

                    days = _days_to_expiry(m.get("end_date_iso") or m.get("endDateIso") or "")
                    if days < 0.5 or days > 45:
                        continue

                    cache.markets[cid] = {
                        "cid":       cid,
                        "title":     title,
                        "strike":    strike,
                        "direction": direction,
                        "days":      days,
                        "yes_tok":   yes_tok,
                        "no_tok":    no_tok,
                        "liquidity": float(m.get("liquidity", 0) or 0),
                    }
                    cache.prices[cid] = yes_price
                    count += 1
        except Exception as e:
            print(f"  ⚠ Market fetch error ({slug}): {e}")

    return count


# ─────────────────────────────────────────────
#  BACKGROUND: Market refresh thread
#  Updates prices every 5s — off the hot path
# ─────────────────────────────────────────────

def market_refresh_thread(cache: MarketCache, clob_host: str,
                          running_flag: list) -> None:
    """
    Keeps MarketCache.prices fresh without blocking the price thread.
    Uses persistent connection to Polymarket CLOB.
    Also refreshes days_to_expiry so FV table stays accurate.
    """
    host = clob_host.replace("https://","").rstrip("/")
    ctx  = ssl.create_default_context()

    while running_flag[0]:
        time.sleep(5)
        try:
            conn = http.client.HTTPSConnection(host, context=ctx, timeout=5)
            conn.connect()
            for cid, m in list(cache.markets.items()):
                tok = m.get("yes_tok","")
                if not tok:
                    continue
                try:
                    conn.request("GET", f"/book?token_id={tok}",
                                 headers={"Connection":"keep-alive"})
                    resp = conn.getresponse()
                    if resp.status == 200:
                        data = json.loads(resp.read())
                        bids = data.get("bids",[])
                        asks = data.get("asks",[])
                        bid  = float(bids[0]["price"]) if bids else 0.0
                        ask  = float(asks[0]["price"]) if asks else 1.0
                        cache.prices[cid] = (bid + ask) / 2
                    else:
                        resp.read()
                except Exception:
                    try:
                        conn.close()
                        conn = http.client.HTTPSConnection(host, context=ctx, timeout=5)
                        conn.connect()
                    except Exception:
                        break

                # Update days to expiry
                try:
                    from datetime import datetime, timezone
                    end_iso = cache.markets[cid].get("end_iso","")
                    if end_iso:
                        dt = datetime.fromisoformat(end_iso.replace("Z","+00:00"))
                        d  = (dt - datetime.now(timezone.utc)).total_seconds()/86400
                        cache.markets[cid]["days"] = max(0.0, d)
                except Exception:
                    pass

            conn.close()
        except Exception as e:
            pass   # Silently retry — never interrupt main threads


# ─────────────────────────────────────────────
#  STATS PRINTER THREAD
# ─────────────────────────────────────────────

def stats_thread(engine: HotPathEngine, running_flag: list) -> None:
    while running_flag[0]:
        time.sleep(30)
        engine.print_stats()

        # Latency report
        lat = engine.latency_stats()
        if lat and lat.get("samples", 0) > 10:
            p50  = lat["p50_us"]
            p99  = lat["p99_us"]
            mean = lat["mean_us"]
            grade = (
                "🟢 EXCELLENT" if p99 < 1000 else
                "🟡 GOOD"      if p99 < 5000 else
                "🟠 OK"        if p99 < 20000 else
                "🔴 SLOW"
            )
            print(f"  Tick→signal latency: p50={p50:.0f}µs  "
                  f"p99={p99:.0f}µs  mean={mean:.0f}µs  {grade}")
            if p99 > 10000:
                print("  ⚠ High latency detected. Consider:")
                print("    → Colocate server near AWS us-east-1")
                print("    → pip install uvloop")
                print("    → Reduce max_concurrent_positions")


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────

def main():
    cfg = load_config()

    mode = "📄 PAPER" if cfg["paper_trading"] else "🔴 LIVE"
    print(f"\n{'═'*60}")
    print(f"  BTC Latency Arb Bot  —  FAST MODE")
    print(f"  Mode:      {mode}")
    print(f"  Vel trig:  {cfg['velocity_threshold']:.0f} USD/s")
    print(f"  Min edge:  {cfg['min_edge']*100:.0f}¢")
    print(f"  Implied V: {cfg['implied_vol']*100:.0f}%")
    print(f"  Base bet:  ${cfg['base_bet']:.0f}  Max: ${cfg['max_bet']:.0f}")
    print(f"{'═'*60}\n")

    if not cfg["paper_trading"] and not cfg["api_key"]:
        print("❌ Live mode requires POLY_API_KEY env var.")
        sys.exit(1)

    # ── Bootstrap markets ────────────────────────────────────────────
    print("  Bootstrapping market cache…")
    engine = HotPathEngine(cfg)
    n = bootstrap_markets(engine.market_cache)
    print(f"  ✓ {n} BTC markets loaded into memory")

    if n == 0:
        print("  ⚠ No markets found — check internet connection")
        sys.exit(1)

    # ── Build initial FV table ───────────────────────────────────────
    # Use a dummy spot price — will be rebuilt on first velocity trigger
    dummy_spot = 95_000.0
    candidates = engine.market_cache.get_candidates(dummy_spot, max_pct=0.99)
    if candidates:
        engine.fv_table.build(candidates, cfg["implied_vol"],
                               dummy_spot, price_range_pct=0.20)
        print(f"  ✓ FV lookup table built for {len(candidates)} markets")

    # ── Start background threads ─────────────────────────────────────
    running = [True]

    ref_thread = threading.Thread(
        target=market_refresh_thread,
        args=(engine.market_cache, cfg["clob_host"], running),
        name="market-refresh",
        daemon=True,
    )
    ref_thread.start()

    st_thread = threading.Thread(
        target=stats_thread,
        args=(engine, running),
        name="stats",
        daemon=True,
    )
    st_thread.start()

    # ── Start hot path ───────────────────────────────────────────────
    engine.start()
    print("  ✓ Hot path running\n")
    print("  Press Ctrl+C to stop\n")

    # ── Signal handler ───────────────────────────────────────────────
    def shutdown(sig, frame):
        print("\n  Shutting down…")
        running[0] = False
        engine.stop()
        engine.print_stats()
        sys.exit(0)

    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # ── Keep main thread alive ───────────────────────────────────────
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
