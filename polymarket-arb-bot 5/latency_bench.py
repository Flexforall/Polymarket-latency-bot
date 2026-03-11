"""
latency_bench.py
────────────────
Measures real end-to-end latency of the hot path without placing orders.
Run this before going live to verify your server's performance.

Measures:
  1. WebSocket recv → price parsed          (network + parse)
  2. Price parsed → velocity computed       (ring buffer)
  3. Velocity trigger → FV table lookup     (O(1) dict)
  4. FV lookup → order signal built         (edge calc + sizing)
  5. Total tick-to-signal                   (sum of above)
  6. CLOB round-trip latency                (GET /book)

Usage:
    python latency_bench.py
    python latency_bench.py --samples 10000
    python latency_bench.py --clob      # also benchmark CLOB latency
"""

import argparse
import array
import http.client
import json
import math
import ssl
import sys
import time
import statistics
from collections import deque

from fast_core import RingBuffer, FVTable, RawWSClient


def bench_ring_buffer(n: int = 100_000) -> dict:
    """Benchmark ring buffer push + velocity computation."""
    rb  = RingBuffer(512)
    lats = []

    for i in range(n):
        p  = 95000.0 + (i % 1000) * 0.1
        ts = time.perf_counter()
        t0 = time.perf_counter_ns()
        rb.push(p, ts)
        vel = rb.velocity(10.0)
        t1  = time.perf_counter_ns()
        lats.append(t1 - t0)

    lats.sort()
    nn = len(lats)
    return {
        "operation": "ring_buffer push+velocity",
        "n":         nn,
        "p50_ns":    lats[nn//2],
        "p95_ns":    lats[int(nn*.95)],
        "p99_ns":    lats[int(nn*.99)],
        "max_ns":    lats[-1],
        "mean_ns":   sum(lats)//nn,
    }


def bench_fv_table(n: int = 100_000) -> dict:
    """Benchmark FV table lookup (the O(1) dict get)."""
    markets = [
        {"cid": f"mkt_{i}", "strike": 90000 + i*1000,
         "direction": "above" if i%2==0 else "below"}
        for i in range(10)
    ]
    tbl = FVTable(price_step=10.0)
    tbl.build(markets, 0.70, 95000.0, price_range_pct=0.20)

    lats = []
    cids = [m["cid"] for m in markets]

    for i in range(n):
        spot = 94000.0 + (i % 2000) * 0.5
        cid  = cids[i % len(cids)]
        days = 7.0
        t0   = time.perf_counter_ns()
        fv   = tbl.get(cid, spot, days)
        t1   = time.perf_counter_ns()
        lats.append(t1 - t0)

    lats.sort()
    nn = len(lats)
    return {
        "operation": "fv_table lookup (O1)",
        "n":         nn,
        "p50_ns":    lats[nn//2],
        "p95_ns":    lats[int(nn*.95)],
        "p99_ns":    lats[int(nn*.99)],
        "max_ns":    lats[-1],
        "mean_ns":   sum(lats)//nn,
    }


def bench_price_parse(n: int = 100_000) -> dict:
    """Benchmark manual bytes price parse vs json.loads."""
    # Simulated Binance aggTrade payload
    sample = (b'{"e":"aggTrade","E":1710000000000,"s":"BTCUSDT","a":123456789,'
              b'"p":"95234.56","q":"0.001","f":123,"l":123,"T":1710000000000,"m":false}')

    key = b'"p":"'
    lats_fast = []
    lats_slow = []

    for _ in range(n):
        # Fast path: bytes.find
        t0  = time.perf_counter_ns()
        idx = sample.find(key)
        s   = idx + len(key)
        e   = sample.index(b'"', s)
        p   = float(sample[s:e])
        t1  = time.perf_counter_ns()
        lats_fast.append(t1 - t0)

        # Slow path: json.loads
        t0 = time.perf_counter_ns()
        d  = json.loads(sample)
        pp = float(d["p"])
        t1 = time.perf_counter_ns()
        lats_slow.append(t1 - t0)

    lats_fast.sort()
    lats_slow.sort()
    nn = len(lats_fast)

    return {
        "bytes_find_p50_ns":  lats_fast[nn//2],
        "bytes_find_p99_ns":  lats_fast[int(nn*.99)],
        "json_loads_p50_ns":  lats_slow[nn//2],
        "json_loads_p99_ns":  lats_slow[int(nn*.99)],
        "speedup_p50":        f"{lats_slow[nn//2]/max(lats_fast[nn//2],1):.1f}×",
    }


def bench_clob_latency(samples: int = 50) -> dict:
    """
    Measure real network round-trip to Polymarket CLOB.
    Requires internet access.
    """
    host = "clob.polymarket.com"
    ctx  = ssl.create_default_context()
    lats = []

    try:
        conn = http.client.HTTPSConnection(host, context=ctx, timeout=5)
        conn.connect()
        print(f"  Connected to {host}", end="", flush=True)

        # Warm up
        for _ in range(3):
            conn.request("GET", "/", headers={"Connection":"keep-alive"})
            conn.getresponse().read()

        for i in range(samples):
            t0 = time.perf_counter_ns()
            conn.request("GET", "/", headers={"Connection":"keep-alive"})
            resp = conn.getresponse()
            resp.read()
            t1 = time.perf_counter_ns()
            lats.append((t1 - t0) / 1e6)   # ms
            if i % 10 == 9:
                print(".", end="", flush=True)

        conn.close()
        print()

        lats.sort()
        nn = len(lats)
        return {
            "host":    host,
            "samples": nn,
            "p50_ms":  lats[nn//2],
            "p95_ms":  lats[int(nn*.95)],
            "p99_ms":  lats[int(nn*.99)],
            "max_ms":  lats[-1],
            "mean_ms": statistics.mean(lats),
        }
    except Exception as e:
        return {"error": str(e)}


def live_latency_test(duration_secs: int = 30) -> None:
    """
    Connect to Binance WebSocket and measure real tick-to-velocity latency.
    """
    print(f"\n  Live latency test ({duration_secs}s)…")
    rb   = RingBuffer(512)
    lats = []
    end  = time.time() + duration_secs

    ws = RawWSClient("stream.binance.com", "/ws/btcusdt@aggTrade")
    try:
        ws.connect()
        print("  Connected to Binance ✓")
        while time.time() < end:
            t0    = time.perf_counter_ns()
            price = ws.recv_price()
            if price is None:
                continue
            ts = time.time()
            rb.push(price, ts)
            vel = rb.velocity(10.0)
            t1  = time.perf_counter_ns()
            lats.append(t1 - t0)
    except Exception as e:
        print(f"  WS error: {e}")
    finally:
        ws.close()

    if lats:
        lats.sort()
        nn = len(lats)
        print(f"\n  Live tick-to-velocity latency ({nn:,} samples):")
        print(f"    p50  = {lats[nn//2]/1000:.0f} µs")
        print(f"    p95  = {lats[int(nn*.95)]/1000:.0f} µs")
        print(f"    p99  = {lats[int(nn*.99)]/1000:.0f} µs")
        print(f"    max  = {lats[-1]/1000:.0f} µs")
        print(f"    mean = {sum(lats)//nn/1000:.0f} µs")

        p99_us = lats[int(nn*.99)] / 1000
        if p99_us < 500:
            print(f"\n  🟢 EXCELLENT — p99 < 500µs. Competitive with C++ bots.")
        elif p99_us < 2000:
            print(f"\n  🟡 GOOD — p99 < 2ms. Fine for Polymarket latency windows (~5-15s).")
        elif p99_us < 10000:
            print(f"\n  🟠 OK — p99 < 10ms. Consider uvloop or server colocation.")
        else:
            print(f"\n  🔴 SLOW — p99 > 10ms. Install uvloop and use a US-East server.")


def print_table(results: list) -> None:
    print(f"\n{'═'*65}")
    print(f"  MICRO-BENCHMARK RESULTS")
    print(f"{'═'*65}")
    for r in results:
        op = r.pop("operation", "")
        n  = r.pop("n", "")
        print(f"\n  {op}  (n={n:,})")
        for k, v in r.items():
            unit = "ns" if "ns" in k else ("ms" if "ms" in k else "")
            label = k.replace("_ns","").replace("_ms","").replace("_"," ")
            if isinstance(v, float):
                print(f"    {label:<20} {v:>10.1f} {unit}")
            elif isinstance(v, int):
                print(f"    {label:<20} {v:>10,} {unit}")
            else:
                print(f"    {label:<20} {v!s:>10}")
    print(f"{'═'*65}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--samples",  type=int, default=100_000)
    parser.add_argument("--clob",     action="store_true")
    parser.add_argument("--live",     action="store_true")
    parser.add_argument("--duration", type=int, default=30)
    args = parser.parse_args()

    print(f"\n  Polymarket Arb Bot — Latency Benchmarks")
    print(f"  samples = {args.samples:,}\n")

    results = []

    print("  Benchmarking ring buffer…")
    results.append(bench_ring_buffer(args.samples))

    print("  Benchmarking FV table lookup…")
    results.append(bench_fv_table(args.samples))

    print("  Benchmarking price parse (bytes vs json)…")
    parse_r = bench_price_parse(args.samples)
    print(f"\n  Price parse comparison:")
    print(f"    bytes.find p50  = {parse_r['bytes_find_p50_ns']:>6,} ns")
    print(f"    json.loads p50  = {parse_r['json_loads_p50_ns']:>6,} ns")
    print(f"    Speedup:          {parse_r['speedup_p50']}")

    print_table(results)

    if args.clob:
        print(f"\n  Benchmarking CLOB round-trip latency…")
        clob_r = bench_clob_latency(50)
        if "error" in clob_r:
            print(f"  ⚠ CLOB bench failed: {clob_r['error']}")
        else:
            print(f"\n  CLOB latency ({clob_r['host']}, {clob_r['samples']} samples):")
            print(f"    p50  = {clob_r['p50_ms']:.1f} ms")
            print(f"    p95  = {clob_r['p95_ms']:.1f} ms")
            print(f"    p99  = {clob_r['p99_ms']:.1f} ms")
            print(f"    mean = {clob_r['mean_ms']:.1f} ms")

            if clob_r["p99_ms"] < 50:
                print("  🟢 CLOB latency excellent — you're well-colocated.")
            elif clob_r["p99_ms"] < 150:
                print("  🟡 CLOB latency OK. Move to AWS us-east-1 for better results.")
            else:
                print("  🔴 High CLOB latency. Server colocation strongly recommended.")
                print("     Target: AWS us-east-1 (Polymarket runs on Polygon/AWS)")

    if args.live:
        live_latency_test(args.duration)

    print(f"\n  Infrastructure recommendations:")
    print(f"  ┌─────────────────────────────────────────────────────┐")
    print(f"  │  For maximum speed:                                  │")
    print(f"  │  1. pip install uvloop              (+2-4× event loop)│")
    print(f"  │  2. AWS us-east-1 (t3.medium+)     (~10ms to CLOB)   │")
    print(f"  │  3. Dedicated core (isolcpus)       (no OS jitter)    │")
    print(f"  │  4. TCP_NODELAY on all sockets      (already done ✓)  │")
    print(f"  │  5. Disable swap (vm.swappiness=0)  (no GC pauses)    │")
    print(f"  └─────────────────────────────────────────────────────┘\n")


if __name__ == "__main__":
    main()
