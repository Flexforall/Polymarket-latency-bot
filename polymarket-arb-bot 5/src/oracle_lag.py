"""
oracle_lag.py
─────────────
Measures the ACTUAL lag between Binance BTC price moves and
Polymarket market repricing. This is the single most important
number for the strategy — it defines the tradeable window.

How it works:
  1. Stream Binance BTC/USDT live
  2. Detect a significant price move (>$50 in <10s)
  3. Poll Polymarket order books every 500ms after the move
  4. Record exactly when each market reprices
  5. Build a distribution of lag times

Run this for 2-4 hours to get statistically significant results.
Output feeds directly into fast_core.py timing parameters.

Usage:
    python oracle_lag.py                  # measure live
    python oracle_lag.py --duration 7200  # 2 hours
    python oracle_lag.py --report         # show saved results
"""

import asyncio
import json
import math
import ssl
import http.client
import socket
import struct
import time
import statistics
import csv
import argparse
from collections import deque
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

RESULTS_PATH = Path("backtest_results/oracle_lag.csv")
SUMMARY_PATH = Path("backtest_results/oracle_lag_summary.json")

# ─────────────────────────────────────────────
#  DATA CLASSES
# ─────────────────────────────────────────────

@dataclass
class MoveEvent:
    """A significant Binance price move."""
    ts:         float
    price_from: float
    price_to:   float
    delta:      float
    velocity:   float   # USD/s
    direction:  str     # "up" or "down"


@dataclass
class LagObservation:
    """One measured lag between Binance move and Polymarket reprice."""
    move_ts:        float
    move_delta:     float
    move_velocity:  float
    market_cid:     str
    market_title:   str
    strike:         float
    direction:      str
    poly_price_before: float
    poly_price_after:  float
    price_change:      float
    lag_secs:          float
    expected_direction: str   # did poly move the right way?
    correct:           bool


# ─────────────────────────────────────────────
#  FAST WEBSOCKET (reuse from fast_core pattern)
# ─────────────────────────────────────────────

class FastWS:
    __slots__ = ("_ssl_sock", "_host", "_path")
    _PRICE_KEY = b'"p":"'

    def __init__(self, host: str, path: str):
        self._host = host
        self._path = path
        self._ssl_sock = None

    def connect(self) -> None:
        import base64, os
        ctx = ssl.create_default_context()
        raw = socket.create_connection((self._host, 443), timeout=10)
        raw.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._ssl_sock = ctx.wrap_socket(raw, server_hostname=self._host)
        key = base64.b64encode(os.urandom(16)).decode()
        req = (f"GET {self._path} HTTP/1.1\r\nHost: {self._host}\r\n"
               f"Upgrade: websocket\r\nConnection: Upgrade\r\n"
               f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n").encode()
        self._ssl_sock.sendall(req)
        resp = b""
        while b"\r\n\r\n" not in resp:
            resp += self._ssl_sock.recv(4096)

    def recv_price(self) -> Optional[float]:
        sock = self._ssl_sock
        try:
            hdr = self._recv_exact(2)
            b0, b1 = hdr[0], hdr[1]
            opcode = b0 & 0x0F
            plen   = b1 & 0x7F
            if plen == 126:
                plen = struct.unpack(">H", self._recv_exact(2))[0]
            elif plen == 127:
                plen = struct.unpack(">Q", self._recv_exact(8))[0]
            if (b1 & 0x80):
                self._recv_exact(4)  # mask (server→client not masked)
            payload = self._recv_exact(plen)
            if opcode == 0x09:
                self._ssl_sock.sendall(bytes([0x8A, 0]))
                return None
            if opcode != 0x01:
                return None
            idx = payload.find(self._PRICE_KEY)
            if idx == -1:
                return None
            s = idx + len(self._PRICE_KEY)
            e = payload.index(b'"', s)
            return float(payload[s:e])
        except Exception:
            return None

    def _recv_exact(self, n: int) -> bytes:
        data = b""
        while len(data) < n:
            chunk = self._ssl_sock.recv(n - len(data))
            if not chunk:
                raise ConnectionError("closed")
            data += chunk
        return data

    def close(self):
        try:
            self._ssl_sock.close()
        except Exception:
            pass


# ─────────────────────────────────────────────
#  POLYMARKET PRICE POLLER
# ─────────────────────────────────────────────

class PolyPoller:
    """
    Polls Polymarket order book prices for watched markets.
    Uses persistent HTTP connection.
    """

    def __init__(self):
        self._conn: Optional[http.client.HTTPSConnection] = None
        self._markets: list[dict] = []   # {cid, title, strike, direction, yes_tok}

    def connect(self) -> None:
        ctx = ssl.create_default_context()
        self._conn = http.client.HTTPSConnection(
            "clob.polymarket.com", context=ctx, timeout=3
        )
        self._conn.connect()

    def load_markets(self) -> int:
        """Fetch BTC price markets from Gamma API."""
        import urllib.request, re
        self._markets = []
        seen = set()
        for slug in ["bitcoin", "crypto"]:
            url = f"https://gamma-api.polymarket.com/markets?active=true&tag_slug={slug}&limit=200"
            try:
                with urllib.request.urlopen(url, timeout=15) as r:
                    items = json.loads(r.read())
                    if isinstance(items, dict):
                        items = items.get("markets", [])
                    for m in items:
                        title = m.get("question") or m.get("title") or ""
                        cid   = m.get("conditionId") or m.get("id") or ""
                        if cid in seen or not cid:
                            continue
                        if not re.search(r"bitcoin|btc", title, re.I):
                            continue
                        # Parse strike
                        amounts = re.findall(r"\$[\d,]+(?:[kK])?", title)
                        strike = None
                        for a in amounts:
                            raw = a.replace("$","").replace(",","")
                            v = float(raw[:-1])*1000 if raw.lower().endswith("k") else float(raw)
                            if 10000 < v < 10_000_000:
                                strike = v
                                break
                        if not strike:
                            continue
                        tl = title.lower()
                        if re.search(r"above|over|exceed|reach|hit|higher|more than|at least|break", tl):
                            direction = "above"
                        elif re.search(r"below|under|lower|less than|drop|fall", tl):
                            direction = "below"
                        else:
                            continue
                        tokens = m.get("tokens") or []
                        yes_tok = ""
                        if isinstance(tokens, list) and tokens:
                            t0 = tokens[0]
                            yes_tok = t0.get("token_id", t0) if isinstance(t0, dict) else t0
                        self._markets.append({
                            "cid": cid, "title": title[:60],
                            "strike": strike, "direction": direction,
                            "yes_tok": yes_tok,
                        })
                        seen.add(cid)
            except Exception as e:
                print(f"  Market load error: {e}")
        return len(self._markets)

    def get_prices(self) -> dict[str, float]:
        """Fetch YES prices for all markets. Returns {cid: yes_price}."""
        prices = {}
        if not self._conn:
            return prices
        for m in self._markets:
            tok = m.get("yes_tok", "")
            if not tok:
                continue
            try:
                self._conn.request("GET", f"/book?token_id={tok}",
                                   headers={"Connection": "keep-alive"})
                resp = self._conn.getresponse()
                if resp.status == 200:
                    data = json.loads(resp.read())
                    bids = data.get("bids", [])
                    asks = data.get("asks", [])
                    bid  = float(bids[0]["price"]) if bids else 0.0
                    ask  = float(asks[0]["price"]) if asks else 1.0
                    prices[m["cid"]] = (bid + ask) / 2
                else:
                    resp.read()
            except Exception:
                try:
                    ctx = ssl.create_default_context()
                    self._conn = http.client.HTTPSConnection(
                        "clob.polymarket.com", context=ctx, timeout=3
                    )
                    self._conn.connect()
                except Exception:
                    pass
        return prices

    @property
    def markets(self) -> list[dict]:
        return self._markets


# ─────────────────────────────────────────────
#  LAG MEASURER
# ─────────────────────────────────────────────

class LagMeasurer:

    def __init__(self, min_move: float = 50.0, velocity_thresh: float = 30.0):
        self.min_move        = min_move        # min $move to count
        self.velocity_thresh = velocity_thresh # min USD/s
        self.observations:  list[LagObservation] = []
        self.move_events:   list[MoveEvent]       = []
        self._ring_p = deque(maxlen=300)
        self._ring_t = deque(maxlen=300)

    def _velocity(self, window: float = 10.0) -> float:
        if len(self._ring_p) < 2:
            return 0.0
        now    = self._ring_t[-1]
        cutoff = now - window
        pts = [(p,t) for p,t in zip(self._ring_p, self._ring_t) if t >= cutoff]
        if len(pts) < 2:
            return 0.0
        return (pts[-1][0] - pts[0][0]) / max(0.1, pts[-1][1] - pts[0][1])

    def push_price(self, price: float, ts: float) -> Optional[MoveEvent]:
        self._ring_p.append(price)
        self._ring_t.append(ts)
        if len(self._ring_p) < 10:
            return None
        vel    = self._velocity(10.0)
        ref_p  = self._ring_p[-10]
        delta  = price - ref_p
        if abs(delta) >= self.min_move and abs(vel) >= self.velocity_thresh:
            evt = MoveEvent(
                ts=ts, price_from=ref_p, price_to=price,
                delta=delta, velocity=vel,
                direction="up" if delta > 0 else "down",
            )
            return evt
        return None

    def record_lag(
        self,
        move: MoveEvent,
        market: dict,
        price_before: float,
        price_after: float,
        lag_secs: float,
    ) -> None:
        price_change = price_after - price_before
        # Did poly move in the right direction?
        if move.direction == "up" and market["direction"] == "above":
            expected = "up"
        elif move.direction == "down" and market["direction"] == "below":
            expected = "up"
        else:
            expected = "down"
        correct = (price_change > 0.005 and expected == "up") or \
                  (price_change < -0.005 and expected == "down")

        obs = LagObservation(
            move_ts=move.ts, move_delta=move.delta,
            move_velocity=move.velocity,
            market_cid=market["cid"], market_title=market["title"],
            strike=market["strike"], direction=market["direction"],
            poly_price_before=price_before, poly_price_after=price_after,
            price_change=price_change, lag_secs=lag_secs,
            expected_direction=expected, correct=correct,
        )
        self.observations.append(obs)

    def summary(self) -> dict:
        obs = self.observations
        if not obs:
            return {}
        lags      = [o.lag_secs for o in obs]
        repriced  = [o for o in obs if abs(o.price_change) > 0.005]
        correct   = [o for o in repriced if o.correct]
        lags_repr = [o.lag_secs for o in repriced]
        return {
            "total_moves":         len(self.move_events),
            "total_observations":  len(obs),
            "repriced_count":      len(repriced),
            "reprice_rate":        len(repriced) / max(len(obs), 1),
            "correct_direction":   len(correct) / max(len(repriced), 1),
            "lag_p10_secs":        _pct(lags_repr, 10),
            "lag_p25_secs":        _pct(lags_repr, 25),
            "lag_p50_secs":        _pct(lags_repr, 50),
            "lag_p75_secs":        _pct(lags_repr, 75),
            "lag_p90_secs":        _pct(lags_repr, 90),
            "lag_mean_secs":       statistics.mean(lags_repr) if lags_repr else 0,
            "avg_price_change":    statistics.mean(abs(o.price_change) for o in repriced) if repriced else 0,
            # Optimal entry window: after p10 lag (market hasn't repriced yet)
            # but before p50 lag (most repricing happens before here)
            "optimal_entry_window_secs": _pct(lags_repr, 10),
            "close_window_secs":         _pct(lags_repr, 75),
        }


def _pct(vals: list, p: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    return s[int(len(s) * p / 100)]


# ─────────────────────────────────────────────
#  MAIN MEASUREMENT LOOP
# ─────────────────────────────────────────────

async def measure(duration_secs: int = 3600) -> dict:
    print(f"\n  Oracle Lag Measurer — running for {duration_secs//60} min")
    print(f"  Streaming Binance + polling Polymarket every 500ms after moves\n")

    poller  = PolyPoller()
    measurer = LagMeasurer(min_move=50.0, velocity_thresh=30.0)

    print("  Loading Polymarket markets…")
    poller.connect()
    n = poller.load_markets()
    print(f"  ✓ {n} BTC markets loaded")

    # Baseline prices
    print("  Fetching baseline prices…")
    baseline = poller.get_prices()
    print(f"  ✓ {len(baseline)} baseline prices\n")

    ws = FastWS("stream.binance.com", "/ws/btcusdt@aggTrade")
    ws.connect()
    print("  ✓ Binance connected. Waiting for price moves…\n")

    end_ts         = time.time() + duration_secs
    active_moves:  list[tuple[MoveEvent, dict[str, float]]] = []
    # active_moves = list of (move_event, prices_at_move_time)
    last_poll      = 0.0
    last_status    = time.time()
    ticks          = 0

    try:
        while time.time() < end_ts:
            price = ws.recv_price()
            if price is None:
                continue
            ts    = time.time()
            ticks += 1

            # Check for new move
            evt = measurer.push_price(price, ts)
            if evt:
                snap = dict(baseline)  # snapshot of prices before move
                active_moves.append((evt, snap))
                measurer.move_events.append(evt)
                print(f"  ⚡ Move detected: ${evt.price_from:,.0f} → ${evt.price_to:,.0f} "
                      f"(Δ{evt.delta:+.0f}, vel={evt.velocity:+.1f}$/s)")

            # Poll Polymarket every 500ms if there are active moves
            now = time.time()
            if active_moves and now - last_poll >= 0.5:
                last_poll    = now
                current_prices = poller.get_prices()
                baseline.update(current_prices)

                still_active = []
                for move, prices_before in active_moves:
                    age = now - move.ts
                    repriced_count = 0

                    for mkt in poller.markets:
                        cid    = mkt["cid"]
                        p_before = prices_before.get(cid, 0.5)
                        p_after  = current_prices.get(cid, p_before)

                        if abs(p_after - p_before) > 0.005:
                            measurer.record_lag(move, mkt, p_before, p_after, age)
                            repriced_count += 1

                    if repriced_count > 0:
                        print(f"    ↳ {repriced_count} markets repriced after {age:.1f}s")

                    # Stop tracking after 120s
                    if age < 120:
                        still_active.append((move, prices_before))

                active_moves = still_active

            # Status every 60s
            if now - last_status >= 60:
                last_status = now
                s = measurer.summary()
                if s:
                    print(f"\n  [{int((end_ts-now)/60)}min left] "
                          f"moves={s.get('total_moves',0)} "
                          f"obs={s.get('total_observations',0)} "
                          f"p50_lag={s.get('lag_p50_secs',0):.1f}s\n")

    except KeyboardInterrupt:
        print("\n  Stopped early")
    finally:
        ws.close()

    return measurer.summary()


def save_results(summary: dict) -> None:
    RESULTS_PATH.parent.mkdir(exist_ok=True)
    with open(SUMMARY_PATH, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\n  Results saved → {SUMMARY_PATH}")


def print_summary(s: dict) -> None:
    if not s:
        print("  No observations collected.")
        return
    print(f"\n{'═'*60}")
    print(f"  ORACLE LAG RESULTS")
    print(f"{'═'*60}")
    print(f"  Moves detected:       {s.get('total_moves', 0)}")
    print(f"  Markets observed:     {s.get('total_observations', 0)}")
    print(f"  Repriced:             {s.get('repriced_count', 0)} "
          f"({s.get('reprice_rate', 0):.0%} reprice rate)")
    print(f"  Correct direction:    {s.get('correct_direction', 0):.0%}")
    print(f"\n  Lag distribution (repriced markets only):")
    print(f"    P10  = {s.get('lag_p10_secs', 0):.1f}s   ← enter BEFORE this")
    print(f"    P25  = {s.get('lag_p25_secs', 0):.1f}s")
    print(f"    P50  = {s.get('lag_p50_secs', 0):.1f}s   ← most repricing done by here")
    print(f"    P75  = {s.get('lag_p75_secs', 0):.1f}s   ← close position by here")
    print(f"    P90  = {s.get('lag_p90_secs', 0):.1f}s")
    print(f"    Mean = {s.get('lag_mean_secs', 0):.1f}s")
    print(f"\n  ✅ Optimal entry window:  < {s.get('optimal_entry_window_secs', 5):.1f}s after Binance move")
    print(f"  ✅ Close position by:       {s.get('close_window_secs', 20):.1f}s after entry")
    print(f"\n  Feed these into config.py:")
    print(f"    velocity_window_secs = {max(3, int(s.get('optimal_entry_window_secs', 5)))}")
    print(f"    max_hold_seconds     = {max(30, int(s.get('close_window_secs', 20) * 2))}")
    print(f"{'═'*60}\n")


def show_saved_report() -> None:
    if not SUMMARY_PATH.exists():
        print("  No saved results. Run without --report first.")
        return
    with open(SUMMARY_PATH) as f:
        s = json.load(f)
    print_summary(s)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=3600, help="seconds to run")
    parser.add_argument("--report",   action="store_true",   help="show saved results")
    parser.add_argument("--min-move", type=float, default=50.0)
    args = parser.parse_args()

    if args.report:
        show_saved_report()
        return

    summary = await measure(args.duration)
    print_summary(summary)
    save_results(summary)


if __name__ == "__main__":
    asyncio.run(main())
