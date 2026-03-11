"""
backtest.py
-----------
Backtests the arb strategy using:
  - Binance BTC/USDT historical klines (1-second candles via REST API)
  - Polymarket historical price series (CLOB timeseries API)

Replays price feed through the fair value model, simulates entries/exits,
and reports full performance metrics. Use this to tune velocity_threshold
and implied_vol until backtest win rate matches the target wallet.

Usage:
    python backtest.py                            # last 30 days, default config
    python backtest.py --days 60                  # last 60 days
    python backtest.py --start 2024-11-01 --end 2024-12-31
    python backtest.py --sweep                    # grid-search best parameters
    python backtest.py --wallet 0xABC...          # compare vs real wallet trades
"""

import asyncio
import sys
import csv
import json
import math
import time
import statistics
import argparse
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional
import aiohttp

# ─────────────────────────────────────────────
#  ENDPOINTS
# ─────────────────────────────────────────────
BINANCE_REST   = "https://api.binance.com/api/v3"
POLY_GAMMA     = "https://gamma-api.polymarket.com"
POLY_CLOB      = "https://clob.polymarket.com"
POLY_DATA      = "https://data-api.polymarket.com"

DEFAULT_WALLET = "0xde17f7144fbd0eddb2679132c10ff5e74b120988"
OUTPUT_DIR     = Path("backtest_results")


# ══════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════

def ts_to_dt(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def dt_to_ts(s: str) -> float:
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp()

def fmt(n: float) -> str:
    return f"${n:+,.2f}" if n != 0 else "$0.00"

def pct(n: float) -> str:
    return f"{n*100:.1f}¢"


# ══════════════════════════════════════════════════════════════════════
#  MATH (same as fair_value.py — self-contained for backtest)
# ══════════════════════════════════════════════════════════════════════

def norm_cdf(x: float) -> float:
    a1,a2,a3,a4,a5 = .254829592,-.284496736,1.421413741,-1.453152027,1.061405429
    p = .3275911
    sign = 1 if x >= 0 else -1
    x = abs(x) / math.sqrt(2)
    t = 1 / (1 + p * x)
    y = 1 - (((((a5*t+a4)*t)+a3)*t+a2)*t+a1)*t*math.exp(-x*x)
    return .5*(1+sign*y)

def fair_prob(spot: float, strike: float, days: float, vol: float,
              direction: str, smile: bool = True) -> float:
    if days <= 0:
        return 1.0 if (direction=="above" and spot>strike) or \
                      (direction=="below" and spot<strike) else 0.0
    T = days / 365
    adj_vol = vol
    if smile:
        m = math.log(spot/strike)
        adj_vol = min(2.0, max(0.20, vol + 0.20*m**2 + 0.03*abs(m)))
    d1 = (math.log(spot/strike) + .5*adj_vol**2*T) / (adj_vol*math.sqrt(T))
    p  = norm_cdf(d1)
    return p if direction == "above" else 1 - p

def velocity(prices: list, timestamps: list, window: float = 10.0) -> float:
    if len(prices) < 2:
        return 0.0
    now     = timestamps[-1]
    cutoff  = now - window
    recent  = [(p,t) for p,t in zip(prices,timestamps) if t >= cutoff]
    if len(recent) < 2:
        return 0.0
    return (recent[-1][0] - recent[0][0]) / max(.1, recent[-1][1] - recent[0][1])


# ══════════════════════════════════════════════════════════════════════
#  PARAMETER SET
# ══════════════════════════════════════════════════════════════════════

@dataclass
class Params:
    velocity_threshold:   float = 80.0    # USD/s
    velocity_window:      int   = 10      # seconds
    min_edge:             float = 0.05    # 5¢
    min_edge_to_hold:     float = 0.02
    implied_vol:          float = 0.70
    vol_smile:            bool  = True
    max_hold_secs:        int   = 1800
    base_bet:             float = 50.0
    max_bet:              float = 500.0
    scaling_factor:       float = 2.0
    max_concurrent:       int   = 8
    max_strike_pct:       float = 0.08
    min_days_expiry:      float = 0.5
    max_days_expiry:      float = 45.0
    deep_itm:             float = 0.85
    slippage:             float = 0.005   # 0.5¢ assumed slippage per trade

    def label(self) -> str:
        return (f"vel{self.velocity_threshold:.0f}_"
                f"edge{self.min_edge*100:.0f}c_"
                f"vol{self.implied_vol*100:.0f}pct")


# ══════════════════════════════════════════════════════════════════════
#  DATA CLASSES
# ══════════════════════════════════════════════════════════════════════

@dataclass
class Candle:
    ts:    float    # open timestamp (seconds)
    open:  float
    high:  float
    low:   float
    close: float
    vol:   float

@dataclass
class MarketSnapshot:
    condition_id:  str
    title:         str
    direction:     str
    strike:        float
    days_to_expiry: float
    yes_price:     float    # Polymarket YES price at this timestamp
    ts:            float    # snapshot timestamp

@dataclass
class BacktestTrade:
    id:            int
    condition_id:  str
    title:         str
    direction:     str
    strike:        float
    side:          str      # BUY_YES / BUY_NO
    entry_ts:      float
    entry_spot:    float
    entry_poly:    float    # Polymarket YES price at entry
    entry_fv:      float    # Our fair value at entry
    edge_at_entry: float
    size_usdc:     float
    shares:        float
    exit_ts:       Optional[float]  = None
    exit_poly:     Optional[float]  = None
    exit_fv:       Optional[float]  = None
    pnl:           Optional[float]  = None
    exit_reason:   str = ""
    days_to_expiry: float = 0.0

    @property
    def hold_secs(self) -> float:
        if self.exit_ts:
            return self.exit_ts - self.entry_ts
        return 0.0

    @property
    def won(self) -> bool:
        return (self.pnl or 0) > 0


@dataclass
class BacktestResult:
    params:           Params
    start_ts:         float
    end_ts:           float
    total_trades:     int   = 0
    wins:             int   = 0
    losses:           int   = 0
    total_pnl:        float = 0.0
    total_volume:     float = 0.0
    roi_pct:          float = 0.0
    win_rate:         float = 0.0
    avg_hold_secs:    float = 0.0
    avg_edge:         float = 0.0
    avg_pnl_per_trade: float = 0.0
    max_drawdown:     float = 0.0
    sharpe:           float = 0.0
    velocity_triggers: int  = 0
    signals_fired:    int   = 0
    trades:           list  = field(default_factory=list)
    equity_curve:     list  = field(default_factory=list)   # (ts, cumulative_pnl)


# ══════════════════════════════════════════════════════════════════════
#  DATA FETCHERS
# ══════════════════════════════════════════════════════════════════════

async def fetch_binance_klines(
    session: aiohttp.ClientSession,
    start_ts: float,
    end_ts: float,
    interval: str = "1s",
) -> list[Candle]:
    """
    Fetch BTC/USDT klines from Binance REST API.
    Uses 1-second candles for high resolution.
    Falls back to 1-minute if 1s not available for the date range.
    """
    candles = []
    chunk = 1000        # Binance max per request
    cur   = int(start_ts * 1000)
    end_ms = int(end_ts * 1000)

    print(f"    Fetching Binance klines ({interval}) …", end="", flush=True)
    requests = 0

    while cur < end_ms:
        try:
            url = (f"{BINANCE_REST}/klines"
                   f"?symbol=BTCUSDT&interval={interval}"
                   f"&startTime={cur}&endTime={end_ms}&limit={chunk}")
            async with session.get(url) as r:
                if r.status == 400:
                    # 1s might not be available — try 1m
                    if interval == "1s":
                        print(f" (1s unavailable, switching to 1m) …", end="", flush=True)
                        return await fetch_binance_klines(session, start_ts, end_ts, "1m")
                    break
                if not r.ok:
                    print(f"\n    ⚠ Binance HTTP {r.status}")
                    break
                data = await r.json()
                if not data:
                    break
                for k in data:
                    candles.append(Candle(
                        ts    = float(k[0]) / 1000,
                        open  = float(k[1]),
                        high  = float(k[2]),
                        low   = float(k[3]),
                        close = float(k[4]),
                        vol   = float(k[5]),
                    ))
                cur = int(data[-1][0]) + 1
                requests += 1
                # Rate limit: Binance allows 1200 req/min
                if requests % 10 == 0:
                    await asyncio.sleep(0.1)
        except Exception as e:
            print(f"\n    ⚠ Binance fetch error: {e}")
            break

    print(f" {len(candles):,} candles ✓")
    return candles


async def fetch_poly_markets(
    session: aiohttp.ClientSession,
) -> list[dict]:
    """Fetch all BTC price markets (active + recently closed)."""
    markets = []
    seen = set()

    for slug in ["bitcoin", "crypto"]:
        for active in ["true", "false"]:
            try:
                url = f"{POLY_GAMMA}/markets?tag_slug={slug}&active={active}&limit=200"
                async with session.get(url) as r:
                    if not r.ok:
                        continue
                    data = await r.json()
                    items = data if isinstance(data, list) else data.get("markets", [])
                    for m in items:
                        mid = m.get("conditionId") or m.get("id") or ""
                        if mid in seen:
                            continue
                        title = m.get("question") or m.get("title") or ""
                        if not _parse_btc_market(title):
                            continue
                        seen.add(mid)
                        markets.append(m)
            except Exception:
                pass

    print(f"    Found {len(markets)} BTC price markets ✓")
    return markets


async def fetch_poly_price_history(
    session: aiohttp.ClientSession,
    market: dict,
    start_ts: float,
    end_ts: float,
) -> list[tuple[float, float]]:
    """
    Fetch Polymarket YES price history for a market.
    Returns list of (timestamp, yes_price).
    Uses CLOB timeseries endpoint.
    """
    token_id = ""
    tokens = market.get("tokens") or market.get("clobTokenIds") or []
    if isinstance(tokens, list) and tokens:
        t = tokens[0]
        token_id = t.get("token_id", t) if isinstance(t, dict) else t

    if not token_id:
        # Try outcomePrices fallback — no history available
        return []

    history = []
    try:
        # CLOB timeseries: /prices-history?market=<token_id>&interval=1h&startTs=...
        url = (f"{POLY_CLOB}/prices-history"
               f"?market={token_id}"
               f"&interval=1min"
               f"&startTs={int(start_ts)}&endTs={int(end_ts)}&fidelity=60")
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if not r.ok:
                return []
            data = await r.json()
            pts = data.get("history") or data if isinstance(data, list) else []
            for pt in pts:
                ts = float(pt.get("t") or pt.get("ts") or pt.get("timestamp") or 0)
                p  = float(pt.get("p") or pt.get("price") or 0)
                if ts and p:
                    if p > 1:
                        p /= 100
                    history.append((ts, p))
    except Exception:
        pass

    return sorted(history, key=lambda x: x[0])


def _parse_btc_market(title: str) -> Optional[tuple[float, str]]:
    import re
    if not re.search(r"bitcoin|btc", title, re.I):
        return None
    t = title.lower()
    direction = None
    if re.search(r"above|over|exceed|reach|hit|higher|more than|at least|break", t):
        direction = "above"
    elif re.search(r"below|under|lower|less than|drop|fall", t):
        direction = "below"
    if not direction:
        return None
    amounts = re.findall(r"\$[\d,]+(?:[kK])?(?:\.\d+)?", title)
    for amt in amounts:
        raw = amt.replace("$","").replace(",","")
        v = float(raw[:-1])*1000 if raw.lower().endswith("k") else float(raw)
        if 10_000 < v < 10_000_000:
            return v, direction
    return None


def _parse_expiry(market: dict) -> float:
    raw = market.get("end_date_iso") or market.get("endDateIso") or ""
    if raw:
        try:
            dt = datetime.fromisoformat(raw.replace("Z","+00:00"))
            return (dt - datetime.now(timezone.utc)).total_seconds() / 86400
        except Exception:
            pass
    return 14.0


# ══════════════════════════════════════════════════════════════════════
#  BACKTEST ENGINE
# ══════════════════════════════════════════════════════════════════════

class Backtester:

    def __init__(self, params: Params):
        self.p = params

    def run(
        self,
        candles:        list[Candle],
        market_data:    dict[str, list[tuple[float,float]]],   # cid → [(ts, price)]
        market_meta:    dict[str, dict],                        # cid → market dict
    ) -> BacktestResult:

        p = self.p
        result = BacktestResult(
            params   = p,
            start_ts = candles[0].ts  if candles else 0,
            end_ts   = candles[-1].ts if candles else 0,
        )

        # Build price/ts arrays for velocity
        prices     : list[float] = []
        timestamps : list[float] = []

        open_positions: dict[str, BacktestTrade] = {}   # cid → trade
        trade_id = 0
        equity   = 0.0

        # Build a sorted event timeline:
        # every candle close = a price tick
        # every minute = re-evaluate open positions

        last_scan_ts   = 0.0
        last_equity_ts = 0.0

        for candle in candles:
            ts    = candle.ts
            price = candle.close

            prices.append(price)
            timestamps.append(ts)
            if len(prices) > 300:
                prices.pop(0)
                timestamps.pop(0)

            # ── Evaluate existing positions ──────────────────────────
            for cid in list(open_positions.keys()):
                trade = open_positions[cid]
                poly_price = _interpolate_price(market_data.get(cid,[]), ts)
                if poly_price is None:
                    continue

                days_left = _days_remaining(market_meta[cid], ts)
                fv = fair_prob(price, trade.strike, days_left, p.implied_vol,
                               trade.direction, p.vol_smile)

                if trade.side == "BUY_YES":
                    cur_price     = poly_price
                    remaining_edge = fv - poly_price
                else:
                    cur_price      = 1.0 - poly_price
                    remaining_edge = (1.0 - fv) - (1.0 - poly_price)

                hold = ts - trade.entry_ts
                exit_reason = None

                if cur_price >= p.deep_itm and trade.side == "BUY_YES":
                    pass    # hold to resolution
                elif remaining_edge <= p.min_edge_to_hold:
                    exit_reason = "edge_closed"
                elif remaining_edge <= -p.min_edge_to_hold:
                    exit_reason = "stop_loss"
                elif hold >= p.max_hold_secs:
                    exit_reason = "max_hold"

                if exit_reason:
                    exit_price = cur_price - p.slippage   # simulate slippage out
                    pnl = (exit_price - trade.entry_poly) * trade.shares \
                          if trade.side == "BUY_YES" \
                          else ((1-exit_price) - (1-trade.entry_poly)) * trade.shares

                    trade.exit_ts     = ts
                    trade.exit_poly   = poly_price
                    trade.exit_fv     = fv
                    trade.pnl         = pnl
                    trade.exit_reason = exit_reason

                    equity += pnl
                    result.total_trades += 1
                    result.total_pnl    += pnl
                    result.total_volume += trade.size_usdc
                    if pnl > 0:
                        result.wins += 1
                    else:
                        result.losses += 1
                    result.trades.append(trade)

                    del open_positions[cid]

                    # Equity curve point
                    result.equity_curve.append((ts, equity))

            # ── Velocity check ───────────────────────────────────────
            if len(prices) < 3:
                continue

            vel = velocity(prices, timestamps, p.velocity_window)
            if abs(vel) < p.velocity_threshold:
                continue

            # Throttle: don't scan more than once per 2 seconds
            if ts - last_scan_ts < 2.0:
                continue
            last_scan_ts = ts
            result.velocity_triggers += 1

            # ── Scan markets for arb ─────────────────────────────────
            burst_entries = 0
            for cid, history in market_data.items():
                if burst_entries >= p.max_concurrent:
                    break
                if cid in open_positions:
                    continue
                if len(open_positions) >= p.max_concurrent:
                    break

                meta      = market_meta.get(cid)
                if not meta:
                    continue

                poly_price = _interpolate_price(history, ts)
                if poly_price is None:
                    continue

                parsed = _parse_btc_market(meta.get("question") or meta.get("title") or "")
                if not parsed:
                    continue
                strike, direction = parsed

                days_left = _days_remaining(meta, ts)
                if not (p.min_days_expiry <= days_left <= p.max_days_expiry):
                    continue

                spot_pct = abs(price - strike) / price
                if spot_pct > p.max_strike_pct:
                    continue

                fv   = fair_prob(price, strike, days_left, p.implied_vol, direction, p.vol_smile)
                edge = fv - poly_price

                if abs(edge) < p.min_edge:
                    continue

                signal = "BUY_YES" if edge > 0 else "BUY_NO"

                # Size the bet
                ratio = abs(edge) / p.min_edge
                size  = min(p.max_bet, p.base_bet * (ratio ** p.scaling_factor))

                entry_price = (poly_price + p.slippage) if signal == "BUY_YES" \
                              else (1 - poly_price + p.slippage)
                shares = size / max(entry_price, 0.001)

                trade_id += 1
                trade = BacktestTrade(
                    id            = trade_id,
                    condition_id  = cid,
                    title         = (meta.get("question") or meta.get("title") or "")[:60],
                    direction     = direction,
                    strike        = strike,
                    side          = signal,
                    entry_ts      = ts,
                    entry_spot    = price,
                    entry_poly    = poly_price,
                    entry_fv      = fv,
                    edge_at_entry = edge,
                    size_usdc     = size,
                    shares        = shares,
                    days_to_expiry = days_left,
                )
                open_positions[cid] = trade
                result.signals_fired += 1
                burst_entries += 1

        # ── Force-close remaining open positions at last price ───────
        last_candle = candles[-1] if candles else None
        if last_candle:
            for cid, trade in open_positions.items():
                poly_price = _interpolate_price(market_data.get(cid,[]), last_candle.ts)
                if poly_price is None:
                    continue
                exit_price = poly_price - p.slippage if trade.side == "BUY_YES" \
                             else (1 - poly_price - p.slippage)
                pnl = (exit_price - trade.entry_poly) * trade.shares \
                      if trade.side == "BUY_YES" \
                      else ((1-exit_price) - (1-trade.entry_poly)) * trade.shares
                trade.exit_ts     = last_candle.ts
                trade.exit_poly   = poly_price
                trade.pnl         = pnl
                trade.exit_reason = "end_of_backtest"
                equity += pnl
                result.total_trades += 1
                result.total_pnl    += pnl
                result.total_volume += trade.size_usdc
                if pnl > 0:
                    result.wins += 1
                else:
                    result.losses += 1
                result.trades.append(trade)
                result.equity_curve.append((last_candle.ts, equity))

        # ── Compute summary stats ────────────────────────────────────
        if result.total_trades > 0:
            result.win_rate          = result.wins / result.total_trades
            result.avg_pnl_per_trade = result.total_pnl / result.total_trades
            result.roi_pct           = (result.total_pnl / max(result.total_volume,1)) * 100

        hold_times = [t.hold_secs for t in result.trades if t.hold_secs > 0]
        if hold_times:
            result.avg_hold_secs = statistics.mean(hold_times)

        edges = [t.edge_at_entry for t in result.trades]
        if edges:
            result.avg_edge = statistics.mean(edges)

        # Max drawdown from equity curve
        if result.equity_curve:
            peak = 0.0
            dd   = 0.0
            for _, eq in result.equity_curve:
                peak = max(peak, eq)
                dd   = min(dd, eq - peak)
            result.max_drawdown = dd

        # Sharpe (annualised, assuming each trade = 1 period)
        if result.trades:
            trade_pnls = [t.pnl or 0 for t in result.trades]
            if len(trade_pnls) > 1:
                mean_pnl = statistics.mean(trade_pnls)
                std_pnl  = statistics.stdev(trade_pnls)
                if std_pnl > 0:
                    # Annualise by trades per year estimate
                    days = max(1, (result.end_ts - result.start_ts) / 86400)
                    tpy  = result.total_trades * (365 / days)
                    result.sharpe = (mean_pnl / std_pnl) * math.sqrt(tpy)

        return result


def _interpolate_price(
    history: list[tuple[float,float]],
    ts: float,
) -> Optional[float]:
    """Get Polymarket YES price at a given timestamp via linear interpolation."""
    if not history:
        return None
    if ts <= history[0][0]:
        return history[0][1]
    if ts >= history[-1][0]:
        return history[-1][1]
    # Binary search
    lo, hi = 0, len(history) - 1
    while lo < hi - 1:
        mid = (lo + hi) // 2
        if history[mid][0] <= ts:
            lo = mid
        else:
            hi = mid
    t0, p0 = history[lo]
    t1, p1 = history[hi]
    if t1 == t0:
        return p0
    frac = (ts - t0) / (t1 - t0)
    return p0 + frac * (p1 - p0)


def _days_remaining(market: dict, at_ts: float) -> float:
    raw = market.get("end_date_iso") or market.get("endDateIso") or ""
    if not raw:
        return 14.0
    try:
        dt = datetime.fromisoformat(raw.replace("Z","+00:00"))
        return (dt.timestamp() - at_ts) / 86400
    except Exception:
        return 14.0


# ══════════════════════════════════════════════════════════════════════
#  REPORT
# ══════════════════════════════════════════════════════════════════════

def print_result(r: BacktestResult, label: str = "") -> None:
    tag = f" [{label}]" if label else ""
    print(f"\n{'═'*65}")
    print(f"  BACKTEST RESULT{tag}")
    print(f"  {ts_to_dt(r.start_ts)}  →  {ts_to_dt(r.end_ts)}")
    print(f"{'═'*65}")

    def row(k, v, note=""):
        ns = f"  ← {note}" if note else ""
        print(f"  {k:<32} {str(v):<22}{ns}")

    row("Velocity triggers",    r.velocity_triggers)
    row("Signals fired",        r.signals_fired)
    row("Trades taken",         r.total_trades)
    row("Wins / Losses",        f"{r.wins} / {r.losses}")
    row("Win rate",             f"{r.win_rate:.1%}")
    row("Total PnL",            fmt(r.total_pnl))
    row("ROI on volume",        f"{r.roi_pct:+.2f}%")
    row("Avg PnL / trade",      fmt(r.avg_pnl_per_trade))
    row("Avg hold time",        f"{r.avg_hold_secs:.0f}s")
    row("Avg edge at entry",    pct(r.avg_edge))
    row("Max drawdown",         fmt(r.max_drawdown))
    row("Sharpe ratio",         f"{r.sharpe:.2f}")
    print(f"{'─'*65}")
    print(f"  PARAMS: vel={r.params.velocity_threshold:.0f}$/s  "
          f"edge={r.params.min_edge*100:.0f}¢  "
          f"vol={r.params.implied_vol*100:.0f}%")
    print(f"{'═'*65}")


def save_trades_csv(r: BacktestResult, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "id","title","side","direction","strike",
            "entry_ts","entry_spot","entry_poly","entry_fv","edge_at_entry",
            "exit_ts","exit_poly","exit_fv","hold_secs",
            "size_usdc","shares","pnl","exit_reason","days_to_expiry",
        ])
        w.writeheader()
        for t in r.trades:
            w.writerow({
                "id":           t.id,
                "title":        t.title,
                "side":         t.side,
                "direction":    t.direction,
                "strike":       t.strike,
                "entry_ts":     ts_to_dt(t.entry_ts),
                "entry_spot":   f"{t.entry_spot:.2f}",
                "entry_poly":   f"{t.entry_poly:.4f}",
                "entry_fv":     f"{t.entry_fv:.4f}",
                "edge_at_entry":f"{t.edge_at_entry:+.4f}",
                "exit_ts":      ts_to_dt(t.exit_ts) if t.exit_ts else "",
                "exit_poly":    f"{t.exit_poly:.4f}" if t.exit_poly else "",
                "exit_fv":      f"{t.exit_fv:.4f}" if t.exit_fv else "",
                "hold_secs":    f"{t.hold_secs:.0f}",
                "size_usdc":    f"{t.size_usdc:.2f}",
                "shares":       f"{t.shares:.4f}",
                "pnl":          f"{t.pnl:+.4f}" if t.pnl is not None else "",
                "exit_reason":  t.exit_reason,
                "days_to_expiry":f"{t.days_to_expiry:.2f}",
            })
    print(f"  Trades saved → {path}")


def save_equity_csv(r: BacktestResult, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp","cumulative_pnl"])
        for ts, eq in r.equity_curve:
            w.writerow([ts_to_dt(ts), f"{eq:.4f}"])
    print(f"  Equity curve → {path}")


# ══════════════════════════════════════════════════════════════════════
#  PARAMETER SWEEP (grid search)
# ══════════════════════════════════════════════════════════════════════

def sweep_results_table(results: list[BacktestResult]) -> None:
    sorted_r = sorted(results, key=lambda r: r.total_pnl, reverse=True)

    print(f"\n{'═'*90}")
    print(f"  PARAMETER SWEEP — TOP CONFIGURATIONS")
    print(f"{'═'*90}")
    print(f"  {'vel $/s':<10} {'edge ¢':<8} {'vol %':<8} {'trades':<8} "
          f"{'win%':<8} {'PnL':>10} {'ROI%':>8} {'sharpe':>8}")
    print(f"  {'─'*80}")
    for r in sorted_r[:20]:
        p = r.params
        print(f"  {p.velocity_threshold:<10.0f} {p.min_edge*100:<8.0f} "
              f"{p.implied_vol*100:<8.0f} {r.total_trades:<8} "
              f"{r.win_rate:<8.1%} {r.total_pnl:>10.2f} "
              f"{r.roi_pct:>8.2f}% {r.sharpe:>8.2f}")
    print(f"{'═'*90}")

    best = sorted_r[0]
    print(f"\n  ✅ BEST CONFIG:")
    print(f"     velocity_threshold = {best.params.velocity_threshold}")
    print(f"     min_edge           = {best.params.min_edge}")
    print(f"     implied_vol        = {best.params.implied_vol}")
    print(f"     → PnL={fmt(best.total_pnl)}  win_rate={best.win_rate:.1%}  sharpe={best.sharpe:.2f}")


# ══════════════════════════════════════════════════════════════════════
#  WALLET COMPARISON
# ══════════════════════════════════════════════════════════════════════

async def compare_to_wallet(
    session: aiohttp.ClientSession,
    result: BacktestResult,
    wallet: str,
    start_ts: float,
    end_ts: float,
) -> None:
    """Compare backtest performance to the actual wallet's trades in the same period."""
    print(f"\n  Fetching wallet trades for comparison…")
    try:
        url = f"{POLY_DATA}/activity?user={wallet}&type=TRADE,REDEEM"
        trades = []
        for page in range(10):
            async with session.get(f"{url}&limit=500&offset={page*500}") as r:
                if not r.ok: break
                data = await r.json()
                items = data if isinstance(data, list) else data.get("data",[])
                period = [t for t in items
                          if start_ts <= float(t.get("timestamp",0)) <= end_ts]
                trades.extend(period)
                if len(items) < 500: break

        btc_buys  = [t for t in trades if t.get("type")=="TRADE" and t.get("side")=="BUY"
                     and _parse_btc_market(t.get("title",""))]
        btc_sells = [t for t in trades if t.get("type")=="TRADE" and t.get("side")=="SELL"
                     and _parse_btc_market(t.get("title",""))]
        redeems   = [t for t in trades if t.get("type")=="REDEEM"]

        w_in  = sum(t.get("usdcSize",0) for t in btc_buys)
        w_out = sum(t.get("usdcSize",0) for t in btc_sells)
        w_red = sum(t.get("usdcSize",0) for t in redeems)
        w_pnl = w_out + w_red - w_in

        print(f"\n{'─'*65}")
        print(f"  WALLET vs BACKTEST COMPARISON (same period)")
        print(f"{'─'*65}")
        def cmp(label, wallet_val, bt_val, fmt_fn=str):
            print(f"  {label:<28} wallet={fmt_fn(wallet_val):<16} backtest={fmt_fn(bt_val)}")
        cmp("BTC trades",    len(btc_buys), result.total_trades)
        cmp("Total PnL",     w_pnl, result.total_pnl, fmt)
        cmp("ROI",
            f"{(w_pnl/max(w_in,1))*100:.1f}%",
            f"{result.roi_pct:.1f}%")
        cmp("Avg entry price",
            f"{statistics.mean(t['price'] for t in btc_buys):.3f}" if btc_buys else "—",
            f"{statistics.mean(t.entry_poly for t in result.trades):.3f}" if result.trades else "—")
        print(f"{'─'*65}")
        gap = result.total_pnl - w_pnl
        print(f"  PnL gap: {fmt(gap)}  {'(backtest over-performing)' if gap>0 else '(backtest under-performing)'}")
        print(f"  Tune velocity_threshold {'down' if gap<0 else 'up'} to close the gap.")
    except Exception as e:
        print(f"  ⚠ Wallet comparison failed: {e}")


# ══════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════

async def main():
    parser = argparse.ArgumentParser(description="Polymarket BTC Arb Backtester")
    parser.add_argument("--days",   type=int,   default=30,   help="Number of days to backtest")
    parser.add_argument("--start",  type=str,   default="",   help="Start date YYYY-MM-DD")
    parser.add_argument("--end",    type=str,   default="",   help="End date YYYY-MM-DD")
    parser.add_argument("--sweep",  action="store_true",       help="Grid search parameters")
    parser.add_argument("--wallet", type=str,   default=DEFAULT_WALLET, help="Compare to wallet")
    parser.add_argument("--no-compare", action="store_true",  help="Skip wallet comparison")
    args = parser.parse_args()

    # Time range
    now = time.time()
    if args.start and args.end:
        start_ts = dt_to_ts(args.start)
        end_ts   = dt_to_ts(args.end)
    else:
        end_ts   = now
        start_ts = now - args.days * 86400

    print(f"\n{'═'*65}")
    print(f"  Polymarket BTC Arb — Backtester")
    print(f"  Period: {ts_to_dt(start_ts)} → {ts_to_dt(end_ts)}")
    print(f"  Sweep:  {'YES — grid searching parameters' if args.sweep else 'NO — single run'}")
    print(f"{'═'*65}\n")

    OUTPUT_DIR.mkdir(exist_ok=True)
    session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30),
        headers={"User-Agent": "polymarket-backtest/1.0"},
    )

    try:
        # ── 1. Fetch Binance price data ──────────────────────────────
        print("  [1/3] Fetching Binance BTC/USDT spot price history…")
        candles = await fetch_binance_klines(session, start_ts, end_ts, "1m")
        if not candles:
            print("  ❌ No Binance data — check date range or connection")
            return

        # ── 2. Fetch Polymarket markets + price history ──────────────
        print("\n  [2/3] Fetching Polymarket BTC markets + price history…")
        raw_markets = await fetch_poly_markets(session)

        market_meta: dict[str, dict]                     = {}
        market_data: dict[str, list[tuple[float,float]]] = {}

        print(f"    Fetching price history for {len(raw_markets)} markets…")
        fetched = 0
        for m in raw_markets:
            cid = m.get("conditionId") or m.get("id") or ""
            if not cid:
                continue
            history = await fetch_poly_price_history(session, m, start_ts, end_ts)
            if len(history) < 2:
                continue
            market_meta[cid] = m
            market_data[cid] = history
            fetched += 1
            if fetched % 10 == 0:
                print(f"    … {fetched}/{len(raw_markets)} markets with history", end="\r")
            await asyncio.sleep(0.05)   # be gentle on API

        print(f"    {fetched} markets with usable price history ✓          ")

        if not market_data:
            print("  ⚠ No Polymarket price history found. The CLOB API may not have")
            print("    historical data for this period. Try a more recent date range.")
            return

        # ── 3. Run backtest(s) ───────────────────────────────────────
        print("\n  [3/3] Running backtest engine…")

        if args.sweep:
            # Grid search
            velocity_vals = [40, 60, 80, 100, 120, 150, 200]
            edge_vals     = [0.03, 0.04, 0.05, 0.06, 0.08, 0.10]
            vol_vals      = [0.60, 0.65, 0.70, 0.75, 0.80, 0.90]

            all_results = []
            total = len(velocity_vals) * len(edge_vals) * len(vol_vals)
            done  = 0

            for vel in velocity_vals:
                for edge in edge_vals:
                    for vol in vol_vals:
                        p = Params(
                            velocity_threshold=vel,
                            min_edge=edge,
                            implied_vol=vol,
                        )
                        bt = Backtester(p)
                        r  = bt.run(candles, market_data, market_meta)
                        all_results.append(r)
                        done += 1
                        print(f"    Sweep {done}/{total}: vel={vel} edge={edge*100:.0f}¢ "
                              f"vol={vol*100:.0f}%  → "
                              f"trades={r.total_trades} pnl={r.total_pnl:+.2f}", end="\r")

            print(f"\n    Sweep complete: {total} configurations tested")
            sweep_results_table(all_results)

            # Save sweep summary
            sweep_path = OUTPUT_DIR / "sweep_results.csv"
            with open(sweep_path, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=[
                    "velocity","min_edge","implied_vol","trades","wins",
                    "win_rate","total_pnl","roi_pct","sharpe","max_drawdown",
                    "avg_hold_secs","velocity_triggers","signals_fired",
                ])
                w.writeheader()
                for r in sorted(all_results, key=lambda x: x.total_pnl, reverse=True):
                    w.writerow({
                        "velocity":          r.params.velocity_threshold,
                        "min_edge":          r.params.min_edge,
                        "implied_vol":       r.params.implied_vol,
                        "trades":            r.total_trades,
                        "wins":              r.wins,
                        "win_rate":          f"{r.win_rate:.4f}",
                        "total_pnl":         f"{r.total_pnl:.4f}",
                        "roi_pct":           f"{r.roi_pct:.4f}",
                        "sharpe":            f"{r.sharpe:.4f}",
                        "max_drawdown":      f"{r.max_drawdown:.4f}",
                        "avg_hold_secs":     f"{r.avg_hold_secs:.1f}",
                        "velocity_triggers": r.velocity_triggers,
                        "signals_fired":     r.signals_fired,
                    })
            print(f"\n  Sweep saved → {sweep_path}")

            # Show best config and compare to wallet
            best = sorted(all_results, key=lambda r: r.total_pnl, reverse=True)[0]
            if not args.no_compare:
                await compare_to_wallet(session, best, args.wallet, start_ts, end_ts)

            # Write best params as calibrated config hint
            print(f"\n  ── RECOMMENDED CONFIG UPDATE ───────────────────────────")
            print(f"  Update src/config.py with these sweep-optimised values:")
            print(f"     velocity_threshold = {best.params.velocity_threshold}")
            print(f"     min_edge_to_enter  = {best.params.min_edge}")
            print(f"     implied_vol        = {best.params.implied_vol}")
            print(f"  Or re-run calibrate.py — it will incorporate these.\n")

        else:
            # Single run with default / calibrated params
            # Try to load calibrated params from config.py
            try:
                import importlib.util, sys as _sys
                spec = importlib.util.spec_from_file_location("config", "src/config.py")
                cfg  = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(cfg)
                s = cfg.STRATEGY
                p = Params(
                    velocity_threshold = s.price_velocity_threshold,
                    velocity_window    = s.velocity_window_secs,
                    min_edge           = s.min_edge_to_enter,
                    min_edge_to_hold   = s.min_edge_to_hold,
                    implied_vol        = s.implied_vol,
                    max_hold_secs      = s.max_hold_seconds,
                    base_bet           = s.base_bet_usdc,
                    max_bet            = s.max_bet_usdc,
                    scaling_factor     = s.bet_scaling_factor,
                    max_concurrent     = s.max_concurrent_positions,
                    deep_itm           = s.deep_itm_threshold,
                )
                print("  Using calibrated params from src/config.py")
            except Exception:
                p = Params()
                print("  Using default params (run calibrate.py first for wallet-specific values)")

            bt = Backtester(p)
            r  = bt.run(candles, market_data, market_meta)
            print_result(r)

            ts_str = datetime.fromtimestamp(start_ts).strftime("%Y%m%d")
            save_trades_csv(r, OUTPUT_DIR / f"trades_{ts_str}.csv")
            save_equity_csv(r, OUTPUT_DIR / f"equity_{ts_str}.csv")

            if not args.no_compare:
                await compare_to_wallet(session, r, args.wallet, start_ts, end_ts)

    finally:
        await session.close()

    print(f"\n  All results saved to ./{OUTPUT_DIR}/\n")


if __name__ == "__main__":
    asyncio.run(main())
