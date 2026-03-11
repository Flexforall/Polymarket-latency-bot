# BTC Latency Arb Bot — Polymarket × Binance

Reverse-engineered replication of a high-frequency BTC latency arbitrage strategy
on Polymarket. Monitors Binance real-time price and exploits Polymarket's repricing lag
to buy mispriced BTC outcome tokens.

---

## How It Works

### The Strategy (Reverse-Engineered)

```
Binance BTC/USDT price spikes $150 in 8 seconds
           │
           ▼
Bot detects velocity > 80 USD/sec threshold
           │
           ▼
Scans Polymarket BTC markets near current spot
  e.g. "Will BTC exceed $96,000 by March 15?"
           │
           ▼
Computes fair value via log-normal model
  Fair value = 68¢  |  Polymarket price = 55¢  → Edge = +13¢
           │
           ▼
Buys YES tokens at 55¢ (Polymarket hasn't repriced yet)
           │
           ▼  (5–30 seconds later)
Polymarket reprices to 67¢
           │
           ▼
Bot exits → profit = (0.67 - 0.55) × shares
```

### Key Parameters (from wallet analysis)

| Parameter              | Value        | Source                    |
|------------------------|--------------|---------------------------|
| Velocity trigger       | ~80 USD/s    | Burst cluster timing      |
| Min edge to enter      | 5¢           | Avg entry/exit spread     |
| Avg hold time          | 2–15 minutes | Round-trip analysis       |
| Position scaling       | Edge-based   | Bet size distribution     |
| YES/NO bias            | Slight YES   | 60%+ YES buys on BTC mkts |
| Hold-to-resolution     | >85¢ ITM     | Sell ratio analysis       |
| Max concurrent bets    | ~8           | Burst + position data     |

---

## Project Structure

```
polymarket-arb-bot/
├── main.py              ← Bot orchestrator / entry point
├── requirements.txt
├── .env.example         ← Copy to .env with your credentials
├── src/
│   ├── config.py        ← ALL configurable parameters here
│   ├── price_feed.py    ← Binance WebSocket BTC/USDT feed
│   ├── market_scanner.py← Polymarket market discovery + CLOB pricing
│   ├── fair_value.py    ← Log-normal fair value + edge model
│   ├── trader.py        ← Order execution (paper + live)
│   └── logger.py        ← Structured logging + CSV trade log
└── logs/
    ├── bot.log          ← Rotating log file
    └── trades.csv       ← Every entry/exit with PnL
```

---

## Setup

### 1. Requirements

- Python 3.11+
- Polygon wallet with USDC (for live trading)
- Polymarket API credentials

```bash
pip install -r requirements.txt
```

### 2. Configuration

```bash
cp .env.example .env
# Edit .env with your credentials
```

Or set environment variables directly:

```bash
export POLY_PRIVATE_KEY="0x..."
export POLY_API_KEY="..."
export POLY_API_SECRET="..."
export POLY_API_PASSPHRASE="..."
```

### 3. Run in Paper Trading Mode (default — no real money)

```bash
python main.py
```

You will see live output like:
```
2024-03-11 14:23:01  INFO  main  ─────────────────────────────
2024-03-11 14:23:01  INFO  main  BTC Latency Arb Bot
2024-03-11 14:23:01  INFO  main  Mode: 📄 PAPER TRADING
...
2024-03-11 14:23:15  INFO  main  ⚡ Scanning 6 markets | spot=$95,420 | vel=+127.3$/s
2024-03-11 14:23:15  INFO  main    🎯 SIGNAL: BUY_YES | Will BTC exceed $96,000 by Mar 15 | fv=0.612 poly=0.490 edge=+0.122
2024-03-11 14:23:15  INFO  trader ✅ ENTERED | BUY_YES | ... | price=0.498 | size=$50.00
```

---

## Tuning the Strategy

All parameters are in `src/config.py`. Key ones to tune:

### Edge threshold
```python
min_edge_to_enter: float = 0.05   # 5¢ minimum
```
Lower = more trades, more risk. Higher = fewer but higher quality.
Start at 8¢ and lower once you validate the model.

### Velocity trigger
```python
price_velocity_threshold: float = 80.0   # USD/sec
```
Lower catches slower moves. Too low = false signals.
Monitor `vel=` in logs to calibrate.

### Implied volatility
```python
implied_vol: float = 0.70   # 70% annualised
```
Check BTC DVOL index (Deribit) for current market IV.
Higher vol → wider fair value ranges → more signals.

### Position sizing
```python
base_bet_usdc: float = 50.0
max_bet_usdc:  float = 500.0
bet_scaling_factor: float = 2.0
```
`bet = base × (edge / min_edge) ^ scaling_factor`
At min_edge (5¢): bet = $50. At 10¢ edge: bet = $200.

---

## Going Live

**Only do this after:**
1. Running paper mode for at least 1 week
2. Verifying win rate > 55% on paper trades
3. IT review of all API key handling
4. Setting conservative position limits

Steps:
1. Get Polymarket API credentials: https://docs.polymarket.com
2. Fund your Polygon wallet with USDC
3. Set `PAPER_TRADING=false` in `.env`
4. Start with `max_bet_usdc=50` and `max_total_exposure_usdc=200`
5. Monitor `logs/trades.csv` closely

---

## Risk Warnings

- **This is experimental software.** Paper trade first.
- Polymarket CLOB can have outages. Orders may not fill.
- BTC volatility can spike unexpectedly — the model uses static vol.
- Latency advantage disappears if Polymarket improves their oracle speed.
- Never trade more than you can afford to lose.
- This code does not constitute financial advice.

---

## IT Review Checklist

- [ ] Private key is loaded from env var only — never hardcoded
- [ ] `.env` is in `.gitignore`
- [ ] `PAPER_TRADING=true` is the default
- [ ] Daily loss limit (`max_daily_loss_usdc`) is set
- [ ] Max exposure (`max_total_exposure_usdc`) is set
- [ ] All external API calls use `aiohttp` with timeouts
- [ ] WebSocket reconnects with exponential backoff
- [ ] All exceptions are caught and logged — bot never crashes silently
- [ ] CSV trade log records every order for audit trail
