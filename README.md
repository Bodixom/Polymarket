# Polymarket execution-research repo

This repository is focused on **market data capture stability** and **safe execution diagnostics**, not production trading.

## What exists in this repo
- `polymarket_snapshot_logger.py` — rolling BTC 5-minute Polymarket snapshot logger (current primary logger).
- `live_test_harness.py` — safe-by-default execution diagnostics harness (dry-run only).
- `docs/backtest_assumptions.md` — assumption tiers for interpreting backtests/sims.

## Current logger status
- Snapshot capture supports short intervals and rolls to each new BTC 5-minute market.
- `0.2s` interval is the recommended stable setting.
- Prior instability near `0.1s` was likely worsened by per-row flush behavior and timing drift.

## Run stable snapshot capture (recommended 0.2s)
```bash
python3 polymarket_snapshot_logger.py \
  --out snapshots.csv \
  --interval 0.2 \
  --rollover-poll 1.0 \
  --flush-interval 1.0 \
  --flush-every-rows 25
```

### Optional stress test (0.1s)
```bash
python3 polymarket_snapshot_logger.py --out snapshots_100ms.csv --interval 0.1
```
If you still observe instability at `0.1s`, keep production research capture at `0.2s` and inspect websocket/snapshot diagnostics in stderr JSON logs.

## Diagnostics emitted by logger
Structured stderr diagnostics include:
- market rollover details (`market_rollover`, `rollover_ws_closed`),
- websocket open/disconnect/reconnect reasons and counters,
- snapshot slow-tick/jitter warnings,
- rows written per market and total writer stop summary,
- staleness fields in each row (`secs_since_last_*`).

## Run safe live execution diagnostics harness
The harness is **dry-run only** in current implementation and does not place real orders.

```bash
python3 live_test_harness.py --out live_test_diagnostics.csv --interval 0.5 --max-quotes 100
```

### What the harness measures/logs
Per quote intent/result row:
- quote placed timestamp,
- quote price/size/side,
- whether fill occurred,
- time to fill,
- passive exit vs would-have-required-taking,
- adverse move after fill,
- cancel timing,
- estimated maker/taker path label.

## Backtest/simulation interpretation
See `docs/backtest_assumptions.md` for a concise separation of:
- optimistic ceiling assumptions,
- more conservative assumptions,
- unknowns requiring live execution data.

## Safety constraints
- No wallet secrets required.
- No real-money trading path enabled by default.
- Any future real execution path should remain explicit opt-in and tiny-size only.
