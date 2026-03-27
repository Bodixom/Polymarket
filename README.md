# Polymarket execution-research repo

This repository is focused on **market data capture stability** and **execution validation in dry-run mode**, not production trading.

## Active scripts and purpose
- `polymarket_snapshot_logger.py` — rolling BTC 5-minute Polymarket snapshot logger with diagnostics and rollover support.
- `live_test_harness.py` — safe-by-default dry-run execution telemetry harness. Can infer fill/exit opportunities from snapshot captures.
- `telemetry_calibrated_backtest.py` — calibration and tiered backtest estimates (optimistic / measured / conservative) from harness telemetry.
- `market_quality_score.py` — per-market quality scoring/ranking to decide where execution testing is worthwhile.
- `docs/backtest_assumptions.md` — assumption tiers and interpretation guidance.
- `docs/live_money_readiness_checklist.md` — explicit gates required before tiny live-money testing.

## Stable snapshot capture (recommended baseline: 0.2s)
```bash
python3 polymarket_snapshot_logger.py \
  --out data/snapshots_200ms.csv \
  --interval 0.2 \
  --rollover-poll 1.0 \
  --flush-interval 1.0 \
  --flush-every-rows 25
```

### Optional stress run (0.1s)
```bash
python3 polymarket_snapshot_logger.py \
  --out data/snapshots_100ms.csv \
  --interval 0.1
```
Use diagnostics (`snapshot_tick_slow`, websocket reconnect/disconnect events) to decide if 0.1s is acceptable for your host/network.

## Dry-run execution telemetry (from observed snapshots)
```bash
python3 live_test_harness.py \
  --dry-run \
  --snapshots-csv data/snapshots_200ms.csv \
  --out data/execution_telemetry.csv \
  --max-quotes 500 \
  --interval 0.2 \
  --quote-ttl-s 2.0 \
  --max-hold-s 8.0 \
  --adverse-horizon-s 2.0 \
  --min-exit-edge 0.01
```

Telemetry output includes quote intent, observed touch opportunity, time-to-touch, passive exit opportunity, forced taker path flag, and post-fill adverse movement.

## Telemetry-calibrated backtest tiers
```bash
python3 telemetry_calibrated_backtest.py \
  --telemetry data/execution_telemetry.csv \
  --out-json data/telemetry_calibration.json \
  --edge-bps 20 \
  --taker-fee-bps 8
```

## Market-quality scoring
```bash
python3 market_quality_score.py \
  --snapshots data/snapshots_200ms.csv \
  --out data/market_quality_scores.csv
```

## Safety constraints
- Dry-run is default and live execution is intentionally disabled in harness code.
- No wallet secrets required for any command in this repository.
- Never assume profitability from backtests; treat telemetry-calibrated outputs as planning inputs.
