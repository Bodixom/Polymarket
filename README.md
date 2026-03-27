# Polymarket execution-research repo

This repository is focused on **market data capture stability** and **execution validation in dry-run mode**, not production trading.

## Active scripts and purpose
- `polymarket_snapshot_logger.py` — rolling BTC 5-minute Polymarket snapshot logger with diagnostics and rollover support.
- `live_test_harness.py` — safe-by-default dry-run execution telemetry harness. Derives touch, inferred-fill, and exit-opportunity telemetry from snapshots.
- `telemetry_calibrated_backtest.py` — calibration and tiered backtest estimates (optimistic / measured / conservative), including per-market calibration summaries.
- `market_quality_score.py` — per-market quality scoring/ranking to decide where execution testing is worthwhile.
- `market_probe_report.py` — combined per-market report with quality score + calibration metrics + tier output + recommendation.
- `docs/backtest_assumptions.md` — assumption tiers and interpretation guidance.
- `docs/live_money_readiness_checklist.md` — explicit gates required before tiny live-money testing.

## Stable snapshot capture (recommended baseline: 0.2s)
```bash
python3 polymarket_snapshot_logger.py \
  --out data/snapshots_real_200ms.csv \
  --interval 0.2 \
  --rollover-poll 1.0 \
  --flush-interval 1.0 \
  --flush-every-rows 25
```

Capture for at least 15 minutes to span multiple market rollovers.

## Dry-run execution telemetry (from observed snapshots)
```bash
python3 live_test_harness.py \
  --dry-run \
  --snapshots-csv data/snapshots_real_200ms.csv \
  --out data/execution_telemetry_real.csv \
  --max-quotes 2000 \
  --interval 0.2 \
  --quote-ttl-s 2.0 \
  --max-hold-s 8.0 \
  --adverse-horizon-s 2.0 \
  --min-exit-edge 0.01 \
  --min-touch-observations 2 \
  --min-touch-persistence-ms 300 \
  --max-touch-staleness-s 0.6
```

## Telemetry-calibrated backtest tiers
```bash
python3 telemetry_calibrated_backtest.py \
  --telemetry data/execution_telemetry_real.csv \
  --out-json data/telemetry_calibration_real.json \
  --edge-bps 20 \
  --taker-fee-bps 8
```

## Market-quality scoring
```bash
python3 market_quality_score.py \
  --snapshots data/snapshots_real_200ms.csv \
  --telemetry data/execution_telemetry_real.csv \
  --out data/market_quality_scores_real.csv
```

## Combined probe recommendation report
```bash
python3 market_probe_report.py \
  --quality-csv data/market_quality_scores_real.csv \
  --calibration-json data/telemetry_calibration_real.json \
  --out-csv data/market_probe_report_real.csv \
  --out-json data/market_probe_report_real.json
```

## Safety constraints
- Dry-run is default and live execution is intentionally disabled in harness code.
- No wallet secrets required for any command in this repository.
- Never assume profitability from backtests; treat telemetry-calibrated outputs as planning inputs.
