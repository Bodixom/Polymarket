# Tiny Live-Money Readiness Checklist ($20-$50)

Do **not** run real-money tests by default. This checklist defines minimum gates before any tiny-size execution probe is justified.

## Minimum telemetry sample size
- At least **1,000 quote intents** in dry-run observed mode.
- At least **300 inferred entry fills** observed.
- At least **3 separate market rollovers** covered.

## Minimum execution-quality thresholds
- Passive entry touch frequency: **>= 35%**.
- Passive entry fill-inferred frequency: **>= 25%**.
- Passive exit opportunity rate (given inferred fill): **>= 55%**.
- Forced taker exit fraction (given inferred fill): **<= 45%**.
- Median hold time for passive exits: **<= 6 seconds**.

## Adverse-selection thresholds
- Mean adverse move after inferred fill: **<= 10 bps**.
- 95th percentile adverse move: **<= 30 bps**.

## Environment and logging gates
- Snapshot logger stable at 0.2s with no persistent reconnect storms.
- Staleness fields generally low (market and BTC updates fresh).
- Full command logs, stderr diagnostics, and output CSV artifacts archived.
- Reproducible runbook commands documented and validated by a second operator.

## Governance / risk gates
- Real-money test size capped at **$20-$50 equivalent exposure**.
- Explicit stop condition defined before test starts.
- No unattended process; operator present for full test window.
- Post-test review required before any size increase.

## End-to-end runbook commands (real snapshots)
```bash
# 1) Dependencies
python3 -m pip install -r requirements.txt

# 2) Capture snapshots for >= 15 minutes (multiple 5m rollovers)
python3 polymarket_snapshot_logger.py \
  --out data/snapshots_real_200ms.csv \
  --interval 0.2 \
  --rollover-poll 1.0 \
  --flush-interval 1.0 \
  --flush-every-rows 25

# 3) Dry-run telemetry on captured snapshots
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

# 4) Telemetry-calibrated backtest tiers + per-market stats
python3 telemetry_calibrated_backtest.py \
  --telemetry data/execution_telemetry_real.csv \
  --out-json data/telemetry_calibration_real.json \
  --edge-bps 20 \
  --taker-fee-bps 8

# 5) Market quality ranking (with telemetry-aware opportunity metrics)
python3 market_quality_score.py \
  --snapshots data/snapshots_real_200ms.csv \
  --telemetry data/execution_telemetry_real.csv \
  --out data/market_quality_scores_real.csv

# 6) Combined probe recommendation report
python3 market_probe_report.py \
  --quality-csv data/market_quality_scores_real.csv \
  --calibration-json data/telemetry_calibration_real.json \
  --out-csv data/market_probe_report_real.csv \
  --out-json data/market_probe_report_real.json
```

If any gate fails, continue dry-run telemetry and calibration work first.
