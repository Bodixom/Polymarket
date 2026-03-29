# Polymarket execution-research repo

This repository is focused on **market data capture stability** and **execution validation in dry-run mode**, not production trading.

## Active scripts and purpose
- `polymarket_snapshot_logger.py` - rolling BTC 5-minute Polymarket snapshot logger with diagnostics and rollover support.
- `live_paper_trader.py` - live BTC 5-minute paper-trading runner with terminal dashboard, rollover handling, CSV event/trade logs, and shutdown summaries.
- `live_test_harness.py` - safe-by-default dry-run execution telemetry harness. Derives touch, inferred-fill, and exit-opportunity telemetry from snapshots.
- `telemetry_calibrated_backtest.py` - calibration and tiered backtest estimates (optimistic / measured / conservative), including per-market calibration summaries.
- `market_quality_score.py` - per-market quality scoring/ranking to decide where execution testing is worthwhile.
- `market_probe_report.py` - combined per-market report with quality score + calibration metrics + tier output + recommendation.
- `docs/backtest_assumptions.md` - assumption tiers and interpretation guidance.
- `docs/live_money_readiness_checklist.md` - explicit gates required before tiny live-money testing.

## Validated local workflow (PowerShell, Windows)
```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Recommended operating point:
```powershell
.\.venv\Scripts\python.exe polymarket_snapshot_logger.py `
  --out data/snapshots_real_200ms.csv `
  --interval 0.2 `
  --rollover-poll 1.0 `
  --flush-interval 1.0 `
  --flush-every-rows 25 `
  2> data/logger_real_200ms.stderr.log
```

Let the logger run for at least 15 minutes, then stop it manually. Capture stderr so rollover and reconnect diagnostics are preserved.

Optional local stress run:
```powershell
.\.venv\Scripts\python.exe polymarket_snapshot_logger.py `
  --out data/snapshots_stress_100ms.csv `
  --interval 0.1 `
  --rollover-poll 1.0 `
  --flush-interval 1.0 `
  --flush-every-rows 25 `
  2> data/logger_stress_100ms.stderr.log
```

Dry-run telemetry on the captured snapshots:
```powershell
.\.venv\Scripts\python.exe live_test_harness.py `
  --dry-run `
  --snapshots-csv data/snapshots_real_200ms.csv `
  --out data/execution_telemetry_real.csv `
  --max-quotes 10000 `
  --interval 0.2 `
  --quote-ttl-s 2.0 `
  --max-hold-s 8.0 `
  --adverse-horizon-s 2.0 `
  --min-exit-edge 0.01 `
  --min-touch-observations 2 `
  --min-touch-persistence-ms 300 `
  --max-touch-staleness-s 0.6 `
  --max-touch-btc-staleness-s 2.0
```

Telemetry-calibrated backtest tiers:
```powershell
.\.venv\Scripts\python.exe telemetry_calibrated_backtest.py `
  --telemetry data/execution_telemetry_real.csv `
  --out-json data/telemetry_calibration_real.json `
  --edge-bps 20 `
  --taker-fee-bps 8
```

Market-quality scoring:
```powershell
.\.venv\Scripts\python.exe market_quality_score.py `
  --snapshots data/snapshots_real_200ms.csv `
  --telemetry data/execution_telemetry_real.csv `
  --out data/market_quality_scores_real.csv
```

Combined probe recommendation report:
```powershell
.\.venv\Scripts\python.exe market_probe_report.py `
  --quality-csv data/market_quality_scores_real.csv `
  --calibration-json data/telemetry_calibration_real.json `
  --out-csv data/market_probe_report_real.csv `
  --out-json data/market_probe_report_real.json
```

## Live paper trader
`live_paper_trader.py` reuses the same BTC reference feed, Polymarket websocket subscription, and 5-minute rollover handling as the snapshot logger, but it keeps the market state in memory so a paper-only strategy can react in real time.

What it does:
- Tracks the active BTC 5-minute market, YES/NO prices, BTC reference move, recent momentum, and simple order-flow imbalance.
- Runs a conservative one-position-at-a-time paper strategy that can trade long `YES` or long `NO`.
- Supports `early-exit` mode and `expiry` mode.
- Refreshes a curses-free terminal dashboard in place.
- Writes `paper_trades.csv`, `paper_events.csv`, `paper_equity.csv`, `session_summary.json`, and `session_summary.csv` into a timestamped run directory.

48-hour live paper run:
```powershell
.\.venv\Scripts\python.exe live_paper_trader.py `
  --paper-bankroll 1000 `
  --stake 25 `
  --max-position-notional 25 `
  --fee-bps 10 `
  --slippage-bps 15 `
  --fill-style mid `
  --mode early-exit `
  --min-edge 0.08 `
  --max-hold-s 90 `
  --near-expiry-window-s 45 `
  --btc-momentum-window-s 30 `
  --flow-window-s 15 `
  --heartbeat-timeout-s 5 `
  --refresh-ms 250 `
  --log-dir data/paper_runs
```

Approximate assumptions:
- Default fills are paper fills only. `mid` mode uses mid plus a slippage penalty on entry and mid minus a slippage penalty on exit.
- `taker` mode leans on ask-in / bid-out. `maker` mode is a more optimistic bid-in / ask-out paper assumption.
- Expiry settlement is inferred from captured BTC reference prices around expiry, not official Polymarket resolution messages.
- Results are for strategy instrumentation only and are not evidence of live executable profitability.

Local replay smoke path:
```powershell
.\.venv\Scripts\python.exe live_paper_trader.py `
  --replay-csv data/snapshots_real_200ms.csv `
  --replay-speedup 200 `
  --mode early-exit `
  --log-dir data/paper_replay_runs
```

## What We Learned
- Real runtime validation on 2026-03-27 UTC kept the `0.2s` logger stable across four BTC 5-minute markets. Realized cadence was 200.6ms mean / 210ms p95, and the only gaps above 0.5s were rollover gaps.
- Rollover handling and reconnect behavior matched expectations. The Polymarket websocket closed once per market boundary and re-opened cleanly; no BTC websocket reconnects were observed. Market-message stale-period fraction above 1s was 0.0 in the real capture.
- `0.1s` is locally viable, but it is not the recommended baseline. It held 100.4ms mean / 112ms p95 cadence, yet it raised duplicate top-of-book rows to 84.1% from 77.2% at `0.2s`, so it mostly increases I/O without materially improving execution realism.
- Measured execution realism from the real `0.2s` run was weak for a live-money probe: touch 34.36%, fill-inferred 25.93%, passive exit opportunity given fill 38.24%, forced taker exit given fill 61.76%, adverse move mean 152.68 bps after touch and 277.41 bps after inferred fill. No market qualified for a tiny live-money probe in this run.

## Safety constraints
- Dry-run is default and live execution is intentionally disabled in harness code.
- `live_paper_trader.py` is paper trading only. It does not submit orders, sign transactions, use wallets, or accept private keys.
- No wallet secrets required for any command in this repository.
- Never assume profitability from backtests; treat telemetry-calibrated outputs as planning inputs.
