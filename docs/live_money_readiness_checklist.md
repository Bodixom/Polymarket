# Tiny Live-Money Readiness Checklist ($20-$50)

Do **not** run real-money tests by default. This checklist defines minimum gates before any tiny-size execution probe is justified.

## Minimum telemetry sample size
- At least **1,000 quote intents** in dry-run observed mode.
- At least **300 entry opportunities** observed.
- At least **3 separate market rollovers** covered.

## Minimum execution-quality thresholds
- Passive entry opportunity rate: **>= 30%**.
- Passive exit opportunity rate (given entry): **>= 55%**.
- Forced taker exit fraction (given entry): **<= 45%**.
- Median hold time for passive exits: **<= 6 seconds**.

## Adverse-selection thresholds
- Mean adverse move after intended fill: **<= 10 bps**.
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

If any gate fails, continue dry-run telemetry and calibration work first.
