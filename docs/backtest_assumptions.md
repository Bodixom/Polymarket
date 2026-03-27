# Backtest Assumption Tiers

This repo contains data capture, dry-run execution telemetry generation, and a telemetry-calibrated backtest layer.

## Why calibration exists
Naive touch-fill assumptions overstate realizable edge. The calibration flow replaces hand-picked constants with observed rates from snapshot-derived telemetry.

## Inputs from telemetry
From `live_test_harness.py --snapshots-csv ...` rows:
- passive entry fill-opportunity rate,
- passive exit-opportunity rate (conditional on entry opportunity),
- hold-time distribution,
- adverse movement after intended fill,
- fraction of entries that would require taker exit under strict passive policy.

## Tier definitions used by `telemetry_calibrated_backtest.py`

### 1) Optimistic upper bound
- Slightly boosts measured fill/exit probabilities.
- Uses full configured edge and baseline taker fee assumptions.
- Purpose: ceiling estimate, not expectation.

### 2) Telemetry-calibrated estimate
- Uses measured probabilities and observed adverse move directly.
- Represents the current best dry-run estimate.

### 3) Hard conservative lower bound
- Scales down measured passive rates.
- Scales edge down and taker costs up.
- Treat as stress case for go/no-go readiness.

## Remaining unknowns (cannot be resolved in dry-run only)
- True queue-priority placement and partial-fill mechanics.
- Hidden liquidity and trade-through behavior vs top-of-book touches.
- Real cancel/replace acknowledgment timing under load.

## Interpretation guidance
- If only optimistic tier looks positive, strategy is likely not robust.
- Telemetry-calibrated tier should remain positive with reasonable sample size before any tiny live-money test is considered.
- Conservative tier should not show catastrophic downside if strict risk controls are in place.
