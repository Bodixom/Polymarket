# Backtest Assumption Tiers

This repo currently contains data capture plus execution-diagnostics tooling. There is **no production-grade backtest engine checked in** right now.

## Optimistic ceiling assumptions (for upper-bound intuition only)
- Touch price equals fill.
- Entry is maker and exit is also maker with no queue delays.
- No partial fills, no missed fills, no stale quote risk.
- Zero adverse selection cost around fills.

## Conservative assumptions (closer to plausible reality)
- Maker entry attempts can miss due to queue position.
- A meaningful share of exits require taking instead of passive unwind.
- Passive fill probabilities decay when market moves away.
- Partial fill and cancel latency matter for realized edge.

## Unknowns that require live execution evidence
- True queue priority behavior in target markets.
- Fill probability by spread state, volatility regime, and refresh cadence.
- Passive exit hit-rate and median time-to-exit.
- Realized adverse move after entry fills.
- Realized maker/taker mix under strict risk controls.

## Practical interpretation
- Treat optimistic results as a theoretical ceiling, not expected PnL.
- Conservative simulations are still model-dependent and incomplete.
- The live-test harness should be used to estimate fill/exit reality before any profitability claims.
