# Paper Trading Notes

## Fill assumptions
- The runner is paper-only. It never submits an order, signs a transaction, or touches wallet credentials.
- `--fill-style mid` is the default. Entry uses mid plus `--slippage-bps`; exit uses mid minus `--slippage-bps`.
- `--fill-style taker` uses ask-in and bid-out where available, then applies the same slippage penalty.
- `--fill-style maker` is a more optimistic paper assumption that uses bid-in and ask-out without trying to model queue priority.
- Fees are applied as a simple `--fee-bps` percentage of traded notional. This is a configurable approximation, not a venue-verified fee model.
- The dashboard tracks both mark-to-mid and conservative mark-to-exit PnL so optimistic and more realistic paper marks stay separated.

## Known limitations
- Expiry settlement is inferred from the captured BTC reference price around market rollover. The script does not wait for or verify official Polymarket resolution events.
- Order-flow confirmation is inferred from recent quote and trade updates on the subscribed YES and NO tokens. Hidden liquidity, queue position, and partial fills are not modeled.
- Missing or stale websocket updates are handled conservatively by blocking new entries, logging feed-staleness events, and using fail-safe closes in early-exit mode when the feed stays stale for too long.
- Replay mode is a debugging aid driven by historical snapshot rows. It is useful for smoke testing and log validation, but it is not a perfect reconstruction of the original live message cadence.
- The default strategy is intentionally conservative and simple. It is meant to exercise the live paper-trading plumbing, not to claim edge or profitability.

## Why paper results are not live-execution results
- Paper fills do not include true order queue dynamics, partial fills, cancellations, self-competition, or venue-specific matching behavior.
- Exit liquidity can disappear between snapshots. Conservative mark-to-exit helps, but it still remains a model.
- BTC reference prices and Polymarket market prices can diverge in timing and microstructure during fast moves.
- Strategy outputs are sensitive to fee, slippage, freshness, and settlement assumptions. Changing those assumptions can materially change the paper result.
- A profitable paper session should be treated as instrumentation output, not evidence that real capital would achieve the same fills or PnL.
