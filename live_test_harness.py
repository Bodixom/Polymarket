#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import random
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


@dataclass
class QuoteIntent:
    quote_id: str
    ts_utc: str
    side: str
    price: float
    size: float


class LiveTestHarness:
    """Minimal execution diagnostic harness.

    Default is fully safe: DRY-RUN only, no real order placement path.
    """

    def __init__(
        self,
        out_csv: str,
        dry_run: bool,
        interval: float,
        max_quotes: int,
    ) -> None:
        self.out_csv = out_csv
        self.dry_run = dry_run
        self.interval = interval
        self.max_quotes = max_quotes
        self.stop = False

    def _emit(self, event: str, **fields: Any) -> None:
        payload = {"ts_utc": now_iso(), "event": event}
        payload.update(fields)
        print(json.dumps(payload, default=str))

    def _place_quote(self, idx: int) -> QuoteIntent:
        side = "buy" if idx % 2 == 0 else "sell"
        px = round(0.45 + (idx % 10) * 0.005, 4)
        size = 1.0
        quote_id = f"q-{idx}-{int(time.time() * 1000)}"

        # Safety guard: real execution is intentionally not implemented in this harness.
        if not self.dry_run:
            raise RuntimeError(
                "Live execution is disabled in this harness by design. "
                "Re-run with --dry-run (default) for diagnostics-only mode."
            )

        self._emit("quote_intent", quote_id=quote_id, side=side, price=px, size=size, mode="dry_run")
        return QuoteIntent(quote_id=quote_id, ts_utc=now_iso(), side=side, price=px, size=size)

    def _simulate_fill_outcome(self, quote: QuoteIntent) -> dict[str, Any]:
        # Diagnostic-only synthetic outcome. Replace later with real execution telemetry.
        filled = random.random() < 0.3
        time_to_fill_ms = int(random.uniform(150, 2500)) if filled else None
        passive_exit = bool(filled and random.random() < 0.6)
        would_require_taking = bool(filled and not passive_exit)
        adverse_move_bps = round(random.uniform(-8, 12), 2) if filled else None
        cancel_after_ms = int(random.uniform(500, 5000)) if not filled else None
        path = (
            "maker_entry_maker_exit"
            if filled and passive_exit
            else "maker_entry_taker_exit"
            if filled
            else "no_fill"
        )

        return {
            "quote_id": quote.quote_id,
            "quote_ts_utc": quote.ts_utc,
            "quote_side": quote.side,
            "quote_price": quote.price,
            "quote_size": quote.size,
            "filled": filled,
            "fill_ts_utc": now_iso() if filled else None,
            "time_to_fill_ms": time_to_fill_ms,
            "passive_exit": passive_exit if filled else None,
            "would_require_taking": would_require_taking if filled else None,
            "adverse_move_bps_post_fill": adverse_move_bps,
            "cancel_ts_utc": now_iso() if cancel_after_ms is not None else None,
            "cancel_after_ms": cancel_after_ms,
            "maker_taker_path": path,
            "mode": "dry_run",
        }

    def run(self) -> None:
        fieldnames = [
            "quote_id",
            "quote_ts_utc",
            "quote_side",
            "quote_price",
            "quote_size",
            "filled",
            "fill_ts_utc",
            "time_to_fill_ms",
            "passive_exit",
            "would_require_taking",
            "adverse_move_bps_post_fill",
            "cancel_ts_utc",
            "cancel_after_ms",
            "maker_taker_path",
            "mode",
        ]

        with open(self.out_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(self.max_quotes):
                if self.stop:
                    break
                q = self._place_quote(i)
                row = self._simulate_fill_outcome(q)
                writer.writerow(row)
                if i % 20 == 0:
                    f.flush()
                self._emit("quote_result", **row)
                time.sleep(self.interval)

            f.flush()


def main() -> int:
    parser = argparse.ArgumentParser(description="Safe-by-default Polymarket execution diagnostics harness")
    parser.add_argument("--out", default="live_test_diagnostics.csv", help="Output CSV for diagnostic rows")
    parser.add_argument("--interval", type=float, default=0.5, help="Seconds between quote intents")
    parser.add_argument("--max-quotes", type=int, default=100, help="Number of intents to emit")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Diagnostics only. Real execution disabled by default and in current implementation.",
    )
    args = parser.parse_args()

    harness = LiveTestHarness(
        out_csv=args.out,
        dry_run=args.dry_run,
        interval=args.interval,
        max_quotes=args.max_quotes,
    )

    def _stop(*_args: Any) -> None:
        harness.stop = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    harness.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
