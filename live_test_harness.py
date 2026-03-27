#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import mean
from typing import Any, Optional


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def parse_ts(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


@dataclass
class QuoteIntent:
    quote_id: str
    quote_ts_utc: str
    side: str
    price: float
    size: float
    slug: str


class LiveTestHarness:
    """Safe-by-default execution telemetry harness.

    - Default mode is dry-run and never places real orders.
    - In snapshot mode, generates execution-style telemetry from real observed orderbook snapshots.
    """

    def __init__(
        self,
        out_csv: str,
        dry_run: bool,
        interval: float,
        max_quotes: int,
        snapshots_csv: Optional[str],
        quote_ttl_s: float,
        max_hold_s: float,
        adverse_horizon_s: float,
        size: float,
        min_exit_edge: float,
    ) -> None:
        self.out_csv = out_csv
        self.dry_run = dry_run
        self.interval = interval
        self.max_quotes = max_quotes
        self.snapshots_csv = snapshots_csv
        self.quote_ttl_s = quote_ttl_s
        self.max_hold_s = max_hold_s
        self.adverse_horizon_s = adverse_horizon_s
        self.size = size
        self.min_exit_edge = min_exit_edge
        self.stop = False

    def _emit(self, event: str, **fields: Any) -> None:
        payload = {"ts_utc": now_iso(), "event": event}
        payload.update(fields)
        print(json.dumps(payload, default=str))

    def _load_snapshots(self) -> list[dict[str, Any]]:
        if not self.snapshots_csv:
            return []
        rows: list[dict[str, Any]] = []
        with open(self.snapshots_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    row["ts"] = parse_ts(str(row["ts_utc"]))
                except Exception:
                    continue
                for key in ["yes_best_bid", "yes_best_ask", "yes_mid", "secs_since_last_market_msg", "secs_since_last_btc_update"]:
                    val = row.get(key)
                    row[key] = float(val) if val not in (None, "") else None
                rows.append(row)
        rows.sort(key=lambda r: r["ts"])
        return rows

    def _find_index_at_or_after(self, rows: list[dict[str, Any]], start_idx: int, target_ts: datetime) -> int:
        i = start_idx
        while i < len(rows) and rows[i]["ts"] < target_ts:
            i += 1
        return i

    def _extract_quote(self, row: dict[str, Any], idx: int) -> Optional[QuoteIntent]:
        bid = row.get("yes_best_bid")
        ask = row.get("yes_best_ask")
        slug = str(row.get("slug") or "")
        if bid is None or ask is None or not slug:
            return None
        side = "buy" if idx % 2 == 0 else "sell"
        price = bid if side == "buy" else ask
        return QuoteIntent(
            quote_id=f"q-{idx}-{int(row['ts'].timestamp() * 1000)}",
            quote_ts_utc=row["ts_utc"],
            side=side,
            price=price,
            size=self.size,
            slug=slug,
        )

    def _touches_quote(self, quote: QuoteIntent, row: dict[str, Any]) -> bool:
        bid = row.get("yes_best_bid")
        ask = row.get("yes_best_ask")
        if bid is None or ask is None:
            return False
        if quote.side == "buy":
            return ask <= quote.price
        return bid >= quote.price

    def _touches_exit(self, entry_side: str, exit_price: float, row: dict[str, Any]) -> bool:
        bid = row.get("yes_best_bid")
        ask = row.get("yes_best_ask")
        if bid is None or ask is None:
            return False
        if entry_side == "buy":
            return bid >= exit_price
        return ask <= exit_price

    def _mid(self, row: dict[str, Any]) -> Optional[float]:
        bid = row.get("yes_best_bid")
        ask = row.get("yes_best_ask")
        if bid is None or ask is None:
            return None
        return (bid + ask) / 2.0

    def _adverse_bps(self, entry_side: str, entry_mid: Optional[float], later_mid: Optional[float]) -> Optional[float]:
        if entry_mid is None or later_mid is None:
            return None
        if entry_side == "buy":
            move = (later_mid - entry_mid) / max(entry_mid, 1e-9)
            return round(-move * 10000, 2)
        move = (entry_mid - later_mid) / max(entry_mid, 1e-9)
        return round(-move * 10000, 2)

    def run_snapshot_mode(self) -> None:
        rows = self._load_snapshots()
        if not rows:
            raise RuntimeError("No valid rows found in snapshots CSV")

        fieldnames = [
            "quote_id", "quote_ts_utc", "slug", "quote_side", "quote_price", "quote_size", "mode",
            "fill_opportunity", "fill_opportunity_ts_utc", "time_to_touch_ms", "cancel_ts_utc", "cancel_after_ms",
            "entry_mid", "intended_exit_price", "passive_exit_opportunity", "passive_exit_ts_utc", "hold_ms",
            "forced_taker_exit", "maker_taker_path", "adverse_move_bps_post_fill",
            "quote_staleness_s", "btc_staleness_s",
        ]

        step = max(1, int(self.interval / 0.2))
        idxs = list(range(0, len(rows), step))[: self.max_quotes]

        with open(self.out_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for q_idx, i in enumerate(idxs):
                if self.stop:
                    break
                row = rows[i]
                quote = self._extract_quote(row, q_idx)
                if quote is None:
                    continue

                quote_ts = parse_ts(quote.quote_ts_utc)
                fill_deadline = quote_ts.timestamp() + self.quote_ttl_s
                fill_found_idx: Optional[int] = None

                j = i
                while j < len(rows) and rows[j]["slug"] == quote.slug and rows[j]["ts"].timestamp() <= fill_deadline:
                    if self._touches_quote(quote, rows[j]):
                        fill_found_idx = j
                        break
                    j += 1

                fill_opportunity = fill_found_idx is not None
                fill_ts: Optional[datetime] = rows[fill_found_idx]["ts"] if fill_found_idx is not None else None
                cancel_ts = quote_ts if fill_opportunity else datetime.fromtimestamp(fill_deadline, tz=timezone.utc)
                cancel_after_ms = 0 if fill_opportunity else int(self.quote_ttl_s * 1000)
                time_to_touch_ms = (
                    int((fill_ts - quote_ts).total_seconds() * 1000)
                    if fill_ts is not None else None
                )

                passive_exit_opportunity: Optional[bool] = None
                passive_exit_ts: Optional[datetime] = None
                forced_taker_exit: Optional[bool] = None
                hold_ms: Optional[int] = None
                path = "no_fill"
                adverse_bps: Optional[float] = None
                entry_mid = self._mid(row)
                intended_exit_price: Optional[float] = None

                if fill_opportunity and fill_found_idx is not None:
                    fill_row = rows[fill_found_idx]
                    entry_mid = self._mid(fill_row)
                    intended_exit_price = (
                        quote.price + self.min_exit_edge if quote.side == "buy" else quote.price - self.min_exit_edge
                    )
                    exit_deadline = fill_row["ts"].timestamp() + self.max_hold_s

                    k = fill_found_idx
                    while k < len(rows) and rows[k]["slug"] == quote.slug and rows[k]["ts"].timestamp() <= exit_deadline:
                        if self._touches_exit(quote.side, intended_exit_price, rows[k]):
                            passive_exit_opportunity = True
                            passive_exit_ts = rows[k]["ts"]
                            break
                        k += 1

                    if passive_exit_opportunity is not True:
                        passive_exit_opportunity = False
                        forced_taker_exit = True
                        hold_ms = int(self.max_hold_s * 1000)
                        path = "maker_entry_taker_exit"
                    else:
                        forced_taker_exit = False
                        hold_ms = int((passive_exit_ts - fill_row["ts"]).total_seconds() * 1000)
                        path = "maker_entry_maker_exit"

                    adverse_ts = fill_row["ts"].timestamp() + self.adverse_horizon_s
                    adv_idx = self._find_index_at_or_after(rows, fill_found_idx, datetime.fromtimestamp(adverse_ts, tz=timezone.utc))
                    later_mid = self._mid(rows[adv_idx]) if adv_idx < len(rows) and rows[adv_idx]["slug"] == quote.slug else None
                    adverse_bps = self._adverse_bps(quote.side, entry_mid, later_mid)

                out_row = {
                    "quote_id": quote.quote_id,
                    "quote_ts_utc": quote.quote_ts_utc,
                    "slug": quote.slug,
                    "quote_side": quote.side,
                    "quote_price": round(quote.price, 6),
                    "quote_size": quote.size,
                    "mode": "dry_run_observed",
                    "fill_opportunity": fill_opportunity,
                    "fill_opportunity_ts_utc": fill_ts.isoformat(timespec="milliseconds") if fill_ts else None,
                    "time_to_touch_ms": time_to_touch_ms,
                    "cancel_ts_utc": cancel_ts.isoformat(timespec="milliseconds"),
                    "cancel_after_ms": cancel_after_ms,
                    "entry_mid": entry_mid,
                    "intended_exit_price": intended_exit_price,
                    "passive_exit_opportunity": passive_exit_opportunity,
                    "passive_exit_ts_utc": passive_exit_ts.isoformat(timespec="milliseconds") if passive_exit_ts else None,
                    "hold_ms": hold_ms,
                    "forced_taker_exit": forced_taker_exit,
                    "maker_taker_path": path,
                    "adverse_move_bps_post_fill": adverse_bps,
                    "quote_staleness_s": row.get("secs_since_last_market_msg"),
                    "btc_staleness_s": row.get("secs_since_last_btc_update"),
                }
                writer.writerow(out_row)
                self._emit("quote_result", **out_row)

            f.flush()

        # Print quick summary as stdout diagnostic JSON
        with open(self.out_csv, "r", encoding="utf-8") as f:
            r = list(csv.DictReader(f))
        fills = [x for x in r if x["fill_opportunity"] == "True"]
        exits = [x for x in fills if x["passive_exit_opportunity"] == "True"]
        adverse = [float(x["adverse_move_bps_post_fill"]) for x in fills if x["adverse_move_bps_post_fill"] not in ("", "None")]
        self._emit(
            "harness_summary",
            total_quotes=len(r),
            fill_opportunity_rate=(len(fills) / len(r)) if r else 0.0,
            passive_exit_rate_given_fill=(len(exits) / len(fills)) if fills else 0.0,
            mean_adverse_bps=(mean(adverse) if adverse else None),
        )

    def run_synthetic_mode(self) -> None:
        """Legacy synthetic fallback mode (still dry-run only)."""
        fieldnames = [
            "quote_id", "quote_ts_utc", "slug", "quote_side", "quote_price", "quote_size", "mode",
            "fill_opportunity", "fill_opportunity_ts_utc", "time_to_touch_ms", "cancel_ts_utc", "cancel_after_ms",
            "entry_mid", "intended_exit_price", "passive_exit_opportunity", "passive_exit_ts_utc", "hold_ms",
            "forced_taker_exit", "maker_taker_path", "adverse_move_bps_post_fill", "quote_staleness_s", "btc_staleness_s",
        ]
        with open(self.out_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for i in range(self.max_quotes):
                if self.stop:
                    break
                row = {
                    "quote_id": f"q-{i}-{int(time.time() * 1000)}",
                    "quote_ts_utc": now_iso(),
                    "slug": "synthetic",
                    "quote_side": "buy" if i % 2 == 0 else "sell",
                    "quote_price": 0.5,
                    "quote_size": self.size,
                    "mode": "dry_run_synthetic",
                    "fill_opportunity": False,
                    "fill_opportunity_ts_utc": None,
                    "time_to_touch_ms": None,
                    "cancel_ts_utc": now_iso(),
                    "cancel_after_ms": int(self.quote_ttl_s * 1000),
                    "entry_mid": 0.5,
                    "intended_exit_price": 0.51,
                    "passive_exit_opportunity": None,
                    "passive_exit_ts_utc": None,
                    "hold_ms": None,
                    "forced_taker_exit": None,
                    "maker_taker_path": "no_fill",
                    "adverse_move_bps_post_fill": None,
                    "quote_staleness_s": None,
                    "btc_staleness_s": None,
                }
                writer.writerow(row)
                self._emit("quote_result", **row)
                time.sleep(self.interval)
            f.flush()

    def run(self) -> None:
        if not self.dry_run:
            raise RuntimeError("Live execution is disabled in this harness by design. Use --dry-run.")
        if self.snapshots_csv:
            self.run_snapshot_mode()
        else:
            self.run_synthetic_mode()


def main() -> int:
    parser = argparse.ArgumentParser(description="Safe-by-default Polymarket dry-run execution telemetry harness")
    parser.add_argument("--out", default="live_test_diagnostics.csv", help="Output CSV for diagnostic rows")
    parser.add_argument("--interval", type=float, default=0.5, help="Seconds between quote intents in synthetic mode, and quote sampling step in snapshot mode")
    parser.add_argument("--max-quotes", type=int, default=200, help="Number of intents to emit")
    parser.add_argument("--snapshots-csv", default=None, help="If set, derive telemetry from observed snapshot CSV")
    parser.add_argument("--quote-ttl-s", type=float, default=2.0, help="Seconds to wait for passive entry touch before cancel")
    parser.add_argument("--max-hold-s", type=float, default=8.0, help="Seconds to wait for passive exit before forced taker path")
    parser.add_argument("--adverse-horizon-s", type=float, default=2.0, help="Seconds after fill for adverse movement measurement")
    parser.add_argument("--size", type=float, default=1.0, help="Diagnostic quote size")
    parser.add_argument("--min-exit-edge", type=float, default=0.01, help="Exit edge in price units")
    parser.add_argument("--dry-run", action="store_true", default=True, help="Diagnostics only. Real execution disabled by design")
    args = parser.parse_args()

    harness = LiveTestHarness(
        out_csv=args.out,
        dry_run=args.dry_run,
        interval=args.interval,
        max_quotes=args.max_quotes,
        snapshots_csv=args.snapshots_csv,
        quote_ttl_s=args.quote_ttl_s,
        max_hold_s=args.max_hold_s,
        adverse_horizon_s=args.adverse_horizon_s,
        size=args.size,
        min_exit_edge=args.min_exit_edge,
    )

    def _stop(*_args: Any) -> None:
        harness.stop = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    harness.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
