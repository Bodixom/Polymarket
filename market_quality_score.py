#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from statistics import mean
from typing import Any


def fv(v: str | None) -> float | None:
    if v in ("", "None", None):
        return None
    return float(v)


def bv(v: str | None) -> bool | None:
    if v in ("", "None", None):
        return None
    return v == "True"


def load(path: str) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def score_market(rows: list[dict[str, Any]], telemetry_rows: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    spreads = [fv(r.get("yes_spread")) for r in rows]
    spreads = [x for x in spreads if x is not None]
    staleness = [fv(r.get("secs_since_last_market_msg")) for r in rows]
    staleness = [x for x in staleness if x is not None]
    btc_stale = [fv(r.get("secs_since_last_btc_update")) for r in rows]
    btc_stale = [x for x in btc_stale if x is not None]
    btc = [fv(r.get("btc_price")) for r in rows]
    btc = [x for x in btc if x is not None]
    yes_mid = [fv(r.get("yes_mid")) for r in rows]
    yes_mid = [x for x in yes_mid if x is not None]

    quote_updates = sum(1 for r in rows if fv(r.get("secs_since_last_yes_quote_update")) is not None and fv(r.get("secs_since_last_yes_quote_update")) < 0.5)
    trade_updates = sum(1 for r in rows if fv(r.get("secs_since_last_yes_trade_update")) is not None and fv(r.get("secs_since_last_yes_trade_update")) < 1.0)

    tight_spread_ratio = sum(1 for x in spreads if x <= 0.02) / len(spreads) if spreads else 0.0
    fresh_quote_ratio = sum(1 for x in staleness if x <= 0.4) / len(staleness) if staleness else 0.0
    fresh_btc_ratio = sum(1 for x in btc_stale if x <= 1.0) / len(btc_stale) if btc_stale else 0.0
    stale_period_fraction = sum(1 for x in staleness if x > 1.0) / len(staleness) if staleness else 0.0

    btc_vol = abs((max(btc) - min(btc)) / mean(btc)) * 10000 if len(btc) >= 2 else 0.0
    poly_vol = abs((max(yes_mid) - min(yes_mid)) / mean(yes_mid)) * 10000 if len(yes_mid) >= 2 and mean(yes_mid) else 0.0

    update_intensity = min(1.0, (quote_updates + trade_updates) / max(1, len(rows)))

    inferred_entry = 0.0
    inferred_exit = 0.0
    if telemetry_rows:
        touched = [r for r in telemetry_rows if bv(r.get("touch_observed")) is True]
        fills = [r for r in telemetry_rows if bv(r.get("fill_inferred")) is True or bv(r.get("fill_opportunity")) is True]
        exits = [r for r in fills if bv(r.get("exit_opportunity_observed")) is True or bv(r.get("passive_exit_opportunity")) is True]
        inferred_entry = len(fills) / len(telemetry_rows) if telemetry_rows else 0.0
        inferred_exit = len(exits) / len(fills) if fills else 0.0
        if not fills and touched:
            inferred_entry = len(touched) / len(telemetry_rows)

    quality = (
        0.20 * tight_spread_ratio
        + 0.15 * fresh_quote_ratio
        + 0.10 * fresh_btc_ratio
        + 0.15 * min(1.0, update_intensity)
        + 0.10 * min(1.0, poly_vol / 25.0)
        + 0.10 * min(1.0, btc_vol / 25.0)
        + 0.10 * inferred_entry
        + 0.05 * inferred_exit
        + 0.05 * (1.0 - min(1.0, stale_period_fraction))
    ) * 100.0

    return {
        "rows": len(rows),
        "mean_spread": mean(spreads) if spreads else None,
        "tight_spread_ratio": tight_spread_ratio,
        "fresh_quote_ratio": fresh_quote_ratio,
        "fresh_btc_ratio": fresh_btc_ratio,
        "update_intensity": update_intensity,
        "stale_period_fraction": stale_period_fraction,
        "btc_vol_bps": btc_vol,
        "poly_vol_bps": poly_vol,
        "inferred_passive_entry": inferred_entry,
        "inferred_passive_exit_given_entry": inferred_exit,
        "quote_update_count": quote_updates,
        "trade_update_count": trade_updates,
        "quality_score": quality,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Score Polymarket snapshot quality by market slug")
    ap.add_argument("--snapshots", required=True)
    ap.add_argument("--telemetry", default=None, help="Optional telemetry CSV for inferred entry/exit opportunity weighting")
    ap.add_argument("--out", default="market_quality_scores.csv")
    args = ap.parse_args()

    rows = load(args.snapshots)
    by_slug: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        slug = str(r.get("slug") or "")
        if slug:
            by_slug[slug].append(r)

    telemetry_by_slug: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if args.telemetry:
        telemetry_rows = load(args.telemetry)
        for r in telemetry_rows:
            slug = str(r.get("slug") or "")
            if slug:
                telemetry_by_slug[slug].append(r)

    scores = []
    for slug, chunk in by_slug.items():
        s = score_market(chunk, telemetry_rows=telemetry_by_slug.get(slug))
        s["slug"] = slug
        scores.append(s)

    scores.sort(key=lambda x: x["quality_score"], reverse=True)

    fields = [
        "slug", "quality_score", "rows", "mean_spread", "tight_spread_ratio", "fresh_quote_ratio", "fresh_btc_ratio",
        "update_intensity", "stale_period_fraction", "btc_vol_bps", "poly_vol_bps", "inferred_passive_entry",
        "inferred_passive_exit_given_entry", "quote_update_count", "trade_update_count",
    ]
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for s in scores:
            w.writerow(s)

    for s in scores:
        print(
            f"{s['slug']}: score={s['quality_score']:.1f} rows={s['rows']} spread={s['mean_spread']} "
            f"entry={s['inferred_passive_entry']:.2f} exit={s['inferred_passive_exit_given_entry']:.2f} stale={s['stale_period_fraction']:.2f}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
