#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from statistics import mean
from typing import Any


def fv(v: str) -> float | None:
    if v in ("", "None", None):
        return None
    return float(v)


def load(path: str) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def score_market(rows: list[dict[str, Any]]) -> dict[str, Any]:
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

    btc_vol = abs((max(btc) - min(btc)) / mean(btc)) * 10000 if len(btc) >= 2 else 0.0
    poly_vol = abs((max(yes_mid) - min(yes_mid)) / mean(yes_mid)) * 10000 if len(yes_mid) >= 2 and mean(yes_mid) else 0.0

    update_intensity = min(1.0, (quote_updates + trade_updates) / max(1, len(rows)))

    quality = (
        0.25 * tight_spread_ratio
        + 0.2 * fresh_quote_ratio
        + 0.15 * fresh_btc_ratio
        + 0.2 * min(1.0, update_intensity)
        + 0.1 * min(1.0, poly_vol / 25.0)
        + 0.1 * min(1.0, btc_vol / 25.0)
    ) * 100.0

    return {
        "rows": len(rows),
        "mean_spread": mean(spreads) if spreads else None,
        "tight_spread_ratio": tight_spread_ratio,
        "fresh_quote_ratio": fresh_quote_ratio,
        "fresh_btc_ratio": fresh_btc_ratio,
        "btc_vol_bps": btc_vol,
        "poly_vol_bps": poly_vol,
        "quote_update_count": quote_updates,
        "trade_update_count": trade_updates,
        "quality_score": quality,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Score Polymarket snapshot quality by market slug")
    ap.add_argument("--snapshots", required=True)
    ap.add_argument("--out", default="market_quality_scores.csv")
    args = ap.parse_args()

    rows = load(args.snapshots)
    by_slug: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        slug = str(r.get("slug") or "")
        if slug:
            by_slug[slug].append(r)

    scores = []
    for slug, chunk in by_slug.items():
        s = score_market(chunk)
        s["slug"] = slug
        scores.append(s)

    scores.sort(key=lambda x: x["quality_score"], reverse=True)

    fields = [
        "slug", "quality_score", "rows", "mean_spread", "tight_spread_ratio", "fresh_quote_ratio", "fresh_btc_ratio",
        "btc_vol_bps", "poly_vol_bps", "quote_update_count", "trade_update_count",
    ]
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for s in scores:
            w.writerow(s)

    for s in scores:
        print(
            f"{s['slug']}: score={s['quality_score']:.1f} rows={s['rows']} spread={s['mean_spread']} "
            f"fresh_quote={s['fresh_quote_ratio']:.2f} updates={s['quote_update_count']}/{s['trade_update_count']}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
