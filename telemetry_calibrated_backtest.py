#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from statistics import median
from typing import Any


def f(v: str | None) -> float | None:
    if v in ("", "None", None):
        return None
    return float(v)


def b(v: str | None) -> bool | None:
    if v in ("", "None", None):
        return None
    return v == "True"


def load_rows(path: str) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    idx = math.floor(p * (len(values) - 1))
    return sorted(values)[idx]


def calibrate(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    touched = [r for r in rows if b(r.get("touch_observed")) is True]
    fills = [r for r in rows if b(r.get("fill_inferred")) is True or b(r.get("fill_opportunity")) is True]
    exits = [r for r in fills if b(r.get("exit_opportunity_observed")) is True or b(r.get("passive_exit_opportunity")) is True]
    forced = [r for r in fills if b(r.get("forced_taker_exit")) is True]

    ttt = [f(r.get("time_to_touch_ms")) for r in touched]
    ttt = [x for x in ttt if x is not None]
    hold_ms = [f(r.get("hold_ms")) for r in exits]
    hold_ms = [x for x in hold_ms if x is not None]
    adverse = [f(r.get("adverse_move_bps_post_fill")) for r in fills]
    adverse = [x for x in adverse if x is not None]

    return {
        "sample_quotes": total,
        "passive_entry_touch_freq": (len(touched) / total) if total else 0.0,
        "passive_entry_fill_inferred_freq": (len(fills) / total) if total else 0.0,
        "passive_exit_opportunity_freq_given_fill": (len(exits) / len(fills)) if fills else 0.0,
        "forced_taker_exit_frac_given_fill": (len(forced) / len(fills)) if fills else 0.0,
        "time_to_touch_ms": {
            "median": median(ttt) if ttt else None,
            "p90": percentile(ttt, 0.90),
            "p95": percentile(ttt, 0.95),
        },
        "hold_ms": {
            "median": median(hold_ms) if hold_ms else None,
            "p90": percentile(hold_ms, 0.90),
            "p95": percentile(hold_ms, 0.95),
        },
        "adverse_bps_after_touch_or_fill": {
            "mean": (sum(adverse) / len(adverse)) if adverse else None,
            "p95": percentile(adverse, 0.95),
        },
    }


def scenario_eval(cal: dict[str, Any], edge_bps: float, taker_fee_bps: float, conservative_multiplier: float = 1.0) -> dict[str, float]:
    p_entry = cal["passive_entry_fill_inferred_freq"] * conservative_multiplier
    p_exit = cal["passive_exit_opportunity_freq_given_fill"] * conservative_multiplier
    adverse = ((cal.get("adverse_bps_after_touch_or_fill") or {}).get("mean") or 0.0) / max(conservative_multiplier, 1e-9)

    p_entry = max(0.0, min(1.0, p_entry))
    p_exit = max(0.0, min(1.0, p_exit))

    maker_path_ev = edge_bps - adverse
    taker_path_ev = edge_bps - adverse - taker_fee_bps

    ev_per_quote = p_entry * (p_exit * maker_path_ev + (1 - p_exit) * taker_path_ev)

    return {
        "ev_bps_per_quote": ev_per_quote,
        "maker_only_completion_prob": p_entry * p_exit,
        "estimated_forced_taker_rate": p_entry * (1 - p_exit),
        "estimated_filled_rate": p_entry,
    }


def main() -> int:
    p = argparse.ArgumentParser(description="Telemetry-calibrated maker-only backtest tiers")
    p.add_argument("--telemetry", required=True, help="CSV from live_test_harness snapshot dry-run mode")
    p.add_argument("--out-json", default="telemetry_calibration.json")
    p.add_argument("--edge-bps", type=float, default=20.0, help="Gross edge target in bps for completed maker cycle")
    p.add_argument("--taker-fee-bps", type=float, default=8.0, help="Cost if forced to taker exit")
    args = p.parse_args()

    rows = load_rows(args.telemetry)
    cal = calibrate(rows)

    optimistic = scenario_eval(cal, args.edge_bps, args.taker_fee_bps, conservative_multiplier=1.1)
    calibrated = scenario_eval(cal, args.edge_bps, args.taker_fee_bps, conservative_multiplier=1.0)
    conservative = scenario_eval(cal, args.edge_bps * 0.75, args.taker_fee_bps * 1.25, conservative_multiplier=0.75)

    by_slug: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        slug = str(r.get("slug") or "")
        if slug:
            by_slug[slug].append(r)

    per_market = {}
    for slug, chunk in sorted(by_slug.items()):
        c = calibrate(chunk)
        per_market[slug] = {
            "calibration": c,
            "scenarios": {
                "optimistic_upper_bound": scenario_eval(c, args.edge_bps, args.taker_fee_bps, conservative_multiplier=1.1),
                "telemetry_calibrated_estimate": scenario_eval(c, args.edge_bps, args.taker_fee_bps, conservative_multiplier=1.0),
                "hard_conservative_lower_bound": scenario_eval(c, args.edge_bps * 0.75, args.taker_fee_bps * 1.25, conservative_multiplier=0.75),
            },
        }

    out = {
        "calibration": cal,
        "per_market": per_market,
        "scenarios": {
            "optimistic_upper_bound": optimistic,
            "telemetry_calibrated_estimate": calibrated,
            "hard_conservative_lower_bound": conservative,
        },
        "notes": [
            "touch_observed and fill_inferred are separated; fill inference is stricter than a single touch.",
            "queue priority, partial fills, and hidden liquidity remain unknowable in dry-run telemetry.",
        ],
    }

    with open(args.out_json, "w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=2)

    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
