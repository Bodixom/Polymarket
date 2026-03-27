#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
from statistics import median
from typing import Any


def f(v: str) -> float | None:
    if v in ("", "None", None):
        return None
    return float(v)


def b(v: str) -> bool | None:
    if v in ("", "None", None):
        return None
    return v == "True"


def load_rows(path: str) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def calibrate(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    fills = [r for r in rows if b(r.get("fill_opportunity")) is True]
    exits = [r for r in fills if b(r.get("passive_exit_opportunity")) is True]
    forced = [r for r in fills if b(r.get("forced_taker_exit")) is True]
    hold_ms = [f(r.get("hold_ms")) for r in exits]
    hold_ms = [x for x in hold_ms if x is not None]
    adverse = [f(r.get("adverse_move_bps_post_fill")) for r in fills]
    adverse = [x for x in adverse if x is not None]

    return {
        "sample_quotes": total,
        "passive_entry_fill_prob": (len(fills) / total) if total else 0.0,
        "passive_exit_prob_given_fill": (len(exits) / len(fills)) if fills else 0.0,
        "forced_taker_exit_frac_given_fill": (len(forced) / len(fills)) if fills else 0.0,
        "median_hold_ms": median(hold_ms) if hold_ms else None,
        "adverse_bps_mean": (sum(adverse) / len(adverse)) if adverse else None,
        "adverse_bps_p95": sorted(adverse)[math.floor(0.95 * (len(adverse) - 1))] if adverse else None,
    }


def scenario_eval(cal: dict[str, Any], edge_bps: float, taker_fee_bps: float, conservative_multiplier: float = 1.0) -> dict[str, float]:
    p_entry = cal["passive_entry_fill_prob"] * conservative_multiplier
    p_exit = cal["passive_exit_prob_given_fill"] * conservative_multiplier
    forced = cal["forced_taker_exit_frac_given_fill"] / max(conservative_multiplier, 1e-9)
    adverse = (cal["adverse_bps_mean"] or 0.0) / max(conservative_multiplier, 1e-9)

    p_entry = max(0.0, min(1.0, p_entry))
    p_exit = max(0.0, min(1.0, p_exit))
    forced = max(0.0, min(1.0, forced))

    maker_path_ev = edge_bps - adverse
    taker_path_ev = edge_bps - adverse - taker_fee_bps

    ev_per_quote = p_entry * (p_exit * maker_path_ev + (1 - p_exit) * taker_path_ev)
    maker_only_completion = p_entry * p_exit

    return {
        "ev_bps_per_quote": ev_per_quote,
        "maker_only_completion_prob": maker_only_completion,
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

    out = {
        "calibration": cal,
        "scenarios": {
            "optimistic_upper_bound": optimistic,
            "telemetry_calibrated_estimate": calibrated,
            "hard_conservative_lower_bound": conservative,
        },
        "notes": [
            "Dry-run observed-touch telemetry is still a proxy for true queue-priority fills.",
            "Do not interpret output as guaranteed profitability.",
        ],
    }

    with open(args.out_json, "w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=2)

    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
