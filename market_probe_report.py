#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from typing import Any


def load_csv(path: str) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def f(v: str | None) -> float:
    if v in ("", "None", None):
        return 0.0
    return float(v)


def recommendation(
    quality: float,
    entry: float,
    exit_given_fill: float,
    forced_taker_frac: float,
    adverse_mean_bps: float,
    ev_bps_per_quote: float,
    sample_quotes: int,
) -> str:
    if (
        sample_quotes < 150
        or entry < 0.20
        or exit_given_fill < 0.30
        or forced_taker_frac > 0.70
        or ev_bps_per_quote <= -60.0
    ):
        return "ignore"
    if (
        quality >= 60
        and sample_quotes >= 1000
        and entry >= 0.25
        and exit_given_fill >= 0.55
        and forced_taker_frac <= 0.45
        and adverse_mean_bps <= 10.0
        and ev_bps_per_quote > 0
    ):
        return "candidate_for_tiny_live_money_probe"
    return "monitor"


def main() -> int:
    ap = argparse.ArgumentParser(description="Combine market quality + telemetry calibration + backtest tiers")
    ap.add_argument("--quality-csv", required=True)
    ap.add_argument("--calibration-json", required=True)
    ap.add_argument("--out-csv", default="market_probe_report.csv")
    ap.add_argument("--out-json", default="market_probe_report.json")
    args = ap.parse_args()

    quality = {r["slug"]: r for r in load_csv(args.quality_csv)}
    with open(args.calibration_json, "r", encoding="utf-8") as fjson:
        calib = json.load(fjson)

    per_market = calib.get("per_market") or {}
    rows: list[dict[str, Any]] = []
    for slug, pm in per_market.items():
        q = quality.get(slug, {})
        c = pm.get("calibration") or {}
        scenario = (pm.get("scenarios") or {}).get("telemetry_calibrated_estimate") or {}

        quality_score = f(q.get("quality_score"))
        entry = f(c.get("passive_entry_fill_inferred_freq"))
        exit_given_fill = f(c.get("passive_exit_opportunity_freq_given_fill"))
        forced_taker_frac = f(c.get("forced_taker_exit_frac_given_fill"))
        adverse_mean_bps = f((c.get("adverse_bps_after_touch_or_fill") or {}).get("mean"))
        ev = f(scenario.get("ev_bps_per_quote"))
        n = int(c.get("sample_quotes") or 0)

        rows.append(
            {
                "slug": slug,
                "quality_score": quality_score,
                "sample_quotes": n,
                "passive_entry_touch_freq": c.get("passive_entry_touch_freq"),
                "passive_entry_fill_inferred_freq": c.get("passive_entry_fill_inferred_freq"),
                "passive_exit_opportunity_freq_given_fill": c.get("passive_exit_opportunity_freq_given_fill"),
                "forced_taker_exit_frac_given_fill": c.get("forced_taker_exit_frac_given_fill"),
                "time_to_touch_median_ms": (c.get("time_to_touch_ms") or {}).get("median"),
                "hold_median_ms": (c.get("hold_ms") or {}).get("median"),
                "adverse_mean_bps": (c.get("adverse_bps_after_touch_or_fill") or {}).get("mean"),
                "ev_bps_per_quote": ev,
                "maker_only_completion_prob": scenario.get("maker_only_completion_prob"),
                "estimated_forced_taker_rate": scenario.get("estimated_forced_taker_rate"),
                "recommendation": recommendation(
                    quality_score,
                    entry,
                    exit_given_fill,
                    forced_taker_frac,
                    adverse_mean_bps,
                    ev,
                    n,
                ),
            }
        )

    rank = {"candidate_for_tiny_live_money_probe": 2, "monitor": 1, "ignore": 0}
    rows.sort(key=lambda x: (rank.get(x["recommendation"], -1), x["quality_score"], x["ev_bps_per_quote"]), reverse=True)

    fields = [
        "slug",
        "recommendation",
        "quality_score",
        "sample_quotes",
        "passive_entry_touch_freq",
        "passive_entry_fill_inferred_freq",
        "passive_exit_opportunity_freq_given_fill",
        "forced_taker_exit_frac_given_fill",
        "time_to_touch_median_ms",
        "hold_median_ms",
        "adverse_mean_bps",
        "ev_bps_per_quote",
        "maker_only_completion_prob",
        "estimated_forced_taker_rate",
    ]

    with open(args.out_csv, "w", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=fields)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    out = {"markets": rows, "global": calib.get("calibration"), "global_scenarios": calib.get("scenarios")}
    with open(args.out_json, "w", encoding="utf-8") as jout:
        json.dump(out, jout, indent=2)

    for r in rows:
        print(
            f"{r['slug']}: {r['recommendation']} quality={r['quality_score']:.1f} "
            f"entry={f(r['passive_entry_fill_inferred_freq']):.2f} exit={f(r['passive_exit_opportunity_freq_given_fill']):.2f} "
            f"ev={r['ev_bps_per_quote']:.2f}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
