#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from bisect import bisect_left
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from statistics import mean
from typing import Any, Optional


def fv(v: str | None) -> float | None:
    if v in ("", "None", None):
        return None
    return float(v)


def bv(v: str | None) -> bool | None:
    if v in ("", "None", None):
        return None
    return v == "True"


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    vals = sorted(values)
    idx = min(len(vals) - 1, int(len(vals) * p))
    return vals[idx]


def corr(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 3 or len(xs) != len(ys):
        return None
    mx = mean(xs)
    my = mean(ys)
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    var_x = sum((x - mx) ** 2 for x in xs)
    var_y = sum((y - my) ** 2 for y in ys)
    denom = (var_x * var_y) ** 0.5
    if denom <= 0:
        return None
    return cov / denom


def regression_slope(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 2 or len(xs) != len(ys):
        return None
    mx = mean(xs)
    my = mean(ys)
    var_x = sum((x - mx) ** 2 for x in xs)
    if var_x <= 0:
        return None
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    return cov / var_x


@dataclass
class SnapshotRow:
    ts: datetime
    ts_utc: str
    slug: str
    btc_price: float | None
    yes_mid: float | None
    yes_best_bid: float | None
    yes_best_ask: float | None
    no_mid: float | None
    no_best_bid: float | None
    no_best_ask: float | None
    yes_spread: float | None
    secs_since_last_market_msg: float | None
    secs_since_last_btc_update: float | None
    secs_since_last_yes_quote_update: float | None
    secs_since_last_no_quote_update: float | None


@dataclass
class SignalConfig:
    lookback_steps: int
    btc_move_threshold_bps: float
    pm_underreaction_cents: float
    hold_steps: int


def load_snapshots(path: str) -> dict[str, list[SnapshotRow]]:
    by_slug: dict[str, list[SnapshotRow]] = defaultdict(list)
    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            ts_utc = str(row["ts_utc"])
            snap = SnapshotRow(
                ts=datetime.fromisoformat(ts_utc),
                ts_utc=ts_utc,
                slug=str(row["slug"]),
                btc_price=fv(row.get("btc_price")),
                yes_mid=fv(row.get("yes_mid")),
                yes_best_bid=fv(row.get("yes_best_bid")),
                yes_best_ask=fv(row.get("yes_best_ask")),
                no_mid=fv(row.get("no_mid")),
                no_best_bid=fv(row.get("no_best_bid")),
                no_best_ask=fv(row.get("no_best_ask")),
                yes_spread=fv(row.get("yes_spread")),
                secs_since_last_market_msg=fv(row.get("secs_since_last_market_msg")),
                secs_since_last_btc_update=fv(row.get("secs_since_last_btc_update")),
                secs_since_last_yes_quote_update=fv(row.get("secs_since_last_yes_quote_update")),
                secs_since_last_no_quote_update=fv(row.get("secs_since_last_no_quote_update")),
            )
            by_slug[snap.slug].append(snap)

    for rows in by_slug.values():
        rows.sort(key=lambda r: r.ts)

    return by_slug


def load_telemetry(path: str) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def relationship_summary(by_slug: dict[str, list[SnapshotRow]]) -> dict[str, Any]:
    global_x: list[float] = []
    global_y: list[float] = []
    per_market: dict[str, Any] = {}

    for slug, rows in by_slug.items():
        anchor = next((r.btc_price for r in rows if r.btc_price is not None), None)
        if anchor is None:
            continue

        xs: list[float] = []
        ys: list[float] = []
        for row in rows:
            if row.btc_price is None or row.yes_mid is None:
                continue
            xs.append((row.btc_price - anchor) / anchor * 10000.0)
            ys.append(row.yes_mid * 100.0)

        market_corr = corr(xs, ys)
        slope = regression_slope(xs, ys)
        per_market[slug] = {
            "sample_rows": len(xs),
            "corr_yes_mid_cents_vs_btc_delta_from_open_bps": market_corr,
            "r2": (market_corr * market_corr) if market_corr is not None else None,
            "slope_yes_mid_cents_per_btc_bps": slope,
            "anchor_btc_price": anchor,
            "yes_mid_cents_min": min(ys) if ys else None,
            "yes_mid_cents_max": max(ys) if ys else None,
        }
        global_x.extend(xs)
        global_y.extend(ys)

    global_corr = corr(global_x, global_y)
    return {
        "global": {
            "sample_rows": len(global_x),
            "corr_yes_mid_cents_vs_btc_delta_from_open_bps": global_corr,
            "r2": (global_corr * global_corr) if global_corr is not None else None,
            "slope_yes_mid_cents_per_btc_bps": regression_slope(global_x, global_y),
        },
        "per_market": per_market,
    }


def event_study(
    by_slug: dict[str, list[SnapshotRow]],
    lookback_steps: int,
    threshold_bps: float,
    underreaction_cents: float,
    horizons_steps: list[int],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    events: list[dict[str, Any]] = []

    for slug, rows in by_slug.items():
        for i in range(lookback_steps, len(rows) - max(horizons_steps, default=0)):
            start = rows[i - lookback_steps]
            now = rows[i]
            if start.btc_price is None or now.btc_price is None or start.yes_mid is None or now.yes_mid is None:
                continue

            btc_move_bps = (now.btc_price - start.btc_price) / start.btc_price * 10000.0
            pm_move_cents = (now.yes_mid - start.yes_mid) * 100.0

            direction: Optional[int] = None
            side: Optional[str] = None
            if btc_move_bps >= threshold_bps and pm_move_cents <= underreaction_cents:
                direction = 1
                side = "up"
            elif btc_move_bps <= -threshold_bps and pm_move_cents >= -underreaction_cents:
                direction = -1
                side = "down"
            if direction is None or side is None:
                continue

            future_moves: dict[str, float] = {}
            for horizon in horizons_steps:
                later = rows[i + horizon]
                if later.yes_mid is None or now.yes_mid is None:
                    continue
                future_moves[f"{horizon}_steps"] = direction * (later.yes_mid - now.yes_mid) * 100.0

            events.append(
                {
                    "slug": slug,
                    "ts_utc": now.ts_utc,
                    "direction": side,
                    "btc_move_bps": btc_move_bps,
                    "pm_move_cents_same_window": pm_move_cents,
                    **future_moves,
                }
            )

    horizons_summary: dict[str, Any] = {}
    for horizon in horizons_steps:
        key = f"{horizon}_steps"
        vals = [e[key] for e in events if key in e]
        horizons_summary[key] = {
            "seconds": horizon * 0.2,
            "mean_signed_catchup_cents": mean(vals) if vals else None,
            "median_signed_catchup_cents": percentile(vals, 0.5),
            "positive_catchup_fraction": (sum(1 for v in vals if v > 0) / len(vals)) if vals else None,
        }

    best_horizon_key = None
    best_mean = None
    for key, payload in horizons_summary.items():
        val = payload["mean_signed_catchup_cents"]
        if val is None:
            continue
        if best_mean is None or val > best_mean:
            best_mean = val
            best_horizon_key = key

    summary = {
        "event_count": len(events),
        "lookback_seconds": lookback_steps * 0.2,
        "btc_move_threshold_bps": threshold_bps,
        "pm_underreaction_cents": underreaction_cents,
        "horizons": horizons_summary,
        "estimated_lag_seconds_from_max_mean_catchup": horizons_summary.get(best_horizon_key, {}).get("seconds"),
    }
    return summary, events


def classify_signal(rows: list[SnapshotRow], idx: int, config: SignalConfig) -> str | None:
    start = rows[idx - config.lookback_steps]
    now = rows[idx]
    if start.btc_price is None or now.btc_price is None or start.yes_mid is None or now.yes_mid is None:
        return None

    btc_move_bps = (now.btc_price - start.btc_price) / start.btc_price * 10000.0
    pm_move_cents = (now.yes_mid - start.yes_mid) * 100.0

    if btc_move_bps >= config.btc_move_threshold_bps and pm_move_cents <= config.pm_underreaction_cents:
        return "up"
    if btc_move_bps <= -config.btc_move_threshold_bps and pm_move_cents >= -config.pm_underreaction_cents:
        return "down"
    return None


def trade_prices(row: SnapshotRow, side: str) -> tuple[float | None, float | None]:
    if side == "up":
        return row.yes_best_bid, row.yes_best_ask
    return row.no_best_bid, row.no_best_ask


def taker_taker_eval(
    by_slug: dict[str, list[SnapshotRow]],
    config: SignalConfig,
    taker_fee_bps: float,
) -> dict[str, Any]:
    trades: list[dict[str, Any]] = []
    fee_rate = taker_fee_bps / 10000.0

    for slug, rows in by_slug.items():
        i = config.lookback_steps
        while i < len(rows) - config.hold_steps:
            side = classify_signal(rows, i, config)
            if side is None:
                i += 1
                continue

            now = rows[i]
            later = rows[i + config.hold_steps]
            entry_bid, entry_ask = trade_prices(now, side)
            exit_bid, _ = trade_prices(later, side)
            if entry_ask is None or exit_bid is None:
                i += 1
                continue

            gross = exit_bid - entry_ask
            net = gross - fee_rate * entry_ask - fee_rate * exit_bid
            trades.append(
                {
                    "slug": slug,
                    "ts_utc": now.ts_utc,
                    "direction": side,
                    "gross_cents": gross * 100.0,
                    "net_cents": net * 100.0,
                }
            )
            i += config.hold_steps

    net_cents = [t["net_cents"] for t in trades]
    gross_cents = [t["gross_cents"] for t in trades]
    return {
        "config": {
            "lookback_seconds": config.lookback_steps * 0.2,
            "btc_move_threshold_bps": config.btc_move_threshold_bps,
            "pm_underreaction_cents": config.pm_underreaction_cents,
            "hold_seconds": config.hold_steps * 0.2,
        },
        "trade_count": len(trades),
        "win_rate": (sum(1 for x in net_cents if x > 0) / len(net_cents)) if net_cents else None,
        "mean_gross_cents": mean(gross_cents) if gross_cents else None,
        "mean_net_cents": mean(net_cents) if net_cents else None,
        "median_net_cents": percentile(net_cents, 0.5),
        "mean_net_bps_of_notional": (mean(net_cents) * 100.0) if net_cents else None,
    }


def infer_maker_fill(
    rows: list[SnapshotRow],
    start_idx: int,
    side: str,
    ttl_steps: int,
    min_touch_observations: int,
    min_touch_persistence_ms: int,
    max_touch_staleness_s: float,
    max_touch_btc_staleness_s: float,
) -> tuple[int, float] | None:
    bid, _ = trade_prices(rows[start_idx], side)
    if bid is None:
        return None

    touch_start: int | None = None
    touch_end: int | None = None
    touch_count = 0

    for j in range(start_idx, min(len(rows), start_idx + ttl_steps + 1)):
        _, ask = trade_prices(rows[j], side)
        if ask is not None and ask <= bid:
            if touch_start is None:
                touch_start = j
            touch_end = j
            touch_count += 1
        elif touch_start is not None:
            break

    if touch_start is None or touch_end is None:
        return None

    touch_row = rows[touch_start]
    quote_stale = touch_row.secs_since_last_yes_quote_update if side == "up" else touch_row.secs_since_last_no_quote_update
    stale_ok = (
        (quote_stale is None or quote_stale <= max_touch_staleness_s)
        and (touch_row.secs_since_last_market_msg is None or touch_row.secs_since_last_market_msg <= max_touch_staleness_s)
        and (touch_row.secs_since_last_btc_update is None or touch_row.secs_since_last_btc_update <= max_touch_btc_staleness_s)
    )
    persistence_ms = int((rows[touch_end].ts - rows[touch_start].ts).total_seconds() * 1000)
    if touch_count >= min_touch_observations and persistence_ms >= min_touch_persistence_ms and stale_ok:
        return touch_start, bid
    return None


def maker_taker_eval(
    by_slug: dict[str, list[SnapshotRow]],
    config: SignalConfig,
    taker_fee_bps: float,
    maker_ttl_s: float,
    min_touch_observations: int,
    min_touch_persistence_ms: int,
    max_touch_staleness_s: float,
    max_touch_btc_staleness_s: float,
) -> dict[str, Any]:
    signals = 0
    fills = 0
    per_signal_cents: list[float] = []
    per_fill_cents: list[float] = []
    fee_rate = taker_fee_bps / 10000.0
    ttl_steps = max(1, int(round(maker_ttl_s / 0.2)))

    for rows in by_slug.values():
        i = config.lookback_steps
        while i < len(rows) - max(config.hold_steps, ttl_steps):
            side = classify_signal(rows, i, config)
            if side is None:
                i += 1
                continue

            signals += 1
            pnl_cents = 0.0
            fill = infer_maker_fill(
                rows=rows,
                start_idx=i,
                side=side,
                ttl_steps=ttl_steps,
                min_touch_observations=min_touch_observations,
                min_touch_persistence_ms=min_touch_persistence_ms,
                max_touch_staleness_s=max_touch_staleness_s,
                max_touch_btc_staleness_s=max_touch_btc_staleness_s,
            )
            if fill is not None:
                fill_idx, entry_bid = fill
                exit_idx = fill_idx + config.hold_steps
                if exit_idx < len(rows):
                    exit_bid, _ = trade_prices(rows[exit_idx], side)
                    if exit_bid is not None:
                        fills += 1
                        pnl_cents = ((exit_bid - entry_bid) - fee_rate * exit_bid) * 100.0
                        per_fill_cents.append(pnl_cents)
            per_signal_cents.append(pnl_cents)
            i += config.hold_steps

    return {
        "config": {
            "lookback_seconds": config.lookback_steps * 0.2,
            "btc_move_threshold_bps": config.btc_move_threshold_bps,
            "pm_underreaction_cents": config.pm_underreaction_cents,
            "hold_seconds": config.hold_steps * 0.2,
            "maker_ttl_seconds": maker_ttl_s,
        },
        "signal_count": signals,
        "fill_count": fills,
        "fill_rate": (fills / signals) if signals else None,
        "mean_net_cents_per_signal": mean(per_signal_cents) if per_signal_cents else None,
        "mean_net_bps_of_notional_per_signal": (mean(per_signal_cents) * 100.0) if per_signal_cents else None,
        "mean_net_cents_per_fill": mean(per_fill_cents) if per_fill_cents else None,
        "mean_net_bps_of_notional_per_fill": (mean(per_fill_cents) * 100.0) if per_fill_cents else None,
    }


def choose_best(results: list[dict[str, Any]], count_key: str, metric_key: str, min_count: int) -> dict[str, Any] | None:
    eligible = [r for r in results if (r.get(count_key) or 0) >= min_count and r.get(metric_key) is not None]
    if not eligible:
        return None
    return max(eligible, key=lambda r: r[metric_key])


def strategy_grid(
    by_slug: dict[str, list[SnapshotRow]],
    taker_fee_bps: float,
    maker_ttl_s: float,
    min_touch_observations: int,
    min_touch_persistence_ms: int,
    max_touch_staleness_s: float,
    max_touch_btc_staleness_s: float,
) -> dict[str, Any]:
    taker_results: list[dict[str, Any]] = []
    hybrid_results: list[dict[str, Any]] = []

    configs = [
        SignalConfig(lookback_steps=2, btc_move_threshold_bps=0.3, pm_underreaction_cents=0.5, hold_steps=10),
        SignalConfig(lookback_steps=3, btc_move_threshold_bps=0.5, pm_underreaction_cents=1.0, hold_steps=10),
        SignalConfig(lookback_steps=5, btc_move_threshold_bps=1.0, pm_underreaction_cents=0.5, hold_steps=10),
        SignalConfig(lookback_steps=5, btc_move_threshold_bps=1.5, pm_underreaction_cents=0.5, hold_steps=10),
        SignalConfig(lookback_steps=5, btc_move_threshold_bps=0.75, pm_underreaction_cents=1.0, hold_steps=5),
    ]

    for config in configs:
        taker_results.append(taker_taker_eval(by_slug, config, taker_fee_bps))
        hybrid_results.append(
            maker_taker_eval(
                by_slug=by_slug,
                config=config,
                taker_fee_bps=taker_fee_bps,
                maker_ttl_s=maker_ttl_s,
                min_touch_observations=min_touch_observations,
                min_touch_persistence_ms=min_touch_persistence_ms,
                max_touch_staleness_s=max_touch_staleness_s,
                max_touch_btc_staleness_s=max_touch_btc_staleness_s,
            )
        )

    return {
        "taker_taker": {
            "tested": taker_results,
            "best_any_sample": choose_best(taker_results, "trade_count", "mean_net_cents", min_count=1),
            "best_min_10_trades": choose_best(taker_results, "trade_count", "mean_net_cents", min_count=10),
            "best_min_25_trades": choose_best(taker_results, "trade_count", "mean_net_cents", min_count=25),
        },
        "maker_taker_hybrid": {
            "tested": hybrid_results,
            "best_any_sample": choose_best(hybrid_results, "signal_count", "mean_net_cents_per_signal", min_count=1),
            "best_min_25_signals": choose_best(hybrid_results, "signal_count", "mean_net_cents_per_signal", min_count=25),
        },
    }


def maker_safety_by_vol(
    by_slug: dict[str, list[SnapshotRow]],
    telemetry_rows: list[dict[str, Any]] | None,
) -> dict[str, Any] | None:
    if not telemetry_rows:
        return None

    ts_index = {slug: [row.ts for row in rows] for slug, rows in by_slug.items()}
    for rows in by_slug.values():
        for i, row in enumerate(rows):
            if i >= 5 and rows[i - 5].btc_price is not None and row.btc_price is not None:
                setattr(row, "btc_1s_abs_move_bps", abs((row.btc_price - rows[i - 5].btc_price) / rows[i - 5].btc_price * 10000.0))
            else:
                setattr(row, "btc_1s_abs_move_bps", None)

    def nearest_snapshot(slug: str, ts: datetime) -> SnapshotRow | None:
        rows = by_slug.get(slug)
        if not rows:
            return None
        idx = bisect_left(ts_index[slug], ts)
        if idx >= len(rows):
            idx = len(rows) - 1
        return rows[idx]

    enriched: list[dict[str, Any]] = []
    vol_values: list[float] = []
    for row in telemetry_rows:
        slug = str(row.get("slug") or "")
        ts_raw = row.get("quote_ts_utc")
        if not slug or not ts_raw:
            continue
        snap = nearest_snapshot(slug, datetime.fromisoformat(str(ts_raw)))
        if snap is None:
            continue
        vol = getattr(snap, "btc_1s_abs_move_bps", None)
        if vol is not None:
            vol_values.append(vol)
        enriched.append({**row, "btc_1s_abs_move_bps": vol, "yes_spread": snap.yes_spread})

    if not vol_values:
        return None

    q1 = percentile(vol_values, 0.25)
    q3 = percentile(vol_values, 0.75)

    def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
        fills = [r for r in rows if bv(r.get("fill_inferred")) is True]
        exits = [r for r in fills if bv(r.get("exit_opportunity_observed")) is True]
        forced = [r for r in fills if bv(r.get("forced_taker_exit")) is True]
        adverse = [fv(r.get("adverse_move_bps_post_fill")) for r in fills]
        adverse = [x for x in adverse if x is not None]
        return {
            "quotes": len(rows),
            "fill_rate": (len(fills) / len(rows)) if rows else None,
            "passive_exit_given_fill": (len(exits) / len(fills)) if fills else None,
            "forced_taker_given_fill": (len(forced) / len(fills)) if fills else None,
            "mean_adverse_bps_post_fill": mean(adverse) if adverse else None,
        }

    low = [r for r in enriched if r["btc_1s_abs_move_bps"] is not None and q1 is not None and r["btc_1s_abs_move_bps"] < q1]
    mid = [r for r in enriched if r["btc_1s_abs_move_bps"] is not None and q1 is not None and q3 is not None and q1 <= r["btc_1s_abs_move_bps"] < q3]
    high = [r for r in enriched if r["btc_1s_abs_move_bps"] is not None and q3 is not None and r["btc_1s_abs_move_bps"] >= q3]
    low_tight = [r for r in low if fv(r.get("yes_spread")) is not None and fv(r.get("yes_spread")) <= 0.01]

    return {
        "btc_1s_abs_move_bps_quartiles": {"q25": q1, "q75": q3},
        "buckets": {
            "low_volatility": summarize(low),
            "mid_volatility": summarize(mid),
            "high_volatility": summarize(high),
            "low_volatility_tight_spread": summarize(low_tight),
        },
    }


def write_events_csv(path: str, events: list[dict[str, Any]]) -> None:
    if not events:
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write("")
        return

    fields = list(events[0].keys())
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in events:
            writer.writerow(row)


def main() -> int:
    ap = argparse.ArgumentParser(description="Analyze BTC/Polymarket lag and directional microstructure from real snapshot data")
    ap.add_argument("--snapshots", required=True)
    ap.add_argument("--telemetry", default=None, help="Optional telemetry CSV for maker-safety segmentation")
    ap.add_argument("--out-json", default="btc_microstructure_analysis.json")
    ap.add_argument("--events-csv", default="btc_underreaction_events.csv")
    ap.add_argument("--taker-fee-bps", type=float, default=8.0)
    ap.add_argument("--maker-ttl-s", type=float, default=2.0)
    ap.add_argument("--min-touch-observations", type=int, default=2)
    ap.add_argument("--min-touch-persistence-ms", type=int, default=300)
    ap.add_argument("--max-touch-staleness-s", type=float, default=0.6)
    ap.add_argument("--max-touch-btc-staleness-s", type=float, default=2.0)
    args = ap.parse_args()

    by_slug = load_snapshots(args.snapshots)
    telemetry_rows = load_telemetry(args.telemetry) if args.telemetry else None

    abs_1s_btc_moves: list[float] = []
    for rows in by_slug.values():
        for i in range(5, len(rows)):
            start = rows[i - 5]
            now = rows[i]
            if start.btc_price is not None and now.btc_price is not None:
                abs_1s_btc_moves.append(abs((now.btc_price - start.btc_price) / start.btc_price * 10000.0))

    p90 = percentile(abs_1s_btc_moves, 0.90) or 0.0
    p95 = percentile(abs_1s_btc_moves, 0.95) or 0.0
    p99 = percentile(abs_1s_btc_moves, 0.99) or 0.0

    horizons = [1, 2, 3, 5, 8, 10]
    event_90, _ = event_study(by_slug, lookback_steps=5, threshold_bps=p90, underreaction_cents=0.5, horizons_steps=horizons)
    event_95, events_95 = event_study(by_slug, lookback_steps=5, threshold_bps=p95, underreaction_cents=0.5, horizons_steps=horizons)
    event_99, events_99 = event_study(by_slug, lookback_steps=5, threshold_bps=p99, underreaction_cents=0.5, horizons_steps=horizons)

    top_events = sorted(
        events_99,
        key=lambda x: abs(float(x["btc_move_bps"])),
        reverse=True,
    )
    write_events_csv(args.events_csv, top_events[:50])

    grid = strategy_grid(
        by_slug=by_slug,
        taker_fee_bps=args.taker_fee_bps,
        maker_ttl_s=args.maker_ttl_s,
        min_touch_observations=args.min_touch_observations,
        min_touch_persistence_ms=args.min_touch_persistence_ms,
        max_touch_staleness_s=args.max_touch_staleness_s,
        max_touch_btc_staleness_s=args.max_touch_btc_staleness_s,
    )

    report = {
        "relationship": relationship_summary(by_slug),
        "lag_event_studies": {
            "p90_abs_1s_btc_move": event_90,
            "p95_abs_1s_btc_move": event_95,
            "p99_abs_1s_btc_move": event_99,
        },
        "strategy_comparison": grid,
        "maker_safety_by_volatility": maker_safety_by_vol(by_slug, telemetry_rows),
        "interpretation": {
            "directional_edge_exists": bool(
                (grid["taker_taker"]["best_min_25_trades"] or {}).get("mean_net_cents", -1.0) > 0.0
                or (grid["taker_taker"]["best_min_10_trades"] or {}).get("mean_net_cents", -1.0) > 0.0
            ),
            "hybrid_beats_pure_maker_negative_68bps": bool(
                ((grid["maker_taker_hybrid"]["best_min_25_signals"] or {}).get("mean_net_bps_of_notional_per_signal") or -10_000.0) > -68.03406453784665
            ),
        },
    }

    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"Global BTC/YES-mid corr vs open-anchor move: {report['relationship']['global']['corr_yes_mid_cents_vs_btc_delta_from_open_bps']:.3f}")
    print(f"P95 lag estimate from event study: {event_95['estimated_lag_seconds_from_max_mean_catchup']}")
    best_25 = grid["taker_taker"]["best_min_25_trades"]
    if best_25:
        print(
            "Best taker+taker with >=25 trades: "
            f"net={best_25['mean_net_cents']:.3f}c/trade ({best_25['mean_net_bps_of_notional']:.1f} bps notional)"
        )
    best_10 = grid["taker_taker"]["best_min_10_trades"]
    if best_10:
        print(
            "Best taker+taker with >=10 trades: "
            f"net={best_10['mean_net_cents']:.3f}c/trade ({best_10['mean_net_bps_of_notional']:.1f} bps notional)"
        )
    hybrid_25 = grid["maker_taker_hybrid"]["best_min_25_signals"]
    if hybrid_25:
        print(
            "Best maker+taker hybrid with >=25 signals: "
            f"net={hybrid_25['mean_net_cents_per_signal']:.3f}c/signal "
            f"({hybrid_25['mean_net_bps_of_notional_per_signal']:.1f} bps notional)"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
