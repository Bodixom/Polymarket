#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import signal
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Iterable, Optional

from polymarket_live_feed import LiveFeedAdapter, now_iso


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def safe_float(value: Any) -> Optional[float]:
    try:
        if value in ("", "None", None):
            return None
        return float(value)
    except Exception:
        return None


def pct(value: Optional[float]) -> str:
    if value is None:
        return "-"
    return f"{value * 100:.1f}%"


def price_text(value: Optional[float]) -> str:
    if value is None:
        return "-"
    return f"{value:.4f}"


def money(value: Optional[float]) -> str:
    if value is None:
        return "-"
    return f"{value:.2f}"


@dataclass
class StrategyDecision:
    action: str
    side: Optional[str]
    signal: str
    reason: str
    edge: Optional[float]
    probability_yes: Optional[float]
    confidence: float


@dataclass
class PaperPosition:
    trade_id: int
    market_id: str
    market_slug: str
    side: str
    entry_time: str
    entry_dt: Optional[datetime]
    entry_monotonic: float
    entry_price: float
    entry_mid_price: float
    size: float
    notional: float
    entry_fee: float
    entry_reason: str
    market_btc_open_price: Optional[float]


class StrategyEngine:
    def __init__(
        self,
        min_edge: float,
        near_expiry_window_s: float,
        max_hold_s: float,
        max_entries_per_market: int,
        cooldown_s: float,
    ) -> None:
        self.min_edge = min_edge
        self.near_expiry_window_s = near_expiry_window_s
        self.max_hold_s = max_hold_s
        self.max_entries_per_market = max_entries_per_market
        self.cooldown_s = cooldown_s

    def evaluate(
        self,
        snapshot: dict[str, Any],
        position: Optional[PaperPosition],
        mode: str,
        market_entries: int,
        seconds_since_last_exit: Optional[float],
    ) -> StrategyDecision:
        snapshot_dt = None
        try:
            snapshot_dt = parse_ts(str(snapshot.get("ts_utc")))
        except Exception:
            snapshot_dt = None

        if not snapshot.get("feed_is_fresh"):
            return StrategyDecision("wait", None, "FLAT", "feed_stale", None, None, 0.0)

        yes_price = safe_float(snapshot.get("inferred_yes_price"))
        no_price = safe_float(snapshot.get("inferred_no_price"))
        if yes_price is None or no_price is None:
            return StrategyDecision("wait", None, "FLAT", "prices_incomplete", None, None, 0.0)

        seconds_to_expiry = safe_float(snapshot.get("seconds_to_expiry"))
        if seconds_to_expiry is None or seconds_to_expiry <= 0:
            return StrategyDecision("wait", None, "FLAT", "market_expired", None, None, 0.0)

        if position is None and seconds_to_expiry > self.near_expiry_window_s:
            return StrategyDecision("wait", None, "FLAT", "outside_near_expiry_window", None, None, 0.0)

        if position is None and market_entries >= self.max_entries_per_market:
            return StrategyDecision("wait", None, "FLAT", "market_entry_limit", None, None, 0.0)

        if position is None and seconds_since_last_exit is not None and seconds_since_last_exit < self.cooldown_s:
            return StrategyDecision("wait", None, "FLAT", "cooldown", None, None, 0.0)

        yes_spread = safe_float(snapshot.get("yes_spread"))
        no_spread = safe_float(snapshot.get("no_spread"))
        widest_spread = max(x for x in [yes_spread, no_spread, 0.0] if x is not None)
        if position is None and widest_spread > 0.04:
            return StrategyDecision("wait", None, "FLAT", "spread_too_wide", None, None, 0.0)

        btc_open_move = safe_float(snapshot.get("btc_move_bps_from_open")) or 0.0
        btc_momentum = safe_float(snapshot.get("btc_momentum_bps_window")) or 0.0
        flow_imbalance = safe_float(snapshot.get("net_flow_imbalance")) or 0.0
        elapsed_fraction = safe_float(snapshot.get("elapsed_fraction"))
        elapsed_fraction = elapsed_fraction if elapsed_fraction is not None else 0.0

        open_weight = 0.35 + (0.45 * elapsed_fraction)
        momentum_weight = 0.45 - (0.20 * elapsed_fraction)
        flow_weight = 10.0
        composite_score = (btc_open_move * open_weight) + (btc_momentum * momentum_weight) + (flow_imbalance * flow_weight)

        probability_yes = clamp(0.5 + (math.tanh(composite_score / 22.0) * 0.30), 0.02, 0.98)
        edge_yes = probability_yes - yes_price
        edge_no = (1.0 - probability_yes) - no_price
        confidence = min(1.0, abs(composite_score) / 35.0)

        best_side = "YES" if edge_yes >= edge_no else "NO"
        best_edge = edge_yes if best_side == "YES" else edge_no

        if position is None:
            if best_edge < self.min_edge:
                return StrategyDecision("wait", None, "FLAT", "edge_below_threshold", best_edge, probability_yes, confidence)
            if best_side == "YES" and flow_imbalance < -0.25:
                return StrategyDecision("wait", None, "FLAT", "flow_conflicts_yes", best_edge, probability_yes, confidence)
            if best_side == "NO" and flow_imbalance > 0.25:
                return StrategyDecision("wait", None, "FLAT", "flow_conflicts_no", best_edge, probability_yes, confidence)
            return StrategyDecision("enter", best_side, f"LONG_{best_side}", "edge_confirmed", best_edge, probability_yes, confidence)

        if position.entry_dt is not None and snapshot_dt is not None:
            hold_seconds = max(0.0, (snapshot_dt - position.entry_dt).total_seconds())
        else:
            hold_seconds = time.monotonic() - position.entry_monotonic
        current_edge = edge_yes if position.side == "YES" else edge_no
        opposite_edge = edge_no if position.side == "YES" else edge_yes

        if mode == "early-exit":
            if hold_seconds >= self.max_hold_s:
                return StrategyDecision("exit", position.side, f"HOLD_{position.side}", "max_hold_reached", current_edge, probability_yes, confidence)
            if seconds_to_expiry <= self.near_expiry_window_s:
                return StrategyDecision("exit", position.side, f"HOLD_{position.side}", "near_expiry_exit", current_edge, probability_yes, confidence)
            if opposite_edge >= max(self.min_edge * 0.75, current_edge + 0.02):
                return StrategyDecision("exit", position.side, f"HOLD_{position.side}", "signal_reversal", current_edge, probability_yes, confidence)
            if current_edge <= -(self.min_edge * 0.5):
                return StrategyDecision("exit", position.side, f"HOLD_{position.side}", "edge_broken", current_edge, probability_yes, confidence)

        return StrategyDecision("hold", position.side, f"HOLD_{position.side}", "position_open", current_edge, probability_yes, confidence)


class PaperBroker:
    def __init__(self, bankroll: float, fee_bps: float, slippage_bps: float, fill_style: str) -> None:
        self.starting_bankroll = bankroll
        self.fee_bps = fee_bps
        self.slippage_bps = slippage_bps
        self.fill_style = fill_style
        self.position: Optional[PaperPosition] = None
        self.trade_seq = 0
        self.realized_pnl = 0.0
        self.total_fees = 0.0
        self.wins = 0
        self.losses = 0
        self.last_exit_monotonic: Optional[float] = None
        self.max_drawdown = 0.0
        self.peak_equity = bankroll
        self.market_stats: dict[str, dict[str, Any]] = defaultdict(lambda: {"trades": 0, "wins": 0, "losses": 0, "net_pnl": 0.0})

    def _current_price_fields(self, snapshot: dict[str, Any], side: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
        prefix = "yes" if side == "YES" else "no"
        bid = safe_float(snapshot.get(f"{prefix}_best_bid"))
        ask = safe_float(snapshot.get(f"{prefix}_best_ask"))
        mid = safe_float(snapshot.get(f"inferred_{prefix}_price"))
        return bid, ask, mid

    def _clamp_price(self, value: float) -> float:
        return clamp(value, 0.001, 0.999)

    def _entry_fill_price(self, snapshot: dict[str, Any], side: str) -> Optional[float]:
        bid, ask, mid = self._current_price_fields(snapshot, side)
        slip = self.slippage_bps / 10000.0
        if self.fill_style == "maker":
            base = bid if bid is not None else mid
            return self._clamp_price(base) if base is not None else None
        if self.fill_style == "taker":
            base = ask if ask is not None else mid
            return self._clamp_price(base * (1.0 + slip)) if base is not None else None
        base = mid
        return self._clamp_price(base * (1.0 + slip)) if base is not None else None

    def _exit_fill_price(self, snapshot: dict[str, Any], side: str, conservative: bool) -> Optional[float]:
        bid, ask, mid = self._current_price_fields(snapshot, side)
        slip = self.slippage_bps / 10000.0
        if self.fill_style == "maker" and not conservative:
            base = ask if ask is not None else mid
            return self._clamp_price(base) if base is not None else None
        if self.fill_style == "taker" or conservative:
            base = bid if bid is not None else mid
            return self._clamp_price(base * (1.0 - slip)) if base is not None else None
        base = mid
        return self._clamp_price(base * (1.0 - slip)) if base is not None else None

    def _fee(self, notional: float) -> float:
        return notional * (self.fee_bps / 10000.0)

    def enter(self, snapshot: dict[str, Any], side: str, notional: float, reason: str) -> dict[str, Any]:
        if self.position is not None:
            raise RuntimeError("Cannot open a second paper position")
        fill_price = self._entry_fill_price(snapshot, side)
        if fill_price is None or fill_price <= 0:
            raise RuntimeError("Cannot infer entry fill price")
        mid_price = safe_float(snapshot.get("inferred_yes_price" if side == "YES" else "inferred_no_price")) or fill_price
        size = notional / fill_price
        entry_fee = self._fee(notional)
        self.trade_seq += 1
        self.total_fees += entry_fee
        self.position = PaperPosition(
            trade_id=self.trade_seq,
            market_id=str(snapshot.get("slug") or ""),
            market_slug=str(snapshot.get("slug") or ""),
            side=side,
            entry_time=str(snapshot.get("ts_utc") or now_iso()),
            entry_dt=(parse_ts(str(snapshot.get("ts_utc"))) if snapshot.get("ts_utc") else None),
            entry_monotonic=time.monotonic(),
            entry_price=fill_price,
            entry_mid_price=mid_price,
            size=size,
            notional=notional,
            entry_fee=entry_fee,
            entry_reason=reason,
            market_btc_open_price=safe_float(snapshot.get("market_btc_open_price")),
        )
        return {
            "trade_id": self.position.trade_id,
            "market_id": self.position.market_id,
            "market_slug": self.position.market_slug,
            "side": side,
            "entry_time": self.position.entry_time,
            "entry_price": fill_price,
            "entry_mid_price": mid_price,
            "size": size,
            "notional": notional,
            "entry_fee": entry_fee,
            "entry_reason": reason,
        }

    def mark(self, snapshot: Optional[dict[str, Any]]) -> dict[str, Optional[float]]:
        if self.position is None or snapshot is None:
            equity = self.starting_bankroll + self.realized_pnl
            self.peak_equity = max(self.peak_equity, equity)
            self.max_drawdown = max(self.max_drawdown, self.peak_equity - equity)
            return {
                "mark_price_mid": None,
                "mark_price_exit": None,
                "unrealized_mid": 0.0,
                "unrealized_exit": 0.0,
                "running_equity": equity,
            }

        mid_price = safe_float(snapshot.get("inferred_yes_price" if self.position.side == "YES" else "inferred_no_price"))
        exit_price = self._exit_fill_price(snapshot, self.position.side, conservative=True)
        exit_fee = self._fee(self.position.size * exit_price) if exit_price is not None else 0.0

        unrealized_mid = None
        if mid_price is not None:
            unrealized_mid = (self.position.size * (mid_price - self.position.entry_price)) - self.position.entry_fee

        unrealized_exit = None
        if exit_price is not None:
            unrealized_exit = (self.position.size * (exit_price - self.position.entry_price)) - self.position.entry_fee - exit_fee

        running_equity = self.starting_bankroll + self.realized_pnl + (unrealized_exit or 0.0)
        self.peak_equity = max(self.peak_equity, running_equity)
        self.max_drawdown = max(self.max_drawdown, self.peak_equity - running_equity)
        return {
            "mark_price_mid": mid_price,
            "mark_price_exit": exit_price,
            "unrealized_mid": unrealized_mid,
            "unrealized_exit": unrealized_exit,
            "running_equity": running_equity,
        }

    def close(self, snapshot: dict[str, Any], reason: str, settlement_price: Optional[float] = None) -> dict[str, Any]:
        if self.position is None:
            raise RuntimeError("No open paper position to close")

        position = self.position
        exit_time = str(snapshot.get("ts_utc") or now_iso())
        exit_price = settlement_price if settlement_price is not None else self._exit_fill_price(snapshot, position.side, conservative=True)
        if exit_price is None:
            raise RuntimeError("Cannot infer exit fill price")

        exit_notional = position.size * exit_price
        exit_fee = 0.0 if settlement_price is not None else self._fee(exit_notional)
        fees_paid = position.entry_fee + exit_fee
        gross_pnl = position.size * (exit_price - position.entry_price)
        net_pnl = gross_pnl - fees_paid
        exit_dt = None
        try:
            exit_dt = parse_ts(exit_time)
        except Exception:
            exit_dt = None
        if position.entry_dt is not None and exit_dt is not None:
            hold_seconds = max(0.0, (exit_dt - position.entry_dt).total_seconds())
        else:
            hold_seconds = max(0.0, time.monotonic() - position.entry_monotonic)

        self.realized_pnl += net_pnl
        self.total_fees += exit_fee
        self.last_exit_monotonic = time.monotonic()
        self.position = None

        market_summary = self.market_stats[position.market_slug]
        market_summary["trades"] += 1
        market_summary["net_pnl"] += net_pnl
        if net_pnl > 0:
            self.wins += 1
            market_summary["wins"] += 1
        else:
            self.losses += 1
            market_summary["losses"] += 1

        running_equity = self.starting_bankroll + self.realized_pnl
        self.peak_equity = max(self.peak_equity, running_equity)
        self.max_drawdown = max(self.max_drawdown, self.peak_equity - running_equity)

        return {
            "trade_id": position.trade_id,
            "entry_time": position.entry_time,
            "exit_time": exit_time,
            "market_id": position.market_id,
            "market_slug": position.market_slug,
            "side": position.side,
            "entry_price": position.entry_price,
            "exit_price": exit_price,
            "size": position.size,
            "fees_paid": fees_paid,
            "gross_pnl": gross_pnl,
            "net_pnl": net_pnl,
            "hold_seconds": hold_seconds,
            "entry_reason": position.entry_reason,
            "exit_reason": reason,
        }

    def settle_expired(self, snapshot: dict[str, Any], reason: str, btc_end_price: Optional[float]) -> dict[str, Any]:
        if self.position is None:
            raise RuntimeError("No open paper position to settle")
        btc_open = self.position.market_btc_open_price
        if btc_open is None or btc_end_price is None:
            return self.close(snapshot, reason=reason)
        if btc_end_price > btc_open:
            payout_yes = 1.0
        elif btc_end_price < btc_open:
            payout_yes = 0.0
        else:
            payout_yes = 0.5
        settlement_price = payout_yes if self.position.side == "YES" else (1.0 - payout_yes)
        return self.close(snapshot, reason=reason, settlement_price=settlement_price)

    @property
    def closed_trades(self) -> int:
        return self.wins + self.losses


class TradeLogger:
    def __init__(self, run_dir: Path) -> None:
        self.run_dir = run_dir
        self.trade_rows: list[dict[str, Any]] = []
        self.event_rows: list[dict[str, Any]] = []
        self.equity_rows: list[dict[str, Any]] = []

        self.trade_fields = [
            "trade_id", "entry_time", "exit_time", "market_id", "market_slug", "side",
            "entry_price", "exit_price", "size", "fees_paid", "gross_pnl", "net_pnl",
            "hold_seconds", "entry_reason", "exit_reason",
        ]
        self.event_fields = ["ts_utc", "event", "market_id", "market_slug", "side", "signal", "reason", "details_json"]
        self.equity_fields = [
            "ts_utc", "market_slug", "position_side", "entry_price", "mark_price_mid", "mark_price_exit",
            "unrealized_mid", "unrealized_exit", "realized_pnl", "running_equity", "max_drawdown",
        ]

        self.trade_path = run_dir / "paper_trades.csv"
        self.event_path = run_dir / "paper_events.csv"
        self.equity_path = run_dir / "paper_equity.csv"
        self.flush_interval_s = 2.0
        now_monotonic = time.monotonic()
        self._trade_last_flush = now_monotonic
        self._event_last_flush = now_monotonic
        self._equity_last_flush = now_monotonic

        self._trade_handle, self._trade_writer = self._open_csv(self.trade_path, self.trade_fields)
        self._event_handle, self._event_writer = self._open_csv(self.event_path, self.event_fields)
        self._equity_handle, self._equity_writer = self._open_csv(self.equity_path, self.equity_fields)

    def log_trade(self, row: dict[str, Any]) -> None:
        self.trade_rows.append(row)
        self._trade_writer.writerow(row)
        self._flush_handle(self._trade_handle, "_trade_last_flush", force=True, fsync=True)

    def log_event(
        self,
        event: str,
        market_slug: Optional[str] = None,
        side: Optional[str] = None,
        signal: Optional[str] = None,
        reason: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        row = {
            "ts_utc": now_iso(),
            "event": event,
            "market_id": market_slug or "",
            "market_slug": market_slug or "",
            "side": side or "",
            "signal": signal or "",
            "reason": reason or "",
            "details_json": json.dumps(details or {}, sort_keys=True),
        }
        self.event_rows.append(row)
        self._event_writer.writerow(row)
        self._flush_handle(self._event_handle, "_event_last_flush")

    def log_equity(self, snapshot: Optional[dict[str, Any]], broker: PaperBroker, marks: dict[str, Optional[float]]) -> None:
        position = broker.position
        row = {
            "ts_utc": str((snapshot or {}).get("ts_utc") or now_iso()),
            "market_slug": str((snapshot or {}).get("slug") or ""),
            "position_side": position.side if position else "",
            "entry_price": position.entry_price if position else None,
            "mark_price_mid": marks.get("mark_price_mid"),
            "mark_price_exit": marks.get("mark_price_exit"),
            "unrealized_mid": marks.get("unrealized_mid"),
            "unrealized_exit": marks.get("unrealized_exit"),
            "realized_pnl": broker.realized_pnl,
            "running_equity": marks.get("running_equity"),
            "max_drawdown": broker.max_drawdown,
        }
        self.equity_rows.append(row)
        self._equity_writer.writerow(row)
        self._flush_handle(self._equity_handle, "_equity_last_flush")

    def flush(self, force_fsync: bool = False) -> None:
        self._flush_handle(self._trade_handle, "_trade_last_flush", force=True, fsync=force_fsync)
        self._flush_handle(self._event_handle, "_event_last_flush", force=True, fsync=force_fsync)
        self._flush_handle(self._equity_handle, "_equity_last_flush", force=True, fsync=force_fsync)

    def close(self) -> None:
        self.flush(force_fsync=True)
        for handle in (self._trade_handle, self._event_handle, self._equity_handle):
            handle.close()

    def _open_csv(self, path: Path, fields: list[str]) -> tuple[Any, csv.DictWriter]:
        handle = open(path, "w", newline="", encoding="utf-8", buffering=1)
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        handle.flush()
        return handle, writer

    def _flush_handle(self, handle: Any, last_flush_attr: str, force: bool = False, fsync: bool = False) -> None:
        now_monotonic = time.monotonic()
        last_flush = getattr(self, last_flush_attr)
        if not force and (now_monotonic - last_flush) < self.flush_interval_s:
            return
        handle.flush()
        if fsync:
            os.fsync(handle.fileno())
        setattr(self, last_flush_attr, now_monotonic)


class TerminalDashboard:
    def __init__(self, refresh_ms: int) -> None:
        self.refresh_ms = refresh_ms

    def render(
        self,
        snapshot: Optional[dict[str, Any]],
        decision: StrategyDecision,
        broker: PaperBroker,
        marks: dict[str, Optional[float]],
        mode: str,
        run_dir: Path,
    ) -> None:
        lines: list[str] = []
        lines.append("Polymarket BTC 5m Paper Trader")
        lines.append(f"Timestamp: {str((snapshot or {}).get('ts_utc') or now_iso())}")
        lines.append(f"Mode: {mode}  Refresh: {self.refresh_ms}ms  Run dir: {run_dir}")

        if snapshot is None:
            lines.append("Waiting for market data...")
            sys.stdout.write("\x1b[2J\x1b[H" + "\n".join(lines) + "\n")
            sys.stdout.flush()
            return

        lines.append(
            f"Market: {snapshot.get('slug', '-')}  YES token: {snapshot.get('yes_token_id', '-')}  NO token: {snapshot.get('no_token_id', '-')}"
        )
        lines.append(
            "Prices: "
            f"YES {price_text(safe_float(snapshot.get('inferred_yes_price')))} "
            f"(bid {price_text(safe_float(snapshot.get('yes_best_bid')))} ask {price_text(safe_float(snapshot.get('yes_best_ask')))})  "
            f"NO {price_text(safe_float(snapshot.get('inferred_no_price')))} "
            f"(bid {price_text(safe_float(snapshot.get('no_best_bid')))} ask {price_text(safe_float(snapshot.get('no_best_ask')))})"
        )
        lines.append(
            "Model: "
            f"signal {decision.signal}  action {decision.action}  reason {decision.reason}  "
            f"edge {price_text(decision.edge)}  p_yes {pct(decision.probability_yes)}"
        )
        lines.append(
            "BTC: "
            f"spot {money(safe_float(snapshot.get('btc_price')))}  "
            f"open_move {money(safe_float(snapshot.get('btc_move_bps_from_open')))}bps  "
            f"momentum {money(safe_float(snapshot.get('btc_momentum_bps_window')))}bps  "
            f"flow {price_text(safe_float(snapshot.get('net_flow_imbalance')))}  "
            f"t_exp {money(safe_float(snapshot.get('seconds_to_expiry')))}s  "
            f"fresh {snapshot.get('feed_is_fresh')}"
        )

        position = broker.position
        if position is None:
            lines.append("Position: flat")
        else:
            lines.append(
                "Position: "
                f"{position.side}  size {position.size:.2f}  entry {position.entry_price:.4f}  "
                f"mark_mid {price_text(marks.get('mark_price_mid'))}  mark_exit {price_text(marks.get('mark_price_exit'))}"
            )
            lines.append(
                "PnL: "
                f"unreal_mid {money(marks.get('unrealized_mid'))}  "
                f"unreal_exit {money(marks.get('unrealized_exit'))}  "
                f"realized {money(broker.realized_pnl)}"
            )

        trades = broker.closed_trades
        win_rate = (broker.wins / trades) if trades else 0.0
        lines.append(
            "Session: "
            f"trades {trades}  wins {broker.wins}  losses {broker.losses}  "
            f"win_rate {pct(win_rate)}  max_dd {money(broker.max_drawdown)}  "
            f"equity {money(marks.get('running_equity'))}"
        )

        lines.append("Per-market summary:")
        recent_items = list(broker.market_stats.items())[-5:]
        if not recent_items:
            lines.append("  no closed trades yet")
        else:
            for slug, stats in recent_items:
                total = int(stats["trades"])
                wr = (stats["wins"] / total) if total else 0.0
                lines.append(f"  {slug}: trades={total} pnl={stats['net_pnl']:.2f} win_rate={wr * 100:.1f}%")

        sys.stdout.write("\x1b[2J\x1b[H" + "\n".join(lines) + "\n")
        sys.stdout.flush()


class ReplaySnapshotSource:
    def __init__(self, path: str, heartbeat_timeout_s: float, btc_window_s: float, flow_window_s: float) -> None:
        self.rows = self._load_rows(path)
        self.heartbeat_timeout_s = heartbeat_timeout_s
        self.btc_window_s = btc_window_s
        self.flow_window_s = flow_window_s

    def _load_rows(self, path: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        with open(path, "r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                try:
                    row["_ts"] = parse_ts(str(row["ts_utc"]))
                except Exception:
                    continue
                rows.append(row)
        rows.sort(key=lambda item: item["_ts"])
        return rows

    def iter_snapshots(self, speedup: float) -> Iterable[dict[str, Any]]:
        btc_history: Deque[tuple[datetime, float]] = deque(maxlen=4096)
        yes_trades: Deque[tuple[datetime, float, float, str]] = deque(maxlen=2048)
        no_trades: Deque[tuple[datetime, float, float, str]] = deque(maxlen=2048)
        market_open_btc: dict[str, float] = {}
        previous_ts: Optional[datetime] = None
        previous_yes_trade: Optional[tuple[Optional[float], Optional[float], Optional[str]]] = None
        previous_no_trade: Optional[tuple[Optional[float], Optional[float], Optional[str]]] = None

        for row in self.rows:
            ts = row["_ts"]
            if previous_ts is not None and speedup > 0:
                delay = max(0.0, (ts - previous_ts).total_seconds() / speedup)
                if delay > 0:
                    time.sleep(delay)
            previous_ts = ts

            slug = str(row.get("slug") or "")
            btc_price = safe_float(row.get("btc_price"))
            if btc_price is not None:
                btc_history.append((ts, btc_price))
                market_open_btc.setdefault(slug, btc_price)

            yes_trade = (safe_float(row.get("yes_last_price")), safe_float(row.get("yes_last_size")), row.get("yes_last_side"))
            no_trade = (safe_float(row.get("no_last_price")), safe_float(row.get("no_last_size")), row.get("no_last_side"))
            if yes_trade != previous_yes_trade and yes_trade[0] is not None and yes_trade[1] is not None:
                yes_trades.append((ts, yes_trade[0], yes_trade[1], str(yes_trade[2] or "")))
            if no_trade != previous_no_trade and no_trade[0] is not None and no_trade[1] is not None:
                no_trades.append((ts, no_trade[0], no_trade[1], str(no_trade[2] or "")))
            previous_yes_trade = yes_trade
            previous_no_trade = no_trade

            momentum_cutoff = ts.timestamp() - self.btc_window_s
            flow_cutoff = ts.timestamp() - self.flow_window_s
            btc_recent = [item for item in btc_history if item[0].timestamp() >= momentum_cutoff]
            yes_recent = [item for item in yes_trades if item[0].timestamp() >= flow_cutoff]
            no_recent = [item for item in no_trades if item[0].timestamp() >= flow_cutoff]

            def imbalance(events: list[tuple[datetime, float, float, str]]) -> tuple[float, float]:
                signed = 0.0
                total = 0.0
                for _, _, size, side in events:
                    sign = 1.0 if side.upper() == "BUY" else -1.0 if side.upper() == "SELL" else 0.0
                    signed += sign * size
                    total += size
                return ((signed / total) if total else 0.0), total

            btc_reference = btc_recent[0][1] if btc_recent else None
            btc_momentum_bps = None if btc_price is None or btc_reference in (None, 0.0) else ((btc_price - btc_reference) / btc_reference) * 10000.0
            btc_open = market_open_btc.get(slug)
            btc_move_bps = None if btc_price is None or btc_open in (None, 0.0) else ((btc_price - btc_open) / btc_open) * 10000.0
            yes_flow, yes_volume = imbalance(yes_recent)
            no_flow, no_volume = imbalance(no_recent)

            yes_mid = safe_float(row.get("yes_mid"))
            no_mid = safe_float(row.get("no_mid"))
            inferred_yes = yes_mid if yes_mid is not None else (1.0 - no_mid if no_mid is not None else None)
            inferred_no = no_mid if no_mid is not None else (1.0 - yes_mid if yes_mid is not None else None)

            market_start_ts = int(slug.split("-")[-1]) if slug else None
            expiry_ts = market_start_ts + 300 if market_start_ts is not None else None
            seconds_to_expiry = None if expiry_ts is None else (expiry_ts - ts.timestamp())

            snapshot = dict(row)
            snapshot.update(
                {
                    "ts_utc": ts.isoformat(timespec="milliseconds"),
                    "market_start_ts": market_start_ts,
                    "market_expiry_ts": expiry_ts,
                    "market_btc_open_price": btc_open,
                    "market_btc_open_ts_utc": None,
                    "btc_momentum_bps_window": btc_momentum_bps,
                    "btc_move_bps_from_open": btc_move_bps,
                    "yes_flow_imbalance": yes_flow,
                    "no_flow_imbalance": no_flow,
                    "net_flow_imbalance": (yes_flow - no_flow) / 2.0,
                    "yes_flow_volume": yes_volume,
                    "no_flow_volume": no_volume,
                    "inferred_yes_price": inferred_yes,
                    "inferred_no_price": inferred_no,
                    "seconds_to_expiry": seconds_to_expiry,
                    "elapsed_fraction": clamp(1.0 - (seconds_to_expiry / 300.0), 0.0, 1.0) if seconds_to_expiry is not None else None,
                    "feed_is_fresh": (
                        (safe_float(row.get("secs_since_last_btc_update")) or 0.0) <= self.heartbeat_timeout_s
                        and (safe_float(row.get("secs_since_last_market_msg")) or 0.0) <= self.heartbeat_timeout_s
                    ),
                }
            )
            yield snapshot


def build_run_dir(base_dir: str) -> Path:
    run_dir = Path(base_dir) / f"paper_run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def build_summary(
    args: argparse.Namespace,
    broker: PaperBroker,
    logger: TradeLogger,
    run_dir: Path,
    started_at: float,
    finished_at: float,
) -> dict[str, Any]:
    closed_trades = broker.closed_trades
    win_rate = (broker.wins / closed_trades) if closed_trades else 0.0
    summary = {
        "run_dir": str(run_dir),
        "started_at_utc": datetime.fromtimestamp(started_at, tz=timezone.utc).isoformat(timespec="seconds"),
        "finished_at_utc": datetime.fromtimestamp(finished_at, tz=timezone.utc).isoformat(timespec="seconds"),
        "duration_seconds": finished_at - started_at,
        "mode": args.mode,
        "data_source": "replay" if args.replay_csv else "live",
        "paper_bankroll": args.paper_bankroll,
        "stake": args.stake,
        "max_position_notional": args.max_position_notional,
        "fee_bps": args.fee_bps,
        "slippage_bps": args.slippage_bps,
        "fill_style": args.fill_style,
        "min_edge": args.min_edge,
        "max_hold_s": args.max_hold_s,
        "near_expiry_window_s": args.near_expiry_window_s,
        "btc_momentum_window_s": args.btc_momentum_window_s,
        "flow_window_s": args.flow_window_s,
        "heartbeat_timeout_s": args.heartbeat_timeout_s,
        "closed_trades": closed_trades,
        "wins": broker.wins,
        "losses": broker.losses,
        "win_rate": win_rate,
        "realized_pnl": broker.realized_pnl,
        "total_fees": broker.total_fees,
        "max_drawdown": broker.max_drawdown,
        "ending_equity": broker.starting_bankroll + broker.realized_pnl,
        "event_rows": len(logger.event_rows),
        "equity_rows": len(logger.equity_rows),
        "per_market": broker.market_stats,
        "notes": [
            "Paper-only simulation. No order submission, wallet signing, or private key handling is present.",
            "Expiry settlement is inferred from captured BTC reference prices, not official Polymarket resolution events.",
            "Fees and slippage are configurable assumptions, not venue-verified execution costs.",
        ],
    }
    return summary


def write_summary_files(run_dir: Path, summary: dict[str, Any]) -> None:
    summary_json = run_dir / "session_summary.json"
    summary_csv = run_dir / "session_summary.csv"
    with open(summary_json, "w", encoding="utf-8", buffering=1) as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())

    csv_row = dict(summary)
    csv_row["per_market"] = json.dumps(summary["per_market"], sort_keys=True)
    csv_row["notes"] = " | ".join(summary["notes"])
    fieldnames = list(csv_row.keys())
    with open(summary_csv, "w", newline="", encoding="utf-8", buffering=1) as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(csv_row)
        handle.flush()
        os.fsync(handle.fileno())


def process_snapshot(
    snapshot: dict[str, Any],
    strategy: StrategyEngine,
    broker: PaperBroker,
    logger: TradeLogger,
    dashboard: TerminalDashboard,
    args: argparse.Namespace,
    run_dir: Path,
    market_entries: dict[str, int],
    stale_state: dict[str, Any],
    previous_slug: dict[str, Optional[str]],
) -> None:
    current_slug = str(snapshot.get("slug") or "")
    if previous_slug["value"] != current_slug:
        logger.log_event("market_rollover_seen", market_slug=current_slug, details={"previous_slug": previous_slug["value"]})
        previous_slug["value"] = current_slug

    if snapshot.get("feed_is_fresh"):
        if stale_state["active"]:
            logger.log_event("feed_resumed", market_slug=current_slug)
        stale_state["active"] = False
        stale_state["started"] = None
    else:
        if not stale_state["active"]:
            stale_state["active"] = True
            stale_state["started"] = time.monotonic()
            logger.log_event("feed_stale", market_slug=current_slug)

    if broker.position is not None and broker.position.market_slug != current_slug:
        exit_snapshot = dict(snapshot)
        exit_snapshot["slug"] = broker.position.market_slug
        try:
            closed = broker.settle_expired(exit_snapshot, reason="market_rollover_settlement", btc_end_price=safe_float(snapshot.get("btc_price")))
            logger.log_trade(closed)
            logger.log_event("paper_exit", market_slug=closed["market_slug"], side=closed["side"], reason=closed["exit_reason"], details=closed)
        except Exception as exc:
            logger.log_event("rollover_settlement_failed", market_slug=broker.position.market_slug if broker.position else current_slug, reason=str(exc))

    if broker.position is not None and safe_float(snapshot.get("seconds_to_expiry")) is not None and safe_float(snapshot.get("seconds_to_expiry")) <= 0:
        try:
            closed = broker.settle_expired(snapshot, reason="expiry_settlement", btc_end_price=safe_float(snapshot.get("btc_price")))
            logger.log_trade(closed)
            logger.log_event("paper_exit", market_slug=closed["market_slug"], side=closed["side"], reason=closed["exit_reason"], details=closed)
        except Exception as exc:
            logger.log_event("expiry_settlement_failed", market_slug=broker.position.market_slug if broker.position else current_slug, reason=str(exc))

    decision = strategy.evaluate(
        snapshot=snapshot,
        position=broker.position,
        mode=args.mode,
        market_entries=market_entries[current_slug],
        seconds_since_last_exit=(None if broker.last_exit_monotonic is None else time.monotonic() - broker.last_exit_monotonic),
    )

    if broker.position is not None and args.mode == "early-exit" and stale_state["active"] and stale_state["started"] is not None:
        if time.monotonic() - stale_state["started"] >= args.heartbeat_timeout_s * 2.0:
            decision = StrategyDecision("exit", broker.position.side, f"HOLD_{broker.position.side}", "stale_feed_fail_safe", decision.edge, decision.probability_yes, decision.confidence)

    if broker.position is None and decision.action == "enter" and decision.side is not None:
        notional = min(args.stake, args.max_position_notional, broker.starting_bankroll + broker.realized_pnl)
        if notional > 0:
            try:
                opened = broker.enter(snapshot, decision.side, notional, decision.reason)
                market_entries[current_slug] += 1
                logger.log_event("paper_entry", market_slug=current_slug, side=decision.side, signal=decision.signal, reason=decision.reason, details=opened)
            except Exception as exc:
                logger.log_event("paper_entry_failed", market_slug=current_slug, side=decision.side, signal=decision.signal, reason=str(exc))
    elif broker.position is not None and decision.action == "exit":
        try:
            closed = broker.close(snapshot, reason=decision.reason)
            logger.log_trade(closed)
            logger.log_event("paper_exit", market_slug=closed["market_slug"], side=closed["side"], signal=decision.signal, reason=closed["exit_reason"], details=closed)
        except Exception as exc:
            logger.log_event("paper_exit_failed", market_slug=current_slug, side=broker.position.side if broker.position else None, signal=decision.signal, reason=str(exc))

    marks = broker.mark(snapshot)
    logger.log_equity(snapshot, broker, marks)
    dashboard.render(snapshot, decision, broker, marks, args.mode, run_dir)


def run_live(args: argparse.Namespace, run_dir: Path) -> tuple[PaperBroker, TradeLogger]:
    feed = LiveFeedAdapter(btc_source=args.btc_source, quiet=args.quiet, rollover_poll=args.rollover_poll)
    logger = TradeLogger(run_dir)
    broker = PaperBroker(args.paper_bankroll, args.fee_bps, args.slippage_bps, args.fill_style)
    dashboard = TerminalDashboard(args.refresh_ms)
    strategy = StrategyEngine(args.min_edge, args.near_expiry_window_s, args.max_hold_s, args.max_entries_per_market, args.cooldown_s)

    stop_state = {"stop": False}
    market_entries: dict[str, int] = defaultdict(int)
    stale_state = {"active": False, "started": None}
    previous_slug = {"value": None}

    def handle_stop(*_args: object) -> None:
        stop_state["stop"] = True
        feed.stop()

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    logger.log_event("session_start", details={"mode": args.mode, "data_source": "live"})
    feed.start()
    last_snapshot: Optional[dict[str, Any]] = None
    try:
        while not stop_state["stop"]:
            snapshot = feed.live_snapshot(
                btc_momentum_window_s=args.btc_momentum_window_s,
                flow_window_s=args.flow_window_s,
                heartbeat_timeout_s=args.heartbeat_timeout_s,
            )
            if snapshot is not None:
                last_snapshot = snapshot
                process_snapshot(snapshot, strategy, broker, logger, dashboard, args, run_dir, market_entries, stale_state, previous_slug)
            time.sleep(args.refresh_ms / 1000.0)
    finally:
        feed.stop()
        feed.join(timeout=3)

    if broker.position is not None and last_snapshot is not None:
        try:
            closed = broker.close(last_snapshot, reason="shutdown_close")
            logger.log_trade(closed)
            logger.log_event("paper_exit", market_slug=closed["market_slug"], side=closed["side"], reason=closed["exit_reason"], details=closed)
        except Exception as exc:
            logger.log_event("shutdown_close_failed", market_slug=broker.position.market_slug, side=broker.position.side, reason=str(exc))

    logger.log_event("session_stop", details={"data_source": "live"})
    return broker, logger


def run_replay(args: argparse.Namespace, run_dir: Path) -> tuple[PaperBroker, TradeLogger]:
    source = ReplaySnapshotSource(args.replay_csv, args.heartbeat_timeout_s, args.btc_momentum_window_s, args.flow_window_s)
    logger = TradeLogger(run_dir)
    broker = PaperBroker(args.paper_bankroll, args.fee_bps, args.slippage_bps, args.fill_style)
    dashboard = TerminalDashboard(args.refresh_ms)
    strategy = StrategyEngine(args.min_edge, args.near_expiry_window_s, args.max_hold_s, args.max_entries_per_market, args.cooldown_s)
    market_entries: dict[str, int] = defaultdict(int)
    stale_state = {"active": False, "started": None}
    previous_slug = {"value": None}

    logger.log_event("session_start", details={"mode": args.mode, "data_source": "replay", "replay_csv": args.replay_csv})
    for snapshot in source.iter_snapshots(speedup=args.replay_speedup):
        process_snapshot(snapshot, strategy, broker, logger, dashboard, args, run_dir, market_entries, stale_state, previous_slug)

    if broker.position is not None:
        closed = broker.close(snapshot, reason="replay_end_close")
        logger.log_trade(closed)
        logger.log_event("paper_exit", market_slug=closed["market_slug"], side=closed["side"], reason=closed["exit_reason"], details=closed)

    logger.log_event("session_stop", details={"data_source": "replay"})
    return broker, logger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Live paper-trading runner for BTC 5-minute Polymarket markets")
    parser.add_argument("--paper-bankroll", type=float, default=1000.0)
    parser.add_argument("--stake", type=float, default=25.0, help="Per-trade paper notional in dollars")
    parser.add_argument("--max-position-notional", type=float, default=50.0)
    parser.add_argument("--fee-bps", type=float, default=10.0)
    parser.add_argument("--slippage-bps", type=float, default=15.0)
    parser.add_argument("--fill-style", choices=["mid", "maker", "taker"], default="mid")
    parser.add_argument("--mode", choices=["expiry", "early-exit"], default="early-exit")
    parser.add_argument("--min-edge", type=float, default=0.08)
    parser.add_argument("--max-hold-s", type=float, default=90.0)
    parser.add_argument("--near-expiry-window-s", type=float, default=45.0)
    parser.add_argument("--btc-momentum-window-s", type=float, default=30.0)
    parser.add_argument("--flow-window-s", type=float, default=15.0)
    parser.add_argument("--log-dir", default="data/paper_runs")
    parser.add_argument("--refresh-ms", type=int, default=250)
    parser.add_argument("--heartbeat-timeout-s", type=float, default=5.0)
    parser.add_argument("--btc-source", choices=["chainlink", "binance"], default="chainlink")
    parser.add_argument("--rollover-poll", type=float, default=1.0)
    parser.add_argument("--replay-csv", default=None, help="Optional snapshot CSV for local smoke/replay mode")
    parser.add_argument("--replay-speedup", type=float, default=100.0)
    parser.add_argument("--max-entries-per-market", type=int, default=1)
    parser.add_argument("--cooldown-s", type=float, default=20.0)
    parser.add_argument("--quiet", action="store_true")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    run_dir = build_run_dir(args.log_dir)
    started_at = time.time()
    logger: Optional[TradeLogger] = None

    try:
        if args.replay_csv:
            broker, logger = run_replay(args, run_dir)
        else:
            broker, logger = run_live(args, run_dir)

        logger.flush(force_fsync=True)
        finished_at = time.time()
        summary = build_summary(args, broker, logger, run_dir, started_at, finished_at)
        write_summary_files(run_dir, summary)
        return 0
    finally:
        if logger is not None:
            logger.close()


if __name__ == "__main__":
    raise SystemExit(main())
