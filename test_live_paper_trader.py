import argparse
import tempfile
import unittest
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from live_paper_trader import (
    PaperBroker,
    PaperPosition,
    ReplaySnapshotSource,
    StrategyEngine,
    TradeLogger,
    process_snapshot,
)


class NoOpDashboard:
    def render(self, *args, **kwargs) -> None:
        return None


def make_snapshot(
    *,
    ts: datetime,
    slug: str,
    seconds_to_expiry: float,
    yes_price: float = 0.40,
    no_price: float = 0.60,
    yes_bid: float = 0.39,
    yes_ask: float = 0.41,
    no_bid: float = 0.59,
    no_ask: float = 0.61,
    btc_move_bps_from_open: float = 120.0,
    btc_momentum_bps_window: float = 60.0,
    net_flow_imbalance: float = 0.15,
    feed_is_fresh: bool = True,
) -> dict[str, object]:
    elapsed_fraction = max(0.0, min(1.0, 1.0 - (seconds_to_expiry / 300.0)))
    return {
        "ts_utc": ts.isoformat(timespec="milliseconds"),
        "slug": slug,
        "feed_is_fresh": feed_is_fresh,
        "inferred_yes_price": yes_price,
        "inferred_no_price": no_price,
        "yes_best_bid": yes_bid,
        "yes_best_ask": yes_ask,
        "no_best_bid": no_bid,
        "no_best_ask": no_ask,
        "seconds_to_expiry": seconds_to_expiry,
        "btc_move_bps_from_open": btc_move_bps_from_open,
        "btc_momentum_bps_window": btc_momentum_bps_window,
        "net_flow_imbalance": net_flow_imbalance,
        "elapsed_fraction": elapsed_fraction,
        "yes_spread": yes_ask - yes_bid,
        "no_spread": no_ask - no_bid,
        "btc_price": 70000.0,
    }


def make_args() -> argparse.Namespace:
    return argparse.Namespace(
        mode="early-exit",
        stake=25.0,
        max_position_notional=25.0,
        heartbeat_timeout_s=5.0,
    )


class StrategyEngineNearExpiryGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.strategy = StrategyEngine(
            min_edge=0.08,
            near_expiry_window_s=45.0,
            max_hold_s=90.0,
            max_entries_per_market=1,
            cooldown_s=20.0,
        )
        self.base_ts = datetime(2026, 3, 27, 6, 42, 0, tzinfo=timezone.utc)
        self.slug = "btc-updown-5m-1774593600"

    def test_blocks_new_entry_outside_near_expiry_window(self) -> None:
        snapshot = make_snapshot(ts=self.base_ts, slug=self.slug, seconds_to_expiry=240.0)

        decision = self.strategy.evaluate(
            snapshot=snapshot,
            position=None,
            mode="early-exit",
            market_entries=0,
            seconds_since_last_exit=None,
        )

        self.assertEqual("wait", decision.action)
        self.assertEqual("outside_near_expiry_window", decision.reason)
        self.assertIsNone(decision.side)

    def test_allows_entry_inside_near_expiry_window(self) -> None:
        snapshot = make_snapshot(ts=self.base_ts, slug=self.slug, seconds_to_expiry=30.0)

        decision = self.strategy.evaluate(
            snapshot=snapshot,
            position=None,
            mode="early-exit",
            market_entries=0,
            seconds_since_last_exit=None,
        )

        self.assertEqual("enter", decision.action)
        self.assertEqual("YES", decision.side)
        self.assertEqual("edge_confirmed", decision.reason)

    def test_existing_position_is_not_blocked_outside_window(self) -> None:
        entry_ts = self.base_ts - timedelta(seconds=10)
        position = PaperPosition(
            trade_id=1,
            market_id=self.slug,
            market_slug=self.slug,
            side="YES",
            entry_time=entry_ts.isoformat(timespec="milliseconds"),
            entry_dt=entry_ts,
            entry_monotonic=0.0,
            entry_price=0.40,
            entry_mid_price=0.40,
            size=10.0,
            notional=4.0,
            entry_fee=0.01,
            entry_reason="edge_confirmed",
            market_btc_open_price=70000.0,
        )
        snapshot = make_snapshot(ts=self.base_ts, slug=self.slug, seconds_to_expiry=240.0)

        decision = self.strategy.evaluate(
            snapshot=snapshot,
            position=position,
            mode="early-exit",
            market_entries=1,
            seconds_since_last_exit=None,
        )

        self.assertEqual("hold", decision.action)
        self.assertEqual("position_open", decision.reason)
        self.assertEqual("YES", decision.side)

    def test_smoke_no_position_opens_in_first_several_seconds_of_new_market(self) -> None:
        args = make_args()
        broker = PaperBroker(bankroll=1000.0, fee_bps=10.0, slippage_bps=15.0, fill_style="mid")
        dashboard = NoOpDashboard()
        market_entries = defaultdict(int)
        stale_state = {"active": False, "started": None}
        previous_slug = {"value": None}

        with tempfile.TemporaryDirectory() as temp_dir:
            logger = TradeLogger(Path(temp_dir))
            try:
                for offset_s in range(0, 8):
                    snapshot = make_snapshot(
                        ts=self.base_ts + timedelta(seconds=offset_s),
                        slug=self.slug,
                        seconds_to_expiry=300.0 - offset_s,
                    )
                    process_snapshot(
                        snapshot=snapshot,
                        strategy=self.strategy,
                        broker=broker,
                        logger=logger,
                        dashboard=dashboard,
                        args=args,
                        run_dir=Path(temp_dir),
                        market_entries=market_entries,
                        stale_state=stale_state,
                        previous_slug=previous_slug,
                    )
            finally:
                logger.close()

        self.assertIsNone(broker.position)
        self.assertEqual([], logger.trade_rows)
        self.assertFalse(any(row["event"] == "paper_entry" for row in logger.event_rows))

    def test_replay_smoke_trades_are_not_opened_outside_window(self) -> None:
        args = make_args()
        broker = PaperBroker(bankroll=1000.0, fee_bps=10.0, slippage_bps=15.0, fill_style="mid")
        dashboard = NoOpDashboard()
        market_entries = defaultdict(int)
        stale_state = {"active": False, "started": None}
        previous_slug = {"value": None}
        replay_path = Path(__file__).resolve().parent / "data" / "snapshots_replay_smoke.csv"
        source = ReplaySnapshotSource(str(replay_path), args.heartbeat_timeout_s, btc_window_s=30.0, flow_window_s=15.0)

        with tempfile.TemporaryDirectory() as temp_dir:
            logger = TradeLogger(Path(temp_dir))
            try:
                for snapshot in source.iter_snapshots(speedup=0):
                    process_snapshot(
                        snapshot=snapshot,
                        strategy=self.strategy,
                        broker=broker,
                        logger=logger,
                        dashboard=dashboard,
                        args=args,
                        run_dir=Path(temp_dir),
                        market_entries=market_entries,
                        stale_state=stale_state,
                        previous_slug=previous_slug,
                    )
            finally:
                logger.close()

        self.assertEqual([], logger.trade_rows)
        self.assertFalse(any(row["event"] == "paper_entry" for row in logger.event_rows))


if __name__ == "__main__":
    unittest.main()
