#!/usr/bin/env python3
from __future__ import annotations

import argparse
import signal
import threading
import time

from polymarket_live_feed import LiveFeedAdapter, snapshot_writer


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="snapshots.csv")
    parser.add_argument("--interval", type=float, default=1.0, help="Snapshot interval in seconds")
    parser.add_argument("--btc-source", choices=["chainlink", "binance"], default="chainlink")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--rollover-poll", type=float, default=1.0, help="How often to check for new 5m market")
    parser.add_argument("--flush-interval", type=float, default=1.0, help="How often to flush CSV in seconds")
    parser.add_argument("--flush-every-rows", type=int, default=25, help="Flush CSV every N rows")
    args = parser.parse_args()

    feed = LiveFeedAdapter(
        btc_source=args.btc_source,
        quiet=args.quiet,
        rollover_poll=args.rollover_poll,
    )

    def handle_stop(*_args: object) -> None:
        feed.stop()

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    feed.start()
    writer_thread = threading.Thread(
        target=snapshot_writer,
        args=(feed.state, feed.stop_event, args.out, args.interval, True, args.flush_interval, args.flush_every_rows),
        daemon=True,
    )
    writer_thread.start()

    try:
        while not feed.stop_event.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        feed.stop()

    writer_thread.join(timeout=3)
    feed.join(timeout=3)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
