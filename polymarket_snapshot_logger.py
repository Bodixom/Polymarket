#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import requests
import websocket

GAMMA_BASE = "https://gamma-api.polymarket.com"
POLY_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
RTDS_WS_URL = "wss://ws-live-data.polymarket.com"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def slug_from_event_url(event_url: str) -> str:
    return urlparse(event_url).path.rstrip("/").split("/")[-1]


def current_btc_5m_event_url() -> str:
    now = int(time.time())
    window = now - (now % 300)
    return f"https://polymarket.com/event/btc-updown-5m-{window}"


def fetch_market(slug: str, retries: int = 5, delay: float = 1.0) -> Dict[str, Any]:
    last_err: Optional[Exception] = None

    for _ in range(retries):
        try:
            r = requests.get(f"{GAMMA_BASE}/markets/slug/{slug}", timeout=20)
            if r.status_code == 200:
                return r.json()

            r = requests.get(f"{GAMMA_BASE}/markets", params={"slug": slug}, timeout=20)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list) and data:
                return data[0]

            raise RuntimeError(f"No market found for slug: {slug}")
        except Exception as e:
            last_err = e
            time.sleep(delay)

    raise RuntimeError(f"Failed to fetch market {slug}: {last_err}")


def ensure_list(v: Any) -> list[str]:
    if isinstance(v, list):
        return [str(x) for x in v]
    if isinstance(v, str):
        try:
            parsed = json.loads(v)
            if isinstance(parsed, list):
                return [str(i) for i in parsed]
        except Exception:
            pass
    return []


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None


def best_bid_ask_from_book(data: Dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    bids = data.get("bids") or []
    asks = data.get("asks") or []

    best_bid = None
    best_ask = None

    if isinstance(bids, list):
        bid_vals = [safe_float(b.get("price")) for b in bids if isinstance(b, dict)]
        bid_vals = [v for v in bid_vals if v is not None]
        if bid_vals:
            best_bid = max(bid_vals)

    if isinstance(asks, list):
        ask_vals = [safe_float(a.get("price")) for a in asks if isinstance(a, dict)]
        ask_vals = [v for v in ask_vals if v is not None]
        if ask_vals:
            best_ask = min(ask_vals)

    return best_bid, best_ask


class State:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.slug: Optional[str] = None
        self.question: Optional[str] = None
        self.yes_token: Optional[str] = None
        self.no_token: Optional[str] = None
        self.expiry_ts: Optional[int] = None

        self.assets: Dict[str, Dict[str, Any]] = {}
        self.btc_price: Optional[float] = None
        self.btc_ts_utc: Optional[str] = None

    def reset_market(self, slug: str, question: str, yes_token: str, no_token: str) -> None:
        with self.lock:
            self.slug = slug
            self.question = question
            self.yes_token = yes_token
            self.no_token = no_token
            self.expiry_ts = int(slug.split("-")[-1]) + 300
            self.assets = {
                yes_token: {
                    "best_bid": None,
                    "best_ask": None,
                    "spread": None,
                    "last_price": None,
                    "last_size": None,
                    "last_side": None,
                },
                no_token: {
                    "best_bid": None,
                    "best_ask": None,
                    "spread": None,
                    "last_price": None,
                    "last_size": None,
                    "last_side": None,
                },
            }

    def update_btc(self, price: float) -> None:
        with self.lock:
            self.btc_price = price
            self.btc_ts_utc = now_iso()

    def update_asset_book(
        self,
        asset_id: str,
        best_bid: Optional[float],
        best_ask: Optional[float],
        spread: Optional[float],
    ) -> None:
        with self.lock:
            if asset_id not in self.assets:
                return
            if best_bid is not None:
                self.assets[asset_id]["best_bid"] = best_bid
            if best_ask is not None:
                self.assets[asset_id]["best_ask"] = best_ask
            if spread is not None:
                self.assets[asset_id]["spread"] = spread

    def update_asset_trade(
        self,
        asset_id: str,
        price: Optional[float],
        size: Optional[float],
        side: Optional[str],
    ) -> None:
        with self.lock:
            if asset_id not in self.assets:
                return
            if price is not None:
                self.assets[asset_id]["last_price"] = price
            if size is not None:
                self.assets[asset_id]["last_size"] = size
            if side is not None:
                self.assets[asset_id]["last_side"] = side

    def snapshot_row(self) -> Optional[Dict[str, Any]]:
        with self.lock:
            if not self.slug or not self.yes_token or not self.no_token:
                return None

            y = self.assets.get(self.yes_token, {})
            n = self.assets.get(self.no_token, {})

            y_bid = y.get("best_bid")
            y_ask = y.get("best_ask")
            n_bid = n.get("best_bid")
            n_ask = n.get("best_ask")

            y_mid = (y_bid + y_ask) / 2.0 if y_bid is not None and y_ask is not None else None
            n_mid = (n_bid + n_ask) / 2.0 if n_bid is not None and n_ask is not None else None

            sum_mid = (y_mid + n_mid) if y_mid is not None and n_mid is not None else None
            mid_dev = (sum_mid - 1.0) if sum_mid is not None else None

            seconds_to_expiry = None
            if self.expiry_ts is not None:
                seconds_to_expiry = self.expiry_ts - time.time()

            return {
                "ts_utc": now_iso(),
                "slug": self.slug,
                "question": self.question,
                "yes_token_id": self.yes_token,
                "no_token_id": self.no_token,
                "yes_best_bid": y_bid,
                "yes_best_ask": y_ask,
                "yes_mid": y_mid,
                "yes_spread": y.get("spread"),
                "yes_last_price": y.get("last_price"),
                "yes_last_size": y.get("last_size"),
                "yes_last_side": y.get("last_side"),
                "no_best_bid": n_bid,
                "no_best_ask": n_ask,
                "no_mid": n_mid,
                "no_spread": n.get("spread"),
                "no_last_price": n.get("last_price"),
                "no_last_size": n.get("last_size"),
                "no_last_side": n.get("last_side"),
                "btc_price": self.btc_price,
                "btc_ts_utc": self.btc_ts_utc,
                "seconds_to_expiry": seconds_to_expiry,
                "sum_mid": sum_mid,
                "mid_deviation_from_1": mid_dev,
            }


def start_rtds_ws(state: State, stop_event: threading.Event, source: str = "chainlink") -> None:
    topic = "crypto_prices_chainlink" if source == "chainlink" else "crypto_prices"
    filters = '{"symbol":"btc/usd"}' if source == "chainlink" else "btcusdt"

    def on_open(ws):
        ws.send(json.dumps({
            "action": "subscribe",
            "subscriptions": [{
                "topic": topic,
                "type": "update",
                "filters": filters,
            }],
        }))

    def on_message(ws, message):
        try:
            data = json.loads(message)
        except Exception:
            return

        if not isinstance(data, dict):
            return
        if data.get("topic") != topic:
            return

        payload = data.get("payload") or {}
        if not isinstance(payload, dict):
            return

        value = safe_float(payload.get("value"))
        if value is not None:
            state.update_btc(value)

    while not stop_event.is_set():
        ws = websocket.WebSocketApp(
            RTDS_WS_URL,
            on_open=on_open,
            on_message=on_message,
        )
        try:
            ws.run_forever()
        except Exception as e:
            if not stop_event.is_set():
                print(f"BTC WS error: {e}", file=sys.stderr)
        if not stop_event.is_set():
            time.sleep(2)


def start_poly_heartbeat(ws: websocket.WebSocketApp, stop_event: threading.Event) -> threading.Thread:
    def _beat():
        while not stop_event.is_set():
            try:
                ws.send(json.dumps({}))
            except Exception:
                break
            time.sleep(10)

    t = threading.Thread(target=_beat, daemon=True)
    t.start()
    return t


def start_poly_ws(state: State, stop_event: threading.Event, quiet: bool = False, rollover_poll: float = 1.0) -> None:
    current_slug: Optional[str] = None
    ws: Optional[websocket.WebSocketApp] = None
    ws_thread: Optional[threading.Thread] = None
    ws_stop: Optional[threading.Event] = None
    ws_lock = threading.Lock()

    def close_current_ws() -> None:
        nonlocal ws, ws_thread, ws_stop
        with ws_lock:
            old_ws = ws
            old_thread = ws_thread
            old_stop = ws_stop
            ws = None
            ws_thread = None
            ws_stop = None

        if old_stop is not None:
            old_stop.set()

        if old_ws is not None:
            try:
                old_ws.close()
            except Exception:
                pass

        if old_thread is not None and old_thread.is_alive():
            old_thread.join(timeout=3)

    def build_ws_for_market(slug: str, question: str, yes_token: str, no_token: str) -> tuple[websocket.WebSocketApp, threading.Event]:
        local_stop = threading.Event()

        def on_open(local_ws):
            try:
                local_ws.send(json.dumps({
                    "assets_ids": [yes_token, no_token],
                    "type": "market",
                    "custom_feature_enabled": True,
                }))
                start_poly_heartbeat(local_ws, local_stop)
                if not quiet:
                    print(f"Polymarket subscribed: {slug}", file=sys.stderr)
            except Exception as e:
                if not quiet:
                    print(f"POLY subscribe error: {e}", file=sys.stderr)

        def handle_item(item: Dict[str, Any]) -> None:
            event_type = str(item.get("event_type") or item.get("type") or "")
            if event_type == "new_market":
                return

            asset_id = str(item.get("asset_id") or item.get("assetId") or "")

            if event_type == "best_bid_ask":
                state.update_asset_book(
                    asset_id=asset_id,
                    best_bid=safe_float(item.get("best_bid")),
                    best_ask=safe_float(item.get("best_ask")),
                    spread=safe_float(item.get("spread")),
                )

            elif event_type == "book":
                bb, ba = best_bid_ask_from_book(item)
                spread = (ba - bb) if bb is not None and ba is not None else None
                state.update_asset_book(
                    asset_id=asset_id,
                    best_bid=bb,
                    best_ask=ba,
                    spread=spread,
                )

            elif event_type == "last_trade_price":
                state.update_asset_trade(
                    asset_id=asset_id,
                    price=safe_float(item.get("price")),
                    size=safe_float(item.get("size")),
                    side=str(item.get("side")) if item.get("side") is not None else None,
                )

            elif event_type == "price_change":
                pcs = item.get("price_changes") or []
                if isinstance(pcs, list):
                    for pc in pcs:
                        if not isinstance(pc, dict):
                            continue
                        pc_asset = str(pc.get("asset_id") or pc.get("assetId") or "")
                        state.update_asset_trade(
                            asset_id=pc_asset,
                            price=safe_float(pc.get("price")),
                            size=safe_float(pc.get("size")),
                            side=str(pc.get("side")) if pc.get("side") is not None else None,
                        )

        def on_message(local_ws, message):
            if message == "{}":
                return

            try:
                data = json.loads(message)
            except Exception:
                return

            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        handle_item(item)
                return

            if isinstance(data, dict):
                handle_item(data)

        def on_error(local_ws, error):
            if not quiet and not local_stop.is_set():
                print(f"POLY WS error: {error}", file=sys.stderr)

        def on_close(local_ws, status_code, msg):
            local_stop.set()
            if not quiet and not stop_event.is_set():
                print(f"POLY WS closed: {status_code} {msg}", file=sys.stderr)

        local_ws = websocket.WebSocketApp(
            POLY_WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        return local_ws, local_stop

    try:
        while not stop_event.is_set():
            slug = slug_from_event_url(current_btc_5m_event_url())

            with ws_lock:
                thread_dead = ws_thread is None or not ws_thread.is_alive()

            if slug != current_slug or thread_dead:
                market = fetch_market(slug)
                question = str(market.get("question", ""))
                clob_ids = ensure_list(market.get("clobTokenIds") or market.get("clob_token_ids"))
                if len(clob_ids) < 2:
                    time.sleep(2)
                    continue

                yes_token, no_token = clob_ids[0], clob_ids[1]

                close_current_ws()
                state.reset_market(slug, question, yes_token, no_token)
                current_slug = slug

                if not quiet:
                    print(f"Tracking: {question}", file=sys.stderr)
                    print(f"YES token: {yes_token}", file=sys.stderr)
                    print(f"NO  token: {no_token}", file=sys.stderr)

                new_ws, new_stop = build_ws_for_market(slug, question, yes_token, no_token)
                new_thread = threading.Thread(target=new_ws.run_forever, daemon=True)

                with ws_lock:
                    ws = new_ws
                    ws_stop = new_stop
                    ws_thread = new_thread

                new_thread.start()

            time.sleep(rollover_poll)
    finally:
        close_current_ws()


def snapshot_writer(
    state: State,
    stop_event: threading.Event,
    out_csv: str,
    interval: float,
    require_complete: bool = True,
) -> None:
    fieldnames = [
        "ts_utc", "slug", "question",
        "yes_token_id", "no_token_id",
        "yes_best_bid", "yes_best_ask", "yes_mid", "yes_spread",
        "yes_last_price", "yes_last_size", "yes_last_side",
        "no_best_bid", "no_best_ask", "no_mid", "no_spread",
        "no_last_price", "no_last_size", "no_last_side",
        "btc_price", "btc_ts_utc",
        "seconds_to_expiry", "sum_mid", "mid_deviation_from_1",
    ]

    file_exists = False
    try:
        with open(out_csv, "r", encoding="utf-8"):
            file_exists = True
    except FileNotFoundError:
        pass

    with open(out_csv, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()

        while not stop_event.is_set():
            row = state.snapshot_row()
            if row is not None:
                if require_complete:
                    ready = (
                        row["yes_best_bid"] is not None and
                        row["yes_best_ask"] is not None and
                        row["no_best_bid"] is not None and
                        row["no_best_ask"] is not None and
                        row["btc_price"] is not None
                    )
                    if not ready:
                        time.sleep(interval)
                        continue

                writer.writerow(row)
                f.flush()

            time.sleep(interval)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="snapshots.csv")
    parser.add_argument("--interval", type=float, default=1.0, help="Snapshot interval in seconds")
    parser.add_argument("--btc-source", choices=["chainlink", "binance"], default="chainlink")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--rollover-poll", type=float, default=1.0, help="How often to check for new 5m market")
    args = parser.parse_args()

    stop_event = threading.Event()
    state = State()

    def handle_stop(*_args):
        stop_event.set()

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    threads = [
        threading.Thread(
            target=start_rtds_ws,
            args=(state, stop_event, args.btc_source),
            daemon=True,
        ),
        threading.Thread(
            target=start_poly_ws,
            args=(state, stop_event, args.quiet, args.rollover_poll),
            daemon=True,
        ),
        threading.Thread(
            target=snapshot_writer,
            args=(state, stop_event, args.out, args.interval),
            daemon=True,
        ),
    ]

    for t in threads:
        t.start()

    try:
        while not stop_event.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        stop_event.set()

    for t in threads:
        t.join(timeout=3)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
