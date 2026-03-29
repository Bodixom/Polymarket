#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import sys
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, Optional
from urllib.parse import urlparse

import requests
import websocket

GAMMA_BASE = "https://gamma-api.polymarket.com"
POLY_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
RTDS_WS_URL = "wss://ws-live-data.polymarket.com"
SNAPSHOT_FIELDNAMES = [
    "ts_utc", "slug", "question",
    "yes_token_id", "no_token_id",
    "yes_best_bid", "yes_best_ask", "yes_mid", "yes_spread",
    "yes_last_price", "yes_last_size", "yes_last_side",
    "no_best_bid", "no_best_ask", "no_mid", "no_spread",
    "no_last_price", "no_last_size", "no_last_side",
    "btc_price", "btc_ts_utc",
    "seconds_to_expiry", "sum_mid", "mid_deviation_from_1",
    "secs_since_last_btc_update", "secs_since_last_market_msg",
    "secs_since_last_yes_quote_update", "secs_since_last_yes_trade_update",
    "secs_since_last_no_quote_update", "secs_since_last_no_trade_update",
]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def log_diag(event: str, **fields: Any) -> None:
    payload = {"ts_utc": now_iso(), "event": event}
    payload.update(fields)
    print(json.dumps(payload, default=str), file=sys.stderr)


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
            response = requests.get(f"{GAMMA_BASE}/markets/slug/{slug}", timeout=20)
            if response.status_code == 200:
                return response.json()
            response = requests.get(f"{GAMMA_BASE}/markets", params={"slug": slug}, timeout=20)
            response.raise_for_status()
            data = response.json()
            if isinstance(data, list) and data:
                return data[0]
            raise RuntimeError(f"No market found for slug: {slug}")
        except Exception as exc:
            last_err = exc
            time.sleep(delay)
    raise RuntimeError(f"Failed to fetch market {slug}: {last_err}")


def ensure_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(item) for item in parsed]
        except Exception:
            pass
    return []


def safe_float(value: Any) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def best_bid_ask_from_book(data: Dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    bids = data.get("bids") or []
    asks = data.get("asks") or []
    best_bid = None
    best_ask = None
    if isinstance(bids, list):
        bid_values = [safe_float(item.get("price")) for item in bids if isinstance(item, dict)]
        bid_values = [value for value in bid_values if value is not None]
        if bid_values:
            best_bid = max(bid_values)
    if isinstance(asks, list):
        ask_values = [safe_float(item.get("price")) for item in asks if isinstance(item, dict)]
        ask_values = [value for value in ask_values if value is not None]
        if ask_values:
            best_ask = min(ask_values)
    return best_bid, best_ask


def _side_sign(side: Optional[str]) -> float:
    normalized = (side or "").upper()
    if normalized == "BUY":
        return 1.0
    if normalized == "SELL":
        return -1.0
    return 0.0


class MarketState:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.slug: Optional[str] = None
        self.question: Optional[str] = None
        self.yes_token: Optional[str] = None
        self.no_token: Optional[str] = None
        self.expiry_ts: Optional[int] = None
        self.market_start_ts: Optional[int] = None
        self.market_btc_open_price: Optional[float] = None
        self.market_btc_open_ts_utc: Optional[str] = None

        self.assets: Dict[str, Dict[str, Any]] = {}
        self.btc_price: Optional[float] = None
        self.btc_ts_utc: Optional[str] = None
        self.last_btc_update_monotonic: Optional[float] = None
        self.last_market_message_monotonic: Optional[float] = None
        self.btc_history: Deque[tuple[float, float]] = deque(maxlen=4096)

    def _empty_asset(self) -> Dict[str, Any]:
        return {
            "best_bid": None,
            "best_ask": None,
            "spread": None,
            "last_price": None,
            "last_size": None,
            "last_side": None,
            "last_quote_update_monotonic": None,
            "last_trade_update_monotonic": None,
            "quote_history": deque(maxlen=2048),
            "trade_history": deque(maxlen=2048),
        }

    def reset_market(self, slug: str, question: str, yes_token: str, no_token: str) -> None:
        with self.lock:
            self.slug = slug
            self.question = question
            self.yes_token = yes_token
            self.no_token = no_token
            self.market_start_ts = int(slug.split("-")[-1])
            self.expiry_ts = self.market_start_ts + 300
            self.market_btc_open_price = self.btc_price
            self.market_btc_open_ts_utc = self.btc_ts_utc
            self.last_market_message_monotonic = None
            self.assets = {
                yes_token: self._empty_asset(),
                no_token: self._empty_asset(),
            }

    def update_btc(self, price: float) -> None:
        now_mono = time.monotonic()
        with self.lock:
            self.btc_price = price
            self.btc_ts_utc = now_iso()
            self.last_btc_update_monotonic = now_mono
            self.btc_history.append((now_mono, price))
            if self.slug and self.market_btc_open_price is None:
                self.market_btc_open_price = price
                self.market_btc_open_ts_utc = self.btc_ts_utc

    def update_asset_book(
        self,
        asset_id: str,
        best_bid: Optional[float],
        best_ask: Optional[float],
        spread: Optional[float],
    ) -> None:
        now_mono = time.monotonic()
        with self.lock:
            asset = self.assets.get(asset_id)
            if asset is None:
                return
            if best_bid is not None:
                asset["best_bid"] = best_bid
            if best_ask is not None:
                asset["best_ask"] = best_ask
            if spread is not None:
                asset["spread"] = spread
            asset["last_quote_update_monotonic"] = now_mono
            asset["quote_history"].append((now_mono, asset["best_bid"], asset["best_ask"], asset["spread"]))
            self.last_market_message_monotonic = now_mono

    def update_asset_trade(
        self,
        asset_id: str,
        price: Optional[float],
        size: Optional[float],
        side: Optional[str],
    ) -> None:
        now_mono = time.monotonic()
        with self.lock:
            asset = self.assets.get(asset_id)
            if asset is None:
                return
            if price is not None:
                asset["last_price"] = price
            if size is not None:
                asset["last_size"] = size
            if side is not None:
                asset["last_side"] = side
            asset["last_trade_update_monotonic"] = now_mono
            asset["trade_history"].append((now_mono, price, size, side))
            self.last_market_message_monotonic = now_mono

    def snapshot_row(self) -> Optional[Dict[str, Any]]:
        with self.lock:
            if not self.slug or not self.yes_token or not self.no_token:
                return None

            yes_asset = self.assets.get(self.yes_token, {})
            no_asset = self.assets.get(self.no_token, {})

            yes_bid = yes_asset.get("best_bid")
            yes_ask = yes_asset.get("best_ask")
            no_bid = no_asset.get("best_bid")
            no_ask = no_asset.get("best_ask")

            yes_mid = (yes_bid + yes_ask) / 2.0 if yes_bid is not None and yes_ask is not None else None
            no_mid = (no_bid + no_ask) / 2.0 if no_bid is not None and no_ask is not None else None

            sum_mid = (yes_mid + no_mid) if yes_mid is not None and no_mid is not None else None
            mid_deviation = (sum_mid - 1.0) if sum_mid is not None else None

            seconds_to_expiry = None
            if self.expiry_ts is not None:
                seconds_to_expiry = self.expiry_ts - time.time()

            now_mono = time.monotonic()

            def age_seconds(value: Optional[float]) -> Optional[float]:
                if value is None:
                    return None
                return now_mono - value

            return {
                "ts_utc": now_iso(),
                "slug": self.slug,
                "question": self.question,
                "yes_token_id": self.yes_token,
                "no_token_id": self.no_token,
                "yes_best_bid": yes_bid,
                "yes_best_ask": yes_ask,
                "yes_mid": yes_mid,
                "yes_spread": yes_asset.get("spread"),
                "yes_last_price": yes_asset.get("last_price"),
                "yes_last_size": yes_asset.get("last_size"),
                "yes_last_side": yes_asset.get("last_side"),
                "no_best_bid": no_bid,
                "no_best_ask": no_ask,
                "no_mid": no_mid,
                "no_spread": no_asset.get("spread"),
                "no_last_price": no_asset.get("last_price"),
                "no_last_size": no_asset.get("last_size"),
                "no_last_side": no_asset.get("last_side"),
                "btc_price": self.btc_price,
                "btc_ts_utc": self.btc_ts_utc,
                "seconds_to_expiry": seconds_to_expiry,
                "sum_mid": sum_mid,
                "mid_deviation_from_1": mid_deviation,
                "secs_since_last_btc_update": age_seconds(self.last_btc_update_monotonic),
                "secs_since_last_market_msg": age_seconds(self.last_market_message_monotonic),
                "secs_since_last_yes_quote_update": age_seconds(yes_asset.get("last_quote_update_monotonic")),
                "secs_since_last_yes_trade_update": age_seconds(yes_asset.get("last_trade_update_monotonic")),
                "secs_since_last_no_quote_update": age_seconds(no_asset.get("last_quote_update_monotonic")),
                "secs_since_last_no_trade_update": age_seconds(no_asset.get("last_trade_update_monotonic")),
            }

    def live_snapshot(
        self,
        btc_momentum_window_s: float = 30.0,
        flow_window_s: float = 15.0,
        heartbeat_timeout_s: float = 5.0,
    ) -> Optional[Dict[str, Any]]:
        with self.lock:
            if not self.slug or not self.yes_token or not self.no_token:
                return None

            now_mono = time.monotonic()
            yes_asset = self.assets.get(self.yes_token, {})
            no_asset = self.assets.get(self.no_token, {})
            row = self.snapshot_row()
            if row is None:
                return None

            btc_history = list(self.btc_history)
            yes_trades = list(yes_asset.get("trade_history") or [])
            no_trades = list(no_asset.get("trade_history") or [])
            market_btc_open_price = self.market_btc_open_price
            market_btc_open_ts_utc = self.market_btc_open_ts_utc
            market_start_ts = self.market_start_ts
            expiry_ts = self.expiry_ts

        btc_price = row.get("btc_price")
        yes_mid = row.get("yes_mid")
        no_mid = row.get("no_mid")

        def recent_items(values: list[Any], window_s: float) -> list[Any]:
            if window_s <= 0:
                return values[-1:] if values else []
            threshold = now_mono - window_s
            return [item for item in values if item[0] >= threshold]

        def price_change_bps(current: Optional[float], reference: Optional[float]) -> Optional[float]:
            if current is None or reference in (None, 0.0):
                return None
            return ((current - reference) / reference) * 10000.0

        def flow_imbalance(events: list[tuple[float, Optional[float], Optional[float], Optional[str]]]) -> tuple[float, float]:
            signed_volume = 0.0
            total_volume = 0.0
            for _, _, size, side in recent_items(events, flow_window_s):
                if size is None or size <= 0:
                    continue
                signed_volume += _side_sign(side) * size
                total_volume += size
            imbalance = (signed_volume / total_volume) if total_volume > 0 else 0.0
            return imbalance, total_volume

        recent_btc = recent_items(btc_history, btc_momentum_window_s)
        btc_reference = recent_btc[0][1] if recent_btc else None
        btc_momentum_bps = price_change_bps(btc_price, btc_reference)
        btc_move_bps_from_open = price_change_bps(btc_price, market_btc_open_price)

        yes_flow_imbalance, yes_flow_volume = flow_imbalance(yes_trades)
        no_flow_imbalance, no_flow_volume = flow_imbalance(no_trades)
        net_flow_imbalance = (yes_flow_imbalance - no_flow_imbalance) / 2.0

        inferred_yes_price = yes_mid
        inferred_no_price = no_mid
        if inferred_yes_price is None and inferred_no_price is not None:
            inferred_yes_price = 1.0 - inferred_no_price
        if inferred_no_price is None and inferred_yes_price is not None:
            inferred_no_price = 1.0 - inferred_yes_price

        secs_since_btc = row.get("secs_since_last_btc_update")
        secs_since_market = row.get("secs_since_last_market_msg")
        feed_is_fresh = (
            secs_since_btc is not None
            and secs_since_market is not None
            and secs_since_btc <= heartbeat_timeout_s
            and secs_since_market <= heartbeat_timeout_s
        )

        elapsed_seconds = None
        elapsed_fraction = None
        seconds_to_expiry = row.get("seconds_to_expiry")
        if market_start_ts is not None and expiry_ts is not None:
            elapsed_seconds = max(0.0, min(float(expiry_ts - market_start_ts), time.time() - market_start_ts))
            total_window = max(1.0, float(expiry_ts - market_start_ts))
            elapsed_fraction = max(0.0, min(1.0, elapsed_seconds / total_window))

        row.update(
            {
                "market_start_ts": market_start_ts,
                "market_expiry_ts": expiry_ts,
                "market_btc_open_price": market_btc_open_price,
                "market_btc_open_ts_utc": market_btc_open_ts_utc,
                "btc_momentum_bps_window": btc_momentum_bps,
                "btc_move_bps_from_open": btc_move_bps_from_open,
                "yes_flow_imbalance": yes_flow_imbalance,
                "no_flow_imbalance": no_flow_imbalance,
                "net_flow_imbalance": net_flow_imbalance,
                "yes_flow_volume": yes_flow_volume,
                "no_flow_volume": no_flow_volume,
                "inferred_yes_price": inferred_yes_price,
                "inferred_no_price": inferred_no_price,
                "feed_is_fresh": feed_is_fresh,
                "elapsed_seconds": elapsed_seconds,
                "elapsed_fraction": elapsed_fraction,
                "seconds_to_expiry": seconds_to_expiry,
            }
        )
        return row


class WsDiag:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.reconnects: dict[str, int] = defaultdict(int)
        self.disconnects: dict[str, int] = defaultdict(int)

    def mark_reconnect(self, name: str, reason: str) -> None:
        with self.lock:
            self.reconnects[name] += 1
            count = self.reconnects[name]
        log_diag("ws_reconnect", websocket=name, reason=reason, reconnect_count=count)

    def mark_disconnect(self, name: str, reason: str) -> None:
        with self.lock:
            self.disconnects[name] += 1
            count = self.disconnects[name]
        log_diag("ws_disconnect", websocket=name, reason=reason, disconnect_count=count)


def start_rtds_ws(state: MarketState, stop_event: threading.Event, ws_diag: WsDiag, source: str = "chainlink") -> None:
    topic = "crypto_prices_chainlink" if source == "chainlink" else "crypto_prices"
    filters = '{"symbol":"btc/usd"}' if source == "chainlink" else "btcusdt"

    def on_open(ws: websocket.WebSocketApp) -> None:
        ws.send(json.dumps({
            "action": "subscribe",
            "subscriptions": [{
                "topic": topic,
                "type": "update",
                "filters": filters,
            }],
        }))
        log_diag("ws_open", websocket="btc", topic=topic)

    def on_message(ws: websocket.WebSocketApp, message: str) -> None:
        try:
            data = json.loads(message)
        except Exception:
            return

        if not isinstance(data, dict) or data.get("topic") != topic:
            return

        payload = data.get("payload") or {}
        if not isinstance(payload, dict):
            return

        value = safe_float(payload.get("value"))
        if value is not None:
            state.update_btc(value)

    def on_error(ws: websocket.WebSocketApp, error: Any) -> None:
        if not stop_event.is_set():
            ws_diag.mark_disconnect("btc", f"error:{error}")

    def on_close(ws: websocket.WebSocketApp, status_code: Any, msg: Any) -> None:
        if not stop_event.is_set():
            ws_diag.mark_disconnect("btc", f"close:{status_code}:{msg}")

    while not stop_event.is_set():
        ws = websocket.WebSocketApp(
            RTDS_WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        try:
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as exc:
            if not stop_event.is_set():
                ws_diag.mark_disconnect("btc", f"exception:{exc}")
        if not stop_event.is_set():
            ws_diag.mark_reconnect("btc", "loop_restart")
            time.sleep(2)


def start_poly_heartbeat(ws: websocket.WebSocketApp, stop_event: threading.Event) -> threading.Thread:
    def beat() -> None:
        while not stop_event.is_set():
            try:
                ws.send(json.dumps({}))
            except Exception:
                break
            time.sleep(10)

    thread = threading.Thread(target=beat, daemon=True)
    thread.start()
    return thread


def start_poly_ws(
    state: MarketState,
    stop_event: threading.Event,
    ws_diag: WsDiag,
    quiet: bool = False,
    rollover_poll: float = 1.0,
) -> None:
    current_slug: Optional[str] = None
    ws: Optional[websocket.WebSocketApp] = None
    ws_thread: Optional[threading.Thread] = None
    ws_stop: Optional[threading.Event] = None
    ws_lock = threading.Lock()

    def close_current_ws(reason: str) -> None:
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

        if reason != "init":
            log_diag("rollover_ws_closed", reason=reason)

    def build_ws_for_market(slug: str, question: str, yes_token: str, no_token: str) -> tuple[websocket.WebSocketApp, threading.Event]:
        local_stop = threading.Event()

        def on_open(local_ws: websocket.WebSocketApp) -> None:
            try:
                local_ws.send(json.dumps({
                    "assets_ids": [yes_token, no_token],
                    "type": "market",
                    "custom_feature_enabled": True,
                }))
                start_poly_heartbeat(local_ws, local_stop)
                log_diag("ws_open", websocket="poly", slug=slug, yes_token=yes_token, no_token=no_token)
                if not quiet:
                    print(f"Polymarket subscribed: {slug}", file=sys.stderr)
            except Exception as exc:
                if not quiet:
                    print(f"POLY subscribe error: {exc}", file=sys.stderr)

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
                best_bid, best_ask = best_bid_ask_from_book(item)
                spread = (best_ask - best_bid) if best_bid is not None and best_ask is not None else None
                state.update_asset_book(
                    asset_id=asset_id,
                    best_bid=best_bid,
                    best_ask=best_ask,
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
                changes = item.get("price_changes") or []
                if isinstance(changes, list):
                    for change in changes:
                        if not isinstance(change, dict):
                            continue
                        change_asset_id = str(change.get("asset_id") or change.get("assetId") or "")
                        state.update_asset_trade(
                            asset_id=change_asset_id,
                            price=safe_float(change.get("price")),
                            size=safe_float(change.get("size")),
                            side=str(change.get("side")) if change.get("side") is not None else None,
                        )

        def on_message(local_ws: websocket.WebSocketApp, message: str) -> None:
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

        def on_error(local_ws: websocket.WebSocketApp, error: Any) -> None:
            if not quiet and not local_stop.is_set():
                print(f"POLY WS error: {error}", file=sys.stderr)
            if not local_stop.is_set():
                ws_diag.mark_disconnect("poly", f"error:{error}")

        def on_close(local_ws: websocket.WebSocketApp, status_code: Any, msg: Any) -> None:
            local_stop.set()
            if not quiet and not stop_event.is_set():
                print(f"POLY WS closed: {status_code} {msg}", file=sys.stderr)
            if not stop_event.is_set():
                ws_diag.mark_disconnect("poly", f"close:{status_code}:{msg}")

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
                previous = current_slug
                market = fetch_market(slug)
                question = str(market.get("question", ""))
                clob_ids = ensure_list(market.get("clobTokenIds") or market.get("clob_token_ids"))
                if len(clob_ids) < 2:
                    time.sleep(2)
                    continue

                yes_token, no_token = clob_ids[0], clob_ids[1]

                close_current_ws("rollover_or_restart")
                state.reset_market(slug, question, yes_token, no_token)
                current_slug = slug

                log_diag(
                    "market_rollover",
                    previous_slug=previous,
                    new_slug=slug,
                    question=question,
                    yes_token=yes_token,
                    no_token=no_token,
                    restart_due_to_dead_thread=thread_dead,
                )

                if not quiet:
                    print(f"Tracking: {question}", file=sys.stderr)
                    print(f"YES token: {yes_token}", file=sys.stderr)
                    print(f"NO  token: {no_token}", file=sys.stderr)

                new_ws, new_stop = build_ws_for_market(slug, question, yes_token, no_token)
                new_thread = threading.Thread(target=lambda: new_ws.run_forever(ping_interval=20, ping_timeout=10), daemon=True)

                with ws_lock:
                    ws = new_ws
                    ws_stop = new_stop
                    ws_thread = new_thread

                new_thread.start()
                ws_diag.mark_reconnect("poly", "new_market_or_thread_restart")

            time.sleep(rollover_poll)
    finally:
        close_current_ws("shutdown")


def snapshot_writer(
    state: MarketState,
    stop_event: threading.Event,
    out_csv: str,
    interval: float,
    require_complete: bool = True,
    flush_interval: float = 1.0,
    flush_every_rows: int = 25,
) -> None:
    file_exists = False
    try:
        with open(out_csv, "r", encoding="utf-8"):
            file_exists = True
    except FileNotFoundError:
        pass

    rows_total = 0
    rows_in_market = 0
    current_slug: Optional[str] = None
    next_tick = time.monotonic()
    last_flush = time.monotonic()

    with open(out_csv, "a", newline="", encoding="utf-8", buffering=1024 * 1024) as handle:
        writer = csv.DictWriter(handle, fieldnames=SNAPSHOT_FIELDNAMES)
        if not file_exists:
            writer.writeheader()
            handle.flush()

        while not stop_event.is_set():
            tick_started = time.monotonic()
            row = state.snapshot_row()
            wrote_row = False

            if row is not None:
                slug = row.get("slug")
                if slug != current_slug:
                    if current_slug is not None:
                        log_diag("market_rows_written", slug=current_slug, rows=rows_in_market)
                    current_slug = str(slug) if slug is not None else None
                    rows_in_market = 0

                if require_complete:
                    ready = (
                        row["yes_best_bid"] is not None
                        and row["yes_best_ask"] is not None
                        and row["no_best_bid"] is not None
                        and row["no_best_ask"] is not None
                        and row["btc_price"] is not None
                    )
                    if ready:
                        writer.writerow(row)
                        wrote_row = True
                else:
                    writer.writerow(row)
                    wrote_row = True

            if wrote_row:
                rows_total += 1
                rows_in_market += 1

            now_mono = time.monotonic()
            if (rows_total > 0 and rows_total % flush_every_rows == 0) or (now_mono - last_flush >= flush_interval):
                handle.flush()
                last_flush = now_mono

            elapsed = time.monotonic() - tick_started
            jitter = elapsed - interval
            if jitter > max(0.05, interval * 0.5):
                log_diag(
                    "snapshot_tick_slow",
                    interval=interval,
                    tick_elapsed=elapsed,
                    jitter=jitter,
                    rows_total=rows_total,
                )

            next_tick += interval
            sleep_for = next_tick - time.monotonic()
            if sleep_for <= 0:
                next_tick = time.monotonic()
                continue
            time.sleep(sleep_for)

        handle.flush()

    if current_slug is not None:
        log_diag("market_rows_written", slug=current_slug, rows=rows_in_market)
    log_diag("snapshot_writer_stopped", out_csv=out_csv, rows_total=rows_total)


class LiveFeedAdapter:
    def __init__(
        self,
        btc_source: str = "chainlink",
        quiet: bool = False,
        rollover_poll: float = 1.0,
        state: Optional[MarketState] = None,
        ws_diag: Optional[WsDiag] = None,
    ) -> None:
        self.btc_source = btc_source
        self.quiet = quiet
        self.rollover_poll = rollover_poll
        self.state = state or MarketState()
        self.ws_diag = ws_diag or WsDiag()
        self.stop_event = threading.Event()
        self.threads: list[threading.Thread] = []

    def start(self) -> None:
        if self.threads:
            return
        self.threads = [
            threading.Thread(
                target=start_rtds_ws,
                args=(self.state, self.stop_event, self.ws_diag, self.btc_source),
                daemon=True,
            ),
            threading.Thread(
                target=start_poly_ws,
                args=(self.state, self.stop_event, self.ws_diag, self.quiet, self.rollover_poll),
                daemon=True,
            ),
        ]
        for thread in self.threads:
            thread.start()

    def stop(self) -> None:
        self.stop_event.set()

    def join(self, timeout: float = 3.0) -> None:
        for thread in self.threads:
            thread.join(timeout=timeout)

    def live_snapshot(
        self,
        btc_momentum_window_s: float = 30.0,
        flow_window_s: float = 15.0,
        heartbeat_timeout_s: float = 5.0,
    ) -> Optional[Dict[str, Any]]:
        return self.state.live_snapshot(
            btc_momentum_window_s=btc_momentum_window_s,
            flow_window_s=flow_window_s,
            heartbeat_timeout_s=heartbeat_timeout_s,
        )
