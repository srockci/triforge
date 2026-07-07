"""ILinkGateway — long-poll client that activates a WeChat bot for sending.

Each paired WeChat account gets one ILinkGateway instance. The gateway runs
three daemon threads per instance:
  - getupdates_loop  — long-poll GET /ilink/bot/getupdates (keeps bot ACTIVE)
  - watchdog_loop    — detects stale connections (30s no activity)
  - sender_thread    — consumes enqueue() calls and POSTs sendmessage

GatewayManager is a module-level singleton that boot_from_settings() spawns
for every personal_wechat channel at server startup.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from enum import Enum
from queue import Empty, Queue
from typing import Any, Callable, Dict, List, Optional

import requests

log = logging.getLogger("triforge.ilink_gateway")


class State(str, Enum):
    STARTED = "STARTED"
    WAITING_PAIRING = "WAITING_PAIRING"
    CONNECTING = "CONNECTING"
    ACTIVE = "ACTIVE"
    RECONNECTING = "RECONNECTING"
    DEGRADED = "DEGRADED"


class ILinkGateway:
    """Long-poll gateway for one bot_token.

    Call .start() after construction to begin the background threads.
    Call .stop() during shutdown.
    Call .enqueue(text) from notifier.py to send a message asynchronously.
    """

    def __init__(
        self,
        bot_token: str,
        ilink_bot_id: str,
        baseurl: str,
        channel_key: str,
        *,
        on_state_change: Optional[Callable[[str, State, State], None]] = None,
    ):
        self.bot_token = bot_token
        self.ilink_bot_id = ilink_bot_id
        self.baseurl = baseurl.rstrip("/")
        self.channel_key = channel_key
        self._on_state_change = on_state_change

        self.state: State = State.STARTED
        self._state_lock = threading.Lock()

        self.last_active_at: float = 0.0
        self.last_error: Optional[str] = None
        self.reconnect_count: int = 0
        self.messages_sent_ok: int = 0
        self.messages_dropped: int = 0

        self._stop_event = threading.Event()
        self._send_queue: Queue[str] = Queue()
        self._consecutive_fails = 0
        self._backoff = 1.0

    # ── public API ──

    def start(self) -> None:
        """Spawn daemon threads. Non-blocking."""
        self._set_state(State.CONNECTING)
        threading.Thread(target=self._run_getupdates, daemon=True,
                         name=f"gu-{self.channel_key}").start()
        threading.Thread(target=self._run_watchdog, daemon=True,
                         name=f"wd-{self.channel_key}").start()
        threading.Thread(target=self._run_sender, daemon=True,
                         name=f"sd-{self.channel_key}").start()

    def stop(self, timeout: float = 5.0) -> None:
        """Signal all threads to stop and wait up to *timeout* seconds."""
        self._stop_event.set()
        # Threads are daemon — if timeout expires they'll be killed at exit.

    def enqueue(self, message: str) -> None:
        """Queue a message for async sending. Returns immediately."""
        self._send_queue.put_nowait(message)

    def snapshot(self) -> Dict[str, Any]:
        """Return a JSON-safe summary for the dashboard banner."""
        with self._state_lock:
            return {
                "channel_key": self.channel_key,
                "state": self.state.value,
                "last_error": self.last_error,
                "last_active_at": self.last_active_at,
                "reconnect_count": self.reconnect_count,
                "messages_sent_ok": self.messages_sent_ok,
                "messages_dropped": self.messages_dropped,
                "ilink_bot_id": self.ilink_bot_id,
            }

    # ── internal thread loops ──

    def _run_getupdates(self) -> None:
        """Long-poll loop. Keeps the bot ACTIVE on iLink's side."""
        while not self._stop_event.is_set():
            try:
                from .wechat_bot import ilink_get

                r = ilink_get(
                    self.bot_token, self.baseurl,
                    "/ilink/bot/getupdates", timeout=45.0,
                )
                if r.status_code == 200:
                    self.last_active_at = time.monotonic()
                    data = r.json() if r.content else {}
                    updates = data.get("updates") or []
                    if len(updates) > 100:
                        log.warning(
                            "%s: getupdates returned %d updates, dropping (v1)",
                            self.channel_key, len(updates),
                        )
                    elif updates:
                        log.info(
                            "%s: got %d updates, dropping (v1)",
                            self.channel_key, len(updates),
                        )
                    self._transition_to_active_if_needed()
                    self._consecutive_fails = 0
                    self._backoff = 1.0
                elif r.status_code in (401, 403):
                    self._set_state(
                        State.DEGRADED,
                        last_error=f"auth_{r.status_code}",
                    )
                    break
                else:
                    self._maybe_reconnecting(f"HTTP {r.status_code}")
            except requests.Timeout:
                self._maybe_reconnecting("timeout")
            except requests.RequestException as e:
                self._maybe_reconnecting(str(e))

    def _run_watchdog(self) -> None:
        """If no getupdates activity for 30s, mark RECONNECTING."""
        while not self._stop_event.is_set():
            if self._stop_event.wait(timeout=5.0):
                break
            if self.state is State.ACTIVE:
                age = time.monotonic() - self.last_active_at
                if age > 30.0:
                    self._set_state(
                        State.RECONNECTING,
                        last_error="watchdog: 30s no activity",
                    )

    def _run_sender(self) -> None:
        """Consume from send_queue and POST sendmessage."""
        while not self._stop_event.is_set():
            try:
                msg = self._send_queue.get(timeout=1.0)
            except Empty:
                continue
            if self.state is not State.ACTIVE:
                log.warning(
                    "%s: sender dropping msg, state=%s",
                    self.channel_key, self.state.value,
                )
                self.messages_dropped += 1
                continue
            try:
                from .wechat_bot import (
                    ilink_post,
                    _new_client_id,
                    DEFAULT_CHANNEL_VERSION,
                )
                payload = {
                    "msg": {
                        "from_user_id": "",
                        "to_user_id": self.ilink_bot_id,
                        "client_id": _new_client_id(),
                        "message_type": 2,
                        "message_state": 2,
                        "context_token": "",
                        "item_list": [{"type": 1, "text_item": {"text": msg}}],
                    },
                    "base_info": {"channel_version": DEFAULT_CHANNEL_VERSION},
                }
                r = ilink_post(
                    self.bot_token, self.baseurl,
                    "/ilink/bot/sendmessage", payload,
                )
                if r.status_code // 100 != 2:
                    raise RuntimeError(f"HTTP {r.status_code}")
                self.messages_sent_ok += 1
            except Exception as e:
                log.error("%s: sender error: %s", self.channel_key, e)
                self._maybe_reconnecting(f"sender: {e}")

    # ── state helpers ──

    def _set_state(self, new_state: State, *, last_error: Optional[str] = None) -> None:
        old_state = self.state
        with self._state_lock:
            self.state = new_state
            if last_error:
                self.last_error = last_error
        if old_state != new_state:
            log.info(
                "%s: state %s → %s%s",
                self.channel_key, old_state.value, new_state.value,
                f" ({last_error})" if last_error else "",
            )
            if self._on_state_change:
                self._on_state_change(self.channel_key, old_state, new_state)

    def _transition_to_active_if_needed(self) -> None:
        if self.state in (State.CONNECTING, State.RECONNECTING):
            self._set_state(State.ACTIVE, last_error=None)

    def _maybe_reconnecting(self, reason: str) -> None:
        self._consecutive_fails += 1
        if self._consecutive_fails >= 3:
            self._set_state(State.RECONNECTING, last_error=reason)
            self._backoff = min(self._backoff * 2, 60.0)
            time.sleep(self._backoff)


class GatewayManager:
    """Module-level singleton that manages all ILinkGateway instances."""

    _instance: Optional["GatewayManager"] = None
    _lock = threading.Lock()

    def __init__(self):
        self._gateways: Dict[str, ILinkGateway] = {}

    @classmethod
    def instance(cls) -> "GatewayManager":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @classmethod
    def boot_from_settings(cls) -> int:
        """Read personal_wechat channels from settings and spawn gateways.

        Returns the number of gateways created.
        """
        from .settings import get_settings
        mgr = get_settings()
        cfg = mgr.get()
        channels = cfg.get("notification_channels") or []
        gw = cls.instance()
        count = 0
        for ch in channels:
            if ch.get("type") != "personal_wechat":
                continue
            bot_token = ch.get("bot_token")
            ilink_bot_id = ch.get("ilink_bot_id")
            if not bot_token or not ilink_bot_id:
                continue
            channel_key = ch.get("__channel_key__") or bot_token[:8]
            if channel_key in gw._gateways:
                continue
            gateway = ILinkGateway(
                bot_token=bot_token,
                ilink_bot_id=ilink_bot_id,
                baseurl=ch.get("baseurl", "https://ilinkai.weixin.qq.com"),
                channel_key=channel_key,
            )
            gateway.start()
            gw._gateways[channel_key] = gateway
            count += 1
        if count:
            log.info("GatewayManager: spawned %d gateway(s) from settings", count)
        return count

    @classmethod
    def shutdown_all(cls, timeout: float = 5.0) -> None:
        gw = cls._instance
        if gw is None:
            return
        for key, gateway in gw._gateways.items():
            log.info("GatewayManager: stopping %s", key)
            gateway.stop(timeout=timeout)
        gw._gateways.clear()
        log.info("GatewayManager: all gateways stopped")

    def register(self, channel_key: str, bot_token: str,
                 ilink_bot_id: str, baseurl: str) -> ILinkGateway:
        gateway = ILinkGateway(
            bot_token=bot_token,
            ilink_bot_id=ilink_bot_id,
            baseurl=baseurl,
            channel_key=channel_key,
        )
        gateway.start()
        self._gateways[channel_key] = gateway
        return gateway

    def unregister(self, channel_key: str) -> None:
        gw = self._gateways.pop(channel_key, None)
        if gw:
            gw.stop()

    def lookup(self, channel_key: str) -> Optional[ILinkGateway]:
        return self._gateways.get(channel_key)

    def snapshot(self) -> List[Dict[str, Any]]:
        return [gw.snapshot() for gw in self._gateways.values()]
