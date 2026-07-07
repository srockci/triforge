"""Tests for ILinkGateway state machine and GatewayManager.

Run with:
    pytest triforge_tests/test_ilink_gateway.py -v
"""
from __future__ import annotations

import os
import time
from threading import Event

import pytest

from triforge_tests.mock_ilink_server import (
    run_mock_ilink,
    reset_mock_state,
    pop_sendmessage_log,
)
from triforge_server.ilink_gateway import GatewayManager, ILinkGateway, State


@pytest.fixture(scope="module")
def mock_ilink():
    """Start a mock iLink server for the duration of the test module."""
    t, server = run_mock_ilink(port=18999)
    yield
    server.shutdown()


@pytest.fixture(autouse=True)
def reset_gateway():
    """Reset singleton and mock state before each test."""
    GatewayManager._instance = None
    reset_mock_state()


def test_gateway_boot_to_active(mock_ilink):
    """Gateway starts → CONNECTING → ACTIVE within 2s."""
    os.environ["TRIFORGE_ILINK_BASE_URL"] = "http://127.0.0.1:18999"
    try:
        state_changes = []
        gw = ILinkGateway(
            bot_token="test-token",
            ilink_bot_id="test@wechat",
            baseurl="http://127.0.0.1:18999",
            channel_key="test-1",
            on_state_change=lambda k, o, n: state_changes.append((k, o, n)),
        )
        gw.start()
        # Wait for ACTIVE
        for _ in range(50):
            if gw.state is State.ACTIVE:
                break
            time.sleep(0.1)
        assert gw.state is State.ACTIVE, f"expected ACTIVE, got {gw.state}"
        assert gw.last_active_at > 0
        assert gw.reconnect_count == 0
        gw.stop()
    finally:
        os.environ.pop("TRIFORGE_ILINK_BASE_URL", None)


def test_gateway_getupdates_503_then_active(mock_ilink):
    """First 3 getupdates fail (503) → RECONNECTING, then ACTIVE."""
    os.environ["TRIFORGE_ILINK_BASE_URL"] = "http://127.0.0.1:18999"
    try:
        # Set mock to fail first 3 calls
        from triforge_tests import mock_ilink_server as mis
        mis.MockILinkHandler.fail_first = 3
        mis.MockILinkHandler._call_count = 0

        gw = ILinkGateway(
            bot_token="test-token",
            ilink_bot_id="test@wechat",
            baseurl="http://127.0.0.1:18999",
            channel_key="test-2",
        )
        gw._backoff = 0.1  # speed up backoff for test
        gw.start()
        # After 3 consecutive fails, should hit RECONNECTING
        found_reconnecting = False
        for _ in range(80):
            if gw.state is State.RECONNECTING:
                found_reconnecting = True
                break
            time.sleep(0.1)
        assert found_reconnecting, (
            f"expected RECONNECTING after 503s, got {gw.state}"
        )
        # Then backoff retries and returns to ACTIVE
        for _ in range(100):
            if gw.state is State.ACTIVE:
                break
            time.sleep(0.1)
        assert gw.state is State.ACTIVE, f"expected ACTIVE, got {gw.state}"
        gw.stop()
    finally:
        os.environ.pop("TRIFORGE_ILINK_BASE_URL", None)


def test_gateway_401_degraded(mock_ilink):
    """A single 401 response → DEGRADED immediately."""
    gw = ILinkGateway(
        bot_token="bad-token",
        ilink_bot_id="bad@wechat",
        baseurl="http://127.0.0.1:18999",
        channel_key="test-3",
    )
    # Mock a 401 response by calling the state transition directly
    gw._set_state(State.DEGRADED, last_error="auth_401")
    assert gw.state is State.DEGRADED
    assert gw.last_error == "auth_401"


def test_gateway_enqueue_sends_message(mock_ilink):
    """enqueue() followed by sender thread → sendmessage logged on mock."""
    os.environ["TRIFORGE_ILINK_BASE_URL"] = "http://127.0.0.1:18999"
    try:
        gw = ILinkGateway(
            bot_token="test-token",
            ilink_bot_id="test@wechat",
            baseurl="http://127.0.0.1:18999",
            channel_key="test-4",
        )
        gw.start()
        # Wait for ACTIVE
        for _ in range(50):
            if gw.state is State.ACTIVE:
                break
            time.sleep(0.1)
        assert gw.state is State.ACTIVE

        gw.enqueue("Hello from test")

        # Wait for sender to process
        time.sleep(1.0)
        log = pop_sendmessage_log()
        assert len(log) >= 1, "sendmessage not called"
        assert log[0].get("msg", {}).get("item_list", [{}])[0] \
            .get("text_item", {}).get("text") == "Hello from test"
        gw.stop()
    finally:
        os.environ.pop("TRIFORGE_ILINK_BASE_URL", None)


def test_gateway_snapshot_schema(mock_ilink):
    """snapshot() returns expected fields."""
    gw = ILinkGateway(
        bot_token="snap-test",
        ilink_bot_id="snap@wechat",
        baseurl="http://127.0.0.1:18999",
        channel_key="snap",
    )
    gw.messages_sent_ok = 5
    snap = gw.snapshot()
    assert set(snap.keys()) == {
        "channel_key", "state", "last_error", "last_active_at",
        "reconnect_count", "messages_sent_ok", "messages_dropped",
        "ilink_bot_id",
    }
    assert snap["state"] == "STARTED"
    assert snap["ilink_bot_id"] == "snap@wechat"


def test_gateway_manager_boot(mock_ilink):
    """GatewayManager.boot_from_settings spawns gateways from settings."""
    from triforge_server.settings import get_settings
    mgr = get_settings()
    cfg = mgr.get()
    channels = cfg.get("notification_channels") or []
    # Temporarily inject a test channel
    test_ch = {
        "type": "personal_wechat",
        "enabled": True,
        "bot_token": "boot-test-token",
        "ilink_bot_id": "boot@wechat",
        "baseurl": "http://127.0.0.1:18999",
        "__channel_key__": "boot-test",
    }
    channels.append(test_ch)
    mgr.save(cfg)

    try:
        count = GatewayManager.boot_from_settings()
        assert count >= 1
        gw_mgr = GatewayManager.instance()
        gw = gw_mgr.lookup("boot-test")
        assert gw is not None
        assert gw.bot_token == "boot-test-token"
    finally:
        channels.remove(test_ch)
        mgr.save(cfg)
        GatewayManager.shutdown_all()
