"""Multi-platform notification dispatcher (飞书 / 企业微信 / 钉钉 / Telegram).

Supports two delivery modes per channel:
  simple  — only milestones: run_end (complete/fail/cancel), approval_requested
            (needs user action), agent_error. Low noise.
  complex — every event flows through. Useful while debugging or keeping
            a full audit trail.

Synchronous dispatcher — `publish(ev)` fans out to all enabled channels
in the calling thread. Webhook latency is typically < 100 ms per channel,
so this does not meaningfully back-pressure the request path.

Channel configuration lives in settings.json under
`notification_channels`. Each channel has its own type, mode, and
platform-specific credentials (webhook URL or bot token). Channels can
be enabled/disabled independently.

Per-platform message format: plain text (Markdown) rendered as the
target platform supports. Telegram gets parse_mode=Markdown; Feishu /
DingTalk / WeChat get a card-style or plain-text variant.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import requests

from .events import BoardEvent
from .settings import get_settings
from .store import get_store


log = logging.getLogger("triforge.notifier")

# In-memory notification delivery history (for UI display)
_NOTIFICATION_STATE = type("S", (), {"history": []})()


def record_notification(channel_type: str, kind: str, ok: bool, detail: str) -> None:
    """Append to in-memory ring buffer of delivery outcomes."""
    _NOTIFICATION_STATE.history.append({
        "ts": time.time(),
        "channel": channel_type,
        "kind": kind,
        "ok": ok,
        "detail": detail[:200],
    })
    if len(_NOTIFICATION_STATE.history) > 200:
        del _NOTIFICATION_STATE.history[:-200]


def get_notification_history(limit: int = 50) -> list:
    return list(_NOTIFICATION_STATE.history)[-limit:]


# -------- two-mode policy ------------------------------------------------
SIMPLE_EVENT_KINDS = {
    "run_end",            # completed / failed / cancelled — final state
    "approval_requested", # the user must act on this
    "agent_error",        # anything went wrong mid-pipeline
}
# Everything else (phase_start, phase_end, tool_call, token_usage,
# approval_resolved, run_start, run_resumed, force_stop) only fires
# under complex mode.


def _should_send(mode: str, kind: str) -> bool:
    if mode == "complex":
        return True
    return kind in SIMPLE_EVENT_KINDS


# -------- message formatting ---------------------------------------------

_STATUS_EMOJI = {
    "awaiting_approval": "⏸️",
    "running":            "🏃",
    "completed":          "✅",
    "failed":             "❌",
    "cancelled":          "🚫",
    "interrupted":        "⏳",
}


def _format_run_end(ev: BoardEvent, run_state: Dict[str, Any]) -> str:
    """Final-state notification. Always sent in simple mode."""
    data = ev.data or {}
    status = data.get("status", "unknown")
    err = data.get("error", "")
    summary = data.get("outputs", {}) if status == "completed" else {}
    cost = run_state.get("cost_estimate", 0)
    tok_in = run_state.get("tokens_in", 0)
    tok_out = run_state.get("tokens_out", 0)

    head = f"[TriForge · 完成] {ev.run_id}" if status == "completed" \
        else f"[TriForge · 失败] {ev.run_id}"
    lines = [head]
    if status == "completed":
        out_files = summary if isinstance(summary, list) else []
        if out_files:
            lines.append(f"  产出: {len(out_files)} 个文件")
    if status == "failed" and err:
        lines.append(f"  错误: {err[:200]}")
    if cost:
        lines.append(f"  费用: ¥{cost:.4f}  /  Tokens: {tok_in:,} in, {tok_out:,} out")
    return "\n".join(lines)


def _format_approval(ev: BoardEvent) -> str:
    data = ev.data or {}
    phase = data.get("phase", "?")
    tool = data.get("tool", "?")
    args = data.get("args") or {}
    path = args.get("path", "?")
    preview = (data.get("preview") or "")[:300]
    return (
        f"[TriForge · 待审批] {ev.run_id}  ·  phase={phase}\n"
        f"  Tool: {tool}    Path: {path}\n"
        f"  ---\n  {preview}\n  ..."
    )


def _format_error(ev: BoardEvent) -> str:
    data = ev.data or {}
    phase = data.get("phase", "?")
    err = data.get("error", "unknown")
    return (
        f"[TriForge · 错误] {ev.run_id}  ·  phase={phase}\n"
        f"  {err[:300]}"
    )


def _format_phase(ev: BoardEvent) -> str:
    data = ev.data or {}
    phase = data.get("phase", "?")
    agent = data.get("agent", "?")
    model = data.get("model", "?")
    ok = data.get("ok")
    if ev.kind == "phase_start":
        return f"[TriForge] {ev.run_id}  ·  ▶ phase={phase} ({agent} · {model})"
    if ev.kind == "phase_end":
        suffix = " ✓" if ok else f" ✗ ({data.get('error','')[:80]})"
        return f"[TriForge] {ev.run_id}  ·  ■ phase={phase}{suffix}"
    return f"[TriForge] {ev.run_id}  ·  {phase}"


def _format_tool_call(ev: BoardEvent) -> str:
    data = ev.data or {}
    args = data.get("args") or {}
    return (
        f"[TriForge] {ev.run_id}  ·  {data.get('tool','?')} -> "
        f"{args.get('path','?')}"
    )


def _format_token(ev: BoardEvent) -> str:
    data = ev.data or {}
    return (
        f"[TriForge] {ev.run_id}  ·  +{data.get('tokens_in',0):,} in "
        f"/ +{data.get('tokens_out',0):,} out  "
        f"  ¥{data.get('cost',0):.4f}  ({data.get('model','?')})"
    )


def _format_run_start(ev: BoardEvent) -> str:
    req = (ev.data or {}).get("requirement", "")[:200]
    return f"[TriForge · 启动] {ev.run_id}\n  {req}"


def _format_run_resumed(ev: BoardEvent) -> str:
    return f"[TriForge · 续] {ev.run_id}  ·  phase={ev.data.get('phase','?')}"


_FORMATTERS: Dict[str, Callable[[BoardEvent], str]] = {
    "run_end":              _format_run_end,
    "approval_requested":   _format_approval,
    "agent_error":          _format_error,
    "phase_start":          _format_phase,
    "phase_end":            _format_phase,
    "tool_call":            _format_tool_call,
    "token_usage":          _format_token,
    "run_start":            _format_run_start,
    "run_resumed":          _format_run_resumed,
}


def format_event(ev: BoardEvent) -> Optional[str]:
    """Translate a BoardEvent to a human-readable message, or None if
    we don't have a formatter for it (caller decides whether to skip)."""
    fmt = _FORMATTERS.get(ev.kind)
    if not fmt:
        return None
    try:
        # Hand the run-state to formatters that may want to include
        # cost / tokens in the run_end message.
        snap = get_store().snapshot(ev.run_id) or {}
        if ev.kind == "run_end":
            return _format_run_end(ev, snap)
        return fmt(ev)
    except Exception as e:  # formatting must never crash dispatch
        log.exception("format_event failed for %s", ev.kind)
        return None


# -------- per-platform senders -------------------------------------------

class PlatformError(Exception):
    pass


class Notifier:
    """Base class. Subclasses implement `send` for one platform."""
    platform = "abstract"

    def __init__(self, channel: Dict[str, Any]):
        self.channel = channel

    def send(self, message: str) -> None:
        raise NotImplementedError

    # some platforms support a quick test ping from the UI
    def test(self) -> None:
        self.send(f"[TriForge] 测试消息 — 通道 {self.platform} 接入正常。")


class FeishuNotifier(Notifier):
    platform = "feishu"

    def send(self, message: str) -> None:
        url = self.channel.get("webhook_url")
        if not url:
            raise PlatformError("feishu: webhook_url missing")
        # Feishu supports plain text via {"msg_type":"text"...}. Newer
        # bot types also accept post / interactive cards; we keep text.
        payload = {"msg_type": "text", "content": {"text": message}}
        if self.channel.get("at_all_on_error"):
            payload["content"]["text"] = "<at user_id=\"all\">所有人</at> " + message
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code // 100 != 2:
            raise PlatformError(f"feishu HTTP {r.status_code}: {r.text[:200]}")


class DingTalkNotifier(Notifier):
    platform = "dingtalk"

    def send(self, message: str) -> None:
        url = self.channel.get("webhook_url")
        secret = self.channel.get("secret")
        if not url:
            raise PlatformError("dingtalk: webhook_url missing")
        if secret:
            import base64
            import hashlib
            import hmac
            import urllib.parse
            ts = str(round(time.time() * 1000))
            string_to_sign = f"{ts}\n{secret}"
            digest = hmac.new(
                string_to_sign.encode("utf-8"),
                digestmod=hashlib.sha256,
            ).digest()
            sign = urllib.parse.quote_plus(base64.b64encode(digest))
            url = f"{url}&timestamp={ts}&sign={sign}"
        payload = {"msgtype": "text", "text": {"content": message}}
        if self.channel.get("at_all_on_error"):
            payload["text"]["content"] = "@所有人 " + message
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code // 100 != 2:
            raise PlatformError(f"dingtalk HTTP {r.status_code}: {r.text[:200]}")


class WeChatWorkNotifier(Notifier):
    """Enterprise WeChat 'group robot' webhook (微信 Clawbot style)."""

    platform = "wechatwork"

    def send(self, message: str) -> None:
        url = self.channel.get("webhook_url")
        if not url:
            raise PlatformError("wechatwork: webhook_url missing")
        payload = {"msgtype": "text", "text": {"content": message}}
        if self.channel.get("at_all_on_error"):
            payload["text"]["content"] = "@所有人\n" + message
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code // 100 != 2:
            raise PlatformError(f"wechatwork HTTP {r.status_code}: {r.text[:200]}")


class TelegramNotifier(Notifier):
    platform = "telegram"

    def send(self, message: str) -> None:
        token = self.channel.get("bot_token")
        chat = self.channel.get("chat_id")
        if not token or not chat:
            raise PlatformError("telegram: bot_token and chat_id required")
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        r = requests.post(
            url,
            json={
                "chat_id": chat,
                "text": message,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        if r.status_code // 100 != 2:
            raise PlatformError(f"telegram HTTP {r.status_code}: {r.text[:200]}")


class WeChatBotNotifier(Notifier):
    """Personal WeChat via ILinkGateway (long-poll keep-alive) + direct API.

    The user pairs their WeChat once by scanning a QR that TriForge
    fetched from iLink's `get_bot_qrcode` endpoint. After that, TriForge
    holds the bot_token locally and runs a background ILinkGateway that
    keeps the bot ACTIVE on iLink's side via getupdates long-poll.

    Channel fields:
        bot_token     — iLink bot token (stored after QR scan)
        ilink_bot_id  — the bot's iLink user id
        baseurl       — usually the canonical iLink host; rarely overridden
    """

    platform = "personal_wechat"

    def send(self, message: str) -> None:
        bot_token    = self.channel.get("bot_token")
        ilink_bot_id = self.channel.get("ilink_bot_id")
        if not bot_token or not ilink_bot_id:
            raise PlatformError(
                "personal_wechat: channel is not paired. "
                "Open Settings → Notifications and click 'Connect Personal WeChat'."
            )

        from .ilink_gateway import GatewayManager, State
        channel_key = self.channel.get("__channel_key__") or bot_token[:8]
        gateway = GatewayManager.instance().lookup(channel_key)

        if gateway is None:
            raise PlatformError(
                "personal_wechat: notifier ready but gateway not spawned yet. "
                "If this persists 30s after start, check server logs."
            )

        if gateway.state is State.ACTIVE:
            gateway.enqueue(message)
            return
        if gateway.state is State.DEGRADED:
            raise PlatformError(
                "personal_wechat: bot is DEGRADED (offline / revoked). "
                "Open Settings → Notifications and click 'Re-connect Personal WeChat'."
            )
        raise PlatformError(
            f"personal_wechat: gateway state={gateway.state.value}, "
            f"message not sent. Will recover automatically."
        )


_PLATFORM_REGISTRY: Dict[str, type] = {
    "feishu":         FeishuNotifier,
    "dingtalk":       DingTalkNotifier,
    "wechatwork":     WeChatWorkNotifier,
    "telegram":       TelegramNotifier,
    "personal_wechat": WeChatBotNotifier,
}

# Friendly display labels for UI
PLATFORM_LABELS = {
    "feishu":         "飞书 (Feishu / Lark)",
    "dingtalk":       "钉钉 (DingTalk)",
    "wechatwork":     "企业微信 (WeChat Work, Clawbot-style)",
    "telegram":       "Telegram",
    "personal_wechat": "个人微信 (Personal WeChat via weixin-agent-bridge)",
}


def list_platforms() -> List[Dict[str, str]]:
    return [{"type": t, "label": PLATFORM_LABELS[t]} for t in _PLATFORM_REGISTRY]


def build_notifier(channel: Dict[str, Any]) -> Notifier:
    cls = _PLATFORM_REGISTRY.get(channel.get("type"))
    if not cls:
        raise ValueError(f"unknown platform: {channel.get('type')}")
    return cls(channel)


# -------- synchronous dispatcher ----------------------------------------

def publish(ev: BoardEvent) -> None:
    """Fan-out to all enabled channels according to per-channel mode.
    Synchronous — intended to be called from request handlers or the
    global EventBus emit path. Webhook latency is typically <100 ms,
    so this does not meaningfully block the agent pipeline."""
    _dispatch(ev)


def _dispatch(ev: BoardEvent) -> None:
    """Internal: fan-out to all enabled channels (called from worker)."""
    settings = get_settings().get()
    channels = settings.get("notification_channels") or []
    if not channels:
        return
    for ch in channels:
        if not ch.get("enabled", False):
            continue
        mode = ch.get("mode", "simple")
        if not _should_send(mode, ev.kind):
            continue
        try:
            msg = format_event(ev)
        except Exception:
            log.exception("format failed")
            continue
        if not msg:
            continue
        try:
            notifier = build_notifier(ch)
            notifier.send(msg)
            record_notification(ch.get("type", "?"), ev.kind, True, "ok")
            log.debug("notifier[%s] delivered %s", ch.get("type"), ev.kind)
        except Exception as e:
            record_notification(ch.get("type", "?"), ev.kind, False,
                                f"{type(e).__name__}: {e}")
            log.warning(
                "notifier[%s] failed for %s: %s",
                ch.get("type"), ev.kind, e,
            )



