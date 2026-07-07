"""Telegram webhook handler — routes callback_query and text messages."""

from __future__ import annotations

import hashlib
import logging
import requests
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .pending_approvals import PendingApprovalStore
from .telegram_bot import TelegramBot

log = logging.getLogger("triforge.telegram.webhook")


@dataclass
class ConversationState:
    short_run_id: str
    phase: str
    file_hash: str
    chat_id: int
    message_id: int
    created_at: float


class TelegramWebhookHandler:
    """Routes incoming Telegram updates to approval handlers.

    Singleton per process. Thread-safe.
    """

    _instance: Optional["TelegramWebhookHandler"] = None
    _lock = threading.Lock()

    def __init__(self):
        self.bots: Dict[str, TelegramBot] = {}
        self.channels: Dict[str, Dict[str, Any]] = {}
        self.pending = PendingApprovalStore()
        self.conversations: Dict[Tuple[int, int], ConversationState] = {}
        self._conv_lock = threading.Lock()
        self._port: int = 8000

    @classmethod
    def instance(cls) -> "TelegramWebhookHandler":
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def set_port(self, port: int) -> None:
        self._port = port

    def register_channel(self, channel: Dict[str, Any]) -> None:
        token = channel.get("bot_token")
        if not token:
            return
        self.bots[token] = TelegramBot(token)
        self.channels[token] = channel

    def handle_update(self, update: Dict[str, Any],
                      channel: Dict[str, Any]) -> Dict[str, Any]:
        """Synchronous entry point. Called from board.py via run_in_executor."""
        bot_token = channel.get("bot_token")
        bot = self.bots.get(bot_token)
        if not bot:
            log.error("handle_update: no bot for token %s...", str(bot_token)[:8])
            return {"ok": False, "error": "no bot"}

        if "callback_query" in update:
            return self._handle_callback(update["callback_query"], bot, channel)
        elif "message" in update:
            return self._handle_message(update["message"], bot, channel)
        else:
            log.debug("unhandled update kind: %s", list(update.keys()))
            return {"ok": False, "error": "unhandled kind"}

    def handle_update_dry_run(self, update: Dict[str, Any]) -> Dict[str, Any]:
        """Dry-run for test-webhook endpoint. Does not actually approve."""
        channel = {"bot_token": "__test__"}
        bot = TelegramBot("__test__")
        query = update.get("callback_query", {})
        data = query.get("data", "")
        try:
            action, short_id, phase, file_hash = data.split(":", 3)
        except (ValueError, AttributeError):
            return {"ok": False, "error": "invalid callback_data format"}
        pending = self.pending.get(short_id, file_hash)
        return {
            "ok": True,
            "action": action,
            "short_id": short_id,
            "phase": phase,
            "file_hash": file_hash,
            "pending_exists": pending is not None,
        }

    def _handle_callback(self, query: Dict[str, Any],
                         bot: TelegramBot,
                         channel: Dict[str, Any]) -> Dict[str, Any]:
        user_id = query.get("from", {}).get("id")
        user_name = query.get("from", {}).get("first_name", "unknown")
        data = query.get("data", "")
        chat_id = query.get("message", {}).get("chat", {}).get("id")
        message_id = query.get("message", {}).get("message_id")

        if not self._is_authorized(user_id, channel):
            bot.answer_callback(query["id"], "⛔ Unauthorized user")
            log.warning("unauthorized callback from user_id=%s", user_id)
            return {"ok": False, "error": "unauthorized"}

        try:
            action, short_id, phase, file_hash = data.split(":", 3)
        except ValueError:
            bot.answer_callback(query["id"], "⛔ Invalid callback")
            return {"ok": False, "error": "invalid callback_data"}

        if action == "approve":
            return self._do_approve(short_id, phase, file_hash, "approve",
                                    f"approved via Telegram by {user_name}",
                                    str(user_id), bot, chat_id, message_id,
                                    query["id"], user_name)
        elif action == "reject":
            return self._do_approve(short_id, phase, file_hash, "reject",
                                    "rejected via Telegram",
                                    str(user_id), bot, chat_id, message_id,
                                    query["id"], user_name)
        elif action == "reply":
            with self._conv_lock:
                self.conversations[(user_id, chat_id)] = ConversationState(
                    short_run_id=short_id,
                    phase=phase,
                    file_hash=file_hash,
                    chat_id=chat_id,
                    message_id=message_id,
                    created_at=time.time(),
                )
            bot.answer_callback(
                query["id"],
                "请输入审批意见 (30 字内)",
                show_alert=True,
            )
            return {"ok": True, "action": "reply", "state_set": True}
        else:
            bot.answer_callback(query["id"], f"⛔ Unknown action: {action}")
            return {"ok": False, "error": f"unknown action: {action}"}

    def _handle_message(self, message: Dict[str, Any],
                        bot: TelegramBot,
                        channel: Dict[str, Any]) -> Dict[str, Any]:
        user_id = message.get("from", {}).get("id")
        chat_id = message.get("chat", {}).get("id")
        text = message.get("text", "").strip()

        if not self._is_authorized(user_id, channel):
            return {"ok": False, "error": "unauthorized"}

        with self._conv_lock:
            state = self.conversations.pop((user_id, chat_id), None)
        if not state:
            bot.send_message(chat_id, "无待审批消息。请先点 Reply 按钮。")
            return {"ok": False, "error": "no pending conversation"}

        return self._do_approve(
            state.short_run_id, state.phase, state.file_hash,
            "approve", text[:200], str(user_id),
            bot, state.chat_id, state.message_id,
            None, message.get("from", {}).get("first_name", "unknown"),
        )

    def _do_approve(self, short_id: str, phase: str, file_hash: str,
                    decision: str, comment: str, user_id_str: str,
                    bot: TelegramBot, chat_id: int, message_id: int,
                    callback_query_id: Optional[str],
                    user_name: str) -> Dict[str, Any]:
        pending = self.pending.get(short_id, file_hash)
        if not pending:
            msg = "⛔ 已过期或已审批"
            if callback_query_id:
                bot.answer_callback(callback_query_id, msg)
            else:
                bot.send_message(chat_id, msg)
            return {"ok": False, "error": "expired or already processed"}

        try:
            r = requests.post(
                f"http://127.0.0.1:{self._port}/board/runs/{pending.full_run_id}/approve",
                json={"decision": decision, "comment": comment},
                headers={"X-Approved-By-Telegram": user_id_str},
                timeout=10,
            )
            r.raise_for_status()
        except Exception as e:
            log.exception("internal approve failed: %s", e)
            if callback_query_id:
                bot.answer_callback(callback_query_id,
                                    f"⛔ Approve failed: {type(e).__name__}")
            return {"ok": False, "error": str(e)}

        self.pending.consume(short_id, file_hash)

        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        new_text = (
            f"✓ {decision.capitalize()}d by @{user_name} "
            f"(id={user_id_str}) @ {ts}\n"
            f"  ---\n"
            f"  [TriForge · 待审批] {pending.full_run_id}\n"
            f"  phase={phase}  (已{'批准' if decision=='approve' else '拒绝'} ✓)"
        )
        try:
            bot.edit_message_text(chat_id, message_id, new_text)
        except Exception as e:
            log.warning("editMessageText failed, sending new message: %s", e)
            bot.send_message(chat_id, new_text)

        if callback_query_id:
            bot.answer_callback(
                callback_query_id,
                "✓" if decision == "approve" else "✓ Rejected",
            )

        return {"ok": True, "decision": decision}

    def _is_authorized(self, user_id: int, channel: Dict[str, Any]) -> bool:
        allowed = channel.get("allowed_user_ids") or []
        if not allowed:
            return True
        return user_id in allowed
