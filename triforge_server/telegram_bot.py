"""Thin wrapper around Telegram Bot API (sync, using requests)."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List

import requests

log = logging.getLogger("triforge.telegram")


class TelegramBot:
    """Sync Telegram Bot API client.

    Used from notifier._dispatch (sync) and from board.py async endpoints
    via asyncio.to_thread or run_in_executor.
    """

    BASE = "https://api.telegram.org/bot"

    def __init__(self, bot_token: str):
        self.token = bot_token
        self._base = f"{self.BASE}{bot_token}"

    def _post(self, method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self._base}/{method}"
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code // 100 != 2:
            raise RuntimeError(f"telegram {method} HTTP {r.status_code}: {r.text[:200]}")
        data = r.json()
        if not data.get("ok"):
            raise RuntimeError(f"telegram {method} !ok: {data}")
        return data.get("result")

    def send_message(self, chat_id: int, text: str,
                     parse_mode: str = "Markdown") -> Dict[str, Any]:
        return self._post("sendMessage", {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        })

    def send_with_buttons(self, chat_id: int, text: str,
                          inline_keyboard: List[List[Dict[str, str]]],
                          parse_mode: str = "Markdown") -> Dict[str, Any]:
        return self._post("sendMessage", {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
            "reply_markup": json.dumps({"inline_keyboard": inline_keyboard}),
        })

    def edit_message_text(self, chat_id: int, message_id: int,
                          new_text: str) -> Dict[str, Any]:
        return self._post("editMessageText", {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": new_text,
            "parse_mode": "Markdown",
        })

    def edit_message_reply_markup(self, chat_id: int, message_id: int,
                                  inline_keyboard: List[List[Dict[str, str]]]
                                  ) -> Dict[str, Any]:
        return self._post("editMessageReplyMarkup", {
            "chat_id": chat_id,
            "message_id": message_id,
            "reply_markup": json.dumps({"inline_keyboard": inline_keyboard}),
        })

    def answer_callback(self, callback_query_id: str,
                        text: str = "",
                        show_alert: bool = False) -> Dict[str, Any]:
        return self._post("answerCallbackQuery", {
            "callback_query_id": callback_query_id,
            "text": text[:200],
            "show_alert": show_alert,
        })

    def set_webhook(self, url: str, secret_token: str) -> bool:
        result = self._post("setWebhook", {
            "url": url,
            "secret_token": secret_token,
            "allowed_updates": ["callback_query", "message"],
        })
        return bool(result)

    def delete_webhook(self) -> bool:
        result = self._post("deleteWebhook", {})
        return bool(result)

    def get_updates(self, offset: int = 0, timeout: int = 25) -> List[Dict[str, Any]]:
        result = self._post("getUpdates", {
            "offset": offset,
            "timeout": timeout,
            "allowed_updates": ["callback_query", "message"],
        })
        return result if isinstance(result, list) else []
