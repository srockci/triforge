"""Direct client for Tencent's iLink Bot API.

Reference: iLink_Bot_API_Documentation.md (community reverse-engineered docs).
Used by TriForge to drive personal-WeChat notifications from inside our
own process — no bridge daemon, no OpenClaw, no extra process the
user has to install.

The iLink protocol basics:
  - POST https://ilinkai.weixin.qq.com/ilink/bot/...
  - Bearer token in `Authorization: Bearer <bot_token>` (header name
    `AuthorizationType: ilink_bot_token`)
  - X-WECHAT-UIN: random base64(uint32) per request, anti-replay
  - Body: {msg: {...}, base_info: {channel_version: "1.0.3"}}

For our use case (one-way push notifications) we only need three
endpoints:
  - GET  /ilink/bot/get_bot_qrcode?bot_type=3   → scan-time QR
  - GET  /ilink/bot/get_qrcode_status?qrcode=... → wait for confirmed
  - POST /ilink/bot/sendmessage                  → push a text

We deliberately do NOT call getupdates (long-polling for inbound
messages) — TriForge is notification-only and won't act as an inbound
chat. If we ever want to, we'd add a per-user background task that
holds the getupdates connection and routes inbound messages to an
agent. For now: outbound only.
"""
from __future__ import annotations

import base64
import logging
import os
import secrets
import uuid
from typing import Any, Dict, Optional, Tuple

import requests


log = logging.getLogger("triforge.wechat_bot")


# Default iLink base. TriForge doesn't let users change this — it's
# the single canonical Tencent endpoint, same as OpenClaw uses.
# Override via env var TRIFORGE_ILINK_BASE_URL for testing/staging.
import os as _os
DEFAULT_BASE_URL = _os.environ.get(
    "TRIFORGE_ILINK_BASE_URL", "https://ilinkai.weixin.qq.com"
)

# Channel version, kept in lockstep with the @tencent-weixin/openclaw-weixin
# plugin's reported version. Per the docs: 1.0.3 / 2.0.0 are both seen;
# we default to the more recent one to match newer plugin releases.
DEFAULT_CHANNEL_VERSION = "2.0.0"


def _new_wechat_uin() -> str:
    """Generate the X-WECHAT-UIN header value: random uint32 → str → base64.

    The docs say: "随机 4 字节 → uint32 → 十进制字符串 → base64 编码".
    Each request gets a fresh value to defeat server-side replay checks.
    """
    n = int.from_bytes(secrets.token_bytes(4), "big", signed=False)
    return base64.b64encode(str(n).encode("ascii")).decode("ascii")


def _new_client_id() -> str:
    """Per-message unique UUID. The docs warn that a duplicate client_id
    gets silently dropped, so we always generate a fresh one."""
    return str(uuid.uuid4())


class WeChatBot:
    """Thin wrapper over iLink API for one paired account.

    Construct with the `confirmed` payload returned by
    `/ilink/bot/get_qrcode_status` once the user scans the QR:
        bot = WeChatBot(bot_token=..., ilink_bot_id=..., baseurl=...)
    Or use the convenience classmethod `WeChatBot.from_pairing(...)`
    which handles the long-poll loop for you.
    """

    def __init__(self,
                 bot_token: str,
                 ilink_bot_id: str,
                 baseurl: str = DEFAULT_BASE_URL,
                 to_user_id: Optional[str] = None,
                 channel_version: str = DEFAULT_CHANNEL_VERSION,
                 timeout: float = 10.0):
        self.bot_token = bot_token
        self.ilink_bot_id = ilink_bot_id
        # Default: send "to self" — appears in WeChat as a Saved-messages
        # entry. Most iLink-style bots do this for proactive notifications.
        self.to_user_id = to_user_id or ilink_bot_id
        self.baseurl = baseurl.rstrip("/")
        self.channel_version = channel_version
        self._timeout = timeout

    # ---------- HTTP helpers ----------

    def _headers(self) -> Dict[str, str]:
        return {
            "Content-Type":       "application/json",
            "AuthorizationType":  "ilink_bot_token",
            "Authorization":      f"Bearer {self.bot_token}",
            "X-WECHAT-UIN":       _new_wechat_uin(),
        }

    def _post(self, path: str, payload: Dict[str, Any],
              timeout: Optional[float] = None) -> Dict[str, Any]:
        url = f"{self.baseurl}{path}"
        body = json_dumps(payload)  # utf-8, length-known
        # Some iLink endpoints are picky about Content-Length / chunked
        # transfer; we set it explicitly to avoid "偶发超时" pitfalls.
        headers = self._headers()
        headers["Content-Length"] = str(len(body.encode("utf-8")))
        r = requests.post(
            url, data=body.encode("utf-8"),
            headers=headers,
            timeout=timeout if timeout is not None else self._timeout,
        )
        if r.status_code // 100 != 2:
            raise RuntimeError(
                f"iLink POST {path} → HTTP {r.status_code}: {r.text[:200]}"
            )
        # sendmessage returns `{}` on success — treat empty body as
        # success too.
        if not r.content:
            return {}
        try:
            return r.json()
        except ValueError:
            return {}

    def _get(self, path: str, timeout: Optional[float] = None) -> requests.Response:
        url = f"{self.baseurl}{path}"
        return requests.get(
            url, headers=self._headers(),
            timeout=timeout if timeout is not None else self._timeout,
        )

    # ---------- Public API ----------

    def send_text(self, text: str) -> None:
        """Push a plain-text message to the bound WeChat account.

        Maps to `POST /ilink/bot/sendmessage` with a single text item.
        `context_token` is left empty — the docs say it's required for
        replies, but in practice iLink accepts an empty string for
        proactive sends to a known bot (i.e. self-message). If your
        iLink version rejects this, we can fall back to "send a
        no-op message to get a context_token, then use that" — but
        for the common 1.0.3 / 2.0.0 channels it works.
        """
        msg = {
            "from_user_id":  "",
            "to_user_id":    self.to_user_id,
            "client_id":     _new_client_id(),
            "message_type":  2,   # 2 = BOT
            "message_state": 2,   # 2 = FINISH
            "context_token": "",
            "item_list":     [
                {"type": 1, "text_item": {"text": text}},
            ],
        }
        payload = {
            "msg":       msg,
            "base_info": {"channel_version": self.channel_version},
        }
        # sendmessage returns `{}` on success — that's the documented
        # "200 OK with empty body" behavior, not an error.
        result = self._post("/ilink/bot/sendmessage", payload)
        # Some versions also include `ret: 0`. We don't depend on the
        # payload — an empty dict is success.
        if isinstance(result, dict) and result.get("ret") not in (None, 0):
            raise RuntimeError(
                f"iLink sendmessage ret={result.get('ret')}: {result}"
            )

    # ---------- Pairing helpers (static) ----------

    @staticmethod
    def fetch_qrcode(bot_type: int = 3,
                     baseurl: str = DEFAULT_BASE_URL,
                     timeout: float = 10.0) -> Dict[str, Any]:
        """GET /ilink/bot/get_bot_qrcode?bot_type=N → {qrcode, qrcode_img_content}.

        bot_type=3 is the documented "WeChat personal account" type
        (vs. bot_type=2 for service accounts).
        """
        url = f"{baseurl}/ilink/bot/get_bot_qrcode"
        r = requests.get(
            url,
            params={"bot_type": bot_type},
            headers={
                "AuthorizationType": "ilink_bot_token",
                # No Bearer yet — login QR has no token.
                "X-WECHAT-UIN":      _new_wechat_uin(),
            },
            timeout=timeout,
        )
        if r.status_code // 100 != 2:
            raise RuntimeError(
                f"iLink get_bot_qrcode → HTTP {r.status_code}: {r.text[:200]}"
            )
        data = r.json()
        if "qrcode" not in data or "qrcode_img_content" not in data:
            raise RuntimeError(f"iLink get_bot_qrcode: missing fields: {data}")
        return data

    @staticmethod
    def poll_status(qrcode: str,
                    baseurl: str = DEFAULT_BASE_URL,
                    timeout: float = 45.0) -> Dict[str, Any]:
        """GET /ilink/bot/get_qrcode_status?qrcode=... (long-polling).

        Returns the raw response: {status, bot_token, ilink_bot_id, baseurl}
        if confirmed, or {status: 'wait'|'scaned'|'expired'} otherwise.
        """
        url = f"{baseurl}/ilink/bot/get_qrcode_status"
        r = requests.get(
            url,
            params={"qrcode": qrcode},
            headers={
                "AuthorizationType": "ilink_bot_token",
                "X-WECHAT-UIN":      _new_wechat_uin(),
            },
            timeout=timeout,
        )
        if r.status_code // 100 != 2:
            raise RuntimeError(
                f"iLink get_qrcode_status → HTTP {r.status_code}: {r.text[:200]}"
            )
        return r.json()

    @classmethod
    def from_pairing(cls, qrcode: str, *args, **kwargs) -> "WeChatBot":
        """Block until the user scans + confirms, then return a ready
        WeChatBot. Raises RuntimeError on expired / unexpected status.
        """
        baseurl = kwargs.pop("baseurl", DEFAULT_BASE_URL)
        status = cls.poll_status(qrcode, baseurl=baseurl)
        if status.get("status") != "confirmed":
            raise RuntimeError(f"iLink pairing not confirmed: {status}")
        return cls(
            bot_token=status["bot_token"],
            ilink_bot_id=status["ilink_bot_id"],
            baseurl=status.get("baseurl") or baseurl,
            *args, **kwargs,
        )


def json_dumps(obj: Any) -> str:
    """Tiny wrapper for json.dumps with the iLink-friendly defaults.

    iLink doesn't care about unicode escaping — they speak Chinese
    natively, so we keep ensure_ascii=False. Separators use commas +
    spaces for readability; iLink accepts either.
    """
    import json
    return json.dumps(obj, ensure_ascii=False, separators=(", ", ": "))