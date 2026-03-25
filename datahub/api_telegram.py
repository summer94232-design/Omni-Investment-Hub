# datahub/api_telegram.py
import logging
from typing import Optional
import httpx

logger = logging.getLogger(__name__)

TELEGRAM_BASE_URL = "https://api.telegram.org"


class TelegramBot:
    """Telegram Bot 通知封裝"""

    def __init__(self, bot_token: str, chat_id: str):
        self._token = bot_token
        self._chat_id = chat_id
        self._client = httpx.AsyncClient(timeout=15.0)

    async def close(self) -> None:
        await self._client.aclose()

    async def send(self, message: str, silent: bool = False) -> bool:
        """發送訊息"""
        try:
            resp = await self._client.post(
                f"{TELEGRAM_BASE_URL}/bot{self._token}/sendMessage",
                json={
                    "chat_id":              self._chat_id,
                    "text":                 message,
                    "parse_mode":           "HTML",
                    "disable_notification": silent,
                },
            )
            resp.raise_for_status()
            return True
        except httpx.HTTPError as e:
            logger.error("Telegram 發送失敗: %s", e)
            return False

    async def send_alert(self, title: str, body: str, level: str = "INFO") -> bool:
        """發送格式化警報"""
        emoji = {"INFO": "ℹ️", "WARN": "⚠️", "ERROR": "🚨", "SUCCESS": "✅"}.get(level, "📢")
        message = f"{emoji} <b>{title}</b>\n\n{body}"
        return await self.send(message)

    async def ping(self) -> bool:
        """測試 Bot 是否正常"""
        return await self.send("🟢 Omni-Investment Hub 連線測試成功")