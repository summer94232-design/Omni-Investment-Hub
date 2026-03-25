# datahub/cache_client.py
import json
import logging
from typing import Any, Optional
import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


class CacheClient:
    """Redis Cache-Aside 客戶端"""

    def __init__(self, url: str = "redis://localhost:6379/0"):
        self._url = url
        self._client: Optional[aioredis.Redis] = None

    async def connect(self) -> None:
        self._client = aioredis.from_url(
            self._url,
            encoding="utf-8",
            decode_responses=True,
        )
        await self._client.ping()
        logger.info("Redis 連線成功：%s", self._url)

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()

    async def get(self, key: str) -> Optional[Any]:
        try:
            raw = await self._client.get(key)
            return json.loads(raw) if raw else None
        except Exception as e:
            logger.warning("Redis GET 失敗 key=%s: %s", key, e)
            return None

    async def set(self, key: str, value: Any, ttl: int = 0) -> bool:
        try:
            raw = json.dumps(value, ensure_ascii=False, default=str)
            if ttl > 0:
                await self._client.setex(key, ttl, raw)
            else:
                await self._client.set(key, raw)
            return True
        except Exception as e:
            logger.warning("Redis SET 失敗 key=%s: %s", key, e)
            return False

    async def delete(self, key: str) -> None:
        try:
            await self._client.delete(key)
        except Exception as e:
            logger.warning("Redis DELETE 失敗 key=%s: %s", key, e)

    async def exists(self, key: str) -> bool:
        try:
            return bool(await self._client.exists(key))
        except Exception:
            return False