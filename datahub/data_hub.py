# datahub/data_hub.py
import asyncio
import logging
from typing import Any, Optional
import asyncpg
import yaml
from .cache_client import CacheClient

logger = logging.getLogger(__name__)


class DataHub:
    """統一資料中樞：所有模組透過此類存取 DB 與 Cache"""

    def __init__(self, config_path: str = "config.yaml"):
        with open(config_path, "r", encoding="utf-8") as f:
            self._cfg = yaml.safe_load(f)
        self._pool: Optional[asyncpg.Pool] = None
        self.cache = CacheClient(self._cfg["redis"]["url"])

    async def connect(self) -> None:
        db = self._cfg["database"]
        self._pool = await asyncpg.create_pool(
            host=db.get("host", "localhost"),
            port=db.get("port", 5432),
            user=db["user"],
            password=db["password"],
            database=db["name"],
            min_size=2,
            max_size=10,
        )
        await self.cache.connect()
        logger.info("DataHub 連線完成（PostgreSQL + Redis）")

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
        await self.cache.close()

    async def fetch(self, query: str, *args) -> list[asyncpg.Record]:
        async with self._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args) -> Optional[asyncpg.Record]:
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def execute(self, query: str, *args) -> str:
        async with self._pool.acquire() as conn:
            return await conn.execute(query, *args)

    async def executemany(self, query: str, args_list: list) -> None:
        async with self._pool.acquire() as conn:
            await conn.executemany(query, args_list)