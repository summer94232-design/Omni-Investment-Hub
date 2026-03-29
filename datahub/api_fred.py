# datahub/api_fred.py
import logging
from typing import Any, Optional
import httpx

logger = logging.getLogger(__name__)

FRED_BASE_URL = "https://api.stlouisfed.org/fred"

# 常用系列 ID
SERIES = {
    "fed_funds_rate": "FEDFUNDS",
    "us_10y_yield":   "DGS10",
    "us_2y_yield":    "DGS2",
    "vix":            "VIXCLS",
    "us_cpi_yoy":     "CPIAUCSL",
    "us_pmi":         "MMPCI",        # [BUG 10 修正] 原 MANEMP（製造業就業人數）→ MMPCI（ISM PMI Composite）
    "us_gdp_growth":  "A191RL1Q225SBEA",
    "us_unemployment":"UNRATE",
}


class FredAPI:
    """FRED 總經數據 API 封裝"""

    def __init__(self, api_key: str):
        self._api_key = api_key
        self._client = httpx.AsyncClient(timeout=30.0)

    async def close(self) -> None:
        await self._client.aclose()

    async def get_series(
        self,
        series_id: str,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        limit: int = 10,
    ) -> list[dict]:
        """取得指定系列的觀測值"""
        params: dict[str, Any] = {
            "series_id":   series_id,
            "api_key":     self._api_key,
            "file_type":   "json",
            "sort_order":  "desc",
            "limit":       limit,
        }
        if observation_start:
            params["observation_start"] = observation_start
        if observation_end:
            params["observation_end"] = observation_end

        try:
            resp = await self._client.get(
                f"{FRED_BASE_URL}/series/observations",
                params=params,
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("observations", [])
        except httpx.HTTPError as e:
            logger.error("FRED API 錯誤 series=%s: %s", series_id, e)
            return []

    async def get_latest(self, series_id: str) -> Optional[float]:
        """取得最新一筆有效數值"""
        obs = await self.get_series(series_id, limit=10)
        for o in obs:
            if o.get("value") not in (".", "", None):
                try:
                    return float(o["value"])
                except ValueError:
                    continue
        return None

    async def get_macro_snapshot(self) -> dict[str, Optional[float]]:
        """一次取得所有宏觀指標的最新值"""
        result = {}
        for name, sid in SERIES.items():
            result[name] = await self.get_latest(sid)
            logger.debug("FRED %s (%s) = %s", name, sid, result[name])
        return result