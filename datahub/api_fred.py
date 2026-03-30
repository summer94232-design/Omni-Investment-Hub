# datahub/api_fred.py
# ═══════════════════════════════════════════════════════════════════════════════
# 變更紀錄（v3.3）：
# - [BUG 10 修正] us_pmi Series ID MANEMP → MMPCI（已含於 v3.1）
# - [信用利差] 新增 credit_spread（BAA10Y）與 hy_spread（BAMLH0A0HYM2）
#     用於 LIQUIDITY_CRISIS 黑天鵝情境的真實數據觸發
# - [財經日曆] 新增 get_upcoming_releases()：
#     取得 FRED 未來 N 天的重要數據發布計畫
#     供 EventCalendar 自動寫入 calendar_events（取代純手動新增）
# ═══════════════════════════════════════════════════════════════════════════════

import logging
from datetime import date, timedelta
from typing import Any, Optional
import httpx

logger = logging.getLogger(__name__)

FRED_BASE_URL = "https://api.stlouisfed.org/fred"

SERIES = {
    "fed_funds_rate": "FEDFUNDS",
    "us_10y_yield":   "DGS10",
    "us_2y_yield":    "DGS2",
    "vix":            "VIXCLS",
    "us_cpi_yoy":     "CPIAUCSL",
    "us_pmi":         "MMPCI",          # [BUG 10 修正] 原 MANEMP → MMPCI（ISM PMI Composite）
    "us_gdp_growth":  "A191RL1Q225SBEA",
    "us_unemployment":"UNRATE",
    # ── v3.3 新增 ──────────────────────────────────────────────────────────
    "credit_spread":  "BAA10Y",         # Baa 公司債與 10 年美債利差（bp 單位需 ×100）
    "hy_spread":      "BAMLH0A0HYM2",   # ICE BofA 高收益債 OAS（更激進的壓力指標）
}

# FRED 重大發布的 Release ID（用於自動財經日曆）
IMPORTANT_RELEASE_IDS = {
    10:  "Employment Situation (NFP)",
    21:  "Consumer Price Index (CPI)",
    11:  "GDP",
    82:  "Producer Price Index (PPI)",
    50:  "FOMC Meeting",
    167: "ISM Manufacturing PMI",
    113: "Retail Sales",
    19:  "Housing Starts",
}


class FredAPI:
    """FRED 總經數據 API 封裝"""

    def __init__(self, api_key: str):
        self._api_key = api_key
        self._client  = httpx.AsyncClient(timeout=30.0)

    async def close(self) -> None:
        await self._client.aclose()

    async def get_series(
        self,
        series_id: str,
        observation_start: Optional[str] = None,
        observation_end:   Optional[str] = None,
        limit: int = 10,
    ) -> list[dict]:
        """取得指定系列的觀測值"""
        params: dict[str, Any] = {
            "series_id":  series_id,
            "api_key":    self._api_key,
            "file_type":  "json",
            "sort_order": "desc",
            "limit":      limit,
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
        """一次取得所有宏觀指標的最新值（含 v3.3 新增的信用利差）"""
        result = {}
        for name, sid in SERIES.items():
            result[name] = await self.get_latest(sid)
            logger.debug("FRED %s (%s) = %s", name, sid, result[name])
        return result

    async def get_upcoming_releases(
        self,
        days_ahead: int = 14,
    ) -> list[dict]:
        """
        取得 FRED 未來 N 天的重大數據發布計畫。
        供 EventCalendar 自動寫入 calendar_events，減少人工維護。

        回傳清單格式：
        [
            {
                'release_date': '2026-04-02',
                'release_name': 'Employment Situation (NFP)',
                'release_id':   10,
                'impact_level': 5,   # 1-5 分，依 IMPORTANT_RELEASE_IDS 決定是否重要
            },
            ...
        ]
        只回傳 IMPORTANT_RELEASE_IDS 中定義的重要發布。
        """
        today     = date.today()
        end_date  = today + timedelta(days=days_ahead)
        params: dict[str, Any] = {
            "api_key":      self._api_key,
            "file_type":    "json",
            "realtime_start": str(today),
            "realtime_end":   str(end_date),
            "include_release_dates_with_no_data": "true",
            "limit": 1000,
        }
        try:
            resp = await self._client.get(
                f"{FRED_BASE_URL}/releases/dates",
                params=params,
            )
            resp.raise_for_status()
            data = resp.json()
            raw_releases = data.get("release_dates", [])
        except httpx.HTTPError as e:
            logger.error("FRED releases API 錯誤: %s", e)
            return []

        results = []
        seen = set()
        for r in raw_releases:
            rid  = r.get("release_id")
            rdate = r.get("date")
            if rid in IMPORTANT_RELEASE_IDS and (rid, rdate) not in seen:
                seen.add((rid, rdate))
                results.append({
                    "release_date": rdate,
                    "release_name": IMPORTANT_RELEASE_IDS[rid],
                    "release_id":   rid,
                    "impact_level": 5 if rid in (10, 21, 50) else 4,
                })

        results.sort(key=lambda x: x["release_date"])
        logger.info("FRED 財經日曆：未來 %d 天共 %d 個重大發布", days_ahead, len(results))
        return results