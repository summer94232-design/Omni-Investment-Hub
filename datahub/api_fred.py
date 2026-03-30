# datahub/api_fred.py
# ═══════════════════════════════════════════════════════════════════════════════
# 變更紀錄（v3.4）：
# - [v3.3] 新增 credit_spread / hy_spread / get_upcoming_releases()
# - [v3.4 新增] SERIES 補充 us_5y_yield（DGS5）與 us_30y_yield（DGS30）
#              建構更完整的利率曲線：2Y / 5Y / 10Y / 30Y
#              供 MacroFilter._calc_mrs() 分析曲線形狀（熊平/熊陡/牛平/牛陡）
# ═══════════════════════════════════════════════════════════════════════════════

import logging
from datetime import date, timedelta
from typing import Any, Optional
import httpx

logger = logging.getLogger(__name__)

FRED_BASE_URL = "https://api.stlouisfed.org/fred"

SERIES = {
    # ── 核心利率指標 ───────────────────────────────────────────────────────
    "fed_funds_rate":  "FEDFUNDS",          # 聯邦基金利率
    "us_2y_yield":     "DGS2",              # 2 年期美債殖利率
    "us_5y_yield":     "DGS5",              # 5 年期美債殖利率  ← v3.4 新增
    "us_10y_yield":    "DGS10",             # 10 年期美債殖利率
    "us_30y_yield":    "DGS30",             # 30 年期美債殖利率 ← v3.4 新增
    # ── 市場情緒指標 ───────────────────────────────────────────────────────
    "vix":             "VIXCLS",            # CBOE VIX 恐慌指數
    # ── 通膨 / 景氣指標 ────────────────────────────────────────────────────
    "us_cpi_yoy":      "CPIAUCSL",          # 美國 CPI 年增率
    "us_pmi":          "MMPCI",             # ISM 製造業 PMI（v3.1 修正）
    "us_gdp_growth":   "A191RL1Q225SBEA",   # GDP 實質年增率（季）
    "us_unemployment": "UNRATE",            # 美國失業率
    # ── 信用利差指標 ───────────────────────────────────────────────────────
    "credit_spread":   "BAA10Y",            # Baa 公司債與 10Y 美債利差
    "hy_spread":       "BAMLH0A0HYM2",      # ICE BofA 高收益債 OAS
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
    """FRED 總經數據 API 封裝（v3.4）"""

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
        """
        一次取得所有宏觀指標的最新值。
        v3.4：含 5Y / 30Y 殖利率，供利率曲線形狀分析。
        """
        result = {}
        for name, sid in SERIES.items():
            result[name] = await self.get_latest(sid)
            logger.debug("FRED %s (%s) = %s", name, sid, result[name])
        return result

    async def get_yield_curve(self) -> dict[str, Optional[float]]:
        """
        [v3.4 新增] 取得完整利率曲線快照（2Y / 5Y / 10Y / 30Y）。

        回傳：
        {
            'us_2y':  float | None,
            'us_5y':  float | None,
            'us_10y': float | None,
            'us_30y': float | None,
            'spread_10y_2y':  float | None,  # 常見殖差
            'spread_30y_5y':  float | None,  # 長端殖差
            'curve_shape':    str,            # 'NORMAL' / 'FLAT' / 'INVERTED' / 'HUMPED'
        }
        """
        us_2y  = await self.get_latest(SERIES["us_2y_yield"])
        us_5y  = await self.get_latest(SERIES["us_5y_yield"])
        us_10y = await self.get_latest(SERIES["us_10y_yield"])
        us_30y = await self.get_latest(SERIES["us_30y_yield"])

        spread_10y_2y = None
        spread_30y_5y = None
        curve_shape   = "UNKNOWN"

        if us_2y and us_10y:
            spread_10y_2y = round(us_10y - us_2y, 4)

        if us_5y and us_30y:
            spread_30y_5y = round(us_30y - us_5y, 4)

        # 利率曲線形狀判斷
        if spread_10y_2y is not None:
            if spread_10y_2y > 1.0:
                curve_shape = "NORMAL"       # 正常陡峭
            elif spread_10y_2y > 0.0:
                curve_shape = "FLAT"         # 趨平
            elif spread_10y_2y > -0.5:
                curve_shape = "MILD_INVERT"  # 輕度倒掛（警示）
            else:
                curve_shape = "INVERTED"     # 嚴重倒掛（衰退訊號）

        logger.info(
            "殖利率曲線：2Y=%.3f 5Y=%.3f 10Y=%.3f 30Y=%.3f spread(10-2)=%.3f 形狀=%s",
            us_2y or 0, us_5y or 0, us_10y or 0, us_30y or 0,
            spread_10y_2y or 0, curve_shape,
        )

        return {
            "us_2y":         us_2y,
            "us_5y":         us_5y,
            "us_10y":        us_10y,
            "us_30y":        us_30y,
            "spread_10y_2y": spread_10y_2y,
            "spread_30y_5y": spread_30y_5y,
            "curve_shape":   curve_shape,
        }

    async def get_upcoming_releases(self, days_ahead: int = 14) -> list[dict]:
        """取得 FRED 未來 N 天的重大數據發布計畫（供 EventCalendarAutoSync）"""
        today    = date.today()
        end_date = today + timedelta(days=days_ahead)
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
            rid   = r.get("release_id")
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