# datahub/api_finmind.py
# ═══════════════════════════════════════════════════════════════════════════════
# 變更紀錄（v3.3）：
# - [大盤成交量] 新增 get_twii_volume()
# - [加權指數收盤價] 新增 get_twii_price()
# - [除息點數] 新增 get_upcoming_dividends()
# - [基本面資料] 新增 get_financial_statements()：EPS / ROE / 營收成長率
# - [估值資料]   新增 get_per_pbr()：PE / PB / 殖利率（每日更新）
# - [題材新聞]   新增 get_topic_news_count()：取代 topic_radar 隨機模擬數據
# ═══════════════════════════════════════════════════════════════════════════════

import logging
from datetime import date, timedelta
from typing import Any, Optional
import httpx
import pandas as pd

logger = logging.getLogger(__name__)

FINMIND_BASE_URL = "https://api.finmindtrade.com/api/v4"

TWII_TICKER = "Y9999"


class FinMindAPI:
    """FinMind 台股數據 API 封裝"""

    def __init__(self, token: str):
        self._token = token
        self._client = httpx.AsyncClient(timeout=30.0)
        # 新聞快取（key: trade_date_str → list[dict]），避免同日多次請求
        self._news_cache: dict[str, list[dict]] = {}

    async def close(self) -> None:
        await self._client.aclose()

    async def _get(self, dataset: str, params: dict[str, Any]) -> list[dict]:
        params["token"]   = self._token
        params["dataset"] = dataset
        try:
            resp = await self._client.get(
                f"{FINMIND_BASE_URL}/data",
                params=params,
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") != 200:
                logger.error("FinMind 錯誤: %s", data.get("msg"))
                return []
            return data.get("data", [])
        except httpx.HTTPError as e:
            logger.error("FinMind HTTP 錯誤 dataset=%s: %s", dataset, e)
            return []

    # ── 原有方法 ─────────────────────────────────────────────────────────────

    async def get_stock_price(
        self,
        ticker: str,
        start_date: str,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """取得個股日K（OHLCV）"""
        params: dict[str, Any] = {
            "data_id":    ticker,
            "start_date": start_date,
        }
        if end_date:
            params["end_date"] = end_date
        rows = await self._get("TaiwanStockPrice", params)
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        rename_map = {
            "date": "date", "open": "open", "max": "max",
            "min": "min", "close": "close", "volume": "volume",
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
        df["close"] = pd.to_numeric(df.get("close", pd.Series(dtype=float)), errors="coerce")
        return df

    async def get_chip_data(
        self,
        ticker: str,
        start_date: str,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """取得外資/投信籌碼（InstitutionalInvestors）"""
        params: dict[str, Any] = {
            "data_id":    ticker,
            "start_date": start_date,
        }
        if end_date:
            params["end_date"] = end_date
        rows = await self._get("TaiwanStockInstitutionalInvestors", params)
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    async def get_revenue(
        self,
        ticker: str,
        start_date: str,
    ) -> pd.DataFrame:
        """取得月營收"""
        rows = await self._get("TaiwanStockMonthRevenue", {
            "data_id":    ticker,
            "start_date": start_date,
        })
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        if "revenue" in df.columns:
            df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
        return df

    async def get_margin_trading(
        self,
        ticker: str,
        start_date: str,
    ) -> pd.DataFrame:
        """取得信用交易（融資/融券）"""
        rows = await self._get("TaiwanStockMarginPurchaseShortSale", {
            "data_id":    ticker,
            "start_date": start_date,
        })
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    async def get_twii_price(self, days: int = 5) -> Optional[float]:
        """取得加權指數最新收盤價（供 BasisFilter 計算分母）"""
        start = str(date.today() - timedelta(days=days + 5))
        rows  = await self._get("TaiwanStockPrice", {
            "data_id":    TWII_TICKER,
            "start_date": start,
        })
        if not rows:
            return None
        df = pd.DataFrame(rows).sort_values("date")
        return float(df["close"].iloc[-1]) if not df.empty else None

    async def get_twii_volume(self, days: int = 5) -> Optional[float]:
        """取得大盤日成交量（元）"""
        start = str(date.today() - timedelta(days=days + 5))
        rows  = await self._get("TaiwanStockPrice", {
            "data_id":    TWII_TICKER,
            "start_date": start,
        })
        if not rows:
            return None
        df = pd.DataFrame(rows).sort_values("date")
        return float(df["Trading_money"].iloc[-1]) if "Trading_money" in df.columns and not df.empty else None

    async def get_upcoming_dividends(self, days_ahead: int = 30) -> float:
        """取得未來 N 日預計除息點數加總（供基差計算）"""
        start = str(date.today())
        end   = str(date.today() + timedelta(days=days_ahead))
        rows  = await self._get("TaiwanStockDividendResult", {
            "start_date": start,
            "end_date":   end,
        })
        if not rows:
            return 0.0
        total = sum(float(r.get("StockExDividend", 0) or 0) for r in rows)
        return round(total, 4)

    # ── v3.3 新增方法 ─────────────────────────────────────────────────────────

    async def get_financial_statements(
        self,
        ticker: str,
        quarters: int = 8,
    ) -> pd.DataFrame:
        """
        取得綜合損益表（季報），用於計算 EPS 成長率、ROE。

        dataset: TaiwanStockFinancialStatements
        回傳欄位包含: date, type, value（單位：千元）

        用法範例：
            df = await finmind.get_financial_statements('2330')
            eps_rows = df[df['type'] == 'EPS']
        """
        start = str(date.today() - timedelta(days=365 * (quarters // 4 + 1)))
        rows  = await self._get("TaiwanStockFinancialStatements", {
            "data_id":    ticker,
            "start_date": start,
        })
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        if "value" in df.columns:
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
        return df

    async def get_per_pbr(self, ticker: str) -> dict[str, Optional[float]]:
        """
        取得本益比（PER）、淨值比（PBR）、殖利率（DividendYield）最新值。

        dataset: TaiwanStockPER（每日更新）
        回傳: {'per': float|None, 'pbr': float|None, 'dividend_yield': float|None}
        """
        start = str(date.today() - timedelta(days=10))
        rows  = await self._get("TaiwanStockPER", {
            "data_id":    ticker,
            "start_date": start,
        })
        if not rows:
            return {"per": None, "pbr": None, "dividend_yield": None}

        # 取最後一筆有效值
        rows_sorted = sorted(rows, key=lambda r: r.get("date", ""))
        latest = rows_sorted[-1]
        def _safe_float(val) -> Optional[float]:
            try:
                v = float(val)
                return None if v <= 0 else v
            except (TypeError, ValueError):
                return None

        return {
            "per":            _safe_float(latest.get("PER")),
            "pbr":            _safe_float(latest.get("PBR")),
            "dividend_yield": _safe_float(latest.get("DividendYield")),
        }

    async def get_topic_news_count(
        self,
        topic: str,
        trade_date: date,
    ) -> int:
        """
        計算特定題材關鍵字在 FinMind 新聞資料中的當日出現次數。
        取代 topic_radar._simulate_raw_count() 的隨機模擬邏輯。

        - 採用內部日期快取，同一天只請求一次 API，避免超量計費
        - 搜尋範圍：標題（title）+ 摘要（description）
        - 使用 TaiwanStockNews dataset
        """
        cache_key = str(trade_date)
        if cache_key not in self._news_cache:
            start = str(trade_date - timedelta(days=1))
            end   = str(trade_date)
            rows  = await self._get("TaiwanStockNews", {
                "start_date": start,
                "end_date":   end,
            })
            self._news_cache[cache_key] = rows
            logger.debug("新聞快取建立：%s，共 %d 筆", cache_key, len(rows))

        news = self._news_cache[cache_key]
        count = sum(
            1 for r in news
            if topic in (r.get("title", "") + " " + r.get("description", ""))
        )
        logger.debug("題材關鍵字 [%s] 出現次數：%d", topic, count)
        return count