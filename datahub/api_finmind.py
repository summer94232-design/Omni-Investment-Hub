# datahub/api_finmind.py
# ═══════════════════════════════════════════════════════════════════════════════
# 變更紀錄（v3.4）：
# - [v3.3] 新增 get_twii_volume / get_twii_price / get_upcoming_dividends
#          get_financial_statements / get_per_pbr / get_topic_news_count
# - [v3.4 新增] get_stock_info()：TaiwanStockInfo — 個股基本資料 + 產業分類
#              取代 SECTOR_MAP 硬編碼，支援任意新增 Watchlist 標的
# - [v3.4 新增] get_balance_sheet()：TaiwanStockBalanceSheet — 資產負債表
#              補充 負債比 / 流動比率 等財務健全指標到估值因子
# - [v3.4 新增] get_all_stock_industries()：批量取得全市場產業分類
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
    """FinMind 台股數據 API 封裝（v3.4）"""

    def __init__(self, token: str):
        self._token = token
        self._client = httpx.AsyncClient(timeout=30.0)
        self._news_cache: dict[str, list[dict]] = {}
        # 產業分類快取（當日有效）
        self._sector_cache: dict[str, str] = {}
        self._sector_cache_date: Optional[date] = None

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

    # =========================================================================
    # 原有方法（v3.3，保持不變）
    # =========================================================================

    async def get_stock_price(
        self,
        ticker: str,
        start_date: str,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """取得個股日K（OHLCV）"""
        params: dict[str, Any] = {"data_id": ticker, "start_date": start_date}
        if end_date:
            params["end_date"] = end_date
        rows = await self._get("TaiwanStockPrice", params)
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        rename_map = {"date": "date", "open": "open", "max": "max",
                      "min": "min", "close": "close", "volume": "volume"}
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
        df["close"] = pd.to_numeric(df.get("close", pd.Series(dtype=float)), errors="coerce")
        return df

    async def get_chip_data(self, ticker: str, start_date: str, end_date: Optional[str] = None) -> pd.DataFrame:
        """取得外資/投信籌碼"""
        params: dict[str, Any] = {"data_id": ticker, "start_date": start_date}
        if end_date:
            params["end_date"] = end_date
        rows = await self._get("TaiwanStockInstitutionalInvestors", params)
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    async def get_revenue(self, ticker: str, start_date: str) -> pd.DataFrame:
        """取得月營收"""
        rows = await self._get("TaiwanStockMonthRevenue", {"data_id": ticker, "start_date": start_date})
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        if "revenue" in df.columns:
            df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
        return df

    async def get_margin_trading(self, ticker: str, start_date: str) -> pd.DataFrame:
        """取得信用交易（融資/融券）"""
        rows = await self._get("TaiwanStockMarginPurchaseShortSale",
                               {"data_id": ticker, "start_date": start_date})
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    async def get_twii_price(self, days: int = 5) -> Optional[float]:
        """取得加權指數最新收盤價"""
        start = str(date.today() - timedelta(days=days + 5))
        rows  = await self._get("TaiwanStockPrice", {"data_id": TWII_TICKER, "start_date": start})
        if not rows:
            return None
        df = pd.DataFrame(rows).sort_values("date")
        return float(df["close"].iloc[-1]) if not df.empty else None

    async def get_twii_volume(self, days: int = 5) -> Optional[float]:
        """取得大盤日成交量（元）"""
        start = str(date.today() - timedelta(days=days + 5))
        rows  = await self._get("TaiwanStockPrice", {"data_id": TWII_TICKER, "start_date": start})
        if not rows:
            return None
        df = pd.DataFrame(rows).sort_values("date")
        return float(df["Trading_money"].iloc[-1]) if "Trading_money" in df.columns and not df.empty else None

    async def get_upcoming_dividends(self, days_ahead: int = 30) -> float:
        """取得未來 N 日預計除息點數加總"""
        start = str(date.today())
        end   = str(date.today() + timedelta(days=days_ahead))
        rows  = await self._get("TaiwanStockDividendResult", {"start_date": start, "end_date": end})
        if not rows:
            return 0.0
        total = sum(float(r.get("StockExDividend", 0) or 0) for r in rows)
        return round(total, 4)

    async def get_financial_statements(self, ticker: str, quarters: int = 8) -> pd.DataFrame:
        """取得綜合損益表（季報）"""
        start = str(date.today() - timedelta(days=365 * (quarters // 4 + 1)))
        rows  = await self._get("TaiwanStockFinancialStatements",
                                {"data_id": ticker, "start_date": start})
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        if "value" in df.columns:
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
        return df

    async def get_per_pbr(self, ticker: str) -> dict[str, Optional[float]]:
        """取得 PER / PBR / 殖利率最新值"""
        start = str(date.today() - timedelta(days=10))
        rows  = await self._get("TaiwanStockPER", {"data_id": ticker, "start_date": start})
        if not rows:
            return {"per": None, "pbr": None, "dividend_yield": None}
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

    async def get_topic_news_count(self, topic: str, trade_date: date) -> int:
        """計算題材關鍵字當日新聞出現次數"""
        cache_key = str(trade_date)
        if cache_key not in self._news_cache:
            start = str(trade_date - timedelta(days=1))
            end   = str(trade_date)
            rows  = await self._get("TaiwanStockNews", {"start_date": start, "end_date": end})
            self._news_cache[cache_key] = rows
        news = self._news_cache[cache_key]
        count = sum(
            1 for r in news
            if topic in (r.get("title", "") + " " + r.get("description", ""))
        )
        return count

    # =========================================================================
    # v3.4 新增方法
    # =========================================================================

    async def get_stock_info(self, ticker: str) -> dict[str, Optional[str]]:
        """
        [v3.4 新增] 取得個股基本資料，包含產業分類。
        Dataset: TaiwanStockInfo

        回傳：
        {
            'ticker':        '2330',
            'name':          '台積電',
            'industry_code': '24',        # 產業代碼
            'industry':      '半導體業',  # 產業名稱
            'market':        'twse',      # twse / otc
        }
        失敗時回傳空 dict。
        """
        try:
            rows = await self._get("TaiwanStockInfo", {"data_id": ticker})
            if rows:
                r = rows[0]
                return {
                    "ticker":        ticker,
                    "name":          r.get("stock_name", ""),
                    "industry_code": str(r.get("industry_code", "")),
                    "industry":      r.get("type", "OTHER"),
                    "market":        r.get("market", "twse"),
                }
        except Exception as e:
            logger.warning("FinMind TaiwanStockInfo 失敗 %s：%s", ticker, e)
        return {}

    async def get_all_stock_industries(self) -> dict[str, str]:
        """
        [v3.4 新增] 批量取得全市場個股產業分類（一次性）。
        Dataset: TaiwanStockInfo（不傳 data_id 取全量）

        回傳：{ticker: industry_name}

        供 SelectionEngine 動態建立 SECTOR_MAP，
        替換掉硬編碼只有 14 支的舊版本。
        結果會被快取一整天，避免重複呼叫。
        """
        today = date.today()

        # 使用當日快取
        if self._sector_cache_date == today and self._sector_cache:
            logger.debug("產業分類使用快取（%d 筆）", len(self._sector_cache))
            return self._sector_cache

        try:
            rows = await self._get("TaiwanStockInfo", {})
            if rows:
                sector_map: dict[str, str] = {}
                for r in rows:
                    code     = str(r.get("stock_id", "")).strip()
                    industry = str(r.get("type", "OTHER")).strip()
                    if code:
                        sector_map[code] = industry
                self._sector_cache      = sector_map
                self._sector_cache_date = today
                logger.info("FinMind 產業分類全量載入：%d 筆", len(sector_map))
                return sector_map
        except Exception as e:
            logger.warning("FinMind 產業分類全量查詢失敗：%s", e)

        return {}

    async def get_balance_sheet(
        self,
        ticker: str,
        quarters: int = 4,
    ) -> dict[str, Optional[float]]:
        """
        [v3.4 新增] 取得資產負債表關鍵指標，補充估值因子。
        Dataset: TaiwanStockBalanceSheet

        回傳：
        {
            'debt_ratio':        float | None,  # 負債比率（%）= 總負債 / 總資產
            'current_ratio':     float | None,  # 流動比率 = 流動資產 / 流動負債
            'equity_per_share':  float | None,  # 每股淨值
            'total_assets':      float | None,  # 總資產（千元）
            'total_liabilities': float | None,  # 總負債（千元）
        }
        失敗或資料不足時相關欄位回傳 None。
        """
        start = str(date.today() - timedelta(days=365 * (quarters // 4 + 1)))
        try:
            rows = await self._get("TaiwanStockBalanceSheet", {
                "data_id":    ticker,
                "start_date": start,
            })
            if not rows:
                return {"debt_ratio": None, "current_ratio": None,
                        "equity_per_share": None, "total_assets": None, "total_liabilities": None}

            df = pd.DataFrame(rows)
            if "date" in df.columns:
                df = df.sort_values("date")

            def _latest_value(df: pd.DataFrame, type_name: str) -> Optional[float]:
                """取最新一筆特定類型的值"""
                filtered = df[df["type"] == type_name] if "type" in df.columns else pd.DataFrame()
                if filtered.empty:
                    return None
                val = filtered["value"].iloc[-1]
                try:
                    v = float(val)
                    return v if v != 0 else None
                except (TypeError, ValueError):
                    return None

            total_assets      = _latest_value(df, "TotalAssets")
            total_liabilities = _latest_value(df, "TotalLiabilities")
            current_assets    = _latest_value(df, "CurrentAssets")
            current_liabilities = _latest_value(df, "CurrentLiabilities")
            equity_per_share  = _latest_value(df, "BookValuePerShare")

            # 計算衍生指標
            debt_ratio = None
            if total_assets and total_liabilities and total_assets > 0:
                debt_ratio = round(total_liabilities / total_assets * 100, 2)

            current_ratio = None
            if current_assets and current_liabilities and current_liabilities > 0:
                current_ratio = round(current_assets / current_liabilities, 3)

            return {
                "debt_ratio":        debt_ratio,
                "current_ratio":     current_ratio,
                "equity_per_share":  equity_per_share,
                "total_assets":      total_assets,
                "total_liabilities": total_liabilities,
            }

        except Exception as e:
            logger.warning("FinMind TaiwanStockBalanceSheet 失敗 %s：%s", ticker, e)
            return {"debt_ratio": None, "current_ratio": None,
                    "equity_per_share": None, "total_assets": None, "total_liabilities": None}