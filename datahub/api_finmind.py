# datahub/api_finmind.py
import logging
from typing import Any, Optional
import httpx
import pandas as pd

logger = logging.getLogger(__name__)

FINMIND_BASE_URL = "https://api.finmindtrade.com/api/v4"


class FinMindAPI:
    """FinMind 台股數據 API 封裝"""

    def __init__(self, token: str):
        self._token = token
        self._client = httpx.AsyncClient(timeout=30.0)

    async def close(self) -> None:
        await self._client.aclose()

    async def _get(self, dataset: str, params: dict[str, Any]) -> list[dict]:
        params["token"] = self._token
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

    async def get_stock_price(
        self,
        ticker: str,
        start_date: str,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """取得個股日K（OHLCV）"""
        params: dict[str, Any] = {
            "data_id":   ticker,
            "start_date": start_date,
        }
        if end_date:
            params["end_date"] = end_date
        rows = await self._get("TaiwanStockPrice", params)
        return pd.DataFrame(rows)

    async def get_institutional_investors(
        self,
        ticker: str,
        start_date: str,
    ) -> pd.DataFrame:
        """取得三大法人買賣超"""
        params = {"data_id": ticker, "start_date": start_date}
        rows = await self._get("TaiwanStockInstitutionalInvestorsBuySell", params)
        return pd.DataFrame(rows)

    async def get_margin_trading(
        self,
        ticker: str,
        start_date: str,
    ) -> pd.DataFrame:
        """取得融資融券"""
        params = {"data_id": ticker, "start_date": start_date}
        rows = await self._get("TaiwanStockMarginPurchaseShortSale", params)
        return pd.DataFrame(rows)

    async def get_revenue(
        self,
        ticker: str,
        start_date: str,
    ) -> pd.DataFrame:
        """取得月營收"""
        params = {"data_id": ticker, "start_date": start_date}
        rows = await self._get("TaiwanStockMonthRevenue", params)
        return pd.DataFrame(rows)