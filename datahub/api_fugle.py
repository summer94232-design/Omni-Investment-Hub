# datahub/api_fugle.py
import logging
from typing import Any, Optional
import httpx

logger = logging.getLogger(__name__)

FUGLE_BASE_URL  = "https://api.fugle.tw/marketdata/v1.0"
FUGLE_TRADE_URL = "https://api.fugle.tw/trading/v2.0"


class FugleAPI:
    """Fugle 行情 + 交易 API 封裝"""

    def __init__(
        self,
        api_key: str,
        trade_token: str,
        account: str,
        paper_trading: bool = True,
    ):
        self._api_key      = api_key
        self._trade_token  = trade_token
        self._account      = account
        self._paper_trading = paper_trading
        self._client       = httpx.AsyncClient(timeout=30.0)
        logger.info("FugleAPI 初始化完成（paper_trading=%s）", paper_trading)

    async def close(self) -> None:
        await self._client.aclose()

    # -------------------------------------------------------------------------
    # 行情（股票）
    # -------------------------------------------------------------------------
    async def get_quote(self, ticker: str) -> Optional[dict]:
        """取得即時股票報價"""
        try:
            resp = await self._client.get(
                f"{FUGLE_BASE_URL}/stock/intraday/quote/{ticker}",
                headers={"X-API-KEY": self._api_key},
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPError as e:
            logger.error("Fugle 股票報價錯誤 ticker=%s: %s", ticker, e)
            return None

    # -------------------------------------------------------------------------
    # 行情（期貨）— [BUG 9 修正] 新增期貨專用端點，不再誤用股票端點查 TXF
    # -------------------------------------------------------------------------
    async def get_futures_quote(self, ticker: str) -> Optional[dict]:
        """取得即時期貨報價（使用 futopt 端點）

        Args:
            ticker: 期貨代碼，例如 "TXF"（台指期連續月）

        Returns:
            期貨報價 dict，或 None（失敗時）

        Note:
            台指期應使用此方法，而非 get_quote()。
            get_quote() 使用 stock/intraday/quote 路徑，期貨代碼會回傳 404。
        """
        try:
            resp = await self._client.get(
                f"{FUGLE_BASE_URL}/futopt/intraday/quote/{ticker}",
                headers={"X-API-KEY": self._api_key},
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPError as e:
            logger.error("Fugle 期貨報價錯誤 ticker=%s: %s", ticker, e)
            return None

    async def get_candles(
        self,
        ticker: str,
        start_date: str,
        end_date: Optional[str] = None,
    ) -> list[dict]:
        """取得日K資料"""
        params: dict[str, Any] = {"from": start_date}
        if end_date:
            params["to"] = end_date
        try:
            resp = await self._client.get(
                f"{FUGLE_BASE_URL}/stock/historical/candles/{ticker}",
                headers={"X-API-KEY": self._api_key},
                params=params,
            )
            resp.raise_for_status()
            return resp.json().get("data", [])
        except httpx.HTTPError as e:
            logger.error("Fugle K線錯誤 ticker=%s: %s", ticker, e)
            return []

    # -------------------------------------------------------------------------
    # 交易（Paper Trading 安全護欄）
    # -------------------------------------------------------------------------
    async def place_order(
        self,
        ticker: str,
        side: str,          # "Buy" | "Sell"
        quantity: int,
        price: Optional[float] = None,
        order_type: str = "Limit",
    ) -> Optional[dict]:
        """下單（paper_trading=True 時只記錄不實際送出）"""
        payload = {
            "stock_no":  ticker,
            "buy_sell":  side,
            "quantity":  quantity,
            "price":     price,
            "order_type": order_type,
        }

        if self._paper_trading:
            logger.info("Paper Trading 模式，模擬下單：%s", payload)
            return {"simulated": True, **payload}

        try:
            resp = await self._client.post(
                f"{FUGLE_TRADE_URL}/order",
                headers={"Authorization": f"Bearer {self._trade_token}"},
                json=payload,
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPError as e:
            logger.error("Fugle 下單錯誤 ticker=%s: %s", ticker, e)
            return None

    async def get_positions(self) -> list[dict]:
        """取得目前持倉"""
        if self._paper_trading:
            return []
        try:
            resp = await self._client.get(
                f"{FUGLE_TRADE_URL}/positions",
                headers={"Authorization": f"Bearer {self._trade_token}"},
            )
            resp.raise_for_status()
            return resp.json().get("data", [])
        except httpx.HTTPError as e:
            logger.error("Fugle 持倉查詢錯誤：%s", e)
            return []