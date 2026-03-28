# datahub/api_finmind.py
# ═══════════════════════════════════════════════════════════════════════════════
# 變更紀錄（v3）：
# - [大盤成交量] 新增 get_twii_volume()：
#     拉取加權指數（Y9999）歷史資料，包含 volume（成交量，元）
#     供 MacroFilter 寫入 market_regime.market_volume
# - [加權指數收盤價] 新增 get_twii_price()：
#     取得加權指數近期收盤價，供 MacroFilter 計算基差分母（tsec_index_price）
# - [除息點數] 新增 get_upcoming_dividends()：
#     查詢未來 30 日預計除息個股，加總預估除息點數
#     供 MacroFilter 計算 dividend_adjustment
# ═══════════════════════════════════════════════════════════════════════════════

import logging
from datetime import date, timedelta
from typing import Any, Optional
import httpx
import pandas as pd

logger = logging.getLogger(__name__)

FINMIND_BASE_URL = "https://api.finmindtrade.com/api/v4"

# 加權指數在 FinMind 的代碼
TWII_TICKER = "Y9999"


class FinMindAPI:
    """FinMind 台股數據 API 封裝"""

    def __init__(self, token: str):
        self._token = token
        self._client = httpx.AsyncClient(timeout=30.0)

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

    # ── 原有方法（不變）─────────────────────────────────────────────────────

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

    # ── [v3 新增] 大盤相關方法 ───────────────────────────────────────────────

    async def get_twii_price(
        self,
        start_date: Optional[str] = None,
        limit_days: int = 5,
    ) -> Optional[float]:
        """
        取得加權指數最新收盤價（供基差計算的分母 tsec_index_price）

        FinMind dataset: TaiwanStockPrice，ticker: Y9999
        回傳最近一個交易日的收盤價，無資料時回傳 None

        參數：
            start_date: 起始日期（預設為 30 天前）
            limit_days: 往回查幾天（避免假日無資料）
        """
        if start_date is None:
            start_date = str(date.today() - timedelta(days=30))

        rows = await self._get("TaiwanStockPrice", {
            "data_id":    TWII_TICKER,
            "start_date": start_date,
        })

        if not rows:
            logger.warning("無法取得加權指數收盤價（Y9999）")
            return None

        df = pd.DataFrame(rows).sort_values("date")
        if df.empty or "close" not in df.columns:
            return None

        try:
            latest_close = float(df["close"].iloc[-1])
            logger.debug("加權指數最新收盤價：%.2f（%s）", latest_close, df["date"].iloc[-1])
            return latest_close
        except (ValueError, IndexError):
            return None

    async def get_twii_volume(
        self,
        start_date: Optional[str] = None,
    ) -> Optional[int]:
        """
        取得加權指數最新成交量（供 market_regime.market_volume 寫入）

        FinMind TaiwanStockPrice 的 volume 欄位對 Y9999 代表大盤總成交量（元）
        回傳最近一個交易日的成交量（元），無資料時回傳 None

        注意：
            FinMind 的 Y9999 成交量單位為「元」（非千元），直接存入即可
        """
        if start_date is None:
            start_date = str(date.today() - timedelta(days=10))

        rows = await self._get("TaiwanStockPrice", {
            "data_id":    TWII_TICKER,
            "start_date": start_date,
        })

        if not rows:
            logger.warning("無法取得加權指數成交量（Y9999）")
            return None

        df = pd.DataFrame(rows).sort_values("date")
        if df.empty or "Trading_Volume" not in df.columns:
            # 欄位名稱備援：有些版本用 volume
            vol_col = "volume" if "volume" in df.columns else None
            if vol_col is None:
                logger.warning("Y9999 DataFrame 無 Trading_Volume 或 volume 欄位，欄位：%s", df.columns.tolist())
                return None
        else:
            vol_col = "Trading_Volume"

        try:
            latest_vol = int(float(df[vol_col].iloc[-1]))
            logger.debug("大盤最新成交量：%d 元（%s）", latest_vol, df["date"].iloc[-1])
            return latest_vol
        except (ValueError, IndexError):
            return None

    async def get_twii_history(
        self,
        start_date: str,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        取得加權指數歷史 OHLCV（供 MacroFilter 批次計算 MA 成交量）

        回傳 DataFrame，欄位包含：date, open, max, min, close, Trading_Volume
        """
        params: dict[str, Any] = {
            "data_id":    TWII_TICKER,
            "start_date": start_date,
        }
        if end_date:
            params["end_date"] = end_date
        rows = await self._get("TaiwanStockPrice", params)
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
        return df

    async def get_upcoming_dividends(
        self,
        watchlist: list[str],
        reference_date: Optional[date] = None,
        days_ahead: int = 30,
    ) -> float:
        """
        估算近期除息點數（dividend_adjustment），供基差公式的分母校正。

        計算方式：
            對 watchlist 中各股，查詢未來 days_ahead 日內的除息金額（cash_dividend）
            按各股在加權指數的概估權重加總，轉換為點數。

        簡化假設：
            - 加權指數點數 ≈ 各成分股股價加權，1 元除息 ≈ 指數下降約 0.05 點（粗估）
            - 此估算精度夠用於 Normalized Basis 計算，不需要精確的指數成分股權重

        資料來源：
            FinMind TaiwanStockDividend dataset

        回傳：
            預估未來 30 日除息點數（正值），無資料時回傳 0.0
        """
        if reference_date is None:
            reference_date = date.today()

        start_str = str(reference_date)
        end_str   = str(reference_date + timedelta(days=days_ahead))

        total_div_points = 0.0

        # 對主要大型股（市值佔指數比例高）估算除息影響
        # 完整做法需要精確指數成分股權重，這裡用簡化方式
        MAJOR_TICKERS = ['2330', '2317', '2454', '2303', '2882', '2412']
        target_tickers = [t for t in watchlist if t.strip() in MAJOR_TICKERS]

        if not target_tickers:
            return 0.0

        for ticker in target_tickers:
            try:
                rows = await self._get("TaiwanStockDividend", {
                    "data_id":    ticker.strip(),
                    "start_date": start_str,
                    "end_date":   end_str,
                })
                for row in rows:
                    cash_div = float(row.get("CashEarningsDistribution") or
                                     row.get("cash_dividend") or 0)
                    if cash_div > 0:
                        # 粗估：大型股 1 元現金股息 ≈ 指數 -5~-20 點
                        # 台積電權重最重，其他相對小
                        weight_factor = 20.0 if ticker.strip() == "2330" else 3.0
                        total_div_points += cash_div * weight_factor
                        logger.debug(
                            "除息估算：%s 現金股息 %.2f 元 → 估計影響 %.1f 點",
                            ticker, cash_div, cash_div * weight_factor
                        )
            except Exception as e:
                logger.warning("取得 %s 除息資料失敗：%s", ticker, e)

        logger.info("未來 %d 日預估除息點數：%.1f", days_ahead, total_div_points)
        return round(total_div_points, 2)