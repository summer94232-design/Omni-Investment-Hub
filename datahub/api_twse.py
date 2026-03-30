# datahub/api_twse.py
# ═══════════════════════════════════════════════════════════════════════════════
# TWSE OpenAPI 封裝 — 台指期備援報價 + 個股基本資料
# ───────────────────────────────────────────────────────────────────────────────
# 用途：
#   提供 Fugle API 的備援台指期近月合約報價。
#   當 Fugle API 斷線或回傳錯誤時，MacroFilter 改呼叫此模組，
#   確保 Normalized Basis 計算不中斷。
#
# 資料來源：
#   https://openapi.twse.com.tw — 台灣證交所公開資訊，完全免費，無需金鑰
#   https://www.taifex.com.tw/cht/3/futContractsDate — 期交所期貨成交資訊
#
# 不需要任何 API 金鑰，直接呼叫即可。
# ═══════════════════════════════════════════════════════════════════════════════

import logging
from datetime import date
from typing import Optional
import httpx

logger = logging.getLogger(__name__)

# TWSE OpenAPI
TWSE_BASE_URL  = "https://openapi.twse.com.tw/v1"
# 期交所行情 API（台指期近月）
TAIFEX_BASE_URL = "https://www.taifex.com.tw/cht/3"
# 期交所 JSON API
TAIFEX_API_URL = "https://www.taifex.com.tw/cht/3/futContractsDate"

# 台指期商品代碼
TX_PRODUCT_CODE = "TX"


class TWSEApi:
    """
    台灣證交所 OpenAPI + 期交所行情封裝。

    主要功能：
      1. get_futures_close(product='TX') — 取得台指期近月結算價（備援用）
      2. get_stock_info(ticker)          — 取得個股基本資料（產業分類）
      3. get_market_summary()            — 取得大盤加權指數收盤價

    所有方法在失敗時回傳 None，不拋出例外，供 MacroFilter 靜默降級。
    """

    def __init__(self):
        self._client = httpx.AsyncClient(
            timeout=15.0,
            headers={
                "User-Agent": "Mozilla/5.0 (OmniHub/3.3)",
                "Accept": "application/json",
            },
        )

    async def close(self) -> None:
        await self._client.aclose()

    # ── 台指期近月收盤價 ──────────────────────────────────────────────────────

    async def get_futures_close(self, product: str = TX_PRODUCT_CODE) -> Optional[float]:
        """
        取得台指期近月合約收盤價（供 MacroFilter 基差計算備援）。

        優先嘗試期交所每日行情 API；
        失敗時嘗試 TWSE OpenAPI 的指數期貨欄位。

        回傳：收盤價（float），失敗時回傳 None
        """
        price = await self._get_futures_from_taifex(product)
        if price is not None:
            return price

        # 備援：從 TWSE 大盤指數估算（最後手段）
        logger.warning("期交所 API 失敗，嘗試 TWSE OpenAPI 備援")
        return await self._get_index_from_twse()

    async def _get_futures_from_taifex(self, product: str) -> Optional[float]:
        """從期交所 API 取得期貨近月成交價"""
        try:
            resp = await self._client.get(
                "https://www.taifex.com.tw/cht/3/futDataDown",
                params={
                    "down_type": "1",
                    "commodity_id": product,
                    "queryStartDate": date.today().strftime("%Y/%m/%d"),
                    "queryEndDate":   date.today().strftime("%Y/%m/%d"),
                },
                follow_redirects=True,
            )
            resp.raise_for_status()
            # 期交所回傳 CSV，解析最新近月成交價
            lines = resp.text.strip().splitlines()
            for line in lines[1:]:  # 跳過表頭
                parts = [p.strip().replace(",", "") for p in line.split(",")]
                if len(parts) > 8 and product in parts[0]:
                    try:
                        close_price = float(parts[8])  # 收盤價欄位
                        if close_price > 0:
                            logger.info("期交所 %s 近月收盤：%.0f", product, close_price)
                            return close_price
                    except (ValueError, IndexError):
                        continue
        except Exception as e:
            logger.warning("期交所 CSV API 失敗：%s", e)

        # 嘗試期交所 JSON 格式
        try:
            resp = await self._client.get(
                "https://mis.taifex.com.tw/futures/api/getQuoteList",
                params={"MarketType": "0", "CommodityID": product},
            )
            resp.raise_for_status()
            data = resp.json()
            items = data.get("RTList", [])
            for item in items:
                close = item.get("CLastPrice") or item.get("SettlePrice")
                if close:
                    try:
                        price = float(str(close).replace(",", ""))
                        if price > 0:
                            logger.info("期交所 JSON %s 近月價：%.0f", product, price)
                            return price
                    except ValueError:
                        continue
        except Exception as e:
            logger.warning("期交所 JSON API 失敗：%s", e)

        return None

    async def _get_index_from_twse(self) -> Optional[float]:
        """從 TWSE OpenAPI 取得加權指數（最後備援）"""
        try:
            resp = await self._client.get(
                f"{TWSE_BASE_URL}/exchangeReport/FMTQIK",
            )
            resp.raise_for_status()
            data = resp.json()
            if data:
                latest = data[-1]
                close = latest.get("收盤指數") or latest.get("指數")
                if close:
                    return float(str(close).replace(",", ""))
        except Exception as e:
            logger.warning("TWSE OpenAPI 指數備援失敗：%s", e)
        return None

    # ── 大盤加權指數收盤價 ────────────────────────────────────────────────────

    async def get_market_summary(self) -> Optional[float]:
        """
        取得加權指數今日收盤價。
        供 MacroFilter 在 FinMind 失敗時備援使用。
        """
        try:
            resp = await self._client.get(
                f"{TWSE_BASE_URL}/exchangeReport/FMTQIK",
            )
            resp.raise_for_status()
            data = resp.json()
            if data:
                latest = sorted(data, key=lambda x: x.get("日期", ""))[-1]
                close  = latest.get("收盤指數") or latest.get("指數")
                if close:
                    price = float(str(close).replace(",", ""))
                    logger.info("TWSE 加權指數：%.2f", price)
                    return price
        except Exception as e:
            logger.warning("TWSE 加權指數 API 失敗：%s", e)
        return None

    # ── 個股基本資料（產業分類）─────────────────────────────────────────────

    async def get_stock_info(self, ticker: str) -> Optional[dict]:
        """
        取得個股基本資料，主要用於補充 SECTOR_MAP 未收錄的標的。

        回傳格式：
        {
            'ticker':   '2330',
            'name':     '台積電',
            'industry': '半導體業',  # 台灣產業分類
            'market':   'TWSE',
        }
        失敗時回傳 None。
        """
        try:
            resp = await self._client.get(
                f"{TWSE_BASE_URL}/company/companyInfo/{ticker}",
            )
            resp.raise_for_status()
            data = resp.json()
            if data:
                info = data[0] if isinstance(data, list) else data
                return {
                    "ticker":   ticker,
                    "name":     info.get("公司名稱", ""),
                    "industry": info.get("產業類別", "OTHER"),
                    "market":   "TWSE",
                }
        except Exception as e:
            logger.debug("TWSE 個股資料 %s 失敗：%s", ticker, e)

        # 備援：TWSE 上市公司基本資料完整清單
        try:
            resp = await self._client.get(
                f"{TWSE_BASE_URL}/company/companyInfo",
            )
            resp.raise_for_status()
            companies = resp.json()
            for c in companies:
                code = c.get("公司代號", "").strip()
                if code == ticker.strip():
                    return {
                        "ticker":   ticker,
                        "name":     c.get("公司名稱", ""),
                        "industry": c.get("產業類別", "OTHER"),
                        "market":   "TWSE",
                    }
        except Exception as e:
            logger.debug("TWSE 完整清單查詢失敗：%s", e)

        return None

    # ── 產業分類批量查詢 ──────────────────────────────────────────────────────

    async def get_all_stock_industries(self) -> dict[str, str]:
        """
        取得所有上市股票的產業分類。
        回傳：{ticker: industry_name}

        供 SelectionEngine 建立動態 SECTOR_MAP，
        取代硬編碼的 14 支標的限制。
        """
        industry_map: dict[str, str] = {}
        try:
            resp = await self._client.get(
                f"{TWSE_BASE_URL}/company/companyInfo",
            )
            resp.raise_for_status()
            companies = resp.json()
            for c in companies:
                code     = c.get("公司代號", "").strip()
                industry = c.get("產業類別", "OTHER").strip()
                if code:
                    industry_map[code] = industry
            logger.info("TWSE 產業分類載入：%d 筆", len(industry_map))
        except Exception as e:
            logger.warning("TWSE 產業分類批量查詢失敗：%s", e)
        return industry_map