# datahub/api_yfinance.py
# ═══════════════════════════════════════════════════════════════════════════════
# Yahoo Finance 封裝 — 美股大盤（SPX / NDX）+ 恐慌指標
# ───────────────────────────────────────────────────────────────────────────────
# 用途：
#   提供 MRS 計分所需的美股即時大盤情緒，補強 FRED 歷史數據的滯後性。
#   FRED 的 VIX 資料通常滯後 1 天，yfinance 可取得最新收盤數據。
#
# 安裝：pip install yfinance
# 不需要任何 API 金鑰。
#
# 使用的 Ticker：
#   ^GSPC  — S&P 500 指數
#   ^IXIC  — NASDAQ 綜合指數
#   ^NDX   — NASDAQ 100 指數
#   ^VIX   — CBOE 波動率指數（補強 FRED VIXCLS 的即時性）
#   ^DJI   — 道瓊工業指數
# ═══════════════════════════════════════════════════════════════════════════════

import logging
from datetime import date, timedelta
from typing import Optional
import asyncio

logger = logging.getLogger(__name__)

# 嘗試 import yfinance，若未安裝則靜默降級
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False
    logger.warning("yfinance 未安裝，美股大盤功能將停用。請執行：pip install yfinance")


class YFinanceAPI:
    """
    Yahoo Finance 美股大盤數據封裝。

    所有方法在失敗時回傳 None，不拋出例外，供 MacroFilter 靜默降級。
    若 yfinance 未安裝，所有方法直接回傳 None。

    使用方式：
        yf_api = YFinanceAPI()
        snapshot = await yf_api.get_market_snapshot()
        # {
        #   'spx_close':      5123.45,
        #   'spx_change_pct': -0.012,  # 日漲跌幅
        #   'ndx_close':      18234.56,
        #   'ndx_change_pct': -0.015,
        #   'vix_latest':     18.5,
        #   'spx_above_ma50': True,    # 是否在 50MA 之上
        #   'spx_above_ma200': True,   # 是否在 200MA 之上
        # }
    """

    def __init__(self):
        self._cache: dict[str, dict] = {}
        self._cache_date: Optional[date] = None

    # ── 核心方法 ─────────────────────────────────────────────────────────────

    async def get_market_snapshot(self) -> dict[str, Optional[float | bool]]:
        """
        取得美股大盤完整快照（非同步包裝）。
        使用當日快取避免重複請求。

        回傳：
        {
            'spx_close':       float | None,  # S&P 500 收盤
            'spx_change_pct':  float | None,  # S&P 500 日漲跌幅（-0.01 = -1%）
            'ndx_close':       float | None,  # NASDAQ 100 收盤
            'ndx_change_pct':  float | None,  # NASDAQ 100 日漲跌幅
            'vix_latest':      float | None,  # VIX 最新值
            'spx_above_ma50':  bool  | None,  # SPX > 50MA
            'spx_above_ma200': bool  | None,  # SPX > 200MA
            'spx_rs_20d':      float | None,  # 20 日 RS 動能（%）
        }
        """
        today = date.today()

        # 當日快取
        if self._cache_date == today and self._cache:
            return self._cache

        result = await asyncio.get_event_loop().run_in_executor(
            None, self._fetch_sync
        )
        self._cache      = result
        self._cache_date = today
        return result

    def _fetch_sync(self) -> dict[str, Optional[float | bool]]:
        """同步抓取（在 executor 中執行，避免阻塞事件循環）"""
        empty = {
            "spx_close": None, "spx_change_pct": None,
            "ndx_close": None, "ndx_change_pct": None,
            "vix_latest": None,
            "spx_above_ma50": None, "spx_above_ma200": None,
            "spx_rs_20d": None,
        }

        if not YFINANCE_AVAILABLE:
            return empty

        try:
            # 下載最近 210 天（供 MA200 計算）
            tickers = yf.download(
                ["^GSPC", "^NDX", "^VIX"],
                period="210d",
                interval="1d",
                auto_adjust=True,
                progress=False,
                threads=True,
            )

            if tickers.empty:
                return empty

            # S&P 500
            spx_close = None
            spx_change_pct = None
            spx_above_ma50  = None
            spx_above_ma200 = None
            spx_rs_20d = None

            if ("Close", "^GSPC") in tickers.columns:
                spx_series = tickers[("Close", "^GSPC")].dropna()
                if len(spx_series) >= 2:
                    spx_close = float(spx_series.iloc[-1])
                    prev      = float(spx_series.iloc[-2])
                    spx_change_pct = round((spx_close - prev) / prev, 6) if prev else None

                if len(spx_series) >= 50:
                    ma50 = float(spx_series.tail(50).mean())
                    spx_above_ma50 = spx_close > ma50 if spx_close else None

                if len(spx_series) >= 200:
                    ma200 = float(spx_series.tail(200).mean())
                    spx_above_ma200 = spx_close > ma200 if spx_close else None

                if len(spx_series) >= 21:
                    price_20d_ago = float(spx_series.iloc[-21])
                    spx_rs_20d = round((spx_close - price_20d_ago) / price_20d_ago, 6) if price_20d_ago and spx_close else None

            # NASDAQ 100
            ndx_close = None
            ndx_change_pct = None

            if ("Close", "^NDX") in tickers.columns:
                ndx_series = tickers[("Close", "^NDX")].dropna()
                if len(ndx_series) >= 2:
                    ndx_close = float(ndx_series.iloc[-1])
                    prev_ndx  = float(ndx_series.iloc[-2])
                    ndx_change_pct = round((ndx_close - prev_ndx) / prev_ndx, 6) if prev_ndx else None

            # VIX
            vix_latest = None
            if ("Close", "^VIX") in tickers.columns:
                vix_series = tickers[("Close", "^VIX")].dropna()
                if not vix_series.empty:
                    vix_latest = float(vix_series.iloc[-1])

            result = {
                "spx_close":       round(spx_close, 2)        if spx_close else None,
                "spx_change_pct":  spx_change_pct,
                "ndx_close":       round(ndx_close, 2)        if ndx_close else None,
                "ndx_change_pct":  ndx_change_pct,
                "vix_latest":      round(vix_latest, 2)       if vix_latest else None,
                "spx_above_ma50":  spx_above_ma50,
                "spx_above_ma200": spx_above_ma200,
                "spx_rs_20d":      spx_rs_20d,
            }
            logger.info(
                "YFinance 快照：SPX=%.0f(%.2f%%) NDX=%.0f VIX=%.1f MA50=%s MA200=%s",
                spx_close or 0,
                (spx_change_pct or 0) * 100,
                ndx_close or 0,
                vix_latest or 0,
                spx_above_ma50,
                spx_above_ma200,
            )
            return result

        except Exception as e:
            logger.warning("YFinance 快照抓取失敗：%s", e)
            return empty

    # ── 便利方法 ─────────────────────────────────────────────────────────────

    async def get_spx_close(self) -> Optional[float]:
        """取得 S&P 500 最新收盤價"""
        snap = await self.get_market_snapshot()
        return snap.get("spx_close")

    async def get_vix(self) -> Optional[float]:
        """
        取得最新 VIX（即時性比 FRED 更好）。
        MacroFilter 優先使用此值，FRED VIXCLS 作為備援。
        """
        snap = await self.get_market_snapshot()
        return snap.get("vix_latest")

    async def is_us_market_bullish(self) -> Optional[bool]:
        """
        美股多頭判斷：SPX 同時高於 50MA 和 200MA。
        用於 MRS 加分邏輯的輔助指標。
        """
        snap = await self.get_market_snapshot()
        ma50  = snap.get("spx_above_ma50")
        ma200 = snap.get("spx_above_ma200")
        if ma50 is None or ma200 is None:
            return None
        return bool(ma50 and ma200)