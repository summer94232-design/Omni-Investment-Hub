# modules/macro_filter.py
# ═══════════════════════════════════════════════════════════════════════════════
# 變更紀錄（v3.3）：
# - [BUG 4 修正]  INSERT 補上 fed_funds_rate / us_10y_yield / us_2y_yield（已含於 v3.1）
# - [BUG 9 修正]  台指期改用 get_futures_quote('TXF')（已含於 v3.1）
# - [BUG 11 修正] Redis key 統一管理（已含於 v3.1）
# - [信用利差] 新增 credit_spread / hy_spread 從 FRED 快照寫入 market_regime
#     用於 LIQUIDITY_CRISIS 黑天鵝情境的真實數據觸發
# - [Schema] market_regime 需執行 schema_patch_v3.sql 新增兩個欄位
# ═══════════════════════════════════════════════════════════════════════════════

import json
import logging
from datetime import date, timedelta
from typing import Optional
from datahub.data_hub import DataHub
from datahub.api_fred import FredAPI
from datahub import redis_keys as rk

logger = logging.getLogger(__name__)

MRS_THRESHOLDS = {
    "BULL_TREND": 80,
    "WEAK_BULL":  60,
    "CHOPPY":     40,
}

REGIME_CONFIG = {
    "BULL_TREND": {
        "max_exposure": 0.90,
        "weights": {"momentum": 0.38, "chip": 0.28, "fundamental": 0.22, "valuation": 0.12},
        "atr_multiplier": 3.0,
        "vcp_enabled": True,
    },
    "WEAK_BULL": {
        "max_exposure": 0.70,
        "weights": {"momentum": 0.30, "chip": 0.25, "fundamental": 0.25, "valuation": 0.20},
        "atr_multiplier": 2.5,
        "vcp_enabled": True,
    },
    "CHOPPY": {
        "max_exposure": 0.40,
        "weights": {"momentum": 0.20, "chip": 0.25, "fundamental": 0.30, "valuation": 0.25},
        "atr_multiplier": 2.0,
        "vcp_enabled": False,
    },
    "BEAR_TREND": {
        "max_exposure": 0.10,
        "weights": {"momentum": 0.15, "chip": 0.20, "fundamental": 0.35, "valuation": 0.30},
        "atr_multiplier": 2.0,
        "vcp_enabled": False,
    },
}

FUGLE_TX_TICKER = "TXF"


class MacroFilter:
    """
    宏觀濾網（v3.3）：計算 MRS、分類 Regime、寫入 market_regime。
    v3.3 新增寫入 credit_spread / hy_spread，供 BlackSwan LIQUIDITY_CRISIS 使用。
    """

    def __init__(
        self,
        hub:      DataHub,
        fred:     FredAPI,
        fugle=None,
        finmind=None,
        watchlist: Optional[list] = None,
    ):
        self.hub      = hub
        self.fred     = fred
        self.fugle    = fugle
        self.finmind  = finmind
        self.watchlist = watchlist or []

    def _calc_mrs(self, indicators: dict) -> float:
        """計算市場環境分數（0–100）"""
        score = 50.0

        vix = indicators.get("vix")
        if vix is not None:
            if vix < 15:    score += 15
            elif vix < 20:  score += 10
            elif vix < 25:  score += 5
            elif vix < 30:  score -= 5
            elif vix < 35:  score -= 10
            else:           score -= 20

        us_10y = indicators.get("us_10y_yield")
        us_2y  = indicators.get("us_2y_yield")
        if us_10y and us_2y:
            spread = us_10y - us_2y
            if spread > 1.0:   score += 10
            elif spread > 0.5: score += 5
            elif spread < 0:   score -= 10

        fed = indicators.get("fed_funds_rate")
        if fed is not None:
            if fed < 2.5:    score += 5
            elif fed > 5.5:  score -= 10

        pmi = indicators.get("us_pmi")
        if pmi is not None:
            if pmi > 55:     score += 10
            elif pmi > 50:   score += 5
            elif pmi < 45:   score -= 10

        cpi = indicators.get("us_cpi_yoy")
        if cpi is not None:
            if cpi < 2.5:    score += 5
            elif cpi > 4.0:  score -= 10

        # v3.3：信用利差對 MRS 的影響（spread 過大代表市場風險厭惡）
        credit_spread = indicators.get("credit_spread")
        if credit_spread is not None:
            spread_bp = credit_spread * 100
            if spread_bp > 300:  score -= 15
            elif spread_bp > 200: score -= 8
            elif spread_bp > 150: score -= 3

        return round(max(0, min(100, score)), 2)

    def _classify_regime(self, mrs: float) -> str:
        if mrs >= MRS_THRESHOLDS["BULL_TREND"]:
            return "BULL_TREND"
        elif mrs >= MRS_THRESHOLDS["WEAK_BULL"]:
            return "WEAK_BULL"
        elif mrs >= MRS_THRESHOLDS["CHOPPY"]:
            return "CHOPPY"
        return "BEAR_TREND"

    async def _fetch_basis_data(self) -> tuple[Optional[float], Optional[float], float]:
        """取得台指期基差資料（BUG 9 修正：使用期貨端點）"""
        tx_price   = None
        tsec_price = None
        div_adj    = 0.0

        if self.fugle:
            try:
                quote = await self.fugle.get_futures_quote(FUGLE_TX_TICKER)
                tx_price = float(quote.get("lastPrice") or quote.get("close") or 0) or None
            except Exception as e:
                logger.warning("Fugle 台指期報價失敗：%s", e)

        if self.finmind:
            try:
                tsec_price = await self.finmind.get_twii_price()
            except Exception as e:
                logger.warning("FinMind 加權指數收盤價失敗：%s", e)

            try:
                div_adj = await self.finmind.get_upcoming_dividends()
            except Exception as e:
                logger.warning("FinMind 除息資料失敗：%s", e)

        return tx_price, tsec_price, div_adj

    async def _fetch_market_volume(self) -> Optional[float]:
        """取得大盤日成交量"""
        if self.finmind:
            try:
                return await self.finmind.get_twii_volume()
            except Exception as e:
                logger.warning("FinMind 大盤成交量失敗：%s", e)
        return None

    async def run(self, trade_date: Optional[date] = None) -> dict:
        if trade_date is None:
            trade_date = date.today()

        logger.info("宏觀濾網（v3.3）執行中：%s", trade_date)

        indicators = await self.fred.get_macro_snapshot()
        mrs        = self._calc_mrs(indicators)
        regime     = self._classify_regime(mrs)
        config     = REGIME_CONFIG[regime]

        tx_price, tsec_price, div_adj = await self._fetch_basis_data()
        market_volume                 = await self._fetch_market_volume()

        normalized_basis = None
        if tx_price and tsec_price:
            fair = tsec_price - div_adj
            if fair > 0:
                normalized_basis = (tx_price - fair) / fair

        result = {
            "trade_date":          str(trade_date),
            "regime":              regime,
            "mrs_score":           mrs,
            "max_exposure":        config["max_exposure"],
            "vcp_enabled":         config["vcp_enabled"],
            "vix_level":           indicators.get("vix"),
            "tx_futures_price":    tx_price,
            "tsec_index_price":    tsec_price,
            "dividend_adjustment": div_adj,
            "normalized_basis":    normalized_basis,
            "market_volume":       market_volume,
            "fed_funds_rate":      indicators.get("fed_funds_rate"),
            "us_10y_yield":        indicators.get("us_10y_yield"),
            "us_2y_yield":         indicators.get("us_2y_yield"),
            "credit_spread":       indicators.get("credit_spread"),  # v3.3 新增
            "hy_spread":           indicators.get("hy_spread"),      # v3.3 新增
        }

        # ── 寫入 market_regime ────────────────────────────────────────────────
        await self.hub.execute("""
            INSERT INTO market_regime (
                trade_date, regime, mrs_score, max_exposure_limit,
                factor_weights, atr_multiplier, vcp_enabled,
                vix_level,
                tx_futures_price, tsec_index_price, dividend_adjustment,
                market_volume,
                fed_funds_rate, us_10y_yield, us_2y_yield,
                credit_spread, hy_spread
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,
                $8,
                $9,$10,$11,
                $12,
                $13,$14,$15,
                $16,$17
            )
            ON CONFLICT (trade_date) DO UPDATE SET
                regime              = EXCLUDED.regime,
                mrs_score           = EXCLUDED.mrs_score,
                max_exposure_limit  = EXCLUDED.max_exposure_limit,
                factor_weights      = EXCLUDED.factor_weights,
                atr_multiplier      = EXCLUDED.atr_multiplier,
                vcp_enabled         = EXCLUDED.vcp_enabled,
                vix_level           = EXCLUDED.vix_level,
                tx_futures_price    = EXCLUDED.tx_futures_price,
                tsec_index_price    = EXCLUDED.tsec_index_price,
                dividend_adjustment = EXCLUDED.dividend_adjustment,
                market_volume       = EXCLUDED.market_volume,
                fed_funds_rate      = EXCLUDED.fed_funds_rate,
                us_10y_yield        = EXCLUDED.us_10y_yield,
                us_2y_yield         = EXCLUDED.us_2y_yield,
                credit_spread       = EXCLUDED.credit_spread,
                hy_spread           = EXCLUDED.hy_spread
        """,
            trade_date,
            regime,
            mrs,
            config["max_exposure"],
            json.dumps(config["weights"]),
            config["atr_multiplier"],
            config["vcp_enabled"],
            indicators.get("vix"),
            tx_price,
            tsec_price,
            div_adj,
            market_volume,
            indicators.get("fed_funds_rate"),
            indicators.get("us_10y_yield"),
            indicators.get("us_2y_yield"),
            indicators.get("credit_spread"),   # $16 v3.3
            indicators.get("hy_spread"),        # $17 v3.3
        )

        # ── Redis 快取 ────────────────────────────────────────────────────────
        await self.hub.cache.set(
            rk.key_market_regime_latest(),
            {
                "regime":              regime,
                "mrs_score":           round(mrs, 2),
                "max_exposure":        config["max_exposure"],
                "vix_level":           indicators.get("vix"),
                "tx_futures_price":    tx_price,
                "tsec_index_price":    tsec_price,
                "dividend_adjustment": div_adj,
                "normalized_basis":    result["normalized_basis"],
                "market_volume":       market_volume,
                "credit_spread":       indicators.get("credit_spread"),
            },
            ttl=rk.TTL_24H,
        )

        if normalized_basis is not None:
            await self.hub.cache.set(
                rk.key_market_basis_latest(),
                {
                    "basis": result["normalized_basis"],
                    "tx":    tx_price,
                    "tsec":  tsec_price,
                    "div":   div_adj,
                },
                ttl=rk.TTL_5MIN,
            )

        logger.info(
            "宏觀濾網（v3.3）完成：%s MRS=%.1f Basis=%s CreditSpread=%s",
            regime, mrs,
            f"{normalized_basis*100:.2f}%" if normalized_basis is not None else "N/A",
            f"{indicators.get('credit_spread', 0)*100:.0f}bp"
            if indicators.get("credit_spread") else "N/A",
        )
        return result