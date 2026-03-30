# modules/macro_filter.py
# ═══════════════════════════════════════════════════════════════════════════════
# 變更紀錄（v3.4）：
# - [v3.3.1] GDP / 失業率納入 MRS 計分；hy_spread 邏輯已在 black_swan.py
# - [v3.4 新增] 接入 YFinance SPX / NDX 美股大盤情緒 → MRS 加減分
# - [v3.4 新增] 接入 FRED 5Y / 30Y 殖利率 → 利率曲線形狀分析加入 MRS
# - [v3.4 新增] TWSE OpenAPI 作為 Fugle 台指期報價的備援
#              Fugle 失敗時自動切換，確保基差計算不中斷
# ═══════════════════════════════════════════════════════════════════════════════

import json
import logging
from datetime import date
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
    宏觀濾網（v3.4）。
    v3.4 新增：
      - SPX / NDX 美股大盤情緒（YFinance）納入 MRS 計分
      - 完整利率曲線（2Y/5Y/10Y/30Y）分析
      - TWSE OpenAPI 台指期備援
    """

    def __init__(
        self,
        hub:          DataHub,
        fred:         FredAPI,
        fugle=None,
        finmind=None,
        yfinance_api=None,   # datahub.api_yfinance.YFinanceAPI（可選）
        twse_api=None,       # datahub.api_twse.TWSEApi（可選，Fugle 備援）
        watchlist: Optional[list] = None,
    ):
        self.hub          = hub
        self.fred         = fred
        self.fugle        = fugle
        self.finmind      = finmind
        self.yfinance_api = yfinance_api
        self.twse_api     = twse_api
        self.watchlist    = watchlist or []

    # =========================================================================
    # MRS 計分（v3.4 完整版）
    # =========================================================================

    def _calc_mrs(
        self,
        indicators:  dict,
        us_market:   Optional[dict] = None,
        yield_curve: Optional[dict] = None,
    ) -> float:
        """
        計算市場環境評分（MRS，0–100）。

        v3.4 計分項目總覽：
          VIX（反向）         ±20  （YFinance 更即時版優先）
          殖利率曲線 10Y-2Y   ±15
          殖利率曲線 5Y/30Y   ±5   ← v3.4 新增
          Fed 利率            ±8
          CPI 通膨            ±8
          GDP 成長率          ±10  ← v3.3.1
          失業率              ±10  ← v3.3.1
          PMI                 ±12
          SPX 大盤趨勢        ±10  ← v3.4 新增
          NDX / SPX 動能      ±8   ← v3.4 新增
        """
        score = 50.0

        # ── VIX（優先 YFinance 即時值）────────────────────────────────────────
        vix = indicators.get("vix")
        if us_market and us_market.get("vix_latest"):
            vix = us_market["vix_latest"]  # 即時值覆蓋 FRED 滯後值

        if vix is not None:
            if vix < 15:      score += 20
            elif vix < 20:    score += 10
            elif vix < 25:    score += 0
            elif vix < 35:    score -= 10
            else:             score -= 20

        # ── 殖利率曲線（10Y - 2Y）────────────────────────────────────────────
        us10y = indicators.get("us_10y_yield")
        us2y  = indicators.get("us_2y_yield")
        if us10y is not None and us2y is not None:
            spread_10_2 = float(us10y) - float(us2y)
            if spread_10_2 > 1.0:     score += 15
            elif spread_10_2 > 0.5:   score += 8
            elif spread_10_2 > 0:     score += 3
            elif spread_10_2 > -0.5:  score -= 5
            else:                     score -= 15

        # ── v3.4：5Y / 30Y 輔助利率曲線 ──────────────────────────────────────
        if yield_curve:
            curve_shape = yield_curve.get("curve_shape", "UNKNOWN")
            if curve_shape == "NORMAL":        score += 5
            elif curve_shape == "FLAT":        score += 0
            elif curve_shape == "MILD_INVERT": score -= 3
            elif curve_shape == "INVERTED":    score -= 5

            spread_30_5 = yield_curve.get("spread_30y_5y")
            if spread_30_5 is not None:
                if spread_30_5 > 1.0:  score += 3
                elif spread_30_5 < 0:  score -= 3

        # ── Fed 利率 ──────────────────────────────────────────────────────────
        fed = indicators.get("fed_funds_rate")
        if fed is not None:
            fed_f = float(fed)
            if fed_f < 2.0:    score += 8
            elif fed_f < 4.0:  score += 3
            elif fed_f > 5.5:  score -= 5

        # ── CPI ───────────────────────────────────────────────────────────────
        cpi = indicators.get("us_cpi_yoy")
        if cpi is not None:
            cpi_f = float(cpi)
            if cpi_f < 2.5:    score += 8
            elif cpi_f < 3.5:  score += 3
            elif cpi_f > 5.0:  score -= 8

        # ── GDP（v3.3.1）─────────────────────────────────────────────────────
        gdp = indicators.get("us_gdp_growth")
        if gdp is not None:
            gdp_f = float(gdp)
            if gdp_f > 2.5:  score += 5
            elif gdp_f < 0:  score -= 10

        # ── 失業率（v3.3.1）──────────────────────────────────────────────────
        unemp = indicators.get("us_unemployment")
        if unemp is not None:
            unemp_f = float(unemp)
            if unemp_f < 4.0:  score += 5
            elif unemp_f > 6.0: score -= 10

        # ── PMI ───────────────────────────────────────────────────────────────
        pmi = indicators.get("us_pmi")
        if pmi is not None:
            pmi_f = float(pmi)
            if pmi_f > 55:    score += 12
            elif pmi_f > 50:  score += 6
            elif pmi_f < 45:  score -= 8

        # ── v3.4：SPX 大盤趨勢（YFinance）────────────────────────────────────
        if us_market:
            ma50  = us_market.get("spx_above_ma50")
            ma200 = us_market.get("spx_above_ma200")
            spx_chg = us_market.get("spx_change_pct")

            if ma50 is not None and ma200 is not None:
                if ma50 and ma200:         score += 8   # 多頭排列
                elif not ma50 and not ma200: score -= 10  # 空頭排列
                else:                      score += 2   # 整理

            if spx_chg is not None and spx_chg < -0.03:
                score -= 5  # 單日跌 > 3%

            # NDX 動能
            ndx_chg    = us_market.get("ndx_change_pct")
            spx_rs_20d = us_market.get("spx_rs_20d")

            if ndx_chg is not None:
                if ndx_chg > 0.01:   score += 5
                elif ndx_chg < -0.02: score -= 5

            if spx_rs_20d is not None:
                if spx_rs_20d > 0.05:    score += 3
                elif spx_rs_20d < -0.05: score -= 3

        return max(0.0, min(100.0, score))

    def _classify_regime(self, mrs: float) -> str:
        if mrs >= MRS_THRESHOLDS["BULL_TREND"]:  return "BULL_TREND"
        elif mrs >= MRS_THRESHOLDS["WEAK_BULL"]: return "WEAK_BULL"
        elif mrs >= MRS_THRESHOLDS["CHOPPY"]:    return "CHOPPY"
        else:                                    return "BEAR_TREND"

    # =========================================================================
    # 台指期報價（含 TWSE 備援）
    # =========================================================================

    async def _get_tx_price(self) -> Optional[float]:
        """Fugle 優先，失敗自動切換 TWSE OpenAPI 備援"""
        if self.fugle:
            try:
                quote = await self.fugle.get_futures_quote(FUGLE_TX_TICKER)
                price = quote.get("close") or quote.get("last")
                if price:
                    return float(price)
            except Exception as e:
                logger.warning("Fugle 台指期失敗，切換 TWSE 備援：%s", e)

        if self.twse_api:
            try:
                price = await self.twse_api.get_futures_close("TX")
                if price:
                    logger.info("TWSE 備援台指期：%.0f", price)
                    return price
            except Exception as e:
                logger.warning("TWSE 台指期備援也失敗：%s", e)

        return None

    # =========================================================================
    # 主執行
    # =========================================================================

    async def run(self, trade_date: Optional[date] = None) -> dict:
        if trade_date is None:
            trade_date = date.today()

        indicators = await self.fred.get_macro_snapshot()

        # v3.4：完整殖利率曲線
        yield_curve = None
        try:
            yield_curve = await self.fred.get_yield_curve()
            indicators["us_5y_yield"]  = yield_curve.get("us_5y")
            indicators["us_30y_yield"] = yield_curve.get("us_30y")
        except Exception as e:
            logger.warning("殖利率曲線取得失敗：%s", e)

        # v3.4：YFinance 美股快照
        us_market = None
        if self.yfinance_api:
            try:
                us_market = await self.yfinance_api.get_market_snapshot()
                if us_market.get("vix_latest"):
                    indicators["vix"] = us_market["vix_latest"]
            except Exception as e:
                logger.warning("YFinance 快照失敗：%s", e)

        # 台指期（含備援）
        tx_price         = await self._get_tx_price()
        tsec_price       = None
        div_adj          = None
        market_volume    = None
        normalized_basis = None

        if self.finmind:
            try:
                tsec_price    = await self.finmind.get_twii_price()
                div_adj       = await self.finmind.get_upcoming_dividends()
                market_volume = await self.finmind.get_twii_volume()
            except Exception as e:
                logger.warning("FinMind 大盤數據失敗：%s", e)

        if tsec_price is None and self.twse_api:
            try:
                tsec_price = await self.twse_api.get_market_summary()
            except Exception as e:
                logger.warning("TWSE 大盤備援失敗：%s", e)

        if tx_price and tsec_price and tsec_price > 0:
            adj_tsec = tsec_price - (div_adj or 0)
            normalized_basis = (tx_price - adj_tsec) / adj_tsec if adj_tsec > 0 else None

        # MRS 計算
        mrs    = self._calc_mrs(indicators, us_market=us_market, yield_curve=yield_curve)
        regime = self._classify_regime(mrs)
        config = REGIME_CONFIG[regime]

        result = {
            "regime":              regime,
            "mrs_score":           round(mrs, 2),
            "max_exposure":        config["max_exposure"],
            "factor_weights":      config["weights"],
            "atr_multiplier":      config["atr_multiplier"],
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
            "credit_spread":       indicators.get("credit_spread"),
            "hy_spread":           indicators.get("hy_spread"),
            "us_5y_yield":         indicators.get("us_5y_yield"),
            "us_30y_yield":        indicators.get("us_30y_yield"),
            "yield_curve_shape":   yield_curve.get("curve_shape") if yield_curve else None,
            "spx_close":           us_market.get("spx_close")       if us_market else None,
            "spx_change_pct":      us_market.get("spx_change_pct")  if us_market else None,
            "spx_above_ma200":     us_market.get("spx_above_ma200") if us_market else None,
            "ndx_close":           us_market.get("ndx_close")       if us_market else None,
        }

        # DB 寫入
        await self.hub.execute("""
            INSERT INTO market_regime (
                trade_date, regime, mrs_score, max_exposure_limit,
                factor_weights, atr_multiplier, vcp_enabled,
                vix_level, tx_futures_price, tsec_index_price, dividend_adjustment,
                market_volume, fed_funds_rate, us_10y_yield, us_2y_yield,
                credit_spread, hy_spread
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
            ON CONFLICT (trade_date) DO UPDATE SET
                regime=EXCLUDED.regime, mrs_score=EXCLUDED.mrs_score,
                max_exposure_limit=EXCLUDED.max_exposure_limit,
                factor_weights=EXCLUDED.factor_weights,
                atr_multiplier=EXCLUDED.atr_multiplier,
                vcp_enabled=EXCLUDED.vcp_enabled,
                vix_level=EXCLUDED.vix_level,
                tx_futures_price=EXCLUDED.tx_futures_price,
                tsec_index_price=EXCLUDED.tsec_index_price,
                dividend_adjustment=EXCLUDED.dividend_adjustment,
                market_volume=EXCLUDED.market_volume,
                fed_funds_rate=EXCLUDED.fed_funds_rate,
                us_10y_yield=EXCLUDED.us_10y_yield,
                us_2y_yield=EXCLUDED.us_2y_yield,
                credit_spread=EXCLUDED.credit_spread,
                hy_spread=EXCLUDED.hy_spread
        """,
            trade_date, regime, mrs, config["max_exposure"],
            json.dumps(config["weights"]), config["atr_multiplier"], config["vcp_enabled"],
            indicators.get("vix"), tx_price, tsec_price, div_adj, market_volume,
            indicators.get("fed_funds_rate"), indicators.get("us_10y_yield"),
            indicators.get("us_2y_yield"), indicators.get("credit_spread"),
            indicators.get("hy_spread"),
        )

        # Redis 快取
        await self.hub.cache.set(
            rk.key_market_regime_latest(),
            {
                "regime":            regime,
                "mrs_score":         round(mrs, 2),
                "max_exposure":      config["max_exposure"],
                "vix_level":         indicators.get("vix"),
                "tx_futures_price":  tx_price,
                "tsec_index_price":  tsec_price,
                "normalized_basis":  normalized_basis,
                "market_volume":     market_volume,
                "credit_spread":     indicators.get("credit_spread"),
                "yield_curve_shape": result.get("yield_curve_shape"),
                "spx_close":         result.get("spx_close"),
            },
            ttl=rk.TTL_24H,
        )

        if normalized_basis is not None:
            await self.hub.cache.set(
                rk.key_market_basis_latest(),
                {"basis": normalized_basis, "tx": tx_price,
                 "tsec": tsec_price, "div": div_adj},
                ttl=rk.TTL_5MIN,
            )

        logger.info(
            "宏觀濾網（v3.4）完成：%s MRS=%.1f 曲線=%s SPX=%s Basis=%s",
            regime, mrs,
            result.get("yield_curve_shape", "N/A"),
            f"{result.get('spx_close', 0):.0f}" if result.get("spx_close") else "N/A",
            f"{normalized_basis*100:.2f}%" if normalized_basis is not None else "N/A",
        )
        return result