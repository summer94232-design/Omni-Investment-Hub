# modules/macro_filter.py
import logging
from datetime import date
from typing import Optional
import asyncpg
from datahub.data_hub import DataHub
from datahub.api_fred import FredAPI
from datahub import redis_keys as rk

logger = logging.getLogger(__name__)

# 宏觀環境判斷閾值
MRS_THRESHOLDS = {
    'BULL_TREND':  80,
    'WEAK_BULL':   60,
    'CHOPPY':      40,
    # 低於 40 = BEAR_TREND
}

# 各環境的曝險上限與因子權重
REGIME_CONFIG = {
    'BULL_TREND': {
        'max_exposure': 0.90,
        'weights': {'momentum': 0.38, 'chip': 0.28, 'fundamental': 0.22, 'valuation': 0.12},
        'atr_multiplier': 3.0,
        'vcp_enabled': True,
    },
    'WEAK_BULL': {
        'max_exposure': 0.70,
        'weights': {'momentum': 0.30, 'chip': 0.25, 'fundamental': 0.25, 'valuation': 0.20},
        'atr_multiplier': 2.5,
        'vcp_enabled': True,
    },
    'CHOPPY': {
        'max_exposure': 0.40,
        'weights': {'momentum': 0.20, 'chip': 0.25, 'fundamental': 0.30, 'valuation': 0.25},
        'atr_multiplier': 2.0,
        'vcp_enabled': False,
    },
    'BEAR_TREND': {
        'max_exposure': 0.10,
        'weights': {'momentum': 0.15, 'chip': 0.20, 'fundamental': 0.35, 'valuation': 0.30},
        'atr_multiplier': 2.0,
        'vcp_enabled': False,
    },
}


class MacroFilter:
    """模組⑥ 宏觀濾網：計算 MRS 分數並判斷市場環境"""

    def __init__(self, hub: DataHub, fred: FredAPI):
        self.hub = hub
        self.fred = fred

    def _calc_mrs(self, indicators: dict) -> float:
        """計算 MRS 分數（0-100）"""
        score = 50.0  # 基準分

        # Fed Funds Rate（低利率加分）
        fed = indicators.get('fed_funds_rate')
        if fed is not None:
            if fed < 2.0:
                score += 15
            elif fed < 4.0:
                score += 5
            elif fed > 5.5:
                score -= 15
            else:
                score -= 5

        # VIX（低波動加分）
        vix = indicators.get('vix')
        if vix is not None:
            if vix < 15:
                score += 15
            elif vix < 20:
                score += 5
            elif vix > 30:
                score -= 20
            elif vix > 25:
                score -= 10

        # 10Y-2Y 殖利率差（正斜率加分）
        y10 = indicators.get('us_10y_yield')
        y2 = indicators.get('us_2y_yield')
        if y10 and y2:
            spread = y10 - y2
            if spread > 0.5:
                score += 10
            elif spread > 0:
                score += 5
            elif spread < -0.5:
                score -= 15
            else:
                score -= 5

        return max(0.0, min(100.0, score))

    def _classify_regime(self, mrs: float) -> str:
        if mrs >= MRS_THRESHOLDS['BULL_TREND']:
            return 'BULL_TREND'
        elif mrs >= MRS_THRESHOLDS['WEAK_BULL']:
            return 'WEAK_BULL'
        elif mrs >= MRS_THRESHOLDS['CHOPPY']:
            return 'CHOPPY'
        else:
            return 'BEAR_TREND'

    async def run(self, trade_date: Optional[date] = None) -> dict:
        """執行宏觀濾網，回傳當日環境快照"""
        if trade_date is None:
            trade_date = date.today()

        logger.info("宏觀濾網執行中，交易日：%s", trade_date)

        # 取得總經數據
        indicators = await self.fred.get_macro_snapshot()
        logger.info("FRED 指標：%s", indicators)

        # 計算 MRS
        mrs = self._calc_mrs(indicators)
        regime = self._classify_regime(mrs)
        config = REGIME_CONFIG[regime]

        result = {
            'trade_date':        trade_date,
            'regime':            regime,
            'mrs_score':         round(mrs, 2),
            'max_exposure':      config['max_exposure'],
            'factor_weights':    config['weights'],
            'atr_multiplier':    config['atr_multiplier'],
            'vcp_enabled':       config['vcp_enabled'],
            'vix_level':         indicators.get('vix'),
            'fed_funds_rate':    indicators.get('fed_funds_rate'),
            'us_10y_yield':      indicators.get('us_10y_yield'),
            'us_2y_yield':       indicators.get('us_2y_yield'),
        }

        # 寫入 PostgreSQL
        await self.hub.execute("""
            INSERT INTO market_regime (
                trade_date, regime, mrs_score,
                max_exposure_limit, factor_weights,
                atr_multiplier, vcp_enabled,
                vix_level
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (trade_date) DO UPDATE SET
                regime            = EXCLUDED.regime,
                mrs_score         = EXCLUDED.mrs_score,
                max_exposure_limit= EXCLUDED.max_exposure_limit,
                factor_weights    = EXCLUDED.factor_weights,
                atr_multiplier    = EXCLUDED.atr_multiplier,
                vcp_enabled       = EXCLUDED.vcp_enabled,
                vix_level         = EXCLUDED.vix_level
        """,
            trade_date,
            regime,
            mrs,
            config['max_exposure'],
            str(config['weights']).replace("'", '"'),
            config['atr_multiplier'],
            config['vcp_enabled'],
            indicators.get('vix'),
        )

        # 寫入 Redis 快取
        await self.hub.cache.set(
            rk.key_market_regime_latest(),
            result,
            ttl=rk.TTL_24H,
        )

        logger.info("宏觀濾網完成：%s MRS=%.1f", regime, mrs)
        return result