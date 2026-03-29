# modules/macro_filter.py
# ═══════════════════════════════════════════════════════════════════════════════
# 變更紀錄（v3）：
# - [基差寫入] run() 新增取得並寫入 tx_futures_price / tsec_index_price /
#     dividend_adjustment 至 market_regime 表
#     資料來源優先級：Fugle 即時 → FinMind TWII 歷史收盤 → 跳過
# - [成交量寫入] run() 新增取得並寫入 market_volume（大盤日成交量）
#     資料來源：FinMind Y9999 TaiwanStockPrice
# - [建構子] 新增可選的 fugle 與 finmind 參數（預設 None，保持向下相容）
# - INSERT 語句擴充對應新欄位，ON CONFLICT 同步更新
# ───────────────────────────────────────────────────────────────────────────────
# Bug 修正（v3.1）：
# - [BUG 4]  INSERT market_regime 補上 fed_funds_rate / us_10y_yield / us_2y_yield
# - [BUG 9]  _fetch_basis_data() 使用 get_futures_quote('TXF') 取期貨報價，
#            而非 get_quote()（股票端點），避免實盤環境 404
# - [BUG 11] 基差 Redis key 改用 rk.key_market_basis_latest() 統一管理
# ═══════════════════════════════════════════════════════════════════════════════

import json
import logging
from datetime import date, timedelta
from typing import Optional
from datahub.data_hub import DataHub
from datahub.api_fred import FredAPI
from datahub import redis_keys as rk

logger = logging.getLogger(__name__)

# ── 宏觀環境判斷閾值 ──────────────────────────────────────────────────────────
MRS_THRESHOLDS = {
    'BULL_TREND': 80,
    'WEAK_BULL':  60,
    'CHOPPY':     40,
    # 低於 40 = BEAR_TREND
}

# ── 各環境的曝險上限與因子權重 ────────────────────────────────────────────────
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

# ── [v3] Fugle 台指期近月合約代碼 ─────────────────────────────────────────────
FUGLE_TX_TICKER   = "TXF"     # 台指期連續月（期貨端點）
FUGLE_TWII_TICKER = "TWA00"   # 加權指數（股票端點）


class MacroFilter:
    """模組⑥ 宏觀濾網：計算 MRS 分數並判斷市場環境"""

    def __init__(
        self,
        hub: DataHub,
        fred: FredAPI,
        fugle=None,
        finmind=None,
        watchlist: Optional[list[str]] = None,
    ):
        self.hub       = hub
        self.fred      = fred
        self.fugle     = fugle
        self.finmind   = finmind
        self.watchlist = watchlist or ['2330', '2317', '2454', '2303', '2382']

    # =========================================================================
    # MRS 計算
    # =========================================================================

    def _calc_mrs(self, indicators: dict) -> float:
        """計算 MRS 分數（0-100）"""
        score = 50.0

        fed = indicators.get('fed_funds_rate')
        if fed is not None:
            if fed < 2.0:   score += 15
            elif fed < 4.0: score += 5
            elif fed > 5.5: score -= 15
            else:           score -= 5

        vix = indicators.get('vix')
        if vix is not None:
            if vix < 15:    score += 15
            elif vix < 20:  score += 5
            elif vix > 30:  score -= 20
            elif vix > 25:  score -= 10

        y10 = indicators.get('us_10y_yield')
        y2  = indicators.get('us_2y_yield')
        if y10 and y2:
            spread = y10 - y2
            if spread > 0.5:    score += 10
            elif spread > 0:    score += 5
            elif spread < -0.5: score -= 15
            else:               score -= 5

        return max(0.0, min(100.0, score))

    def _classify_regime(self, mrs: float) -> str:
        if mrs >= MRS_THRESHOLDS['BULL_TREND']: return 'BULL_TREND'
        elif mrs >= MRS_THRESHOLDS['WEAK_BULL']: return 'WEAK_BULL'
        elif mrs >= MRS_THRESHOLDS['CHOPPY']:    return 'CHOPPY'
        else:                                    return 'BEAR_TREND'

    # =========================================================================
    # [v3] 台指期基差資料取得
    # =========================================================================

    async def _fetch_basis_data(self, trade_date: date) -> dict:
        """
        取得台指期基差所需資料。

        [BUG 9 修正] 台指期報價改用 get_futures_quote(FUGLE_TX_TICKER)，
        不再使用 get_quote()（股票端點），避免實盤環境 404。
        """
        result = {
            'tx_futures_price':    None,
            'tsec_index_price':    None,
            'dividend_adjustment': 0.0,
        }

        if self.fugle:
            try:
                # [BUG 9 修正] 期貨用 get_futures_quote，加權指數仍用 get_quote
                futures_quote = await self.fugle.get_futures_quote(FUGLE_TX_TICKER)
                spot_quote    = await self.fugle.get_quote(FUGLE_TWII_TICKER)

                if futures_quote:
                    tx_price = (
                        futures_quote.get('closePrice') or
                        futures_quote.get('lastPrice') or
                        futures_quote.get('referencePrice')
                    )
                    if tx_price:
                        result['tx_futures_price'] = float(tx_price)
                        logger.info("Fugle 台指期收盤價：%.0f", result['tx_futures_price'])

                if spot_quote:
                    tsec_price = (
                        spot_quote.get('closePrice') or
                        spot_quote.get('lastPrice') or
                        spot_quote.get('referencePrice')
                    )
                    if tsec_price:
                        result['tsec_index_price'] = float(tsec_price)
                        logger.info("Fugle 加權指數收盤價：%.2f", result['tsec_index_price'])

            except Exception as e:
                logger.warning("Fugle 基差資料取得失敗，改用 FinMind fallback：%s", e)

        if result['tsec_index_price'] is None and self.finmind:
            try:
                start_str  = str(trade_date - timedelta(days=5))
                tsec_close = await self.finmind.get_twii_price(start_date=start_str)
                if tsec_close:
                    result['tsec_index_price'] = tsec_close
                    logger.info("FinMind 加權指數收盤價：%.2f", tsec_close)
            except Exception as e:
                logger.warning("FinMind 加權指數收盤取得失敗：%s", e)

        if result['tx_futures_price'] is None and result['tsec_index_price'] is not None:
            result['tx_futures_price'] = result['tsec_index_price']
            logger.info("台指期價格 fallback 使用加權指數價格（基差設為 0）")

        # 除息點數估算
        if self.finmind and self.watchlist:
            try:
                div_adj = await self.finmind.get_upcoming_dividends(
                    self.watchlist, trade_date
                )
                result['dividend_adjustment'] = div_adj or 0.0
            except Exception as e:
                logger.debug("除息點數取得失敗（使用 0）：%s", e)

        return result

    # =========================================================================
    # [v3] 大盤成交量
    # =========================================================================

    async def _fetch_market_volume(self, trade_date: date) -> Optional[int]:
        if not self.finmind:
            return None
        try:
            start_str = str(trade_date - timedelta(days=5))
            return await self.finmind.get_twii_volume(start_date=start_str)
        except Exception as e:
            logger.warning("大盤成交量取得失敗：%s", e)
            return None

    # =========================================================================
    # 主執行方法
    # =========================================================================

    async def run(self, trade_date: Optional[date] = None) -> dict:
        """執行宏觀濾網，計算 MRS / Regime，並寫入 DB 與 Redis"""
        if trade_date is None:
            trade_date = date.today()

        logger.info("宏觀濾網（v3）執行中，交易日：%s", trade_date)

        # Step 1：FRED 指標
        indicators = await self.fred.get_macro_snapshot()
        logger.info("FRED 指標：%s", indicators)

        # Step 2：MRS + Regime
        mrs    = self._calc_mrs(indicators)
        regime = self._classify_regime(mrs)
        config = REGIME_CONFIG[regime]

        # Step 3：基差
        basis_data = await self._fetch_basis_data(trade_date)
        tx_price   = basis_data['tx_futures_price']
        tsec_price = basis_data['tsec_index_price']
        div_adj    = basis_data['dividend_adjustment']

        if tx_price and tsec_price and tsec_price > 0:
            normalized_basis = (tx_price - (tsec_price - div_adj)) / tsec_price
            logger.info(
                "基差計算：TX=%.0f TSEC=%.2f DIV=%.1f → Basis=%.4f (%.2f%%)",
                tx_price, tsec_price, div_adj,
                normalized_basis, normalized_basis * 100,
            )
        else:
            normalized_basis = None
            logger.info("基差資料不足，跳過基差計算")

        # Step 4：大盤成交量
        market_volume = await self._fetch_market_volume(trade_date)

        # Step 5：回傳結果
        result = {
            'trade_date':         trade_date,
            'regime':             regime,
            'mrs_score':          round(mrs, 2),
            'max_exposure':       config['max_exposure'],
            'factor_weights':     config['weights'],
            'atr_multiplier':     config['atr_multiplier'],
            'vcp_enabled':        config['vcp_enabled'],
            'vix_level':          indicators.get('vix'),
            'fed_funds_rate':     indicators.get('fed_funds_rate'),
            'us_10y_yield':       indicators.get('us_10y_yield'),
            'us_2y_yield':        indicators.get('us_2y_yield'),
            'tx_futures_price':   tx_price,
            'tsec_index_price':   tsec_price,
            'dividend_adjustment': div_adj,
            'normalized_basis':   round(normalized_basis, 6) if normalized_basis is not None else None,
            'market_volume':      market_volume,
        }

        # Step 6：寫入 PostgreSQL
        # [BUG 4 修正] INSERT 補上 fed_funds_rate / us_10y_yield / us_2y_yield（$13,$14,$15）
        # 原本只有 $1~$12，這三個利率欄位雖然已在 migration.sql PART 7 建好，
        # 但 INSERT 未納入，導致歷史利率資料永遠為 NULL。
        await self.hub.execute("""
            INSERT INTO market_regime (
                trade_date, regime, mrs_score,
                max_exposure_limit, factor_weights,
                atr_multiplier, vcp_enabled,
                vix_level,
                tx_futures_price, tsec_index_price,
                dividend_adjustment, market_volume,
                fed_funds_rate, us_10y_yield, us_2y_yield
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
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
                us_2y_yield         = EXCLUDED.us_2y_yield
        """,
            trade_date,
            regime,
            mrs,
            config['max_exposure'],
            json.dumps(config['weights']),
            config['atr_multiplier'],
            config['vcp_enabled'],
            indicators.get('vix'),
            tx_price,
            tsec_price,
            div_adj,
            market_volume,
            indicators.get('fed_funds_rate'),   # [BUG 4 修正] $13
            indicators.get('us_10y_yield'),     # [BUG 4 修正] $14
            indicators.get('us_2y_yield'),      # [BUG 4 修正] $15
        )

        # Step 7：Redis 快取
        cache_payload = {
            'regime':              regime,
            'mrs_score':           round(mrs, 2),
            'max_exposure':        config['max_exposure'],
            'vix_level':           indicators.get('vix'),
            'tx_futures_price':    tx_price,
            'tsec_index_price':    tsec_price,
            'dividend_adjustment': div_adj,
            'normalized_basis':    result['normalized_basis'],
            'market_volume':       market_volume,
        }
        await self.hub.cache.set(
            rk.key_market_regime_latest(),
            cache_payload,
            ttl=rk.TTL_24H,
        )

        # [BUG 11 修正] 基差 Redis key 改用 rk.key_market_basis_latest() 統一管理
        if normalized_basis is not None:
            await self.hub.cache.set(
                rk.key_market_basis_latest(),
                {
                    'basis': result['normalized_basis'],
                    'tx':    tx_price,
                    'tsec':  tsec_price,
                    'div':   div_adj,
                },
                ttl=rk.TTL_5MIN,
            )
            logger.info(
                "基差 Cache 更新：Basis=%.4f (%.2f%%)",
                normalized_basis, normalized_basis * 100,
            )

        logger.info(
            "宏觀濾網（v3）完成：%s MRS=%.1f Basis=%s Vol=%s",
            regime, mrs,
            f"{normalized_basis*100:.2f}%" if normalized_basis is not None else "N/A",
            f"{market_volume:,}" if market_volume else "N/A",
        )
        return result