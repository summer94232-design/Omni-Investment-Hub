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
# Fugle MarketData API 的期貨代碼格式：TXF1（近月）或 TXF（連續月）
FUGLE_TX_TICKER   = "TXF"     # 台指期連續月（近月合約）
FUGLE_TWII_TICKER = "TWA00"   # 加權指數（Fugle 代碼）


class MacroFilter:
    """模組⑥ 宏觀濾網：計算 MRS 分數並判斷市場環境

    變更紀錄（v3）：
    - [基差寫入] run() 自動取得台指期 TX 價格和加權指數收盤價，
      計算 Normalized Basis 並寫入 market_regime 表，
      讓 BasisFilter 的 DB fallback 路徑能拿到資料
    - [成交量寫入] run() 自動取得大盤日成交量（FinMind Y9999），
      讓 BlackSwan._check_volume_ratio() 的主路徑能正常運作
    - [建構子] 新增可選 fugle / finmind 參數，不傳入時優雅降級
    """

    def __init__(
        self,
        hub: DataHub,
        fred: FredAPI,
        fugle=None,    # 可選：FugleAPI 實例（取即時期貨報價）
        finmind=None,  # 可選：FinMindAPI 實例（取歷史成交量、加權指數）
        watchlist: Optional[list[str]] = None,  # 觀察清單（除息估算用）
    ):
        self.hub      = hub
        self.fred     = fred
        self.fugle    = fugle
        self.finmind  = finmind
        self.watchlist = watchlist or ['2330', '2317', '2454', '2303', '2382']

    # =========================================================================
    # MRS 計算（不變）
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
    # [v3 新增] 台指期基差資料取得
    # =========================================================================

    async def _fetch_basis_data(
        self,
        trade_date: date,
    ) -> dict:
        """
        取得台指期基差所需的三個數值：
            tx_futures_price    : 台指期近月收盤價
            tsec_index_price    : 加權指數收盤價
            dividend_adjustment : 預估除息點數

        優先級：
            Fugle 即時報價 → FinMind 歷史收盤 → 回傳 None（跳過寫入）

        回傳：
            {
                'tx_futures_price':    float or None,
                'tsec_index_price':    float or None,
                'dividend_adjustment': float,
            }
        """
        result = {
            'tx_futures_price':    None,
            'tsec_index_price':    None,
            'dividend_adjustment': 0.0,
        }

        # ── ① 嘗試從 Fugle 取即時報價 ──────────────────────────────────
        if self.fugle:
            try:
                futures_quote = await self.fugle.get_quote(FUGLE_TX_TICKER)
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

        # ── ② FinMind fallback：取歷史收盤（Fugle 失敗或未配置時）─────
        if result['tsec_index_price'] is None and self.finmind:
            try:
                start_str = str(trade_date - timedelta(days=5))
                tsec_close = await self.finmind.get_twii_price(start_date=start_str)
                if tsec_close:
                    result['tsec_index_price'] = tsec_close
                    logger.info("FinMind 加權指數收盤價：%.2f", tsec_close)
            except Exception as e:
                logger.warning("FinMind 加權指數收盤取得失敗：%s", e)

        # ── ③ 台指期價格：若 Fugle 沒拿到，用 tsec_index_price 代入（基差設為 0）──
        #   說明：收盤後 Fugle 期貨行情可能不可用，用現貨價代入
        #   代入後 basis = 0，不觸發任何過濾（保守處理）
        if result['tx_futures_price'] is None and result['tsec_index_price'] is not None:
            result['tx_futures_price'] = result['tsec_index_price']
            logger.info("台指期價格 fallback 使用加權指數價格（基差設為 0）")

        # ── ④ 除息點數估算 ─────────────────────────────────────────────
        if self.finmind and result['tsec_index_price']:
            try:
                div_points = await self.finmind.get_upcoming_dividends(
                    watchlist=self.watchlist,
                    reference_date=trade_date,
                    days_ahead=30,
                )
                result['dividend_adjustment'] = div_points
            except Exception as e:
                logger.warning("除息點數估算失敗（使用 0）：%s", e)

        return result

    # =========================================================================
    # [v3 新增] 大盤成交量取得
    # =========================================================================

    async def _fetch_market_volume(self, trade_date: date) -> Optional[int]:
        """
        取得大盤日成交量（元），寫入 market_regime.market_volume。

        資料來源：FinMind Y9999 TaiwanStockPrice
        無 FinMind 實例時回傳 None（跳過寫入）

        成交量說明：
            FinMind Y9999 的 Trading_Volume 欄位為大盤當日成交量（元）
            BlackSwan._check_volume_ratio() 用 5MA/20MA 比較，單位一致即可
        """
        if not self.finmind:
            logger.debug("無 FinMind 實例，跳過大盤成交量取得")
            return None

        try:
            start_str  = str(trade_date - timedelta(days=5))
            vol = await self.finmind.get_twii_volume(start_date=start_str)
            if vol:
                logger.info("大盤成交量：%d 元", vol)
            return vol
        except Exception as e:
            logger.warning("大盤成交量取得失敗：%s", e)
            return None

    # =========================================================================
    # run() 主函式（v3：擴充寫入欄位）
    # =========================================================================

    async def run(self, trade_date: Optional[date] = None) -> dict:
        """
        執行宏觀濾網，回傳當日環境快照。

        v3 新增：
        1. 取得台指期基差資料（tx_futures_price, tsec_index_price, dividend_adjustment）
        2. 取得大盤成交量（market_volume）
        3. 以上資料一併寫入 market_regime 表，並更新 Redis 快取
        """
        if trade_date is None:
            trade_date = date.today()

        logger.info("宏觀濾網（v3）執行中，交易日：%s", trade_date)

        # ── Step 1：取得 FRED 總經指標 ───────────────────────────────────
        indicators = await self.fred.get_macro_snapshot()
        logger.info("FRED 指標：%s", indicators)

        # ── Step 2：計算 MRS 與 Regime ───────────────────────────────────
        mrs    = self._calc_mrs(indicators)
        regime = self._classify_regime(mrs)
        config = REGIME_CONFIG[regime]

        # ── Step 3：[v3] 取得基差資料 ────────────────────────────────────
        basis_data = await self._fetch_basis_data(trade_date)
        tx_price   = basis_data['tx_futures_price']
        tsec_price = basis_data['tsec_index_price']
        div_adj    = basis_data['dividend_adjustment']

        # 計算 Normalized Basis（供 log 顯示）
        if tx_price and tsec_price and tsec_price > 0:
            normalized_basis = (tx_price - (tsec_price - div_adj)) / tsec_price
            logger.info(
                "基差計算：TX=%.0f TSEC=%.2f DIV=%.1f → Basis=%.4f (%.2f%%)",
                tx_price, tsec_price, div_adj,
                normalized_basis, normalized_basis * 100
            )
        else:
            normalized_basis = None
            logger.info("基差資料不足，跳過基差計算")

        # ── Step 4：[v3] 取得大盤成交量 ──────────────────────────────────
        market_volume = await self._fetch_market_volume(trade_date)

        # ── Step 5：組裝回傳結果 ─────────────────────────────────────────
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
            # [v3 新增]
            'tx_futures_price':   tx_price,
            'tsec_index_price':   tsec_price,
            'dividend_adjustment': div_adj,
            'normalized_basis':   round(normalized_basis, 6) if normalized_basis is not None else None,
            'market_volume':      market_volume,
        }

        # ── Step 6：寫入 PostgreSQL（v3：擴充欄位）──────────────────────
        await self.hub.execute("""
            INSERT INTO market_regime (
                trade_date, regime, mrs_score,
                max_exposure_limit, factor_weights,
                atr_multiplier, vcp_enabled,
                vix_level,
                tx_futures_price, tsec_index_price,
                dividend_adjustment, market_volume
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
            ON CONFLICT (trade_date) DO UPDATE SET
                regime             = EXCLUDED.regime,
                mrs_score          = EXCLUDED.mrs_score,
                max_exposure_limit = EXCLUDED.max_exposure_limit,
                factor_weights     = EXCLUDED.factor_weights,
                atr_multiplier     = EXCLUDED.atr_multiplier,
                vcp_enabled        = EXCLUDED.vcp_enabled,
                vix_level          = EXCLUDED.vix_level,
                tx_futures_price   = EXCLUDED.tx_futures_price,
                tsec_index_price   = EXCLUDED.tsec_index_price,
                dividend_adjustment= EXCLUDED.dividend_adjustment,
                market_volume      = EXCLUDED.market_volume
        """,
            trade_date,
            regime,
            mrs,
            config['max_exposure'],
            json.dumps(config['weights']),
            config['atr_multiplier'],
            config['vcp_enabled'],
            indicators.get('vix'),
            tx_price,       # [v3] 可為 None，PostgreSQL NUMERIC 接受 NULL
            tsec_price,     # [v3]
            div_adj,        # [v3]
            market_volume,  # [v3] 可為 None
        )

        # ── Step 7：寫入 Redis 快取（擴充欄位）──────────────────────────
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

        # [v3] 同時把基差寫入獨立的 basis cache key，供 BasisFilter 快速讀取
        if normalized_basis is not None:
            from datahub import redis_keys as _rk
            BASIS_CACHE_KEY = "market:basis:latest"
            await self.hub.cache.set(
                BASIS_CACHE_KEY,
                {
                    'basis': result['normalized_basis'],
                    'tx':    tx_price,
                    'tsec':  tsec_price,
                    'div':   div_adj,
                },
                ttl=300,  # BasisFilter 的 TTL（5 分鐘）
            )
            logger.info(
                "基差 Cache 更新：Basis=%.4f (%.2f%%)",
                normalized_basis, normalized_basis * 100
            )

        logger.info(
            "宏觀濾網（v3）完成：%s MRS=%.1f Basis=%s Vol=%s",
            regime, mrs,
            f"{normalized_basis*100:.2f}%" if normalized_basis is not None else "N/A",
            f"{market_volume:,}" if market_volume else "N/A",
        )
        return result