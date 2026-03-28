# modules/selection_engine.py
import json
import logging
from datetime import date
from typing import Optional
import pandas as pd
from datahub.data_hub import DataHub
from datahub.api_finmind import FinMindAPI
from datahub import redis_keys as rk

logger = logging.getLogger(__name__)

# 預設因子權重（當 UI 或 Walk-Forward 皆無資料時使用）
DEFAULT_WEIGHTS = {
    'momentum':    0.30,
    'chip':        0.25,
    'fundamental': 0.25,
    'valuation':   0.20,
}


class SelectionEngine:
    """模組② 多因子選股引擎：整合宏觀/籌碼/基本面/動能，輸出個股總分"""

    def __init__(self, hub: DataHub, finmind: FinMindAPI):
        self.hub     = hub
        self.finmind = finmind

    async def _get_active_weights(self, trade_date: date) -> dict:
        """
        取得當前生效的權重邏輯（優先順序）：
        1. UI 手動設定的權重 (儲存於 Redis)
        2. 宏觀濾網根據環境建議的權重 (market_regime)
        3. Walk-Forward 驗證後的最佳權重 (wf_results)
        4. 系統預設值
        """
        # 1. 嘗試從 Redis 取得 UI 儲存的最新設定
        ui_settings = await self.hub.cache.get("portfolio:settings:current")
        if ui_settings and 'weights' in ui_settings:
            logger.info("使用 UI 手動調整之因子權重")
            return ui_settings['weights']

        # 2. 嘗試從宏觀濾網取得動態權重
        macro_row = await self.hub.fetchrow("""
            SELECT factor_weights FROM market_regime
            WHERE trade_date <= $1
            ORDER BY trade_date DESC LIMIT 1
        """, trade_date)
        if macro_row and macro_row['factor_weights']:
            return json.loads(macro_row['factor_weights'])

        # 3. 嘗試從 Walk-Forward 取得建議權重
        wf_row = await self.hub.fetchrow("""
            SELECT recommended_weights FROM wf_results
            WHERE is_active = TRUE
            ORDER BY run_at DESC LIMIT 1
        """)
        if wf_row and wf_row['recommended_weights']:
            return json.loads(wf_row['recommended_weights'])

        return DEFAULT_WEIGHTS

    def _calc_momentum_score(self, price_df: pd.DataFrame) -> float:
        """計算動能分數（0-100）"""
        if price_df.empty or len(price_df) < 20:
            return 50.0

        price_df = price_df.sort_values('date')
        closes   = price_df['close'].astype(float)
        score    = 50.0

        # 20日漲幅
        if len(closes) >= 20:
            ret_20d = (closes.iloc[-1] - closes.iloc[-20]) / closes.iloc[-20]
            if ret_20d > 0.15:    score += 25
            elif ret_20d > 0.05:  score += 15
            elif ret_20d > 0:     score += 5
            elif ret_20d < -0.15: score -= 25
            elif ret_20d < -0.05: score -= 15
            else:                 score -= 5

        # 均線多頭排列
        if len(closes) >= 60:
            ma20  = closes.iloc[-20:].mean()
            ma60  = closes.iloc[-60:].mean()
            price = closes.iloc[-1]
            if price > ma20 > ma60:
                score += 15
            elif price < ma20 < ma60:
                score -= 15

        # 距52週高點
        high_52w     = closes.iloc[-252:].max() if len(closes) >= 252 else closes.max()
        pct_from_high = (closes.iloc[-1] - high_52w) / high_52w
        if pct_from_high > -0.05:   score += 10
        elif pct_from_high < -0.30: score -= 10

        return round(max(0, min(100, score)), 2)

    def _calc_fundamental_score(self, revenue_df: pd.DataFrame) -> float:
        """計算基本面分數（0-100）"""
        if revenue_df.empty:
            return 50.0

        score      = 50.0
        revenue_df = revenue_df.sort_values('date')

        if len(revenue_df) >= 2:
            latest = float(revenue_df['revenue'].iloc[-1])
            prev   = float(revenue_df['revenue'].iloc[-2])
            if prev > 0:
                yoy = (latest - prev) / prev
                if yoy > 0.20:    score += 25
                elif yoy > 0.10:  score += 15
                elif yoy > 0:     score += 5
                elif yoy < -0.20: score -= 25
                elif yoy < -0.10: score -= 15
                else:             score -= 5

        return round(max(0, min(100, score)), 2)

    async def _get_chip_score(self, ticker: str, trade_date: date) -> float:
        """從 chip_monitor 取得 CRS 分數"""
        row = await self.hub.fetchrow("""
            SELECT crs_total FROM chip_monitor
            WHERE ticker = $1 AND trade_date <= $2
            ORDER BY trade_date DESC LIMIT 1
        """, ticker, trade_date)
        return float(row['crs_total']) if row else 50.0

    async def run(self, ticker: str, trade_date: Optional[date] = None) -> dict:
        """執行選股引擎，回傳個股多因子診斷"""
        if trade_date is None:
            trade_date = date.today()

        logger.info("選股引擎執行中：%s %s", ticker, trade_date)
        start_date = str(date(trade_date.year - 1, trade_date.month, trade_date.day))

        # 1. 取得當前權重設定
        weights = await self._get_active_weights(trade_date)

        # 2. 只有在權重 > 0 時才抓取數據
        score_momentum = 50.0
        if weights.get('momentum', 0) > 0:
            price_df       = await self.finmind.get_stock_price(ticker, start_date)
            score_momentum = self._calc_momentum_score(price_df)

        score_fundamental = 50.0
        if weights.get('fundamental', 0) > 0:
            revenue_df        = await self.finmind.get_revenue(ticker, start_date)
            score_fundamental = self._calc_fundamental_score(revenue_df)

        score_chip = 50.0
        if weights.get('chip', 0) > 0:
            score_chip = await self._get_chip_score(ticker, trade_date)

        score_valuation = 50.0  # 預留估值因子空間

        # 3. 計算加權總分
        total_score = (
            score_momentum    * weights.get('momentum', 0) +
            score_chip        * weights.get('chip', 0) +
            score_fundamental * weights.get('fundamental', 0) +
            score_valuation   * weights.get('valuation', 0)
        )

        # 4. 取得宏觀快照供記錄
        regime_row = await self.hub.fetchrow("""
            SELECT regime, mrs_score FROM market_regime
            WHERE trade_date <= $1
            ORDER BY trade_date DESC LIMIT 1
        """, trade_date)
        regime = regime_row['regime']    if regime_row else None
        mrs    = float(regime_row['mrs_score']) if regime_row else None

        result = {
            'ticker':             ticker,
            'trade_date':         trade_date,
            'score_momentum':     score_momentum,
            'score_chip':         score_chip,
            'score_fundamental':  score_fundamental,
            'score_valuation':    score_valuation,
            'weight_momentum':    weights.get('momentum', 0),
            'weight_chip':        weights.get('chip', 0),
            'weight_fundamental': weights.get('fundamental', 0),
            'weight_valuation':   weights.get('valuation', 0),
            'total_score':        round(total_score, 2),
            'regime_at_calc':     regime,
            'mrs_at_calc':        mrs,
        }

        # 5. 寫入資料庫
        # total_score 是 schema 中的 generated column（資料庫自動計算），
        # 不可手動 INSERT，移除此欄位讓 DB 自行產生。
        # walk_forward.py 查詢時直接讀取 DB 算好的值即可。
        await self.hub.execute("""
            INSERT INTO stock_diagnostic (
                trade_date, ticker,
                score_momentum, score_chip, score_fundamental, score_valuation,
                weight_momentum, weight_chip, weight_fundamental, weight_valuation,
                regime_at_calc, mrs_at_calc
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
        """,
            trade_date, ticker,
            score_momentum, score_chip, score_fundamental, score_valuation,
            weights.get('momentum', 0), weights.get('chip', 0),
            weights.get('fundamental', 0), weights.get('valuation', 0),
            regime, mrs,
        )

        logger.info("選股引擎完成：%s 總分=%.1f", ticker, total_score)
        return result