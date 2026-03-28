# modules/selection_engine.py
import logging
from datetime import date
from typing import Optional
import pandas as pd
from datahub.data_hub import DataHub
from datahub.api_finmind import FinMindAPI
from datahub import redis_keys as rk

logger = logging.getLogger(__name__)

# 預設因子權重（會被 WF 驗證結果覆蓋）
DEFAULT_WEIGHTS = {
    'momentum':    0.30,
    'chip':        0.25,
    'fundamental': 0.25,
    'valuation':   0.20,
}

# ── 產業對照表（台股主要族群）────────────────────────────────────────────────
# 格式：ticker → sector_code
# 可依需求擴充；未列入者自動歸為 'OTHER'
TICKER_SECTOR_MAP: dict[str, str] = {
    # 半導體
    '2330': 'SEMI', '2303': 'SEMI', '2454': 'SEMI',
    '3034': 'SEMI', '6770': 'SEMI', '2379': 'SEMI',
    '3711': 'SEMI', '2408': 'SEMI', '6409': 'SEMI',
    # 電子零組件 / 被動元件
    '2327': 'COMPONENT', '2328': 'COMPONENT', '2382': 'COMPONENT',
    '3231': 'COMPONENT',
    # 網通 / 伺服器
    '2317': 'NETWORK', '2354': 'NETWORK', '3044': 'NETWORK',
    '4977': 'NETWORK',
    # AI / 雲端伺服器
    '6669': 'AI_SERVER', '3673': 'AI_SERVER', '2353': 'AI_SERVER',
    # 電信
    '2412': 'TELECOM', '3045': 'TELECOM', '4904': 'TELECOM',
    # 金融
    '2882': 'FINANCE', '2881': 'FINANCE', '2891': 'FINANCE',
    '2886': 'FINANCE', '2884': 'FINANCE',
    # 傳產 / 鋼鐵
    '2002': 'STEEL', '2006': 'STEEL',
    # 航運
    '2603': 'SHIPPING', '2609': 'SHIPPING', '2615': 'SHIPPING',
}

# 產業群聚加分設定
# 當同產業有 N 檔同時觸發訊號，每檔的 total_score 加 bonus
SECTOR_CLUSTER_BONUS = {
    2: 3.0,   # 同產業 2 檔觸發 → +3 分
    3: 6.0,   # 同產業 3 檔觸發 → +6 分
    4: 9.0,   # 同產業 4+ 檔觸發 → +9 分
}
SECTOR_CLUSTER_CAP = 9.0   # 加分上限


def get_sector(ticker: str) -> str:
    """回傳股票所屬產業代碼"""
    return TICKER_SECTOR_MAP.get(ticker.strip(), 'OTHER')


def apply_sector_cluster_bonus(
    results: list[dict],
    score_threshold: float = 55.0,
) -> list[dict]:
    """
    產業群聚效應加權（批次處理）
    
    在選股引擎跑完所有個股後，對同產業同時觸發訊號（≥ threshold）的個股
    施加 bonus，反映「族群共振」帶來的進場信心提升。

    變更紀錄（v2）：
    - [產業群聚] 新增此函式，在 scheduler.py 的 Step 3 後呼叫
    - 不修改資料庫內的 total_score（GENERATED ALWAYS 欄位不可直接改），
      而是在 result dict 新增 sector_bonus 與 final_score 欄位供後續使用
    """
    # 統計各產業觸發數量
    sector_counts: dict[str, int] = {}
    for r in results:
        ticker = r['ticker'].strip()
        sector = get_sector(ticker)
        if r.get('total_score', 0) >= score_threshold and sector != 'OTHER':
            sector_counts[sector] = sector_counts.get(sector, 0) + 1

    # 對每個 result 計算 bonus
    for r in results:
        ticker = r['ticker'].strip()
        sector = get_sector(ticker)
        count  = sector_counts.get(sector, 0)

        # 查找 bonus（取最高符合的 tier）
        bonus = 0.0
        if count >= 2 and sector != 'OTHER':
            for threshold in sorted(SECTOR_CLUSTER_BONUS.keys(), reverse=True):
                if count >= threshold:
                    bonus = min(SECTOR_CLUSTER_BONUS[threshold], SECTOR_CLUSTER_CAP)
                    break

        r['sector']       = sector
        r['sector_count'] = count
        r['sector_bonus'] = round(bonus, 2)
        r['final_score']  = round(r.get('total_score', 0) + bonus, 2)

        if bonus > 0:
            logger.info(
                "產業群聚加分：%s [%s] 族群%d檔共振 +%.1f → final_score=%.1f",
                ticker, sector, count, bonus, r['final_score']
            )

    return results


class SelectionEngine:
    """模組② 多因子選股引擎：整合宏觀/籌碼/基本面/動能，輸出個股總分
    
    變更紀錄（v2）：
    - [產業群聚] 新增 TICKER_SECTOR_MAP 與 apply_sector_cluster_bonus()
      → 由 scheduler.py 批次呼叫，不影響單一個股的 run() 介面
    - run() 回傳結果新增 sector 欄位，方便後續歸因
    """

    def __init__(self, hub: DataHub, finmind: FinMindAPI):
        self.hub = hub
        self.finmind = finmind

    async def _get_factor_weights(self) -> dict:
        """從 Redis 或 DB 取得當前因子權重"""
        cached = await self.hub.cache.get(rk.key_wf_weights_current())
        if cached:
            return cached

        row = await self.hub.fetchrow("""
            SELECT recommended_weights FROM wf_results
            WHERE is_active = TRUE
            ORDER BY run_at DESC LIMIT 1
        """)
        if row and row['recommended_weights']:
            import json
            weights = json.loads(row['recommended_weights'])
            await self.hub.cache.set(rk.key_wf_weights_current(), weights, ttl=rk.TTL_24H)
            return weights

        return DEFAULT_WEIGHTS

    async def _get_macro_weights(self, trade_date: date) -> dict:
        """從宏觀濾網取得動態因子權重"""
        cached = await self.hub.cache.get(rk.key_market_regime_latest())
        if cached and cached.get('factor_weights'):
            return cached['factor_weights']

        row = await self.hub.fetchrow("""
            SELECT factor_weights FROM market_regime
            WHERE trade_date <= $1
            ORDER BY trade_date DESC LIMIT 1
        """, trade_date)

        if row and row['factor_weights']:
            import json
            return json.loads(row['factor_weights'])

        return DEFAULT_WEIGHTS

    def _calc_momentum_score(self, price_df) -> float:
        """計算動能因子分數（0–100）"""
        if price_df is None or price_df.empty or len(price_df) < 60:
            return 50.0

        df     = price_df.sort_values('date')
        closes = df['close'].astype(float)
        score  = 50.0

        # 20/60 日均線位置
        ma20  = closes.tail(20).mean()
        ma60  = closes.tail(60).mean()
        price = closes.iloc[-1]

        if price > ma20 > ma60:
            score += 20
        elif price > ma20:
            score += 10
        elif price < ma20 < ma60:
            score -= 20
        elif price < ma20:
            score -= 10

        # 20 日漲跌幅
        if len(closes) >= 20:
            ret_20d = (closes.iloc[-1] - closes.iloc[-20]) / closes.iloc[-20]
            if ret_20d > 0.15:
                score += 20
            elif ret_20d > 0.05:
                score += 10
            elif ret_20d < -0.15:
                score -= 20
            elif ret_20d < -0.05:
                score -= 10

        return round(max(0, min(100, score)), 2)

    def _calc_fundamental_score(self, revenue_df) -> float:
        """計算基本面因子分數（0–100）"""
        if revenue_df is None or revenue_df.empty:
            return 50.0

        score = 50.0
        df = revenue_df.sort_values('date')

        if len(df) >= 13:
            latest_rev  = float(df['revenue'].iloc[-1])
            yoy_rev     = float(df['revenue'].iloc[-13])
            if yoy_rev > 0:
                yoy = (latest_rev - yoy_rev) / yoy_rev
                if yoy > 0.30:   score += 30
                elif yoy > 0.10: score += 15
                elif yoy > 0:    score += 5
                elif yoy < -0.20: score -= 25
                elif yoy < -0.10: score -= 15
                else:             score -= 5

        return round(max(0, min(100, score)), 2)

    async def _get_chip_score(self, ticker: str, trade_date: date) -> float:
        """從 chip_monitor 取得 CRS 分數"""
        cached = await self.hub.cache.get(rk.key_chip_crs_latest(ticker))
        if cached and cached.get('crs_total') is not None:
            return float(cached['crs_total'])

        row = await self.hub.fetchrow("""
            SELECT crs_total FROM chip_monitor
            WHERE ticker = $1 AND trade_date <= $2
            ORDER BY trade_date DESC LIMIT 1
        """, ticker, trade_date)

        return float(row['crs_total']) if row else 50.0

    async def run(
        self,
        ticker: str,
        trade_date: Optional[date] = None,
    ) -> dict:
        """執行選股引擎，回傳個股多因子診斷"""
        if trade_date is None:
            trade_date = date.today()

        logger.info("選股引擎執行中：%s %s", ticker, trade_date)

        start_date = str(date(trade_date.year - 1, trade_date.month, trade_date.day))

        # 取得數據
        price_df   = await self.finmind.get_stock_price(ticker, start_date)
        revenue_df = await self.finmind.get_revenue(ticker, start_date)

        # 計算各因子分數
        score_momentum    = self._calc_momentum_score(price_df)
        score_fundamental = self._calc_fundamental_score(revenue_df)
        score_chip        = await self._get_chip_score(ticker, trade_date)
        score_valuation   = 50.0  # 預留，後續擴充 P/E 計算

        # 取得動態權重
        weights = await self._get_macro_weights(trade_date)

        # 加權總分
        total_score = (
            score_momentum    * weights.get('momentum', 0.30) +
            score_chip        * weights.get('chip', 0.25) +
            score_fundamental * weights.get('fundamental', 0.25) +
            score_valuation   * weights.get('valuation', 0.20)
        )

        # 取得宏觀環境快照
        regime_row = await self.hub.fetchrow("""
            SELECT regime, mrs_score FROM market_regime
            WHERE trade_date <= $1
            ORDER BY trade_date DESC LIMIT 1
        """, trade_date)

        regime = regime_row['regime']    if regime_row else None
        mrs    = float(regime_row['mrs_score']) if regime_row else None

        # 產業標籤（新增）
        sector = get_sector(ticker)

        result = {
            'ticker':             ticker,
            'trade_date':         trade_date,
            'score_momentum':     score_momentum,
            'score_chip':         score_chip,
            'score_fundamental':  score_fundamental,
            'score_valuation':    score_valuation,
            'weight_momentum':    weights.get('momentum', 0.30),
            'weight_chip':        weights.get('chip', 0.25),
            'weight_fundamental': weights.get('fundamental', 0.25),
            'weight_valuation':   weights.get('valuation', 0.20),
            'total_score':        round(total_score, 2),
            'final_score':        round(total_score, 2),  # 群聚加分後由外部覆蓋
            'sector':             sector,                  # ← 新增
            'sector_bonus':       0.0,                    # ← 新增（外部填充）
            'sector_count':       0,                      # ← 新增（外部填充）
            'regime_at_calc':     regime,
            'mrs_at_calc':        mrs,
        }

        # 寫入 PostgreSQL
        # 注意：stock_diagnostic 是月分區表，不支援跨分區 ON CONFLICT
        # 先刪除同日同檔的舊記錄，再 INSERT（冪等操作）
        await self.hub.execute("""
            DELETE FROM stock_diagnostic
            WHERE trade_date = $1 AND ticker = $2
        """, trade_date, ticker)

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
            weights.get('momentum', 0.30),
            weights.get('chip', 0.25),
            weights.get('fundamental', 0.25),
            weights.get('valuation', 0.20),
            regime, mrs,
        )

        logger.info("選股引擎完成：%s 總分=%.1f [%s]", ticker, total_score, sector)
        return result
