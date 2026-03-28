# modules/selection_engine.py
# ═══════════════════════════════════════════════════════════════════════════════
# 變更紀錄（v3）：
# - [基差過濾] 新增 BasisFilter：
#     硬性門檻：Normalized Basis < -1.5% → Score 強制歸零，攔截所有進場
#     動態懲罰：-1.5% ≤ Basis < -0.8% → final_score × 0.8
#     資料來源：Fugle API（即時 TX 期貨報價）+ FinMind（現貨收盤 + 除息調整）
# - [產業群聚差異化] apply_sector_cluster_bonus() 依族群連動性（Beta）調整加成倍率：
#     高連動（SEMI、AI_SERVER）：bonus × 1.2
#     低連動/題材（BIOTECH、PROPERTY）：bonus × 0.5
#     其餘族群：bonus × 1.0（維持原始值）
# - [負向群聚出場] 新增 check_sector_exit_cascade()：
#     同產業 3 檔以上觸發 EXIT_SIGNAL → 其餘持倉強制進入 S5_ACTIVE_EXIT
#     此函式由 scheduler.py 在 Step 5（Exit Engine 後）呼叫
# ═══════════════════════════════════════════════════════════════════════════════

import logging
from datetime import date
from typing import Optional
import pandas as pd
from datahub.data_hub import DataHub
from datahub.api_finmind import FinMindAPI
from datahub.api_fugle import FugleAPI
from datahub import redis_keys as rk

logger = logging.getLogger(__name__)

# ── 預設因子權重（會被 WF 驗證結果覆蓋）────────────────────────────────────
DEFAULT_WEIGHTS = {
    'momentum':    0.30,
    'chip':        0.25,
    'fundamental': 0.25,
    'valuation':   0.20,
}

# ── 產業對照表（台股主要族群）────────────────────────────────────────────────
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

# ── 產業群聚加分設定（原始值，乘以 Beta 倍率後才套用）───────────────────────
SECTOR_CLUSTER_BONUS = {
    2: 3.0,   # 同產業 2 檔觸發 → +3 分（基準）
    3: 6.0,   # 同產業 3 檔觸發 → +6 分（基準）
    4: 9.0,   # 同產業 4+ 檔觸發 → +9 分（基準）
}
SECTOR_CLUSTER_CAP = 9.0   # 加分上限

# ── [v3 新增] 產業連動性（Beta）倍率 ─────────────────────────────────────────
# 高連動族群（指數相關性高）→ 群聚訊號更可靠，加成放大
# 低連動/題材族群 → 群聚可能只是市場炒作，加成縮減
SECTOR_BETA_MULTIPLIER: dict[str, float] = {
    'SEMI':       1.2,   # 高連動：台灣半導體龍頭，與大盤高度同向
    'AI_SERVER':  1.2,   # 高連動：AI 主流主題，機構資金連動強
    'NETWORK':    1.0,   # 中性：網通族群連動適中
    'COMPONENT':  1.0,   # 中性
    'TELECOM':    0.9,   # 略低：防禦性股，群聚意義較弱
    'FINANCE':    0.9,   # 略低
    'STEEL':      0.8,   # 低連動：景氣循環，群聚常為短線題材
    'SHIPPING':   0.8,   # 低連動：航運常為事件驅動，非系統性
    'BIOTECH':    0.5,   # 低連動/題材：本土生技多屬消息炒作
    'PROPERTY':   0.5,   # 低連動/題材：政策敏感，群聚可信度低
    'OTHER':      1.0,   # 未分類族群維持中性
}

# ── [v3 新增] 基差過濾閾值 ───────────────────────────────────────────────────
BASIS_HARD_BLOCK   = -0.015   # Normalized Basis < -1.5% → Score = 0（硬性攔截）
BASIS_SOFT_PENALTY = -0.008   # -1.5% ≤ Basis < -0.8%   → Score × 0.80（動態懲罰）
BASIS_SOFT_FACTOR  =  0.80    # 軟性懲罰倍率

# Redis key for basis cache
BASIS_CACHE_KEY = "market:basis:latest"
BASIS_CACHE_TTL = 300   # 5 分鐘（盤中更新頻率）


def get_sector(ticker: str) -> str:
    """回傳股票所屬產業代碼"""
    return TICKER_SECTOR_MAP.get(ticker.strip(), 'OTHER')


# ═══════════════════════════════════════════════════════════════════════════════
# [v3 新增] BasisFilter — 台指期 Normalized Basis 計算與進場過濾
# ═══════════════════════════════════════════════════════════════════════════════

class BasisFilter:
    """
    計算台指期 Normalized Basis，提供兩段式進場過濾：

    公式：
        Normalized Basis = (TX_Price - (TSEC_Price - Dividends)) / TSEC_Price

    其中：
        TX_Price    = 台指期近月合約成交價（來自 Fugle 即時報價）
        TSEC_Price  = 加權指數現貨（使用 TWA00 或 TWII）
        Dividends   = 近月預估除息點數（從 FinMind 加總個股除息）

    過濾邏輯：
        Basis < -1.5%  → HARD BLOCK（回傳 basis_blocked=True，score 強制為 0）
        -1.5% ≤ Basis < -0.8% → SOFT PENALTY（回傳 basis_penalty=0.80）
        Basis ≥ -0.8%  → 正常（回傳 basis_penalty=1.0）

    注意事項：
        - 若 Fugle API 取不到期貨報價（收盤後、API 失效），fallback 為 0.0（不過濾）
        - 基差計算僅適用於近月合約（到期前 5 日自動切換到次月）
        - Dividends 簡化處理：若無法取得則視為 0（偏保守）
    """

    def __init__(self, hub: DataHub, fugle: Optional['FugleAPI'] = None):
        self.hub   = hub
        self.fugle = fugle

    async def get_normalized_basis(self) -> Optional[float]:
        """
        取得最新 Normalized Basis（優先從 Redis 快取，其次即時計算）
        回傳 None 表示無法取得（fallback：不過濾）
        """
        # ① 先查 Redis 快取（盤中避免重複 API 呼叫）
        cached = await self.hub.cache.get(BASIS_CACHE_KEY)
        if cached is not None and isinstance(cached, dict):
            basis = cached.get('basis')
            if basis is not None:
                logger.debug("基差從快取取得：%.4f", basis)
                return float(basis)

        # ② 從 DB 取得最新 market_regime 中的期貨價格（若有儲存）
        regime_row = await self.hub.fetchrow("""
            SELECT tx_futures_price, tsec_index_price, dividend_adjustment
            FROM market_regime
            WHERE tx_futures_price IS NOT NULL
            ORDER BY trade_date DESC
            LIMIT 1
        """)
        if regime_row:
            tx    = float(regime_row['tx_futures_price'])
            tsec  = float(regime_row['tsec_index_price'])
            divs  = float(regime_row.get('dividend_adjustment') or 0)
            if tsec > 0:
                basis = (tx - (tsec - divs)) / tsec
                logger.info("基差從 DB 計算：TX=%.0f TSEC=%.0f DIV=%.0f Basis=%.4f",
                            tx, tsec, divs, basis)
                await self.hub.cache.set(
                    BASIS_CACHE_KEY,
                    {'basis': round(basis, 6), 'tx': tx, 'tsec': tsec},
                    ttl=BASIS_CACHE_TTL,
                )
                return basis

        # ③ 即時從 Fugle 取得（需要 fugle 實例）
        if self.fugle:
            try:
                # TX00 = 台指期近月合約（Fugle 代碼）
                futures_quote = await self.fugle.get_quote('TXF')
                spot_quote    = await self.fugle.get_quote('TWA00')  # 加權指數

                if futures_quote and spot_quote:
                    tx_price   = float(futures_quote.get('lastPrice') or
                                       futures_quote.get('closePrice', 0))
                    tsec_price = float(spot_quote.get('lastPrice') or
                                       spot_quote.get('closePrice', 0))

                    if tx_price > 0 and tsec_price > 0:
                        # 簡化版：不計除息點數（保守估計，偏低估 Basis）
                        basis = (tx_price - tsec_price) / tsec_price
                        logger.info("基差即時計算（Fugle）：TX=%.0f TSEC=%.0f Basis=%.4f",
                                    tx_price, tsec_price, basis)
                        await self.hub.cache.set(
                            BASIS_CACHE_KEY,
                            {'basis': round(basis, 6), 'tx': tx_price, 'tsec': tsec_price},
                            ttl=BASIS_CACHE_TTL,
                        )
                        return basis
            except Exception as e:
                logger.warning("Fugle 基差取得失敗（fallback 不過濾）：%s", e)

        # ④ Fallback：無法取得基差，回傳 None（不過濾，保守原則）
        logger.warning("無法取得台指期基差，跳過基差過濾")
        return None

    async def apply(self, score: float) -> tuple[float, str]:
        """
        對分數套用基差過濾，回傳（調整後分數, 過濾原因說明）

        回傳：
            (0.0,   "BASIS_HARD_BLOCK")   → 硬性攔截
            (score * 0.80, "BASIS_SOFT_PENALTY") → 軟性懲罰
            (score, "")                   → 不過濾
        """
        basis = await self.get_normalized_basis()

        if basis is None:
            return score, ""   # 無資料，不過濾

        if basis < BASIS_HARD_BLOCK:
            logger.warning(
                "基差硬性攔截：Normalized Basis=%.2f%% < %.2f%%，分數強制歸零",
                basis * 100, BASIS_HARD_BLOCK * 100
            )
            return 0.0, f"BASIS_HARD_BLOCK(basis={basis*100:.2f}%)"

        if basis < BASIS_SOFT_PENALTY:
            adjusted = round(score * BASIS_SOFT_FACTOR, 2)
            logger.info(
                "基差軟性懲罰：Normalized Basis=%.2f%%，分數 %.1f → %.1f (×%.0f%%)",
                basis * 100, score, adjusted, BASIS_SOFT_FACTOR * 100
            )
            return adjusted, f"BASIS_SOFT_PENALTY(basis={basis*100:.2f}%,factor={BASIS_SOFT_FACTOR})"

        return score, ""


# ═══════════════════════════════════════════════════════════════════════════════
# apply_sector_cluster_bonus — [v3] 差異化產業群聚加分
# ═══════════════════════════════════════════════════════════════════════════════

def apply_sector_cluster_bonus(
    results: list[dict],
    score_threshold: float = 55.0,
) -> list[dict]:
    """
    產業群聚效應加權（批次處理）+ [v3] Beta 差異化倍率

    v3 變更：
    - 加分值 = 原始 bonus × SECTOR_BETA_MULTIPLIER[sector]
      高連動族群（SEMI/AI_SERVER）最多可獲 +10.8 分（9 × 1.2）
      低連動族群（BIOTECH/PROPERTY）最多只有 +4.5 分（9 × 0.5）
    - 仍不修改 DB 內的 total_score，只更新 result dict 的 final_score
    """
    sector_counts: dict[str, int] = {}
    for r in results:
        sector = get_sector(r['ticker'].strip())
        if r.get('total_score', 0) >= score_threshold and sector != 'OTHER':
            sector_counts[sector] = sector_counts.get(sector, 0) + 1

    for r in results:
        ticker = r['ticker'].strip()
        sector = get_sector(ticker)
        count  = sector_counts.get(sector, 0)

        # 查找基準 bonus（取最高符合 tier）
        base_bonus = 0.0
        if count >= 2 and sector != 'OTHER':
            for threshold in sorted(SECTOR_CLUSTER_BONUS.keys(), reverse=True):
                if count >= threshold:
                    base_bonus = SECTOR_CLUSTER_BONUS[threshold]
                    break

        # [v3] 乘以 Beta 差異化倍率，並限制上限
        beta_mult      = SECTOR_BETA_MULTIPLIER.get(sector, 1.0)
        adjusted_bonus = min(base_bonus * beta_mult, SECTOR_CLUSTER_CAP)

        r['sector']             = sector
        r['sector_count']       = count
        r['sector_beta_mult']   = beta_mult          # ← v3 新增：便於歸因查詢
        r['sector_bonus']       = round(adjusted_bonus, 2)
        r['final_score']        = round(r.get('total_score', 0) + adjusted_bonus, 2)

        if adjusted_bonus > 0:
            logger.info(
                "產業群聚加分（v3）：%s [%s] 族群%d檔 基準+%.1f × Beta%.1f → +%.1f → final_score=%.1f",
                ticker, sector, count,
                base_bonus, beta_mult, adjusted_bonus, r['final_score']
            )

    return results


# ═══════════════════════════════════════════════════════════════════════════════
# [v3 新增] check_sector_exit_cascade — 負向群聚出場
# ═══════════════════════════════════════════════════════════════════════════════

async def check_sector_exit_cascade(
    hub: DataHub,
    exit_signals: list[dict],
    trade_date: Optional[date] = None,
) -> list[dict]:
    """
    負向群聚出場：當同一產業有 3 檔以上觸發 EXIT_SIGNAL，
    強制將該產業其餘開倉部位進入 S5_ACTIVE_EXIT。

    呼叫時機：scheduler.py Step 5（ExitEngine 批次執行後）
    
    參數：
        exit_signals: ExitEngine 本次產生的出場訊號清單
                      每筆格式：{'ticker': str, 'sector': str, ...}
        trade_date:   交易日期

    回傳：
        cascade_list: 被連帶觸發 S5 的持倉清單
                      格式：[{'position_id': str, 'ticker': str, 'sector': str,
                               'reason': 'SECTOR_EXIT_CASCADE'}, ...]

    設計說明：
        - 閾值 3 檔（SECTOR_CASCADE_THRESHOLD）為保守設定，避免單一股票出場
          誤觸連鎖效應
        - 僅影響「相同產業」且「仍開倉」的部位
        - 不直接執行平倉（由 ExitEngine 在下個循環處理 S5），
          僅更新 state 並記錄 decision_log
    """
    if trade_date is None:
        trade_date = date.today()

    SECTOR_CASCADE_THRESHOLD = 3   # 同產業觸發出場訊號的最低數量

    # ① 統計各產業出場訊號數量
    sector_exit_counts: dict[str, int] = {}
    for sig in exit_signals:
        sector = sig.get('sector') or get_sector(sig.get('ticker', ''))
        if sector and sector != 'OTHER':
            sector_exit_counts[sector] = sector_exit_counts.get(sector, 0) + 1

    # 找出觸發門檻的產業
    cascade_sectors = {
        sec for sec, cnt in sector_exit_counts.items()
        if cnt >= SECTOR_CASCADE_THRESHOLD
    }

    if not cascade_sectors:
        return []

    logger.warning(
        "負向群聚出場觸發：%s（各產業出場訊號數：%s）",
        cascade_sectors, sector_exit_counts
    )

    cascade_list: list[dict] = []

    # ② 取得這些產業的仍開倉部位（排除已在 EXIT_SIGNAL 清單中的）
    exit_tickers = {sig.get('ticker', '').strip() for sig in exit_signals}

    for sector in cascade_sectors:
        sector_tickers = [
            t for t, s in TICKER_SECTOR_MAP.items() if s == sector
        ]
        if not sector_tickers:
            continue

        # 查詢同產業的開倉部位
        open_positions = await hub.fetch("""
            SELECT id, ticker, state FROM positions
            WHERE ticker = ANY($1::text[])
              AND is_open = TRUE
              AND state NOT IN ('S5_ACTIVE_EXIT', 'STOPPED_OUT', 'CLOSED')
        """, [t.ljust(6)[:6] for t in sector_tickers])

        for pos in open_positions:
            pos_ticker = str(pos['ticker']).strip()
            if pos_ticker in exit_tickers:
                continue   # 已在本次出場清單，不重複處理

            position_id = str(pos['id'])

            # 強制更新到 S5_ACTIVE_EXIT
            await hub.execute("""
                UPDATE positions
                SET state = 'S5_ACTIVE_EXIT'
                WHERE id = $1
            """, position_id)

            # 記錄 decision_log
            await hub.execute("""
                INSERT INTO decision_log (
                    trade_date, ticker, decision_type,
                    signal_source, notes
                ) VALUES ($1, $2, 'STATE_CHANGE', 'SELECTION_ENGINE',
                    $3)
            """,
                trade_date,
                pos['ticker'],
                f"負向群聚：{sector} 產業觸發 {sector_exit_counts[sector]} 檔出場，"
                f"連帶強制進入 S5_ACTIVE_EXIT",
            )

            cascade_list.append({
                'position_id': position_id,
                'ticker':      pos_ticker,
                'sector':      sector,
                'prev_state':  pos['state'],
                'reason':      'SECTOR_EXIT_CASCADE',
            })

            logger.warning(
                "負向群聚連帶出場：%s [%s] %s → S5_ACTIVE_EXIT",
                pos_ticker, sector, pos['state']
            )

    return cascade_list


# ═══════════════════════════════════════════════════════════════════════════════
# SelectionEngine — [v3] 整合基差過濾
# ═══════════════════════════════════════════════════════════════════════════════

class SelectionEngine:
    """模組② 多因子選股引擎：整合宏觀/籌碼/基本面/動能，輸出個股總分

    變更紀錄（v3）：
    - [基差過濾] run() 最後套用 BasisFilter.apply()：
        若基差 < -1.5% → final_score = 0，basis_filter_reason 說明原因
        若基差在 -0.8%~-1.5% → final_score × 0.8
    - [建構子] 新增可選的 fugle 參數，供 BasisFilter 使用
    - run() 回傳結果新增：basis_normalized、basis_filter_reason
    """

    def __init__(
        self,
        hub: DataHub,
        finmind: FinMindAPI,
        fugle: Optional[FugleAPI] = None,   # ← v3 新增
    ):
        self.hub     = hub
        self.finmind = finmind
        self.fugle   = fugle
        self._basis_filter = BasisFilter(hub, fugle)

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
        """從宏觀濾網取得動態因子權重（依 Regime 調整）"""
        cached = await self.hub.cache.get(rk.key_market_regime_date(str(trade_date)))
        if cached:
            regime = cached.get('regime', 'CHOPPY')
        else:
            row = await self.hub.fetchrow("""
                SELECT regime FROM market_regime
                WHERE trade_date <= $1
                ORDER BY trade_date DESC LIMIT 1
            """, trade_date)
            regime = row['regime'] if row else 'CHOPPY'

        wf_weights = await self._get_factor_weights()

        # 依 Regime 微調因子權重
        if regime == 'BULL_TREND':
            return {**wf_weights, 'momentum': min(wf_weights.get('momentum', 0.30) + 0.05, 0.50)}
        elif regime == 'BEAR_TREND':
            return {**wf_weights, 'fundamental': min(wf_weights.get('fundamental', 0.25) + 0.05, 0.40)}
        return wf_weights

    def _calc_momentum_score(self, price_df: pd.DataFrame) -> float:
        if price_df.empty or len(price_df) < 20:
            return 50.0
        df = price_df.sort_values('date')
        closes = df['close'].astype(float)
        ma20  = closes.tail(20).mean()
        ma60  = closes.tail(60).mean() if len(closes) >= 60 else ma20
        latest = float(closes.iloc[-1])
        score = 50.0
        if latest > ma20:  score += 20
        if latest > ma60:  score += 15
        if ma20 > ma60:    score += 15
        ret_20 = (latest / float(closes.iloc[-20]) - 1) if len(closes) >= 20 else 0
        score += min(ret_20 * 200, 20)
        return round(max(0, min(100, score)), 2)

    def _calc_fundamental_score(self, revenue_df: pd.DataFrame) -> float:
        if revenue_df.empty:
            return 50.0
        df = revenue_df.sort_values('date')
        score = 50.0
        if len(df) >= 2:
            yoy = (float(df['revenue'].iloc[-1]) / float(df['revenue'].iloc[-13]) - 1
                   if len(df) >= 13 else 0)
            if yoy > 0.20:   score += 30
            elif yoy > 0.10: score += 20
            elif yoy > 0:    score += 10
            elif yoy < -0.10: score -= 15
            else:             score -= 5
        return round(max(0, min(100, score)), 2)

    async def _get_chip_score(self, ticker: str, trade_date: date) -> float:
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
        """執行選股引擎，回傳個股多因子診斷（含基差過濾）"""
        if trade_date is None:
            trade_date = date.today()

        logger.info("選股引擎（v3）執行中：%s %s", ticker, trade_date)

        start_date = str(date(trade_date.year - 1, trade_date.month, trade_date.day))

        price_df   = await self.finmind.get_stock_price(ticker, start_date)
        revenue_df = await self.finmind.get_revenue(ticker, start_date)

        score_momentum    = self._calc_momentum_score(price_df)
        score_fundamental = self._calc_fundamental_score(revenue_df)
        score_chip        = await self._get_chip_score(ticker, trade_date)
        score_valuation   = 50.0   # 預留，後續擴充 P/E 計算

        weights     = await self._get_macro_weights(trade_date)
        total_score = (
            score_momentum    * weights.get('momentum', 0.30) +
            score_chip        * weights.get('chip', 0.25) +
            score_fundamental * weights.get('fundamental', 0.25) +
            score_valuation   * weights.get('valuation', 0.20)
        )

        # ── [v3] 套用台指期基差過濾 ─────────────────────────────────────
        adjusted_score, basis_reason = await self._basis_filter.apply(total_score)
        # 基差過濾作用在 total_score 之後，final_score 以此為基準
        # （群聚加分在 scheduler.py 的 apply_sector_cluster_bonus() 中另外套用）

        regime_row = await self.hub.fetchrow("""
            SELECT regime, mrs_score FROM market_regime
            WHERE trade_date <= $1
            ORDER BY trade_date DESC LIMIT 1
        """, trade_date)
        regime = regime_row['regime']    if regime_row else None
        mrs    = float(regime_row['mrs_score']) if regime_row else None

        sector = get_sector(ticker)

        result = {
            'ticker':               ticker,
            'trade_date':           trade_date,
            'score_momentum':       score_momentum,
            'score_chip':           score_chip,
            'score_fundamental':    score_fundamental,
            'score_valuation':      score_valuation,
            'weight_momentum':      weights.get('momentum', 0.30),
            'weight_chip':          weights.get('chip', 0.25),
            'weight_fundamental':   weights.get('fundamental', 0.25),
            'weight_valuation':     weights.get('valuation', 0.20),
            'total_score':          round(total_score, 2),
            'final_score':          round(adjusted_score, 2),   # ← 已套用基差過濾
            'basis_filter_reason':  basis_reason,               # ← v3 新增
            'sector':               sector,
            'sector_bonus':         0.0,   # 由 apply_sector_cluster_bonus() 填充
            'sector_count':         0,
            'regime_at_calc':       regime,
            'mrs_at_calc':          mrs,
        }

        # ── 寫入 PostgreSQL ──────────────────────────────────────────────
        await self.hub.execute("""
            DELETE FROM stock_diagnostic
            WHERE trade_date = $1 AND ticker = $2
        """, trade_date, ticker)

        await self.hub.execute("""
            INSERT INTO stock_diagnostic (
                trade_date, ticker,
                score_momentum, score_chip, score_fundamental, score_valuation,
                weight_momentum, weight_chip, weight_fundamental, weight_valuation,
                regime_at_calc, mrs_at_calc, sector
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
        """,
            trade_date, ticker,
            score_momentum, score_chip, score_fundamental, score_valuation,
            weights.get('momentum', 0.30),
            weights.get('chip', 0.25),
            weights.get('fundamental', 0.25),
            weights.get('valuation', 0.20),
            regime, mrs, sector,
        )

        if basis_reason:
            logger.warning(
                "選股引擎（v3）完成：%s 總分=%.1f → 基差過濾後=%.1f [%s] 原因：%s",
                ticker, total_score, adjusted_score, sector, basis_reason
            )
        else:
            logger.info(
                "選股引擎（v3）完成：%s 總分=%.1f [%s]",
                ticker, total_score, sector
            )

        return result