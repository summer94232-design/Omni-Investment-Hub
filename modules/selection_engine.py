# modules/selection_engine.py
# ═══════════════════════════════════════════════════════════════════════════════
# 變更紀錄（v3）：
# - [基差過濾] 新增 BasisFilter
# - [產業群聚差異化] apply_sector_cluster_bonus()
# - [負向群聚出場] check_sector_exit_cascade()
# ───────────────────────────────────────────────────────────────────────────────
# Bug 修正（v3.1）：
# - [BUG ①]  BasisFilter.get_normalized_basis() 改用 get_futures_quote('TXF')
#            不再誤用股票端點 get_quote('TXF')（實盤會回傳 404）
# - [BUG ②]  BASIS_CACHE_KEY 改用 rk.key_market_basis_latest() 統一管理
#            不再 hardcode "market:basis:latest"
# - [BUG ③]  stock_diagnostic INSERT 補上 total_score / final_score /
#            basis_filter_reason / sector_bonus / sector_beta_mult
#            原本這五個欄位全部遺漏，導致後端報錯 column does not exist
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

# ── 預設因子權重（會被 WF 驗證結果覆蓋）────────────────────────────────────────
DEFAULT_WEIGHTS = {
    'momentum':    0.30,
    'chip':        0.25,
    'fundamental': 0.25,
    'valuation':   0.20,
}

# ── 產業對照表（台股主要族群）──────────────────────────────────────────────────
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

# ── 產業群聚加分設定 ────────────────────────────────────────────────────────────
SECTOR_CLUSTER_BONUS = {2: 3.0, 3: 6.0, 4: 9.0}
SECTOR_CLUSTER_CAP   = 9.0

# ── 產業連動性（Beta）倍率 ──────────────────────────────────────────────────────
SECTOR_BETA_MULTIPLIER: dict[str, float] = {
    'SEMI':       1.2,
    'AI_SERVER':  1.2,
    'NETWORK':    1.0,
    'COMPONENT':  1.0,
    'TELECOM':    0.9,
    'FINANCE':    0.9,
    'STEEL':      0.8,
    'SHIPPING':   0.8,
    'BIOTECH':    0.5,
    'PROPERTY':   0.5,
    'OTHER':      1.0,
}

# ── 基差過濾閾值 ────────────────────────────────────────────────────────────────
BASIS_HARD_BLOCK   = -0.015   # Normalized Basis < -1.5% → Score 強制歸零
BASIS_SOFT_PENALTY = -0.008   # -1.5% ≤ Basis < -0.8%   → Score × 0.80
BASIS_SOFT_FACTOR  =  0.80
BASIS_CACHE_TTL    =  300     # 5 分鐘


def get_sector(ticker: str) -> str:
    """回傳股票所屬產業代碼"""
    return TICKER_SECTOR_MAP.get(ticker.strip(), 'OTHER')


# ═══════════════════════════════════════════════════════════════════════════════
# BasisFilter — 台指期 Normalized Basis 計算與進場過濾
# ═══════════════════════════════════════════════════════════════════════════════

class BasisFilter:
    """
    計算台指期 Normalized Basis，提供兩段式進場過濾：

        Normalized Basis = (TX_Price - (TSEC_Price - Dividends)) / TSEC_Price

    過濾邏輯：
        Basis < -1.5%            → HARD BLOCK（score 強制為 0）
        -1.5% ≤ Basis < -0.8%   → SOFT PENALTY（score × 0.80）
        Basis ≥ -0.8%            → 正常（不調整）

    資料來源優先順序：
        ① Redis 快取（TTL=300s）
        ② market_regime DB（由 MacroFilter 每日寫入）
        ③ Fugle 即時報價（盤中即時）
        ④ Fallback → 回傳 None（不過濾，保守原則）
    """

    def __init__(self, hub: DataHub, fugle: Optional['FugleAPI'] = None):
        self.hub   = hub
        self.fugle = fugle

    async def get_normalized_basis(self) -> Optional[float]:
        # ① Redis 快取
        # [BUG ② 修正] 使用 rk.key_market_basis_latest()，不再 hardcode
        cached = await self.hub.cache.get(rk.key_market_basis_latest())
        if cached is not None and isinstance(cached, dict):
            basis = cached.get('basis')
            if basis is not None:
                logger.debug("基差從快取取得：%.4f", basis)
                return float(basis)

        # ② DB fallback（由 MacroFilter.run() 每日寫入）
        regime_row = await self.hub.fetchrow("""
            SELECT tx_futures_price, tsec_index_price, dividend_adjustment
            FROM market_regime
            WHERE tx_futures_price IS NOT NULL
            ORDER BY trade_date DESC LIMIT 1
        """)
        if regime_row:
            tx   = float(regime_row['tx_futures_price'])
            tsec = float(regime_row['tsec_index_price'])
            divs = float(regime_row.get('dividend_adjustment') or 0)
            if tsec > 0:
                basis = (tx - (tsec - divs)) / tsec
                logger.info("基差從 DB 計算：TX=%.0f TSEC=%.0f DIV=%.0f Basis=%.4f",
                            tx, tsec, divs, basis)
                await self.hub.cache.set(
                    rk.key_market_basis_latest(),
                    {'basis': round(basis, 6), 'tx': tx, 'tsec': tsec},
                    ttl=BASIS_CACHE_TTL,
                )
                return basis

        # ③ Fugle 即時
        if self.fugle:
            try:
                # [BUG ① 修正] 改用 get_futures_quote()，不再誤用 get_quote()（股票端點）
                futures_quote = await self.fugle.get_futures_quote('TXF')
                spot_quote    = await self.fugle.get_quote('TWA00')

                if futures_quote and spot_quote:
                    tx_price   = float(futures_quote.get('lastPrice') or
                                       futures_quote.get('closePrice', 0))
                    tsec_price = float(spot_quote.get('lastPrice') or
                                       spot_quote.get('closePrice', 0))

                    if tx_price > 0 and tsec_price > 0:
                        basis = (tx_price - tsec_price) / tsec_price
                        logger.info("基差即時計算（Fugle）：TX=%.0f TSEC=%.0f Basis=%.4f",
                                    tx_price, tsec_price, basis)
                        await self.hub.cache.set(
                            rk.key_market_basis_latest(),
                            {'basis': round(basis, 6), 'tx': tx_price, 'tsec': tsec_price},
                            ttl=BASIS_CACHE_TTL,
                        )
                        return basis
            except Exception as e:
                logger.warning("Fugle 基差取得失敗（fallback 不過濾）：%s", e)

        # ④ Fallback：不過濾
        logger.warning("無法取得台指期基差，跳過基差過濾")
        return None

    async def apply(self, score: float) -> tuple[float, str]:
        """套用基差過濾，回傳（調整後分數, 原因說明）"""
        basis = await self.get_normalized_basis()

        if basis is None:
            return score, ''

        if basis < BASIS_HARD_BLOCK:
            reason = f"BASIS_HARD_BLOCK（basis={basis*100:.2f}%）"
            logger.warning("基差硬性攔截：%s → score 歸零", reason)
            return 0.0, reason

        if basis < BASIS_SOFT_PENALTY:
            adjusted = round(score * BASIS_SOFT_FACTOR, 2)
            reason   = f"BASIS_SOFT_PENALTY（basis={basis*100:.2f}%，score×{BASIS_SOFT_FACTOR}）"
            logger.info("基差軟性懲罰：%.1f → %.1f [%s]", score, adjusted, reason)
            return adjusted, reason

        return score, ''


# ═══════════════════════════════════════════════════════════════════════════════
# apply_sector_cluster_bonus — 產業群聚差異化加分
# ═══════════════════════════════════════════════════════════════════════════════

def apply_sector_cluster_bonus(
    results: list[dict],
    score_threshold: float = 0.0,
) -> list[dict]:
    """
    [v3] 統計同產業觸發數量，依 Beta 倍率調整加分後套用到 final_score。

    Args:
        results: SelectionEngine.run() 回傳的個股結果清單
        score_threshold: 僅計入 total_score >= 此門檻的股票（避免低分股稀釋群聚）

    Returns:
        更新後的 results（in-place 修改，同時回傳）
    """
    # 統計同產業中達到門檻的股票數
    sector_counts: dict[str, int] = {}
    for r in results:
        if r.get('total_score', 0) < score_threshold:
            continue
        sector = r.get('sector', 'OTHER')
        if sector and sector != 'OTHER':
            sector_counts[sector] = sector_counts.get(sector, 0) + 1

    for r in results:
        ticker = r.get('ticker', '').strip()
        sector = r.get('sector', 'OTHER')
        count  = sector_counts.get(sector, 0)

        if count < 2:
            continue

        count_key  = min(count, 4)
        base_bonus = SECTOR_CLUSTER_BONUS.get(count_key, SECTOR_CLUSTER_BONUS[4])
        beta_mult  = SECTOR_BETA_MULTIPLIER.get(sector, 1.0)
        adj_bonus  = round(min(base_bonus * beta_mult, SECTOR_CLUSTER_CAP), 2)

        r['sector_bonus']     = adj_bonus
        r['sector_count']     = count
        r['sector_beta_mult'] = beta_mult
        r['final_score']      = round(r.get('total_score', 0) + adj_bonus, 2)

        if adj_bonus > 0:
            logger.info(
                "產業群聚加分：%s [%s] 族群%d檔 基準+%.1f × Beta%.1f → +%.1f → final=%.1f",
                ticker, sector, count, base_bonus, beta_mult, adj_bonus, r['final_score'],
            )

    return results


# ═══════════════════════════════════════════════════════════════════════════════
# check_sector_exit_cascade — 負向群聚出場
# ═══════════════════════════════════════════════════════════════════════════════

async def check_sector_exit_cascade(
    hub: DataHub,
    exit_signals: list[dict],
    trade_date: Optional[date] = None,
) -> list[dict]:
    """
    [v3] 負向群聚出場：當同一產業有 ≥3 檔觸發 EXIT_SIGNAL，
    強制將該產業其餘開倉部位進入 S5_ACTIVE_EXIT。

    由 scheduler.py Step 4a 呼叫（ExitEngine 批次執行後）。
    """
    if trade_date is None:
        trade_date = date.today()

    SECTOR_CASCADE_THRESHOLD = 3

    sector_exit_counts: dict[str, int] = {}
    for sig in exit_signals:
        sector = sig.get('sector') or get_sector(sig.get('ticker', ''))
        if sector and sector != 'OTHER':
            sector_exit_counts[sector] = sector_exit_counts.get(sector, 0) + 1

    cascade_sectors = {
        sec for sec, cnt in sector_exit_counts.items()
        if cnt >= SECTOR_CASCADE_THRESHOLD
    }
    if not cascade_sectors:
        return []

    logger.warning("負向群聚出場觸發：%s（出場訊號數：%s）",
                   cascade_sectors, sector_exit_counts)

    cascade_list: list[dict] = []
    exit_tickers = {sig.get('ticker', '').strip() for sig in exit_signals}

    for sector in cascade_sectors:
        sector_tickers = [t for t, s in TICKER_SECTOR_MAP.items() if s == sector]
        if not sector_tickers:
            continue

        open_positions = await hub.fetch("""
            SELECT id, ticker, state FROM positions
            WHERE ticker = ANY($1::text[])
              AND is_open = TRUE
              AND state NOT IN ('S5_ACTIVE_EXIT', 'STOPPED_OUT', 'CLOSED')
        """, [t.ljust(6)[:6] for t in sector_tickers])

        for pos in open_positions:
            pos_ticker  = str(pos['ticker']).strip()
            position_id = str(pos['id'])
            if pos_ticker in exit_tickers:
                continue

            await hub.execute("""
                UPDATE positions SET state = 'S5_ACTIVE_EXIT' WHERE id = $1
            """, position_id)

            await hub.execute("""
                INSERT INTO decision_log (
                    trade_date, ticker, decision_type, signal_source, notes
                ) VALUES ($1, $2, 'STATE_CHANGE', 'SELECTION_ENGINE', $3)
            """,
                trade_date, pos['ticker'],
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
            logger.warning("負向群聚連帶出場：%s [%s] %s → S5_ACTIVE_EXIT",
                           pos_ticker, sector, pos['state'])

    return cascade_list


# ═══════════════════════════════════════════════════════════════════════════════
# SelectionEngine — 多因子選股引擎（含基差過濾）
# ═══════════════════════════════════════════════════════════════════════════════

class SelectionEngine:
    """
    模組② 多因子選股引擎：整合宏觀/籌碼/基本面/動能，輸出個股總分。

    v3 新增：
    - BasisFilter 台指期基差過濾（硬性攔截 / 軟性懲罰）
    - 可選 fugle 參數供 BasisFilter 即時計算基差
    """

    def __init__(
        self,
        hub: DataHub,
        finmind: FinMindAPI,
        fugle: Optional[FugleAPI] = None,
    ):
        self.hub           = hub
        self.finmind       = finmind
        self.fugle         = fugle
        self._basis_filter = BasisFilter(hub, fugle)

    async def _get_factor_weights(self) -> dict:
        """從 Redis 或 DB 取得當前因子權重"""
        cached = await self.hub.cache.get(rk.key_wf_weights_current())
        if cached:
            return cached
        row = await self.hub.fetchrow("""
            SELECT recommended_weights FROM wf_results
            WHERE is_active = TRUE ORDER BY run_at DESC LIMIT 1
        """)
        if row and row['recommended_weights']:
            import json
            weights = json.loads(row['recommended_weights'])
            await self.hub.cache.set(rk.key_wf_weights_current(), weights, ttl=rk.TTL_24H)
            return weights
        return DEFAULT_WEIGHTS

    async def _get_macro_weights(self, trade_date: date) -> dict:
        """從宏觀濾網取得動態因子權重（依 Regime 微調）"""
        cached = await self.hub.cache.get(rk.key_market_regime_date(str(trade_date)))
        if cached:
            regime = cached.get('regime', 'CHOPPY')
        else:
            row = await self.hub.fetchrow("""
                SELECT regime FROM market_regime
                WHERE trade_date <= $1 ORDER BY trade_date DESC LIMIT 1
            """, trade_date)
            regime = row['regime'] if row else 'CHOPPY'

        wf_weights = await self._get_factor_weights()

        if regime == 'BULL_TREND':
            return {**wf_weights,
                    'momentum': min(wf_weights.get('momentum', 0.30) + 0.05, 0.50)}
        elif regime == 'BEAR_TREND':
            return {**wf_weights,
                    'fundamental': min(wf_weights.get('fundamental', 0.25) + 0.05, 0.40)}
        return wf_weights

    def _calc_momentum_score(self, price_df: pd.DataFrame) -> float:
        if price_df.empty or len(price_df) < 20:
            return 50.0
        df     = price_df.sort_values('date')
        closes = df['close'].astype(float)
        ma20   = closes.tail(20).mean()
        ma60   = closes.tail(60).mean() if len(closes) >= 60 else ma20
        latest = float(closes.iloc[-1])
        score  = 50.0
        if latest > ma20:  score += 20
        if latest > ma60:  score += 15
        if ma20 > ma60:    score += 15
        ret_20  = (latest / float(closes.iloc[-20]) - 1) if len(closes) >= 20 else 0
        score  += min(ret_20 * 200, 20)
        return round(max(0, min(100, score)), 2)

    def _calc_fundamental_score(self, revenue_df: pd.DataFrame) -> float:
        if revenue_df.empty:
            return 50.0
        df    = revenue_df.sort_values('date')
        score = 50.0
        if len(df) >= 2:
            yoy = (
                float(df['revenue'].iloc[-1]) /
                float(df['revenue'].iloc[-13])
                if len(df) >= 13 else 0
            ) - 1
            if yoy > 0.20:    score += 30
            elif yoy > 0.10:  score += 20
            elif yoy > 0:     score += 10
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
            score_momentum    * weights.get('momentum',    0.30) +
            score_chip        * weights.get('chip',        0.25) +
            score_fundamental * weights.get('fundamental', 0.25) +
            score_valuation   * weights.get('valuation',   0.20)
        )

        # [v3] 套用台指期基差過濾
        adjusted_score, basis_reason = await self._basis_filter.apply(total_score)

        regime_row = await self.hub.fetchrow("""
            SELECT regime, mrs_score FROM market_regime
            WHERE trade_date <= $1 ORDER BY trade_date DESC LIMIT 1
        """, trade_date)
        regime = regime_row['regime']           if regime_row else None
        mrs    = float(regime_row['mrs_score']) if regime_row else None

        sector    = get_sector(ticker)
        beta_mult = SECTOR_BETA_MULTIPLIER.get(sector, 1.0)

        result = {
            'ticker':               ticker,
            'trade_date':           trade_date,
            'score_momentum':       score_momentum,
            'score_chip':           score_chip,
            'score_fundamental':    score_fundamental,
            'score_valuation':      score_valuation,
            'weight_momentum':      weights.get('momentum',    0.30),
            'weight_chip':          weights.get('chip',        0.25),
            'weight_fundamental':   weights.get('fundamental', 0.25),
            'weight_valuation':     weights.get('valuation',   0.20),
            'total_score':          round(total_score, 2),
            'final_score':          round(adjusted_score, 2),
            'basis_filter_reason':  basis_reason,
            'sector':               sector,
            'sector_bonus':         0.0,      # 由 apply_sector_cluster_bonus() 填充
            'sector_count':         0,
            'sector_beta_mult':     beta_mult,
            'regime_at_calc':       regime,
            'mrs_at_calc':          mrs,
        }

        # ── 寫入 PostgreSQL ──────────────────────────────────────────────────
        # [BUG ③ 修正] 補上全部遺漏欄位：
        #   total_score($11), final_score($12), basis_filter_reason($13),
        #   sector_bonus($14), sector_beta_mult($15)
        await self.hub.execute("""
            DELETE FROM stock_diagnostic
            WHERE trade_date = $1 AND ticker = $2
        """, trade_date, ticker)

        await self.hub.execute("""
            INSERT INTO stock_diagnostic (
                trade_date, ticker,
                score_momentum, score_chip, score_fundamental, score_valuation,
                weight_momentum, weight_chip, weight_fundamental, weight_valuation,
                total_score, final_score,
                basis_filter_reason,
                sector_bonus, sector_beta_mult,
                regime_at_calc, mrs_at_calc, sector
            ) VALUES (
                $1, $2,
                $3, $4, $5, $6,
                $7, $8, $9, $10,
                $11, $12,
                $13,
                $14, $15,
                $16, $17, $18
            )
        """,
            trade_date, ticker,
            score_momentum, score_chip, score_fundamental, score_valuation,
            weights.get('momentum',    0.30),
            weights.get('chip',        0.25),
            weights.get('fundamental', 0.25),
            weights.get('valuation',   0.20),
            round(total_score, 2),      # $11 total_score
            round(adjusted_score, 2),   # $12 final_score
            basis_reason or None,       # $13 basis_filter_reason
            0.0,                        # $14 sector_bonus（群聚加分後由 scheduler 更新）
            beta_mult,                  # $15 sector_beta_mult
            regime,                     # $16 regime_at_calc
            mrs,                        # $17 mrs_at_calc
            sector,                     # $18 sector
        )

        if basis_reason:
            logger.warning(
                "選股引擎完成：%s 總分=%.1f → 基差過濾後=%.1f [%s] 原因：%s",
                ticker, total_score, adjusted_score, sector, basis_reason,
            )
        else:
            logger.info("選股引擎完成：%s 總分=%.1f [%s]", ticker, total_score, sector)

        return result