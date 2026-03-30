# modules/selection_engine.py
# ═══════════════════════════════════════════════════════════════════════════════
# 變更紀錄：
# v3.3：估值因子改用真實 PE/PBR（FinMind TaiwanStockPER）；EPS 成長率季報支援
# v3.4：
#   - 動態產業分類：優先 FinMind TaiwanStockInfo → TWSE OpenAPI → 靜態備援
#   - 估值因子增強：加入資產負債表健全度（負債比 / 流動比率）
#   - 新增 INDUSTRY_TO_SECTOR 映射表 + CONSUMER / ENERGY 分類
#   - SelectionEngine 新增 twse_api 參數 + _sector_cache 內部快取
# ═══════════════════════════════════════════════════════════════════════════════

import json
import logging
from datetime import date
from typing import Optional
import pandas as pd
from datahub.data_hub import DataHub
from datahub.api_finmind import FinMindAPI
from datahub import redis_keys as rk

logger = logging.getLogger(__name__)

# =============================================================================
# 產業分類
# =============================================================================

# FinMind / TWSE 產業名稱 → 系統內部分類代碼
INDUSTRY_TO_SECTOR: dict[str, str] = {
    "半導體業":           "SEMI",
    "電子零組件業":        "SEMI",
    "電腦及週邊設備業":    "AI_SERVER",
    "通信網路業":          "AI_SERVER",
    "電機機械":            "EMS",
    "其他電子業":          "EMS",
    "通信業":              "TELECOM",
    "金融保險業":          "FINANCE",
    "銀行業":              "FINANCE",
    "保險業":              "FINANCE",
    "生技醫療業":          "BIOTECH",
    "醫療器材":            "BIOTECH",
    "汽車工業":            "AUTO",
    "橡膠工業":            "AUTO",
    "建材營造業":          "PROPERTY",
    "不動產業":            "PROPERTY",
    "食品工業":            "CONSUMER",
    "貿易百貨業":          "CONSUMER",
    "油電燃氣業":          "ENERGY",
}

# 靜態備援（僅覆蓋最常用的 Watchlist 標的）
SECTOR_MAP_STATIC: dict[str, str] = {
    "2330": "SEMI",      "2303": "SEMI",      "2308": "SEMI",      "2379": "SEMI",
    "2454": "AI_SERVER", "3711": "AI_SERVER", "6669": "AI_SERVER", "3034": "AI_SERVER",
    "2317": "EMS",       "2354": "EMS",       "2382": "EMS",
    "2412": "TELECOM",   "2498": "TELECOM",
    "2886": "FINANCE",   "2891": "FINANCE",   "2882": "FINANCE",   "2881": "FINANCE",
    "4711": "BIOTECH",   "6548": "BIOTECH",   "4958": "BIOTECH",
    "2207": "AUTO",      "2105": "AUTO",
    "2801": "PROPERTY",  "5522": "PROPERTY",
}

# 向下相容（舊版 SECTOR_MAP 別名）
SECTOR_MAP = SECTOR_MAP_STATIC

SECTOR_BETA_MULTIPLIER: dict[str, float] = {
    "SEMI":       1.2,
    "AI_SERVER":  1.2,
    "EMS":        1.0,
    "TELECOM":    0.8,
    "FINANCE":    0.7,
    "BIOTECH":    0.5,
    "AUTO":       0.8,
    "PROPERTY":   0.5,
    "CONSUMER":   0.7,
    "ENERGY":     0.8,
    "OTHER":      1.0,
}


def get_sector(ticker: str) -> str:
    """靜態產業分類（向下相容用）"""
    return SECTOR_MAP_STATIC.get(ticker.strip(), "OTHER")


def map_industry_to_sector(industry_name: str) -> str:
    """將 FinMind / TWSE 回傳的產業名稱轉換為系統內部分類代碼"""
    if not industry_name:
        return "OTHER"
    if industry_name in INDUSTRY_TO_SECTOR:
        return INDUSTRY_TO_SECTOR[industry_name]
    for key, sector in INDUSTRY_TO_SECTOR.items():
        if key in industry_name or industry_name in key:
            return sector
    return "OTHER"


# =============================================================================
# 基差過濾器
# =============================================================================

class BasisFilter:
    """台指期 Normalized Basis 過濾器"""

    HARD_BLOCK = -0.015   # < -1.5% 硬性攔截
    SOFT_LOWER = -0.015
    SOFT_UPPER = -0.008   # -1.5% ~ -0.8% 軟性懲罰
    SOFT_MULT  = 0.8

    def __init__(self, hub: DataHub):
        self._hub = hub

    async def get_current_basis(self) -> Optional[float]:
        cached = await self._hub.cache.get(rk.key_market_basis_latest())
        if cached and cached.get("basis") is not None:
            return float(cached["basis"])
        row = await self._hub.fetchrow("""
            SELECT tx_futures_price, tsec_index_price, dividend_adjustment
            FROM market_regime
            WHERE tx_futures_price IS NOT NULL
            ORDER BY trade_date DESC LIMIT 1
        """)
        if not row:
            return None
        tx   = float(row["tx_futures_price"])
        tsec = float(row["tsec_index_price"])
        div  = float(row["dividend_adjustment"] or 0)
        fair = tsec - div
        if fair <= 0:
            return None
        return (tx - fair) / fair

    async def apply(self, score: float) -> tuple[float, Optional[str]]:
        basis = await self.get_current_basis()
        if basis is None:
            return score, None
        if basis < self.HARD_BLOCK:
            return 0.0, f"HARD_BLOCK: basis={basis*100:.2f}%"
        if self.SOFT_LOWER <= basis < self.SOFT_UPPER:
            return score * self.SOFT_MULT, f"SOFT_PENALTY: basis={basis*100:.2f}%"
        return score, None


# =============================================================================
# 產業群聚加分
# =============================================================================

async def apply_sector_cluster_bonus(
    hub:        DataHub,
    results:    list[dict],
    trade_date: date,
    threshold:  float = 60.0,
) -> list[dict]:
    """同產業多檔入選時依 Beta 倍率差異化加分"""
    sector_counts: dict[str, list] = {}
    for r in results:
        if r.get("final_score", 0) >= threshold:
            s = r.get("sector", "OTHER")
            sector_counts.setdefault(s, []).append(r["ticker"])

    bonus_map = {2: 3, 3: 6}
    for r in results:
        s     = r.get("sector", "OTHER")
        count = len(sector_counts.get(s, []))
        if count < 2:
            continue
        base_bonus   = bonus_map.get(count, 9 if count >= 4 else 0)
        beta_mult    = SECTOR_BETA_MULTIPLIER.get(s, 1.0)
        actual_bonus = base_bonus * beta_mult
        r["sector_bonus"]     = round(actual_bonus, 2)
        r["sector_count"]     = count
        r["final_score"]      = round(r.get("final_score", 0) + actual_bonus, 2)
        r["sector_beta_mult"] = beta_mult

        await hub.execute("""
            UPDATE stock_diagnostic
            SET sector_bonus=$1, sector_beta_mult=$2, final_score=$3
            WHERE trade_date=$4 AND ticker=$5
        """, actual_bonus, beta_mult, r["final_score"], trade_date, r["ticker"])

    return results


async def check_sector_exit_cascade(
    hub:          DataHub,
    exit_tickers: list[str],
    trade_date:   date,
    min_trigger:  int = 3,
) -> list[str]:
    """同產業 ≥ 3 檔觸發出場時，強制同產業其餘持倉進入 S5"""
    sector_exit_map: dict[str, list] = {}
    for ticker in exit_tickers:
        s = get_sector(ticker)
        sector_exit_map.setdefault(s, []).append(ticker)

    forced = []
    for sector, tickers in sector_exit_map.items():
        if len(tickers) < min_trigger:
            continue
        rows = await hub.fetch("""
            SELECT id, ticker FROM positions
            WHERE is_open = TRUE
              AND state NOT IN ('S5_ACTIVE_EXIT','CLOSED','STOPPED_OUT')
              AND ticker NOT IN ({})
        """.format(",".join(f"'{t}'" for t in tickers)))
        for row in rows:
            if get_sector(row["ticker"]) == sector:
                await hub.execute(
                    "UPDATE positions SET state='S5_ACTIVE_EXIT' WHERE id=$1",
                    row["id"],
                )
                forced.append(row["ticker"])
                logger.warning("負向群聚出場：%s → S5（產業=%s）", row["ticker"], sector)
    return forced


# =============================================================================
# SelectionEngine
# =============================================================================

class SelectionEngine:
    """多因子選股引擎（v3.4）"""

    def __init__(
        self,
        hub:      DataHub,
        finmind:  FinMindAPI,
        twse_api=None,   # datahub.api_twse.TWSEApi（可選，產業分類備援）
    ):
        self.hub           = hub
        self.finmind       = finmind
        self.twse_api      = twse_api
        self._basis_filter = BasisFilter(hub)
        self._sector_cache: dict[str, str] = {}  # 當日產業分類快取

    # =========================================================================
    # 動態產業分類（v3.4）
    # =========================================================================

    async def _get_sector_dynamic(self, ticker: str) -> str:
        """
        v3.4 動態產業分類，取代靜態 get_sector()。

        優先順序：
          1. 當日內部快取（_sector_cache）
          2. FinMind get_all_stock_industries()（批量預熱後在快取）
          3. FinMind get_stock_info(ticker)（單筆查詢）
          4. TWSE OpenAPI get_stock_info(ticker)（備援）
          5. SECTOR_MAP_STATIC 靜態備援
        """
        ticker_clean = ticker.strip()

        # 1. 內部快取
        if ticker_clean in self._sector_cache:
            return self._sector_cache[ticker_clean]

        sector = "OTHER"

        # 2. FinMind 批量快取（若 Scheduler 已呼叫 _warmup_sector_cache）
        if self.finmind and self.finmind._sector_cache:
            raw_industry = self.finmind._sector_cache.get(ticker_clean)
            if raw_industry:
                sector = map_industry_to_sector(raw_industry)

        # 3. FinMind 單筆查詢
        if sector == "OTHER" and self.finmind:
            try:
                info = await self.finmind.get_stock_info(ticker_clean)
                if info and info.get("industry"):
                    sector = map_industry_to_sector(info["industry"])
            except Exception as e:
                logger.debug("FinMind 產業分類失敗 %s：%s", ticker_clean, e)

        # 4. TWSE OpenAPI 備援
        if sector == "OTHER" and self.twse_api:
            try:
                info = await self.twse_api.get_stock_info(ticker_clean)
                if info and info.get("industry"):
                    sector = map_industry_to_sector(info["industry"])
            except Exception as e:
                logger.debug("TWSE 產業分類失敗 %s：%s", ticker_clean, e)

        # 5. 靜態備援
        if sector == "OTHER":
            sector = SECTOR_MAP_STATIC.get(ticker_clean, "OTHER")

        self._sector_cache[ticker_clean] = sector
        return sector

    # =========================================================================
    # 宏觀動態因子權重
    # =========================================================================

    async def _get_macro_weights(self, trade_date: date) -> dict[str, float]:
        """取得 WalkForward 建議的動態權重，依 Regime 微調"""
        default_weights = {
            "momentum": 0.30, "chip": 0.25, "fundamental": 0.25, "valuation": 0.20,
        }
        wf_row = await self.hub.fetchrow("""
            SELECT recommended_weights FROM wf_results
            WHERE status = 'COMPLETED' AND recommendation = 'DEPLOY'
            ORDER BY run_date DESC LIMIT 1
        """)
        if wf_row and wf_row["recommended_weights"]:
            try:
                wf_weights = json.loads(wf_row["recommended_weights"])
            except Exception:
                wf_weights = default_weights
        else:
            wf_weights = default_weights

        regime_row = await self.hub.fetchrow("""
            SELECT regime FROM market_regime
            WHERE trade_date <= $1 ORDER BY trade_date DESC LIMIT 1
        """, trade_date)
        regime = regime_row["regime"] if regime_row else "CHOPPY"

        if regime == "BULL_TREND":
            return {**wf_weights,
                    "momentum": min(wf_weights.get("momentum", 0.30) + 0.05, 0.50)}
        elif regime == "BEAR_TREND":
            return {**wf_weights,
                    "fundamental": min(wf_weights.get("fundamental", 0.25) + 0.05, 0.40)}
        return wf_weights

    # =========================================================================
    # 動能因子
    # =========================================================================

    def _calc_momentum_score(self, price_df: pd.DataFrame) -> float:
        if price_df.empty or len(price_df) < 20:
            return 50.0
        df     = price_df.sort_values("date")
        closes = df["close"].astype(float)
        ma20   = float(closes.tail(20).mean())
        ma60   = float(closes.tail(60).mean()) if len(closes) >= 60 else ma20
        latest = float(closes.iloc[-1])
        score  = 50.0
        if latest > ma20:  score += 20
        if latest > ma60:  score += 15
        if ma20   > ma60:  score += 15
        ret_20 = (latest / float(closes.iloc[-20]) - 1) if len(closes) >= 20 else 0
        score += min(ret_20 * 200, 20)
        return round(max(0.0, min(100.0, score)), 2)

    # =========================================================================
    # 基本面因子
    # =========================================================================

    def _calc_fundamental_score(self, revenue_df: pd.DataFrame) -> float:
        """月營收 YoY 基本面評分（降級版）"""
        if revenue_df.empty:
            return 50.0
        df    = revenue_df.sort_values("date")
        score = 50.0
        if len(df) >= 13:
            yoy = (float(df["revenue"].iloc[-1]) / float(df["revenue"].iloc[-13])) - 1
            if yoy > 0.20:    score += 30
            elif yoy > 0.10:  score += 20
            elif yoy > 0:     score += 10
            elif yoy < -0.10: score -= 15
            else:             score -= 5
        return round(max(0.0, min(100.0, score)), 2)

    async def _calc_fundamental_score_v2(
        self,
        ticker:     str,
        revenue_df: pd.DataFrame,
    ) -> float:
        """
        v3.3 增強版基本面評分。
        優先使用季報 EPS YoY，失敗時降級至月營收 YoY。
        """
        try:
            fin_df = await self.finmind.get_financial_statements(ticker, quarters=8)
            if not fin_df.empty:
                eps_df = fin_df[fin_df["type"] == "EPS"].sort_values("date")
                if len(eps_df) >= 5:
                    latest_eps = float(eps_df["value"].iloc[-1])
                    yoy_eps    = float(eps_df["value"].iloc[-5])
                    if yoy_eps != 0:
                        eps_yoy = (latest_eps - yoy_eps) / abs(yoy_eps)
                        score = 50.0
                        if eps_yoy > 0.30:    score += 35
                        elif eps_yoy > 0.15:  score += 25
                        elif eps_yoy > 0:     score += 12
                        elif eps_yoy < -0.20: score -= 20
                        else:                 score -= 8
                        return round(max(0.0, min(100.0, score)), 2)
        except Exception as e:
            logger.debug("季報 EPS 失敗 %s：%s，改用月營收", ticker, e)

        return self._calc_fundamental_score(revenue_df)

    # =========================================================================
    # 估值因子（v3.4 增強版）
    # =========================================================================

    async def _calc_valuation_score(self, ticker: str) -> float:
        """
        v3.4 估值因子（含資產負債表健全度）。

        PE 評分（0~45）：
          PE < 12 → +45   PE 12-20 → +30   PE 20-35 → +15
          PE 35-50 → 0    PE > 50  → -15
        PBR 評分（0~20）：
          PBR < 1 → +20   PBR 1-2 → +15   PBR 2-3 → +5   PBR ≥ 3 → 0
        殖利率評分（0~10）：
          殖利率 ≥ 5% → +10   3-5% → +5   < 3% → 0
        資產負債表健全度（-15 ~ +18）← v3.4 新增：
          負債比 < 30%  → +10   負債比 > 60%  → -15
          流動比率 > 2.0 → +8   流動比率 < 1.0 → -10
        """
        score = 0.0
        has_data = False

        # PE / PBR / 殖利率
        try:
            val = await self.finmind.get_per_pbr(ticker)
            per = val.get("per")
            pbr = val.get("pbr")
            dy  = val.get("dividend_yield")

            if per is not None:
                has_data = True
                if   per < 12:  score += 45
                elif per < 20:  score += 30
                elif per < 35:  score += 15
                elif per < 50:  score += 0
                else:           score -= 15

            if pbr is not None:
                has_data = True
                if   pbr < 1:   score += 20
                elif pbr < 2:   score += 15
                elif pbr < 3:   score += 5

            if dy is not None:
                has_data = True
                if   dy >= 5.0: score += 10
                elif dy >= 3.0: score += 5

        except Exception as e:
            logger.debug("PE/PBR 取得失敗 %s：%s", ticker, e)

        if not has_data:
            return 50.0  # fallback

        # v3.4：資產負債表健全度
        try:
            bs            = await self.finmind.get_balance_sheet(ticker, quarters=2)
            debt_ratio    = bs.get("debt_ratio")
            current_ratio = bs.get("current_ratio")

            if debt_ratio is not None:
                if   debt_ratio < 30:  score += 10
                elif debt_ratio < 50:  score += 3
                elif debt_ratio > 60:  score -= 15
                elif debt_ratio > 50:  score -= 5

            if current_ratio is not None:
                if   current_ratio > 2.0:  score += 8
                elif current_ratio > 1.5:  score += 4
                elif current_ratio < 1.0:  score -= 10
                elif current_ratio < 1.2:  score -= 4

        except Exception as e:
            logger.debug("資產負債表評分失敗 %s：%s，略過", ticker, e)

        return round(max(0.0, min(100.0, score)), 2)

    # =========================================================================
    # 籌碼因子
    # =========================================================================

    async def _get_chip_score(self, ticker: str, trade_date: date) -> float:
        """從 Redis 快取或 DB 取得最新 CRS 籌碼分數"""
        cached = await self.hub.cache.get(rk.key_chip_crs_latest(ticker))
        if cached and cached.get("crs_total") is not None:
            return float(cached["crs_total"])
        row = await self.hub.fetchrow("""
            SELECT crs_total FROM chip_monitor
            WHERE ticker=$1 AND trade_date<=$2
            ORDER BY trade_date DESC LIMIT 1
        """, ticker, trade_date)
        return float(row["crs_total"]) if row else 50.0

    # =========================================================================
    # 主執行
    # =========================================================================

    async def run(
        self,
        ticker:     str,
        trade_date: Optional[date] = None,
    ) -> dict:
        """執行選股引擎，回傳個股多因子診斷（含基差過濾）"""
        if trade_date is None:
            trade_date = date.today()

        logger.info("選股引擎（v3.4）執行中：%s %s", ticker, trade_date)

        start_date = str(date(trade_date.year - 1, trade_date.month, trade_date.day))

        price_df   = await self.finmind.get_stock_price(ticker, start_date)
        revenue_df = await self.finmind.get_revenue(ticker, start_date)

        score_momentum    = self._calc_momentum_score(price_df)
        score_fundamental = await self._calc_fundamental_score_v2(ticker, revenue_df)
        score_chip        = await self._get_chip_score(ticker, trade_date)
        score_valuation   = await self._calc_valuation_score(ticker)

        weights     = await self._get_macro_weights(trade_date)
        total_score = (
            score_momentum    * weights.get("momentum",    0.30) +
            score_chip        * weights.get("chip",        0.25) +
            score_fundamental * weights.get("fundamental", 0.25) +
            score_valuation   * weights.get("valuation",   0.20)
        )

        adjusted_score, basis_reason = await self._basis_filter.apply(total_score)

        regime_row = await self.hub.fetchrow("""
            SELECT regime, mrs_score FROM market_regime
            WHERE trade_date<=$1 ORDER BY trade_date DESC LIMIT 1
        """, trade_date)
        regime = regime_row["regime"]           if regime_row else None
        mrs    = float(regime_row["mrs_score"]) if regime_row else None

        # v3.4：動態產業分類
        sector    = await self._get_sector_dynamic(ticker)
        beta_mult = SECTOR_BETA_MULTIPLIER.get(sector, 1.0)

        result = {
            "ticker":              ticker,
            "trade_date":          trade_date,
            "score_momentum":      score_momentum,
            "score_chip":          score_chip,
            "score_fundamental":   score_fundamental,
            "score_valuation":     score_valuation,
            "weight_momentum":     weights.get("momentum",    0.30),
            "weight_chip":         weights.get("chip",        0.25),
            "weight_fundamental":  weights.get("fundamental", 0.25),
            "weight_valuation":    weights.get("valuation",   0.20),
            "total_score":         round(total_score, 2),
            "final_score":         round(adjusted_score, 2),
            "basis_filter_reason": basis_reason,
            "sector":              sector,
            "sector_bonus":        0.0,
            "sector_count":        0,
            "sector_beta_mult":    beta_mult,
            "regime_at_calc":      regime,
            "mrs_at_calc":         mrs,
        }

        # 寫入 stock_diagnostic（先刪後插，避免 upsert 分區衝突）
        await self.hub.execute("""
            DELETE FROM stock_diagnostic
            WHERE trade_date=$1 AND ticker=$2
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
                $1,$2,
                $3,$4,$5,$6,
                $7,$8,$9,$10,
                $11,$12,
                $13,
                $14,$15,
                $16,$17,$18
            )
        """,
            trade_date, ticker,
            score_momentum, score_chip, score_fundamental, score_valuation,
            weights.get("momentum",    0.30),
            weights.get("chip",        0.25),
            weights.get("fundamental", 0.25),
            weights.get("valuation",   0.20),
            round(total_score, 2),
            round(adjusted_score, 2),
            basis_reason or None,
            0.0,
            beta_mult,
            regime,
            mrs,
            sector,
        )

        if basis_reason:
            logger.warning(
                "選股引擎完成：%s 總分=%.1f → 基差過濾後=%.1f [%s] %s",
                ticker, total_score, adjusted_score, sector, basis_reason,
            )
        else:
            logger.info(
                "選股引擎完成：%s 總分=%.1f（動=%.0f 籌=%.0f 基=%.0f 值=%.0f）[%s]",
                ticker, total_score,
                score_momentum, score_chip, score_fundamental, score_valuation,
                sector,
            )
        return result