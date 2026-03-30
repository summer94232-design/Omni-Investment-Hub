# modules/selection_engine.py
# ═══════════════════════════════════════════════════════════════════════════════
# 變更紀錄（v3.3）：
# - [BUG 9]   台指期改用 get_futures_quote('TXF')（已含於 v3.1）
# - [BUG ③]   stock_diagnostic INSERT 補全欄位（已含於 v3.1）
# - [估值因子] score_valuation 從固定 50.0 改為真實 PE / PBR 計算
#     資料來源：FinMindAPI.get_per_pbr()（TaiwanStockPER dataset）
#     計分邏輯：
#       PE  < 12 → +30，12-20 → +20，20-35 → +10，35-50 → 0，>50 → -15
#       PBR < 1  → +20，1-2   → +15，2-3   → +5，>3     → 0
#       殖利率 > 5% → +10，3-5% → +5，<3% → 0
#       無資料時 fallback = 50.0
# - [基本面因子] _calc_fundamental_score 新增 EPS 成長率支援（來自財報資料）
#     若有 EPS 季報資料則 YoY 計算精度更高；否則沿用月營收 YoY
# ═══════════════════════════════════════════════════════════════════════════════

import logging
from datetime import date
from typing import Optional
import pandas as pd
from datahub.data_hub import DataHub
from datahub.api_finmind import FinMindAPI
from datahub import redis_keys as rk

logger = logging.getLogger(__name__)

# ── 產業分類 ─────────────────────────────────────────────────────────────────

SECTOR_MAP = {
    "2330": "SEMI", "2303": "SEMI", "2308": "SEMI", "2379": "SEMI",
    "2454": "AI_SERVER", "3711": "AI_SERVER", "6669": "AI_SERVER", "3034": "AI_SERVER",
    "2317": "EMS", "2354": "EMS", "2382": "EMS",
    "2412": "TELECOM", "2498": "TELECOM",
    "2886": "FINANCE", "2891": "FINANCE", "2882": "FINANCE", "2881": "FINANCE",
    "4711": "BIOTECH", "6548": "BIOTECH", "4958": "BIOTECH",
    "2207": "AUTO", "2105": "AUTO",
    "2801": "PROPERTY", "5522": "PROPERTY",
}

SECTOR_BETA_MULTIPLIER = {
    "SEMI":       1.2,
    "AI_SERVER":  1.2,
    "EMS":        1.0,
    "TELECOM":    0.8,
    "FINANCE":    0.7,
    "BIOTECH":    0.5,
    "AUTO":       0.8,
    "PROPERTY":   0.5,
}

def get_sector(ticker: str) -> str:
    return SECTOR_MAP.get(ticker.strip(), "OTHER")


# ── 基差過濾器 ────────────────────────────────────────────────────────────────

class BasisFilter:
    """台指期 Normalized Basis 過濾器（v3）"""

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


# ── 產業群聚 ─────────────────────────────────────────────────────────────────

async def apply_sector_cluster_bonus(
    hub: DataHub,
    results: list[dict],
    trade_date: date,
    threshold: float = 60.0,
) -> list[dict]:
    """對同產業多檔入選的股票依 Beta 倍率差異化加分"""
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
        base_bonus  = bonus_map.get(count, 9 if count >= 4 else 0)
        beta_mult   = SECTOR_BETA_MULTIPLIER.get(s, 1.0)
        actual_bonus = base_bonus * beta_mult
        r["sector_bonus"]      = round(actual_bonus, 2)
        r["sector_count"]      = count
        r["final_score"]       = round(r.get("final_score", 0) + actual_bonus, 2)
        r["sector_beta_mult"]  = beta_mult

        await hub.execute("""
            UPDATE stock_diagnostic
            SET sector_bonus = $1, sector_beta_mult = $2, final_score = $3
            WHERE trade_date = $4 AND ticker = $5
        """, actual_bonus, beta_mult, r["final_score"], trade_date, r["ticker"])

    return results


async def check_sector_exit_cascade(
    hub: DataHub,
    exit_tickers: list[str],
    trade_date: date,
    min_trigger: int = 3,
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
            WHERE is_open = TRUE AND state NOT IN ('S5_ACTIVE_EXIT','CLOSED','STOPPED_OUT')
              AND ticker NOT IN ({})
        """.format(",".join(f"'{t}'" for t in tickers)))
        for row in rows:
            if get_sector(row["ticker"]) == sector:
                await hub.execute("""
                    UPDATE positions SET state = 'S5_ACTIVE_EXIT' WHERE id = $1
                """, row["id"])
                forced.append(row["ticker"])
                logger.warning("負向群聚出場：%s 強制進入 S5（產業=%s）", row["ticker"], sector)
    return forced


# ── SelectionEngine ───────────────────────────────────────────────────────────

class SelectionEngine:
    """多因子選股引擎（v3.3）"""

    def __init__(self, hub: DataHub, finmind: FinMindAPI):
        self.hub           = hub
        self.finmind       = finmind
        self._basis_filter = BasisFilter(hub)

    # ── 宏觀權重 ─────────────────────────────────────────────────────────────

    async def _get_macro_weights(self, trade_date: date) -> dict[str, float]:
        """取得 WalkForward 建議的動態權重，並依 Regime 微調"""
        wf_row = await self.hub.fetchrow("""
            SELECT recommended_weights FROM wf_results
            WHERE status = 'COMPLETED' AND recommendation = 'DEPLOY'
            ORDER BY run_date DESC LIMIT 1
        """)
        import json
        default_weights = {
            "momentum": 0.30, "chip": 0.25, "fundamental": 0.25, "valuation": 0.20,
        }
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
            return {**wf_weights, "momentum": min(wf_weights.get("momentum", 0.30) + 0.05, 0.50)}
        elif regime == "BEAR_TREND":
            return {**wf_weights, "fundamental": min(wf_weights.get("fundamental", 0.25) + 0.05, 0.40)}
        return wf_weights

    # ── 動能因子 ─────────────────────────────────────────────────────────────

    def _calc_momentum_score(self, price_df: pd.DataFrame) -> float:
        if price_df.empty or len(price_df) < 20:
            return 50.0
        df     = price_df.sort_values("date")
        closes = df["close"].astype(float)
        ma20   = closes.tail(20).mean()
        ma60   = closes.tail(60).mean() if len(closes) >= 60 else ma20
        latest = float(closes.iloc[-1])
        score  = 50.0
        if latest > ma20:  score += 20
        if latest > ma60:  score += 15
        if ma20   > ma60:  score += 15
        ret_20  = (latest / float(closes.iloc[-20]) - 1) if len(closes) >= 20 else 0
        score  += min(ret_20 * 200, 20)
        return round(max(0, min(100, score)), 2)

    # ── 基本面因子 ───────────────────────────────────────────────────────────

    def _calc_fundamental_score(self, revenue_df: pd.DataFrame) -> float:
        """以月營收 YoY 計算基本面分數"""
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
        return round(max(0, min(100, score)), 2)

    async def _calc_fundamental_score_v2(
        self,
        ticker: str,
        revenue_df: pd.DataFrame,
    ) -> float:
        """
        v3.3 增強版基本面評分：優先使用季報 EPS 成長率，降級至月營收。
        """
        try:
            fin_df = await self.finmind.get_financial_statements(ticker, quarters=8)
            if not fin_df.empty:
                eps_df = fin_df[fin_df["type"] == "EPS"].sort_values("date")
                if len(eps_df) >= 5:
                    # 最新季 EPS vs 去年同期 EPS（YoY）
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
                        return round(max(0, min(100, score)), 2)
        except Exception as e:
            logger.debug("季報 EPS 計算失敗 %s：%s，改用月營收", ticker, e)

        return self._calc_fundamental_score(revenue_df)

    # ── 估值因子（v3.3 真實計算，取代固定 50.0）────────────────────────────

    async def _calc_valuation_score(self, ticker: str) -> float:
        """
        以 PE ratio / PBR / 殖利率計算估值分數。
        資料來源：FinMind TaiwanStockPER（每日更新）。

        計分表：
          PE < 12          → +30
          12 ≤ PE < 20     → +20
          20 ≤ PE < 35     → +10
          35 ≤ PE < 50     → 0
          PE ≥ 50          → -15
          PBR < 1          → +20
          1 ≤ PBR < 2      → +15
          2 ≤ PBR < 3      → +5
          PBR ≥ 3          → 0
          殖利率 ≥ 5%      → +10
          3% ≤ 殖利率 < 5% → +5
          殖利率 < 3%      → 0
          無資料 fallback  → 50.0
        """
        try:
            val = await self.finmind.get_per_pbr(ticker)
        except Exception as e:
            logger.debug("估值 API 失敗 %s：%s", ticker, e)
            return 50.0

        per = val.get("per")
        pbr = val.get("pbr")
        dy  = val.get("dividend_yield")

        if per is None and pbr is None:
            return 50.0

        score = 50.0

        # PE 評分
        if per is not None:
            if   per < 12:   score += 30
            elif per < 20:   score += 20
            elif per < 35:   score += 10
            elif per < 50:   score += 0
            else:            score -= 15

        # PBR 評分
        if pbr is not None:
            if   pbr < 1:    score += 20
            elif pbr < 2:    score += 15
            elif pbr < 3:    score += 5
            # pbr >= 3 → 0

        # 殖利率評分
        if dy is not None:
            if   dy >= 5.0:  score += 10
            elif dy >= 3.0:  score += 5

        return round(max(0, min(100, score)), 2)

    # ── 籌碼因子 ─────────────────────────────────────────────────────────────

    async def _get_chip_score(self, ticker: str, trade_date: date) -> float:
        cached = await self.hub.cache.get(rk.key_chip_crs_latest(ticker))
        if cached and cached.get("crs_total") is not None:
            return float(cached["crs_total"])
        row = await self.hub.fetchrow("""
            SELECT crs_total FROM chip_monitor
            WHERE ticker = $1 AND trade_date <= $2
            ORDER BY trade_date DESC LIMIT 1
        """, ticker, trade_date)
        return float(row["crs_total"]) if row else 50.0

    # ── 主執行 ───────────────────────────────────────────────────────────────

    async def run(
        self,
        ticker:     str,
        trade_date: Optional[date] = None,
    ) -> dict:
        """執行選股引擎，回傳個股多因子診斷（含基差過濾）"""
        if trade_date is None:
            trade_date = date.today()

        logger.info("選股引擎（v3.3）執行中：%s %s", ticker, trade_date)

        from datetime import date as dt
        start_date = str(dt(trade_date.year - 1, trade_date.month, trade_date.day))

        price_df   = await self.finmind.get_stock_price(ticker, start_date)
        revenue_df = await self.finmind.get_revenue(ticker, start_date)

        score_momentum    = self._calc_momentum_score(price_df)
        score_fundamental = await self._calc_fundamental_score_v2(ticker, revenue_df)
        score_chip        = await self._get_chip_score(ticker, trade_date)
        score_valuation   = await self._calc_valuation_score(ticker)  # v3.3: 真實估值

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
            WHERE trade_date <= $1 ORDER BY trade_date DESC LIMIT 1
        """, trade_date)
        regime = regime_row["regime"]           if regime_row else None
        mrs    = float(regime_row["mrs_score"]) if regime_row else None

        sector    = get_sector(ticker)
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
                "選股引擎完成：%s 總分=%.1f → 基差過濾後=%.1f [%s] 原因：%s",
                ticker, total_score, adjusted_score, sector, basis_reason,
            )
        else:
            logger.info(
                "選股引擎完成：%s 總分=%.1f（動能=%.0f 籌碼=%.0f 基本面=%.0f 估值=%.0f）[%s]",
                ticker, total_score, score_momentum, score_chip,
                score_fundamental, score_valuation, sector,
            )
        return result