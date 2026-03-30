# modules/addon_engine.py
# ═══════════════════════════════════════════════════════════════════════════════
# 加碼引擎（AddonEngine）
#
# 說明：
#   系統目前 signal_source / decision_type 已有 ADDON_SIGNAL / ADDON_EXECUTED，
#   但沒有實際的加碼邏輯模組。本模組填補此缺口。
#
# 加碼觸發條件（所有條件需同時滿足）：
#   1. Regime 為 BULL_TREND 或 WEAK_BULL（熊市不加碼）
#   2. 持倉狀態 = S2_BREAKOUT_CONFIRM（突破確認，停損已移至成本）
#   3. 個股當日 SelectionEngine final_score ≥ ADDON_SCORE_THRESHOLD（預設 68）
#   4. 目前組合曝險 < Regime 曝險上限 × HEADROOM_RATIO（保留安全邊際）
#   5. 同一持倉加碼次數 < MAX_ADDON_TIMES（最多加碼兩次）
#
# 加碼股數計算：
#   - 使用固定 R 法：addon_shares = floor(R_amount / (entry_price × ADD_RISK_RATIO))
#   - ADD_RISK_RATIO 預設 0.5（加碼倉位的風險是原始倉位的一半）
#
# 整合說明：
#   scheduler.py Step 5（PositionManager）執行完後，
#   可新增 Step 5a 呼叫 AddonEngine.run()，
#   回傳的 candidates 列表再透過 SignalBus 發出 ADDON_SIGNAL
# ═══════════════════════════════════════════════════════════════════════════════

import logging
from datetime import date
from typing import Optional
from datahub.data_hub import DataHub
from datahub import redis_keys as rk

logger = logging.getLogger(__name__)

# 加碼參數
ADDON_SCORE_THRESHOLD = 68      # 個股評分門檻（低於此分不加碼）
MAX_ADDON_TIMES       = 2       # 同一持倉最多加碼次數
ADD_RISK_RATIO        = 0.5     # 加碼倉位風險倍率（相對於原始 R）
HEADROOM_RATIO        = 0.85    # 曝險使用率上限（留 15% 安全邊際）

# 允許加碼的 Regime
ADDON_ALLOWED_REGIMES = {"BULL_TREND", "WEAK_BULL"}


class AddonEngine:
    """
    加碼引擎：在 S2 突破確認狀態下，評估加碼機會。

    使用方法：
        addon = AddonEngine(hub)
        candidates = await addon.run(trade_date)
        for c in candidates:
            await bus.emit_addon(c['ticker'], c['addon_shares'], c['reason'])
    """

    def __init__(self, hub: DataHub):
        self.hub = hub

    async def _get_current_regime(self) -> tuple[Optional[str], float]:
        """取得當前 Regime 與曝險上限"""
        row = await self.hub.fetchrow("""
            SELECT regime, max_exposure_limit FROM market_regime
            ORDER BY trade_date DESC LIMIT 1
        """)
        if not row:
            return None, 0.0
        return row["regime"], float(row["max_exposure_limit"] or 0)

    async def _get_portfolio_exposure(self) -> float:
        """取得目前組合曝險百分比"""
        row = await self.hub.fetchrow("""
            SELECT gross_exposure_pct FROM portfolio_health
            ORDER BY snapshot_date DESC LIMIT 1
        """)
        return float(row["gross_exposure_pct"]) if row and row["gross_exposure_pct"] else 0.0

    async def _get_addon_count(self, position_id: int) -> int:
        """查詢某持倉已加碼幾次"""
        row = await self.hub.fetchrow("""
            SELECT COUNT(*) AS cnt FROM decision_log
            WHERE decision_type = 'ADDON_EXECUTED'
              AND notes::jsonb->>'position_id' = $1::text
        """, str(position_id))
        return int(row["cnt"]) if row else 0

    async def _calc_addon_shares(
        self,
        r_amount:    float,
        entry_price: float,
    ) -> int:
        """計算加碼股數（基於固定 R 法）"""
        if entry_price <= 0 or r_amount <= 0:
            return 0
        risk_per_share = entry_price * ADD_RISK_RATIO * 0.01  # 加碼停損約 0.5% 的進場價
        shares = int(r_amount * ADD_RISK_RATIO / max(risk_per_share, 0.01))
        # 最小 1000 股（台股最小交易單位），最大不超過原始持倉
        return max(1000, (shares // 1000) * 1000)

    async def run(
        self,
        trade_date: Optional[date] = None,
    ) -> list[dict]:
        """
        評估所有開放持倉中的加碼機會。

        回傳：達到加碼條件的持倉清單
        [
            {
                'position_id':   int,
                'ticker':        str,
                'state':         'S2_BREAKOUT_CONFIRM',
                'current_score': float,
                'addon_shares':  int,
                'reason':        str,
                'regime':        str,
            },
            ...
        ]
        """
        if trade_date is None:
            trade_date = date.today()

        regime, max_exposure = await self._get_current_regime()
        if regime not in ADDON_ALLOWED_REGIMES:
            logger.info("加碼引擎：當前 Regime=%s，不執行加碼", regime)
            return []

        current_exposure = await self._get_portfolio_exposure()
        exposure_limit   = max_exposure * HEADROOM_RATIO
        if current_exposure >= exposure_limit:
            logger.info(
                "加碼引擎：曝險 %.1f%% ≥ 上限 %.1f%%，不執行加碼",
                current_exposure * 100, exposure_limit * 100,
            )
            return []

        # 查詢 S2 狀態的持倉，並 JOIN 當日評分
        candidates_raw = await self.hub.fetch("""
            SELECT
                p.id          AS position_id,
                p.ticker,
                p.state,
                p.entry_price,
                p.avg_cost,
                p.r_amount,
                p.current_shares,
                COALESCE(s.final_score, 0) AS current_score
            FROM positions p
            LEFT JOIN stock_diagnostic s
                ON s.ticker = p.ticker AND s.trade_date = $1
            WHERE p.is_open = TRUE
              AND p.state = 'S2_BREAKOUT_CONFIRM'
              AND COALESCE(s.final_score, 0) >= $2
            ORDER BY s.final_score DESC NULLS LAST
        """, trade_date, ADDON_SCORE_THRESHOLD)

        results = []
        for row in candidates_raw:
            position_id = row["position_id"]
            ticker      = row["ticker"].strip()

            # 檢查加碼次數
            addon_count = await self._get_addon_count(position_id)
            if addon_count >= MAX_ADDON_TIMES:
                logger.debug("加碼引擎：%s 已加碼 %d 次，跳過", ticker, addon_count)
                continue

            # 計算加碼股數
            addon_shares = await self._calc_addon_shares(
                r_amount=float(row["r_amount"] or 0),
                entry_price=float(row["entry_price"] or 0),
            )
            if addon_shares <= 0:
                continue

            reason = (
                f"S2 加碼：評分={row['current_score']:.1f} "
                f"Regime={regime} "
                f"第{addon_count + 1}次"
            )

            candidate = {
                "position_id":   position_id,
                "ticker":        ticker,
                "state":         row["state"],
                "entry_price":   float(row["entry_price"]),
                "avg_cost":      float(row["avg_cost"] or row["entry_price"]),
                "current_shares":int(row["current_shares"]),
                "addon_shares":  addon_shares,
                "current_score": float(row["current_score"]),
                "addon_count":   addon_count,
                "regime":        regime,
                "reason":        reason,
            }
            results.append(candidate)

            logger.info(
                "加碼候選：%s 評分=%.1f 加碼股數=%d（第%d次）",
                ticker, row["current_score"], addon_shares, addon_count + 1,
            )

        if results:
            logger.info(
                "加碼引擎完成：%d 檔持倉符合加碼條件（Regime=%s 曝險=%.1f%%）",
                len(results), regime, current_exposure * 100,
            )
        else:
            logger.info("加碼引擎：無符合加碼條件的持倉")

        return results

    async def record_addon(
        self,
        position_id:  int,
        ticker:       str,
        addon_shares: int,
        exec_price:   float,
        trade_date:   Optional[date] = None,
    ) -> None:
        """
        記錄加碼執行結果到 decision_log，並更新 positions 的 avg_cost。
        在 PositionManager 執行加碼後呼叫。
        """
        if trade_date is None:
            trade_date = date.today()

        import json
        await self.hub.execute("""
            INSERT INTO decision_log (
                trade_date, decision_type, ticker,
                signal_source, notes
            ) VALUES ($1, 'ADDON_EXECUTED', $2, 'ADDON', $3)
        """, trade_date, ticker, json.dumps({
            "position_id":  position_id,
            "addon_shares": addon_shares,
            "exec_price":   exec_price,
        }, ensure_ascii=False))

        # 更新 positions 的加權平均成本
        row = await self.hub.fetchrow("""
            SELECT current_shares, avg_cost FROM positions WHERE id = $1
        """, position_id)
        if row:
            old_shares   = int(row["current_shares"])
            old_avg_cost = float(row["avg_cost"] or exec_price)
            total_shares = old_shares + addon_shares
            new_avg_cost = (
                (old_shares * old_avg_cost + addon_shares * exec_price) / total_shares
                if total_shares > 0 else exec_price
            )
            await self.hub.execute("""
                UPDATE positions
                SET current_shares = $1, avg_cost = $2
                WHERE id = $3
            """, total_shares, round(new_avg_cost, 2), position_id)
            logger.info(
                "加碼記錄：%s +%d 股 @ %.2f，新均成本 %.2f，總持股 %d",
                ticker, addon_shares, exec_price, new_avg_cost, total_shares,
            )