# modules/walk_forward.py
# ═══════════════════════════════════════════════════════════════════════════════
# Bug 修正（v3.1）：
# - [BUG 6] recommendation 補上 REJECT 分支（avg_r < 0 或 win_rate < 0.25）
#           REJECT 時不將 suggested 權重寫入 Redis，維持現有權重
# - [BUG 7] avg_oos_sharpe 改為實際計算（avg_r / std_r），不再硬編碼 None
#           n_oos_windows 改為依資料期間動態計算，不再硬編碼 1
# - [BUG C] _calc_factor_ic() 標準差改除以 N-1（Bessel 修正）
#           原本除以 N 在小樣本（10~30 筆）時高估 IC 值約 5%，
#           可能讓應 ADJUST 的因子誤判為 DEPLOY
# ═══════════════════════════════════════════════════════════════════════════════

import json
import logging
import math
from datetime import date
from typing import Optional
from datahub.data_hub import DataHub
from datahub import redis_keys as rk

logger = logging.getLogger(__name__)

DEFAULT_WEIGHTS = {'momentum': 0.30, 'chip': 0.25, 'fundamental': 0.25, 'valuation': 0.20}

# Walk-Forward 門檻
DEPLOY_AVG_R    = 0.5
DEPLOY_WIN_RATE = 0.40
REJECT_AVG_R    = 0.0   # avg_r < 0 → REJECT
REJECT_WIN_RATE = 0.25  # win_rate < 0.25 → REJECT


class WalkForward:
    """模組⑫ Walk-Forward 驗證：評估因子穩定性，產生建議權重"""

    def __init__(self, hub: DataHub):
        self.hub = hub

    async def get_historical_performance(
        self,
        months: int = 12,
        trade_date: Optional[date] = None,
    ) -> list[dict]:
        if trade_date is None:
            trade_date = date.today()

        rows = await self.hub.fetch("""
            SELECT
                d.trade_date,
                d.ticker,
                d.total_score,
                d.score_momentum,
                d.score_chip,
                d.score_fundamental,
                d.score_valuation,
                d.regime_at_calc,
                p.r_multiple_current AS r_multiple,
                p.exit_reason
            FROM stock_diagnostic d
            LEFT JOIN positions p ON p.ticker = TRIM(d.ticker)
                AND p.entry_date = d.trade_date
            WHERE d.trade_date >= $1
            ORDER BY d.trade_date
        """, date(trade_date.year - 1, trade_date.month, trade_date.day))

        return [dict(r) for r in rows]

    def _calc_factor_ic(self, records: list[dict], factor: str) -> float:
        valid = [r for r in records
                 if r.get(f'score_{factor}') is not None
                 and r.get('r_multiple') is not None]
        if len(valid) < 10:
            return 0.0

        scores  = [float(r[f'score_{factor}']) for r in valid]
        returns = [float(r['r_multiple']) for r in valid]
        n       = len(valid)

        mean_s = sum(scores)  / n
        mean_r = sum(returns) / n

        # [BUG C 修正] 除以 N-1（樣本標準差，Bessel 修正）
        # 原本除以 N（總體標準差），小樣本下高估 IC 值約 5%
        cov   = sum((s - mean_s) * (r - mean_r) for s, r in zip(scores, returns)) / (n - 1)
        std_s = math.sqrt(sum((s - mean_s) ** 2 for s in scores)  / (n - 1)) if n > 1 else 0
        std_r = math.sqrt(sum((r - mean_r) ** 2 for r in returns) / (n - 1)) if n > 1 else 0

        if std_s == 0 or std_r == 0:
            return 0.0
        return round(cov / (std_s * std_r), 4)

    def _calc_oos_sharpe(self, r_multiples: list[float]) -> Optional[float]:
        """
        [BUG 7 修正] 計算 OOS Sharpe（avg_r / std_r）。
        原本硬編碼為 None。
        注意：此處 std_r 仍使用 N-1 以符合樣本統計。
        """
        if len(r_multiples) < 2:
            return None
        n     = len(r_multiples)
        avg_r = sum(r_multiples) / n
        std_r = math.sqrt(sum((r - avg_r) ** 2 for r in r_multiples) / (n - 1))
        if std_r == 0:
            return None
        return round(avg_r / std_r, 4)

    def _calc_n_oos_windows(self, records: list[dict], oos_months: int = 3, step_months: int = 3) -> int:
        """
        [BUG 7 修正] 動態計算滾動視窗數量。
        原本硬編碼為 1。
        規則：總資料月數 / step_months，最少 1
        """
        if not records:
            return 1
        dates = [r['trade_date'] for r in records if r.get('trade_date')]
        if len(dates) < 2:
            return 1
        min_d = min(dates)
        max_d = max(dates)
        total_months = (max_d.year - min_d.year) * 12 + (max_d.month - min_d.month)
        n_windows = max(1, total_months // step_months)
        return n_windows

    async def run(
        self,
        trade_date: Optional[date] = None,
    ) -> dict:
        if trade_date is None:
            trade_date = date.today()

        records = await self.get_historical_performance(trade_date=trade_date)

        factors   = ['momentum', 'chip', 'fundamental', 'valuation']
        ic_scores = {f: self._calc_factor_ic(records, f) for f in factors}

        # 依 IC 比例計算建議權重（正 IC 才給權重）
        total_ic = sum(max(v, 0) for v in ic_scores.values())
        if total_ic > 0:
            suggested = {f: round(max(ic_scores[f], 0) / total_ic, 4) for f in factors}
        else:
            suggested = DEFAULT_WEIGHTS.copy()

        # ── 績效統計 ─────────────────────────────────────────────────────────
        trades_with_r = [r for r in records if r.get('r_multiple') is not None]
        r_multiples   = [float(r['r_multiple']) for r in trades_with_r]

        avg_r    = sum(r_multiples) / max(len(r_multiples), 1)
        win_rate = len([r for r in r_multiples if r > 0]) / max(len(r_multiples), 1)

        # [BUG 6 修正] 補上 REJECT 分支
        if avg_r > DEPLOY_AVG_R and win_rate > DEPLOY_WIN_RATE:
            recommendation = 'DEPLOY'
        elif avg_r < REJECT_AVG_R or win_rate < REJECT_WIN_RATE:
            recommendation = 'REJECT'
        else:
            recommendation = 'ADJUST'

        # [BUG 7 修正] 動態計算
        avg_oos_sharpe = self._calc_oos_sharpe(r_multiples)
        n_oos_windows  = self._calc_n_oos_windows(records)

        logger.info(
            "Walk-Forward 績效：avg_r=%.3f win_rate=%.3f sharpe=%s n_windows=%d → %s",
            avg_r, win_rate, avg_oos_sharpe, n_oos_windows, recommendation,
        )

        # 寫入 DB
        await self.hub.execute("""
            UPDATE wf_results SET is_active = FALSE WHERE is_active = TRUE
        """)
        await self.hub.execute("""
            INSERT INTO wf_results (
                data_from, data_to, train_months, oos_months, step_months,
                n_oos_windows, avg_oos_sharpe, consistency_rate,
                factor_stability, recommendation, recommended_weights,
                is_active, notes
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
        """,
            date(trade_date.year - 1, trade_date.month, trade_date.day),
            trade_date,
            12, 3, 3,
            n_oos_windows,
            avg_oos_sharpe,
            round(win_rate, 4),
            json.dumps({f: {'ic': ic_scores[f]} for f in factors}),
            recommendation,
            json.dumps(suggested),
            True,
            f"Walk-Forward 驗證 {trade_date}，{len(records)} 筆資料",
        )

        # [BUG 6 修正] REJECT 時不寫入 Redis，維持現有因子權重
        if recommendation != 'REJECT':
            await self.hub.cache.set(
                rk.key_wf_weights_current(),
                suggested,
                ttl=rk.TTL_24H,
            )
            logger.info("Walk-Forward 權重已更新至 Redis：%s", suggested)
        else:
            logger.warning(
                "Walk-Forward 結論為 REJECT（avg_r=%.3f win_rate=%.3f），不更新 Redis 權重",
                avg_r, win_rate,
            )

        logger.info("Walk-Forward 完成：建議=%s 權重=%s", recommendation, suggested)
        return {
            'status':              'OK',
            'records_count':       len(records),
            'ic_scores':           ic_scores,
            'recommended_weights': suggested,
            'recommendation':      recommendation,
            'avg_r':               round(avg_r, 3),
            'win_rate':            round(win_rate, 3),
            'avg_oos_sharpe':      avg_oos_sharpe,
            'n_oos_windows':       n_oos_windows,
        }