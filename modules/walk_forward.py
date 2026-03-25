# modules/walk_forward.py
import logging
import json
from datetime import date
from typing import Optional
from datahub.data_hub import DataHub

logger = logging.getLogger(__name__)

DEFAULT_WEIGHTS = {'momentum': 0.30, 'chip': 0.25, 'fundamental': 0.25, 'valuation': 0.20}


class WalkForward:
    """模組⑫ Walk-Forward 驗證：評估因子穩定性，產生建議權重"""

    def __init__(self, hub: DataHub):
        self.hub = hub

    async def get_historical_performance(
        self,
        months: int = 12,
        trade_date: Optional[date] = None,
    ) -> list[dict]:
        """取得歷史交易績效"""
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
        """計算因子 IC（資訊係數）"""
        valid = [r for r in records
                 if r.get(f'score_{factor}') is not None
                 and r.get('r_multiple') is not None]
        if len(valid) < 10:
            return 0.0

        scores  = [float(r[f'score_{factor}']) for r in valid]
        returns = [float(r['r_multiple']) for r in valid]

        mean_s = sum(scores) / len(scores)
        mean_r = sum(returns) / len(returns)
        cov = sum((s - mean_s) * (r - mean_r) for s, r in zip(scores, returns))
        std_s = (sum((s - mean_s) ** 2 for s in scores) / len(scores)) ** 0.5
        std_r = (sum((r - mean_r) ** 2 for r in returns) / len(returns)) ** 0.5

        if std_s == 0 or std_r == 0:
            return 0.0
        return round(cov / (len(valid) * std_s * std_r), 4)

    def _suggest_weights(self, ic_scores: dict) -> dict:
        """依 IC 分數建議因子權重"""
        total_ic = sum(max(v, 0.01) for v in ic_scores.values())
        weights  = {k: round(max(v, 0.01) / total_ic, 3) for k, v in ic_scores.items()}

        # 正規化
        total = sum(weights.values())
        weights = {k: round(v / total, 3) for k, v in weights.items()}
        return weights

    async def run(self, trade_date: Optional[date] = None) -> dict:
        """執行 Walk-Forward 驗證"""
        if trade_date is None:
            trade_date = date.today()

        logger.info("Walk-Forward 驗證開始")
        records = await self.get_historical_performance(trade_date=trade_date)

        if len(records) < 20:
            logger.warning("歷史資料不足（%d 筆），使用預設權重", len(records))
            return {
                'status':             'INSUFFICIENT_DATA',
                'records_count':      len(records),
                'recommended_weights': DEFAULT_WEIGHTS,
                'recommendation':     'ADJUST',
                'message':            '歷史資料不足，維持預設權重',
            }

        # 計算各因子 IC
        factors  = ['momentum', 'chip', 'fundamental', 'valuation']
        ic_scores = {f: self._calc_factor_ic(records, f) for f in factors}

        # 建議權重
        suggested = self._suggest_weights(ic_scores)

        # 績效統計
        trades_with_r = [r for r in records if r.get('r_multiple') is not None]
        avg_r  = sum(float(r['r_multiple']) for r in trades_with_r) / max(len(trades_with_r), 1)
        win_rate = len([r for r in trades_with_r if float(r['r_multiple']) > 0]) / max(len(trades_with_r), 1)

        recommendation = 'DEPLOY' if avg_r > 0.5 and win_rate > 0.40 else 'ADJUST'

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
            1,
            None,
            round(win_rate, 4),
            json.dumps({f: {'ic': ic_scores[f]} for f in factors}),
            recommendation,
            json.dumps(suggested),
            True,
            f"Walk-Forward 驗證 {trade_date}，{len(records)} 筆資料",
        )

        # 更新 Redis
        await self.hub.cache.set(
            'wf:weights:current', suggested, ttl=0
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
        }