# modules/attribution.py
import logging
from datetime import date, timedelta
from typing import Optional
from datahub.data_hub import DataHub

logger = logging.getLogger(__name__)


class Attribution:
    """模組⑤ 交易歸因進化閉環：分析已平倉交易，找出改善方向"""

    def __init__(self, hub: DataHub):
        self.hub = hub

    async def analyze_closed_positions(
        self,
        days: int = 30,
        trade_date: Optional[date] = None,
    ) -> dict:
        """分析近 N 日已平倉交易"""
        if trade_date is None:
            trade_date = date.today()
        start = trade_date - timedelta(days=days)

        rows = await self.hub.fetch("""
            SELECT
                ticker, strategy_tag, signal_source,
                entry_date, exit_date,
                entry_price, exit_price,
                r_multiple_current AS r_multiple,
                realized_pnl,
                mrs_at_entry, regime_at_entry,
                exit_reason
            FROM positions
            WHERE is_open = FALSE
              AND exit_date >= $1
              AND exit_date <= $2
            ORDER BY exit_date DESC
        """, start, trade_date)

        if not rows:
            return {
                'period':      f"{start} ~ {trade_date}",
                'total_trades': 0,
                'message':     '此期間無已平倉交易',
            }

        trades = [dict(r) for r in rows]
        total  = len(trades)
        wins   = [t for t in trades if (t['r_multiple'] or 0) > 0]
        losses = [t for t in trades if (t['r_multiple'] or 0) <= 0]

        win_rate   = len(wins) / total
        avg_r      = sum(t['r_multiple'] or 0 for t in trades) / total
        avg_win_r  = sum(t['r_multiple'] or 0 for t in wins) / max(len(wins), 1)
        avg_loss_r = sum(t['r_multiple'] or 0 for t in losses) / max(len(losses), 1)
        total_pnl  = sum(float(t['realized_pnl'] or 0) for t in trades)

        # 依市場環境分組
        regime_breakdown = {}
        for t in trades:
            r = t['regime_at_entry'] or 'UNKNOWN'
            if r not in regime_breakdown:
                regime_breakdown[r] = {'count': 0, 'wins': 0, 'avg_r': 0}
            regime_breakdown[r]['count'] += 1
            if (t['r_multiple'] or 0) > 0:
                regime_breakdown[r]['wins'] += 1
            regime_breakdown[r]['avg_r'] += (t['r_multiple'] or 0)
        for r in regime_breakdown:
            n = regime_breakdown[r]['count']
            regime_breakdown[r]['win_rate'] = round(regime_breakdown[r]['wins'] / n, 3)
            regime_breakdown[r]['avg_r'] = round(regime_breakdown[r]['avg_r'] / n, 3)

        # 出場原因分析
        exit_breakdown = {}
        for t in trades:
            reason = t['exit_reason'] or 'UNKNOWN'
            if reason not in exit_breakdown:
                exit_breakdown[reason] = {'count': 0, 'avg_r': 0}
            exit_breakdown[reason]['count'] += 1
            exit_breakdown[reason]['avg_r'] += (t['r_multiple'] or 0)
        for r in exit_breakdown:
            n = exit_breakdown[r]['count']
            exit_breakdown[r]['avg_r'] = round(exit_breakdown[r]['avg_r'] / n, 3)

        # 建議
        suggestions = []
        if win_rate < 0.40:
            suggestions.append(f"勝率偏低（{win_rate*100:.0f}%），考慮提高選股門檻")
        if avg_r < 0.5:
            suggestions.append(f"平均R偏低（{avg_r:.2f}R），檢視出場時機")
        if abs(avg_loss_r) > avg_win_r:
            suggestions.append("虧損幅度大於獲利幅度，考慮縮緊停損")

        result = {
            'period':          f"{start} ~ {trade_date}",
            'total_trades':    total,
            'win_rate':        round(win_rate, 3),
            'avg_r':           round(avg_r, 3),
            'avg_win_r':       round(avg_win_r, 3),
            'avg_loss_r':      round(avg_loss_r, 3),
            'total_pnl':       round(total_pnl, 2),
            'regime_breakdown': regime_breakdown,
            'exit_breakdown':  exit_breakdown,
            'suggestions':     suggestions,
        }

        logger.info(
            "歸因分析完成：%d 筆 勝率=%.0f%% 平均R=%.2f",
            total, win_rate * 100, avg_r
        )
        return result