# modules/attribution.py
import logging
from datetime import date, timedelta
from typing import Optional
from datahub.data_hub import DataHub

logger = logging.getLogger(__name__)

# ── 計畫外交易懲罰設定 ───────────────────────────────────────────────────────
# 若某 ticker 近 N 日內有 M 筆以上的 is_planned=FALSE 交易，
# 下次選股時該 ticker 的分數乘以此懲罰係數
UNPLANNED_PENALTY_WINDOW_DAYS = 30      # 觀察窗口（天）
UNPLANNED_PENALTY_THRESHOLD   = 2       # 觸發懲罰的最低次數
UNPLANNED_PENALTY_FACTOR       = 0.80   # 懲罰倍率（分數 × 0.80）

# ── SECTOR_BETA 判斷設定 ─────────────────────────────────────────────────────
# 若同一產業的同期交易有 >= N 筆且勝率高，判斷為 SECTOR_BETA（族群紅利）
# 否則才有機會被歸類為真正的 ALPHA
SECTOR_BETA_MIN_TRADES  = 3     # 同產業至少幾筆才做判斷
SECTOR_BETA_WIN_RATE    = 0.65  # 族群勝率門檻


class Attribution:
    """模組⑤ 交易歸因進化閉環：分析已平倉交易，找出改善方向

    變更紀錄（v2）：
    - [計畫外交易懲罰] 新增 get_unplanned_penalty_map()：
        計算各 ticker 近期 is_planned=FALSE 比例，回傳懲罰係數 dict，
        供 scheduler.py 在 Step 3 前套用，自動調降情緒性標的的 final_score
    - [SECTOR_BETA] Alpha 來源新增 'SECTOR_BETA' 分類：
        在 analyze_closed_positions() 中，若整個族群集體獲利，
        個別標的的 alpha_source 設為 SECTOR_BETA，而非 ALPHA
    - analyze_closed_positions() 回傳結果新增:
        sector_breakdown:    依產業分組的勝率/平均R
        unplanned_summary:   計畫外交易統計
        alpha_source_breakdown: ALPHA/BETA/SECTOR_BETA/MIXED 分布
    """

    def __init__(self, hub: DataHub):
        self.hub = hub

    # =========================================================================
    # 計畫外交易懲罰（供 scheduler.py 呼叫）
    # =========================================================================

    async def get_unplanned_penalty_map(
        self,
        trade_date: Optional[date] = None,
    ) -> dict[str, float]:
        """
        回傳各 ticker 的懲罰係數（1.0 = 不懲罰，< 1.0 = 懲罰）
        
        使用方式（scheduler.py Step 3 之後）：
            penalty_map = await attribution.get_unplanned_penalty_map(trade_date)
            for r in entry_signals:
                factor = penalty_map.get(r['ticker'].strip(), 1.0)
                r['final_score'] = round(r['final_score'] * factor, 2)
        """
        if trade_date is None:
            trade_date = date.today()
        start = trade_date - timedelta(days=UNPLANNED_PENALTY_WINDOW_DAYS)

        rows = await self.hub.fetch("""
            SELECT
                ticker,
                COUNT(*)                                          AS total,
                COUNT(*) FILTER (WHERE is_planned = FALSE)        AS unplanned
            FROM decision_log
            WHERE trade_date >= $1
              AND trade_date <= $2
              AND ticker IS NOT NULL
            GROUP BY ticker
        """, start, trade_date)

        penalty_map: dict[str, float] = {}
        for row in rows:
            ticker    = str(row['ticker']).strip()
            total     = int(row['total'])
            unplanned = int(row['unplanned'])
            if total > 0 and unplanned >= UNPLANNED_PENALTY_THRESHOLD:
                penalty_map[ticker] = UNPLANNED_PENALTY_FACTOR
                logger.warning(
                    "計畫外交易懲罰：%s 近%d日 %d/%d 筆為計畫外 → 下次分數 ×%.2f",
                    ticker, UNPLANNED_PENALTY_WINDOW_DAYS,
                    unplanned, total, UNPLANNED_PENALTY_FACTOR
                )

        return penalty_map

    # =========================================================================
    # Alpha 來源分類（含 SECTOR_BETA）
    # =========================================================================

    @staticmethod
    def _classify_alpha_source(
        trade: dict,
        sector_stats: dict,   # {sector: {'win_rate': float, 'count': int}}
    ) -> str:
        """
        判斷單筆交易的 alpha 來源：
        
        SECTOR_BETA  → 個股獲利，但所屬產業整體表現也好（族群紅利）
        ALPHA        → 個股獲利，且產業整體表現平淡（真正選股能力）
        BETA         → 個股跟大盤漲（由外部根據 regime_at_entry 判斷）
        MIXED        → 無法明確歸因
        """
        r_multiple = trade.get('r_multiple') or 0
        sector     = trade.get('sector', 'OTHER')

        if r_multiple <= 0:
            return 'MIXED'   # 虧損單不做 alpha 歸因

        sector_info = sector_stats.get(sector, {})
        sector_wr   = sector_info.get('win_rate', 0)
        sector_cnt  = sector_info.get('count', 0)

        # 族群紅利：同產業夠多筆且勝率高
        if (sector != 'OTHER'
                and sector_cnt >= SECTOR_BETA_MIN_TRADES
                and sector_wr >= SECTOR_BETA_WIN_RATE):
            return 'SECTOR_BETA'

        # 大盤紅利：在 BULL_TREND 中進場且報酬平庸（<1R）
        regime = trade.get('regime_at_entry', '')
        if regime == 'BULL_TREND' and r_multiple < 1.0:
            return 'BETA'

        # 其餘獲利：判為真正 Alpha
        return 'ALPHA'

    # =========================================================================
    # 主要分析函式
    # =========================================================================

    async def analyze_closed_positions(
        self,
        days: int = 30,
        trade_date: Optional[date] = None,
    ) -> dict:
        """分析近 N 日已平倉交易（含新增的產業 Beta 與計畫外懲罰統計）"""
        if trade_date is None:
            trade_date = date.today()
        start = trade_date - timedelta(days=days)

        rows = await self.hub.fetch("""
            SELECT
                p.ticker,
                p.strategy_tag,
                p.signal_source,
                p.entry_date,
                p.exit_date,
                p.entry_price,
                p.exit_price,
                p.r_multiple_current  AS r_multiple,
                p.realized_pnl,
                p.mrs_at_entry,
                p.regime_at_entry,
                p.exit_reason,
                -- 計畫外交易統計（JOIN decision_log）
                COUNT(dl.id) FILTER (WHERE dl.is_planned = FALSE)   AS unplanned_count,
                COUNT(dl.id)                                         AS decision_count
            FROM positions p
            LEFT JOIN decision_log dl
                ON dl.position_id = p.id
            WHERE p.is_open = FALSE
              AND p.exit_date >= $1
              AND p.exit_date <= $2
            GROUP BY
                p.ticker, p.strategy_tag, p.signal_source,
                p.entry_date, p.exit_date, p.entry_price, p.exit_price,
                p.r_multiple_current, p.realized_pnl,
                p.mrs_at_entry, p.regime_at_entry, p.exit_reason
            ORDER BY p.exit_date DESC
        """, start, trade_date)

        if not rows:
            return {
                'period':       f"{start} ~ {trade_date}",
                'total_trades': 0,
                'message':      '此期間無已平倉交易',
            }

        # ── 導入 selection_engine 的 TICKER_SECTOR_MAP ─────────────────
        try:
            from modules.selection_engine import get_sector
        except ImportError:
            def get_sector(t): return 'OTHER'

        trades = []
        for r in rows:
            t = dict(r)
            t['sector'] = get_sector(str(t.get('ticker', '')))
            trades.append(t)

        total  = len(trades)
        wins   = [t for t in trades if (t['r_multiple'] or 0) > 0]
        losses = [t for t in trades if (t['r_multiple'] or 0) <= 0]

        win_rate   = len(wins) / total
        avg_r      = sum(t['r_multiple'] or 0 for t in trades) / total
        avg_win_r  = sum(t['r_multiple'] or 0 for t in wins) / max(len(wins), 1)
        avg_loss_r = sum(t['r_multiple'] or 0 for t in losses) / max(len(losses), 1)
        total_pnl  = sum(float(t['realized_pnl'] or 0) for t in trades)

        # ── 依市場環境分組 ───────────────────────────────────────────────
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
            regime_breakdown[r]['avg_r']    = round(regime_breakdown[r]['avg_r'] / n, 3)

        # ── 出場原因分析 ──────────────────────────────────────────────────
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

        # ── [新增] 產業分組統計 ───────────────────────────────────────────
        sector_breakdown: dict[str, dict] = {}
        for t in trades:
            sec = t['sector']
            if sec not in sector_breakdown:
                sector_breakdown[sec] = {'count': 0, 'wins': 0, 'avg_r': 0.0}
            sector_breakdown[sec]['count'] += 1
            if (t['r_multiple'] or 0) > 0:
                sector_breakdown[sec]['wins'] += 1
            sector_breakdown[sec]['avg_r'] += (t['r_multiple'] or 0)
        sector_stats_for_alpha: dict[str, dict] = {}
        for sec, data in sector_breakdown.items():
            n = data['count']
            data['win_rate'] = round(data['wins'] / n, 3)
            data['avg_r']    = round(data['avg_r'] / n, 3)
            sector_stats_for_alpha[sec] = {'win_rate': data['win_rate'], 'count': n}

        # ── [新增] Alpha 來源分類（含 SECTOR_BETA）──────────────────────
        alpha_source_breakdown: dict[str, dict] = {
            'ALPHA':       {'count': 0, 'avg_r': 0.0},
            'BETA':        {'count': 0, 'avg_r': 0.0},
            'SECTOR_BETA': {'count': 0, 'avg_r': 0.0},
            'MIXED':       {'count': 0, 'avg_r': 0.0},
        }
        for t in trades:
            src = self._classify_alpha_source(t, sector_stats_for_alpha)
            t['alpha_source_classified'] = src
            alpha_source_breakdown[src]['count'] += 1
            alpha_source_breakdown[src]['avg_r'] += (t['r_multiple'] or 0)
        for src in alpha_source_breakdown:
            n = alpha_source_breakdown[src]['count']
            if n > 0:
                alpha_source_breakdown[src]['avg_r'] = round(
                    alpha_source_breakdown[src]['avg_r'] / n, 3
                )

        # ── [新增] 計畫外交易統計 ─────────────────────────────────────────
        total_unplanned = sum(int(t.get('unplanned_count') or 0) for t in trades)
        total_decisions = sum(int(t.get('decision_count') or 0) for t in trades)
        unplanned_rate  = round(total_unplanned / max(total_decisions, 1), 3)

        # 找出計畫外交易最多的個股
        unplanned_by_ticker: dict[str, int] = {}
        for t in trades:
            cnt = int(t.get('unplanned_count') or 0)
            if cnt > 0:
                tk = str(t['ticker']).strip()
                unplanned_by_ticker[tk] = unplanned_by_ticker.get(tk, 0) + cnt
        worst_unplanned = sorted(
            unplanned_by_ticker.items(), key=lambda x: x[1], reverse=True
        )[:5]

        unplanned_summary = {
            'total_unplanned_decisions': total_unplanned,
            'unplanned_rate':            unplanned_rate,
            'worst_tickers':             worst_unplanned,
            'penalty_triggered':         [
                tk for tk, cnt in worst_unplanned
                if cnt >= UNPLANNED_PENALTY_THRESHOLD
            ],
        }

        # ── 建議（含新規則）──────────────────────────────────────────────
        suggestions = []
        if win_rate < 0.40:
            suggestions.append(f"勝率偏低（{win_rate*100:.0f}%），考慮提高選股門檻")
        if avg_r < 0.5:
            suggestions.append(f"平均R偏低（{avg_r:.2f}R），檢視出場時機")
        if abs(avg_loss_r) > avg_win_r:
            suggestions.append("虧損幅度大於獲利幅度，考慮縮緊停損")

        # 新增：SECTOR_BETA 比例過高警告
        sb_count = alpha_source_breakdown['SECTOR_BETA']['count']
        alpha_count = alpha_source_breakdown['ALPHA']['count']
        if total > 5 and sb_count > alpha_count:
            suggestions.append(
                f"SECTOR_BETA（{sb_count}筆）> ALPHA（{alpha_count}筆），"
                "獲利主要來自族群紅利，選股能力尚待驗證"
            )

        # 新增：計畫外交易警告
        if unplanned_rate > 0.20:
            suggestions.append(
                f"計畫外交易比例偏高（{unplanned_rate*100:.0f}%），"
                f"情緒性操作標的已觸發懲罰：{unplanned_summary['penalty_triggered']}"
            )

        result = {
            'period':                 f"{start} ~ {trade_date}",
            'total_trades':           total,
            'win_rate':               round(win_rate, 3),
            'avg_r':                  round(avg_r, 3),
            'avg_win_r':              round(avg_win_r, 3),
            'avg_loss_r':             round(avg_loss_r, 3),
            'total_pnl':              round(total_pnl, 2),
            'regime_breakdown':       regime_breakdown,
            'exit_breakdown':         exit_breakdown,
            'sector_breakdown':       sector_breakdown,        # ← 新增
            'alpha_source_breakdown': alpha_source_breakdown,  # ← 新增（含 SECTOR_BETA）
            'unplanned_summary':      unplanned_summary,       # ← 新增
            'suggestions':            suggestions,
        }

        logger.info(
            "歸因分析完成：%d 筆 勝率=%.0f%% 平均R=%.2f "
            "ALPHA=%d SECTOR_BETA=%d 計畫外率=%.0f%%",
            total, win_rate * 100, avg_r,
            alpha_source_breakdown['ALPHA']['count'],
            alpha_source_breakdown['SECTOR_BETA']['count'],
            unplanned_rate * 100,
        )
        return result
