# modules/attribution.py
# ═══════════════════════════════════════════════════════════════════════════════
# 變更紀錄（v3）：
# - [Alpha 拆解] R-Multiple 個股超額 Alpha 計算：
#     individual_alpha = 個股 R - 同產業同期平均 R
#     結果存入 wf_results.notes（JSON 格式）供 Walk-Forward 使用
# - [環境標籤回饋] 統計各 Regime 下的期望值（EV）：
#     若某 Regime 連續 N 期 EV < EV_THRESHOLD，自動提高 SelectionEngine 進場門檻
#     透過寫入 wf_results 表觸發 SelectionEngine._get_macro_weights() 讀取新門檻
# - [SECTOR_BETA] 保留 v2 邏輯，新增同產業平均 R 計算
# ═══════════════════════════════════════════════════════════════════════════════

import json
import logging
from datetime import date, timedelta
from typing import Optional
from datahub.data_hub import DataHub

logger = logging.getLogger(__name__)

# ── 計畫外交易懲罰設定 ────────────────────────────────────────────────────────
UNPLANNED_PENALTY_WINDOW_DAYS = 30
UNPLANNED_PENALTY_THRESHOLD   = 2
UNPLANNED_PENALTY_FACTOR       = 0.80

# ── SECTOR_BETA 判斷設定 ──────────────────────────────────────────────────────
SECTOR_BETA_MIN_TRADES  = 3
SECTOR_BETA_WIN_RATE    = 0.65

# ── [v3 新增] 環境標籤回饋設定 ────────────────────────────────────────────────
# 若某 Regime 連續 N 個分析週期的 EV 都低於門檻，自動提高進場門檻分數
REGIME_EV_FEEDBACK_WINDOW   = 3       # 連續幾期 EV 不足才觸發
REGIME_EV_THRESHOLD         = 0.20    # EV 低於此值視為該 Regime 表現不佳
REGIME_SCORE_THRESHOLD_BUMP = 5.0     # 進場門檻分數提高幾分（預設 60 → 65）
REGIME_SCORE_THRESHOLD_MAX  = 80.0    # 門檻上限，避免無限拉高
REGIME_SCORE_CACHE_KEY      = "regime:score_threshold_override"  # Redis key


class Attribution:
    """模組⑤ 交易歸因進化閉環：分析已平倉交易，找出改善方向

    變更紀錄（v3）：
    - [Alpha 拆解] 新增 calculate_alpha_decomposition()：
        individual_alpha = 個股 R - 同產業平均 R
        結果以 JSON 存入 wf_results.notes
    - [環境回饋] 新增 update_regime_score_threshold()：
        若某 Regime 連續 EV 不足，寫入 Redis 覆蓋進場門檻
    - analyze_closed_positions() 擴充：
        每筆交易新增 individual_alpha 欄位
        回傳結果新增 alpha_decomposition_summary、regime_ev_feedback
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
                    "計畫外交易懲罰：%s 近%d日 %d/%d 筆為計畫外 → ×%.2f",
                    ticker, UNPLANNED_PENALTY_WINDOW_DAYS,
                    unplanned, total, UNPLANNED_PENALTY_FACTOR,
                )
        return penalty_map

    # =========================================================================
    # Alpha 來源分類（含 SECTOR_BETA）
    # =========================================================================

    @staticmethod
    def _classify_alpha_source(
        trade: dict,
        sector_stats: dict,
    ) -> str:
        r_multiple = trade.get('r_multiple') or 0
        sector     = trade.get('sector', 'OTHER')

        if r_multiple <= 0:
            return 'MIXED'

        sector_info = sector_stats.get(sector, {})
        sector_wr   = sector_info.get('win_rate', 0)
        sector_cnt  = sector_info.get('count', 0)

        if (sector != 'OTHER'
                and sector_cnt >= SECTOR_BETA_MIN_TRADES
                and sector_wr >= SECTOR_BETA_WIN_RATE):
            return 'SECTOR_BETA'

        regime = trade.get('regime_at_entry', '')
        if regime == 'BULL_TREND' and r_multiple < 1.0:
            return 'BETA'

        return 'ALPHA'

    # =========================================================================
    # [v3 新增] Alpha 拆解：個股 R - 同產業平均 R
    # =========================================================================

    @staticmethod
    def _calc_individual_alpha(
        trade: dict,
        sector_stats: dict,
    ) -> float:
        """
        計算個股超額 Alpha：

            individual_alpha = 個股 R - 同產業同期平均 R

        正值 → 個股跑贏同族群（真正的選股能力）
        負值 → 個股落後同族群（可能只是跑慢了，非選股問題）
        """
        r_multiple  = float(trade.get('r_multiple') or 0)
        sector      = trade.get('sector', 'OTHER')
        sector_info = sector_stats.get(sector, {})
        sector_avg_r = sector_info.get('avg_r', 0.0)

        return round(r_multiple - sector_avg_r, 3)

    # =========================================================================
    # [v3 新增] 環境回饋：統計各 Regime 下 EV，必要時提高進場門檻
    # =========================================================================

    async def update_regime_score_threshold(
        self,
        regime_breakdown: dict,
        trade_date: Optional[date] = None,
    ) -> dict:
        """
        環境標籤回饋機制：

        若某 Regime 的期望值（avg_r）持續低於 REGIME_EV_THRESHOLD，
        寫入 Redis 提高該 Regime 的進場門檻分數（SelectionEngine 讀取）。

        回傳：
            {regime: {'triggered': bool, 'new_threshold': float, 'reason': str}}
        """
        if trade_date is None:
            trade_date = date.today()

        feedback_result = {}

        for regime, stats in regime_breakdown.items():
            if stats.get('count', 0) < 3:
                feedback_result[regime] = {
                    'triggered': False,
                    'reason': f"樣本數不足（{stats.get('count', 0)} < 3）",
                }
                continue

            avg_r = stats.get('avg_r', 0.0)

            if avg_r >= REGIME_EV_THRESHOLD:
                feedback_result[regime] = {
                    'triggered': False,
                    'avg_r': avg_r,
                    'reason': f"EV={avg_r:.2f} ≥ 門檻 {REGIME_EV_THRESHOLD}，無需調整",
                }
                continue

            # EV 不足：查詢歷史是否已連續 N 期觸發
            history_rows = await self.hub.fetch("""
                SELECT notes FROM wf_results
                WHERE notes LIKE $1
                ORDER BY run_at DESC
                LIMIT $2
            """,
                f'%"regime_ev_feedback"%',
                REGIME_EV_FEEDBACK_WINDOW,
            )

            consecutive_low = 0
            for row in history_rows:
                try:
                    notes_dict = json.loads(row['notes'] or '{}')
                    regime_fb  = notes_dict.get('regime_ev_feedback', {})
                    if regime_fb.get(regime, {}).get('avg_r', 999) < REGIME_EV_THRESHOLD:
                        consecutive_low += 1
                except (json.JSONDecodeError, TypeError):
                    pass

            if consecutive_low >= REGIME_EV_FEEDBACK_WINDOW - 1:
                # 連續不足，觸發門檻提高
                cache_key = f"{REGIME_SCORE_CACHE_KEY}:{regime}"
                current_threshold_data = await self.hub.cache.get(cache_key)
                current_threshold = (
                    float(current_threshold_data.get('threshold', 60.0))
                    if current_threshold_data else 60.0
                )
                new_threshold = min(
                    current_threshold + REGIME_SCORE_THRESHOLD_BUMP,
                    REGIME_SCORE_THRESHOLD_MAX,
                )

                await self.hub.cache.set(
                    cache_key,
                    {
                        'threshold':    new_threshold,
                        'regime':       regime,
                        'updated_at':   str(trade_date),
                        'avg_r':        avg_r,
                        'consecutive':  consecutive_low + 1,
                    },
                    ttl=rk_TTL_24H(),   # 24 小時有效
                )

                reason = (
                    f"Regime={regime} 連續{consecutive_low + 1}期 EV={avg_r:.2f} < "
                    f"{REGIME_EV_THRESHOLD}，進場門檻 {current_threshold:.0f} → "
                    f"{new_threshold:.0f}"
                )
                logger.warning("環境回饋觸發：%s", reason)

                feedback_result[regime] = {
                    'triggered':       True,
                    'avg_r':           avg_r,
                    'new_threshold':   new_threshold,
                    'consecutive_low': consecutive_low + 1,
                    'reason':          reason,
                }
            else:
                feedback_result[regime] = {
                    'triggered':       False,
                    'avg_r':           avg_r,
                    'consecutive_low': consecutive_low,
                    'reason': (
                        f"EV={avg_r:.2f} < 門檻但連續期數不足"
                        f"（{consecutive_low}/{REGIME_EV_FEEDBACK_WINDOW - 1}期）"
                    ),
                }

        return feedback_result

    async def get_regime_score_threshold(
        self,
        regime: str,
        default: float = 60.0,
    ) -> float:
        """
        取得指定 Regime 的進場門檻（考慮環境回饋覆蓋值）
        供 SelectionEngine._get_macro_weights() 或 scheduler.py 呼叫
        """
        cache_key = f"{REGIME_SCORE_CACHE_KEY}:{regime}"
        data = await self.hub.cache.get(cache_key)
        if data and isinstance(data, dict):
            return float(data.get('threshold', default))
        return default

    # =========================================================================
    # [v3 新增] 將 Alpha 分解結果存入 wf_results
    # =========================================================================

    async def save_alpha_decomposition(
        self,
        alpha_data: dict,
        trade_date: Optional[date] = None,
    ) -> None:
        """
        將本期 Alpha 分解結果存入 wf_results.notes（JSON 格式）

        alpha_data 格式：
        {
            'period':  '2026-01-01 ~ 2026-03-28',
            'per_trade_alpha': [
                {'ticker': '2330', 'r': 2.5, 'sector_avg_r': 1.2, 'alpha': 1.3,
                 'alpha_source': 'ALPHA', 'regime': 'BULL_TREND'}, ...
            ],
            'sector_alpha_summary': {
                'SEMI': {'avg_alpha': 0.8, 'count': 5},
                'NETWORK': {'avg_alpha': -0.2, 'count': 2},
            },
            'regime_ev_feedback': {
                'BULL_TREND': {'avg_r': 1.5, 'triggered': False},
                'CHOPPY': {'avg_r': 0.1, 'triggered': True},
            }
        }

        存儲策略：
        - 不新增 wf_results 主記錄（避免影響 Walk-Forward 邏輯）
        - 在最新一筆 is_active=TRUE 的 wf_results 中更新 notes
        - 若無 is_active 記錄，插入一筆 type='ATTRIBUTION_SNAPSHOT'
        """
        if trade_date is None:
            trade_date = date.today()

        notes_json = json.dumps(alpha_data, ensure_ascii=False, default=str)

        # 嘗試更新最新 WF 記錄的 notes
        updated = await self.hub.execute("""
            UPDATE wf_results
            SET notes = $1
            WHERE id = (
                SELECT id FROM wf_results
                ORDER BY run_at DESC
                LIMIT 1
            )
        """, notes_json)

        if updated == 'UPDATE 0':
            # 無現有記錄，插入 snapshot 記錄
            await self.hub.execute("""
                INSERT INTO wf_results (
                    data_from, data_to,
                    train_months, oos_months, step_months,
                    n_oos_windows, consistency_rate,
                    recommendation, is_active, notes
                ) VALUES ($1, $2, 0, 0, 0, 0, 0,
                    'ATTRIBUTION_SNAPSHOT', FALSE, $3)
            """, trade_date, trade_date, notes_json)

        logger.info("Alpha 分解結果已存入 wf_results.notes（%d 字元）", len(notes_json))

    # =========================================================================
    # 主要分析函式（v3 擴充）
    # =========================================================================

    async def analyze_closed_positions(
        self,
        days: int = 30,
        trade_date: Optional[date] = None,
    ) -> dict:
        """分析近 N 日已平倉交易（v3：新增 Alpha 拆解 + 環境回饋）"""
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
                COUNT(dl.id) FILTER (WHERE dl.is_planned = FALSE)  AS unplanned_count,
                COUNT(dl.id)                                        AS decision_count
            FROM positions p
            LEFT JOIN decision_log dl ON dl.position_id = p.id
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

        try:
            from modules.selection_engine import get_sector
        except ImportError:
            def get_sector(t): return 'OTHER'

        trades = [dict(r) for r in rows]
        for t in trades:
            t['sector'] = get_sector(str(t.get('ticker', '')).strip())

        total   = len(trades)
        wins    = [t for t in trades if (t.get('r_multiple') or 0) > 0]
        win_rate = len(wins) / total
        avg_r    = sum(float(t.get('r_multiple') or 0) for t in trades) / total
        avg_win_r  = sum(float(t['r_multiple']) for t in wins) / max(len(wins), 1)
        losses     = [t for t in trades if (t.get('r_multiple') or 0) <= 0]
        avg_loss_r = sum(float(t['r_multiple']) for t in losses) / max(len(losses), 1)
        total_pnl  = sum(float(t.get('realized_pnl') or 0) for t in trades)

        # ── 按 Regime 分組 ────────────────────────────────────────────────
        regime_breakdown: dict[str, dict] = {}
        for t in trades:
            reg = t.get('regime_at_entry') or 'UNKNOWN'
            if reg not in regime_breakdown:
                regime_breakdown[reg] = {'count': 0, 'wins': 0, 'avg_r': 0.0}
            regime_breakdown[reg]['count'] += 1
            if (t.get('r_multiple') or 0) > 0:
                regime_breakdown[reg]['wins'] += 1
            regime_breakdown[reg]['avg_r'] += float(t.get('r_multiple') or 0)
        for reg, data in regime_breakdown.items():
            n = data['count']
            data['win_rate'] = round(data['wins'] / n, 3)
            data['avg_r']    = round(data['avg_r'] / n, 3)

        # ── 按出場原因分組 ────────────────────────────────────────────────
        exit_breakdown: dict[str, int] = {}
        for t in trades:
            reason = t.get('exit_reason') or 'UNKNOWN'
            exit_breakdown[reason] = exit_breakdown.get(reason, 0) + 1

        # ── 按產業分組（含同產業平均 R）─────────────────────────────────
        sector_breakdown: dict[str, dict] = {}
        for t in trades:
            sec = t.get('sector', 'OTHER')
            if sec not in sector_breakdown:
                sector_breakdown[sec] = {'count': 0, 'wins': 0, 'avg_r': 0.0}
            sector_breakdown[sec]['count'] += 1
            if (t.get('r_multiple') or 0) > 0:
                sector_breakdown[sec]['wins'] += 1
            sector_breakdown[sec]['avg_r'] += float(t.get('r_multiple') or 0)
        sector_stats_for_alpha: dict[str, dict] = {}
        for sec, data in sector_breakdown.items():
            n = data['count']
            data['win_rate'] = round(data['wins'] / n, 3)
            data['avg_r']    = round(data['avg_r'] / n, 3)
            sector_stats_for_alpha[sec] = {
                'win_rate': data['win_rate'],
                'count':    n,
                'avg_r':    data['avg_r'],   # ← v3 新增
            }

        # ── Alpha 來源分類（含 SECTOR_BETA）─────────────────────────────
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
            alpha_source_breakdown[src]['avg_r'] += float(t.get('r_multiple') or 0)
        for src in alpha_source_breakdown:
            n = alpha_source_breakdown[src]['count']
            if n > 0:
                alpha_source_breakdown[src]['avg_r'] = round(
                    alpha_source_breakdown[src]['avg_r'] / n, 3
                )

        # ── [v3 新增] Alpha 拆解：個股 R - 同產業平均 R ─────────────────
        per_trade_alpha = []
        sector_alpha_accum: dict[str, list] = {}
        for t in trades:
            ind_alpha = self._calc_individual_alpha(t, sector_stats_for_alpha)
            t['individual_alpha'] = ind_alpha
            sec = t.get('sector', 'OTHER')
            sector_alpha_accum.setdefault(sec, []).append(ind_alpha)
            per_trade_alpha.append({
                'ticker':        str(t.get('ticker', '')).strip(),
                'r':             float(t.get('r_multiple') or 0),
                'sector_avg_r':  sector_stats_for_alpha.get(sec, {}).get('avg_r', 0.0),
                'individual_alpha': ind_alpha,
                'alpha_source':  t.get('alpha_source_classified', 'MIXED'),
                'regime':        t.get('regime_at_entry', 'UNKNOWN'),
            })

        # 產業 Alpha 匯總
        sector_alpha_summary: dict[str, dict] = {}
        for sec, alphas in sector_alpha_accum.items():
            sector_alpha_summary[sec] = {
                'avg_alpha': round(sum(alphas) / len(alphas), 3),
                'count':     len(alphas),
                'top_stocks': sorted(
                    [pa for pa in per_trade_alpha if pa.get('ticker') in
                     [str(t.get('ticker', '')).strip()
                      for t in trades if t.get('sector') == sec]],
                    key=lambda x: x['individual_alpha'], reverse=True
                )[:3],   # 前三名
            }

        alpha_decomposition = {
            'period':                str(start) + ' ~ ' + str(trade_date),
            'per_trade_alpha':       per_trade_alpha,
            'sector_alpha_summary':  sector_alpha_summary,
        }

        # ── [v3 新增] 環境標籤回饋 ───────────────────────────────────────
        regime_ev_feedback = await self.update_regime_score_threshold(
            regime_breakdown, trade_date
        )
        alpha_decomposition['regime_ev_feedback'] = regime_ev_feedback

        # ── 存入 wf_results ───────────────────────────────────────────────
        await self.save_alpha_decomposition(alpha_decomposition, trade_date)

        # ── 計畫外交易統計 ────────────────────────────────────────────────
        total_unplanned = sum(int(t.get('unplanned_count') or 0) for t in trades)
        total_decisions = sum(int(t.get('decision_count') or 0) for t in trades)
        unplanned_rate  = round(total_unplanned / max(total_decisions, 1), 3)

        unplanned_by_ticker: dict[str, int] = {}
        for t in trades:
            cnt = int(t.get('unplanned_count') or 0)
            if cnt > 0:
                tk = str(t.get('ticker', '')).strip()
                unplanned_by_ticker[tk] = unplanned_by_ticker.get(tk, 0) + cnt
        worst_unplanned = sorted(
            unplanned_by_ticker.items(), key=lambda x: x[1], reverse=True
        )[:5]

        # 取得已觸發懲罰的 ticker
        penalty_map = await self.get_unplanned_penalty_map(trade_date)
        unplanned_summary = {
            'total_unplanned_decisions': total_unplanned,
            'unplanned_rate':            unplanned_rate,
            'worst_tickers':             worst_unplanned,
            'penalty_triggered':         list(penalty_map.keys()),
        }

        # ── 建議 ──────────────────────────────────────────────────────────
        suggestions = []
        if win_rate < 0.40:
            suggestions.append(f"勝率偏低（{win_rate*100:.0f}%），考慮提高選股門檻")
        if avg_r < 0.5:
            suggestions.append(f"平均R偏低（{avg_r:.2f}R），檢視出場時機")
        if abs(avg_loss_r) > avg_win_r:
            suggestions.append("虧損幅度大於獲利幅度，考慮縮緊停損")

        sb_count    = alpha_source_breakdown['SECTOR_BETA']['count']
        alpha_count = alpha_source_breakdown['ALPHA']['count']
        if total > 5 and sb_count > alpha_count:
            suggestions.append(
                f"SECTOR_BETA（{sb_count}筆）> ALPHA（{alpha_count}筆），"
                "獲利主要來自族群紅利，真實選股能力待驗證"
            )

        if unplanned_rate > 0.20:
            suggestions.append(
                f"計畫外交易比例偏高（{unplanned_rate*100:.0f}%），"
                f"情緒性操作標的：{unplanned_summary['penalty_triggered']}"
            )

        # [v3] 新增環境回饋建議
        for regime, fb in regime_ev_feedback.items():
            if fb.get('triggered'):
                suggestions.append(
                    f"[環境回饋] {regime} 模式 EV 長期偏低，"
                    f"進場門檻已自動提高至 {fb.get('new_threshold', 65):.0f} 分"
                )

        # [v3] 產業 Alpha 排行提示
        best_sector  = max(sector_alpha_summary, key=lambda s: sector_alpha_summary[s]['avg_alpha'], default=None)
        worst_sector = min(sector_alpha_summary, key=lambda s: sector_alpha_summary[s]['avg_alpha'], default=None)
        if best_sector:
            suggestions.append(
                f"選股能力最強產業：{best_sector}（超額Alpha={sector_alpha_summary[best_sector]['avg_alpha']:.2f}R）"
            )
        if worst_sector and worst_sector != best_sector:
            alpha_val = sector_alpha_summary[worst_sector]['avg_alpha']
            if alpha_val < -0.3:
                suggestions.append(
                    f"選股能力最弱產業：{worst_sector}（超額Alpha={alpha_val:.2f}R），"
                    "考慮降低該族群進場機率"
                )

        result = {
            'period':                     f"{start} ~ {trade_date}",
            'total_trades':               total,
            'win_rate':                   round(win_rate, 3),
            'avg_r':                      round(avg_r, 3),
            'avg_win_r':                  round(avg_win_r, 3),
            'avg_loss_r':                 round(avg_loss_r, 3),
            'total_pnl':                  round(total_pnl, 2),
            'regime_breakdown':           regime_breakdown,
            'exit_breakdown':             exit_breakdown,
            'sector_breakdown':           sector_breakdown,
            'alpha_source_breakdown':     alpha_source_breakdown,
            'unplanned_summary':          unplanned_summary,
            'alpha_decomposition':        alpha_decomposition,      # ← v3 新增
            'regime_ev_feedback':         regime_ev_feedback,       # ← v3 新增
            'sector_alpha_summary':       sector_alpha_summary,     # ← v3 新增
            'suggestions':                suggestions,
        }

        logger.info(
            "歸因分析（v3）完成：%d 筆 勝率=%.0f%% 平均R=%.2f "
            "ALPHA=%d SECTOR_BETA=%d 環境回饋=%d 項觸發",
            total, win_rate * 100, avg_r,
            alpha_source_breakdown['ALPHA']['count'],
            alpha_source_breakdown['SECTOR_BETA']['count'],
            sum(1 for fb in regime_ev_feedback.values() if fb.get('triggered')),
        )
        return result


def rk_TTL_24H() -> int:
    """Redis TTL 常數（避免直接 import redis_keys 造成循環依賴）"""
    return 86_400