# modules/black_swan.py
# ═══════════════════════════════════════════════════════════════════════════════
# 變更紀錄（v3）：
# - [流動性偵測] 新增兩項流動性預警指標
# - [實際減倉執行] execute_action() 連動 PositionManager
# - [新增情境] LIQUIDITY_VOLUME / LIQUIDITY_BASIS
# ───────────────────────────────────────────────────────────────────────────────
# Bug 修正（v3.1）：
# - [BUG 1] CRASH_20PCT 定義改為 market_drop: 0.05（與 description「>5%」一致），
#           觸發邏輯補上 market_drop 雙重確認（VIX > 40 且跌幅 > 5%）
# ═══════════════════════════════════════════════════════════════════════════════

import logging
from datetime import date, timedelta
from typing import Optional
from datahub.data_hub import DataHub
from datahub.api_telegram import TelegramBot
from datahub import redis_keys as rk

logger = logging.getLogger(__name__)

# ── 黑天鵝情境定義（v3 + BUG 1 修正）──────────────────────────────────────────
BLACK_SWAN_SCENARIOS = [
    {
        'id':          'CRASH_20PCT',
        'name':        '市場閃崩',
        # [BUG 1 修正] market_drop 從 0.20 改為 0.05，與 description「>5%」一致
        'trigger':     {'vix_spike': 40, 'market_drop': 0.05},
        'action':      'REDUCE_EXPOSURE_50PCT',
        # [BUG 1 修正] description 更新為 ">5%"，與 market_drop 值一致
        'description': 'VIX 突破 40 且大盤單日跌幅 > 5%',
    },
    {
        'id':          'RATE_SHOCK',
        'name':        '利率衝擊',
        'trigger':     {'fed_rate_change': 0.75},
        'action':      'HALT_NEW_ENTRIES',
        'description': 'Fed 單次升息超過 75bp',
    },
    {
        'id':          'GEOPOLITICAL',
        'name':        '地緣政治危機',
        'trigger':     {'vix_level': 35, 'duration_days': 3},
        'action':      'REDUCE_EXPOSURE_30PCT',
        'description': 'VIX 連續 3 日高於 35',
    },
    {
        'id':          'LIQUIDITY_CRISIS',
        'name':        '流動性危機（信用利差）',
        'trigger':     {'spread_widen': 200},
        'action':      'EMERGENCY_EXIT',
        'description': '信用利差擴大超過 200bp',
    },
    {
        'id':          'LIQUIDITY_BASIS',
        'name':        '基差背離恐慌',
        'trigger':     {'basis_threshold': -0.012, 'basis_duration_min': 30},
        'action':      'REDUCE_EXPOSURE_30PCT',
        'description': '台指期逆價差 > -1.2% 且持續 30 分鐘（大戶系統性避險）',
    },
    {
        'id':          'LIQUIDITY_VOLUME',
        'name':        '成交量枯竭',
        'trigger':     {'volume_ratio': 0.40},
        'action':      'REDUCE_EXPOSURE_30PCT',
        'description': '5日均量 < 20日均量 40%（買盤嚴重萎縮）',
    },
]

# ── 緊急減倉目標曝險比例 ──────────────────────────────────────────────────────
EMERGENCY_EXPOSURE_TARGETS = {
    'REDUCE_EXPOSURE_50PCT': 0.50,
    'REDUCE_EXPOSURE_30PCT': 0.30,
    'EMERGENCY_EXIT':        0.00,
    'HALT_NEW_ENTRIES':      None,
}

BASIS_PANIC_START_KEY = "blackswan:basis_panic_start"


class BlackSwan:
    """模組⑨ 黑天鵝情境腳本：異常市場自動應對（v3 + BUG 1 修正）"""

    def __init__(self, hub: DataHub, telegram: TelegramBot):
        self.hub      = hub
        self.telegram = telegram

    # =========================================================================
    # [v3] 流動性偵測輔助函式
    # =========================================================================

    async def _check_basis_duration(
        self,
        current_basis: Optional[float],
        basis_threshold: float = -0.012,
        required_minutes: int = 30,
    ) -> tuple[bool, int]:
        if current_basis is None:
            return False, 0

        import datetime as _dt

        if current_basis < basis_threshold:
            start_raw = await self.hub.cache.get(BASIS_PANIC_START_KEY)
            if start_raw is None:
                now_str = _dt.datetime.utcnow().isoformat()
                await self.hub.cache.set(BASIS_PANIC_START_KEY, now_str, ttl=7200)
                return False, 0

            try:
                start_dt  = _dt.datetime.fromisoformat(start_raw)
                elapsed   = (_dt.datetime.utcnow() - start_dt).total_seconds() / 60
                triggered = elapsed >= required_minutes
                return triggered, int(elapsed)
            except Exception:
                return False, 0
        else:
            await self.hub.cache.delete(BASIS_PANIC_START_KEY)
            return False, 0

    async def _check_volume_ratio(
        self,
        trade_date: date,
        volume_ratio_threshold: float = 0.40,
    ) -> tuple[bool, float]:
        rows = await self.hub.fetch("""
            SELECT trade_date, market_volume FROM market_regime
            WHERE trade_date <= $1 AND market_volume IS NOT NULL
            ORDER BY trade_date DESC
            LIMIT 20
        """, trade_date)

        if len(rows) >= 20:
            volumes = [float(r['market_volume']) for r in rows]
            ma5   = sum(volumes[:5])  / 5
            ma20  = sum(volumes[:20]) / 20
            ratio = ma5 / ma20 if ma20 > 0 else 1.0
            triggered = ratio < volume_ratio_threshold
            if triggered:
                logger.warning("成交量萎縮警報：5MA/20MA=%.2f < 門檻 %.2f", ratio, volume_ratio_threshold)
            return triggered, round(ratio, 3)

        chip_rows = await self.hub.fetch("""
            SELECT trade_date, SUM(volume_ratio_5d20d) / COUNT(*) AS avg_ratio
            FROM chip_monitor
            WHERE trade_date <= $1
            GROUP BY trade_date
            ORDER BY trade_date DESC
            LIMIT 5
        """, trade_date)

        if chip_rows:
            avg_ratio = sum(float(r['avg_ratio'] or 1.0) for r in chip_rows) / len(chip_rows)
            triggered = avg_ratio < volume_ratio_threshold
            return triggered, round(avg_ratio, 3)

        logger.debug("無足夠成交量資料，跳過流動性成交量檢查")
        return False, 1.0

    async def _get_market_daily_return(self, trade_date: date) -> Optional[float]:
        """
        [BUG 1 修正] 取得大盤當日跌幅，供 CRASH_20PCT 雙重確認使用。

        從 market_regime 取最近兩日加權指數，計算日報酬。
        無法取得時回傳 None（保守原則：不觸發）。
        """
        rows = await self.hub.fetch("""
            SELECT trade_date, tsec_index_price FROM market_regime
            WHERE trade_date <= $1 AND tsec_index_price IS NOT NULL
            ORDER BY trade_date DESC
            LIMIT 2
        """, trade_date)

        if len(rows) < 2:
            logger.debug("market_regime 資料不足，無法計算大盤日報酬")
            return None

        today_price     = float(rows[0]['tsec_index_price'])
        yesterday_price = float(rows[1]['tsec_index_price'])

        if yesterday_price <= 0:
            return None

        daily_return = (today_price - yesterday_price) / yesterday_price
        logger.debug("大盤日報酬：%.4f (%.2f%%)", daily_return, daily_return * 100)
        return daily_return

    # =========================================================================
    # check_triggers（v3 + BUG 1 修正）
    # =========================================================================

    async def check_triggers(
        self,
        vix: Optional[float] = None,
        trade_date: Optional[date] = None,
        current_basis: Optional[float] = None,
    ) -> list[dict]:
        if trade_date is None:
            trade_date = date.today()

        triggered = []

        regime_row = await self.hub.fetchrow("""
            SELECT vix_level, mrs_score, regime,
                   tx_futures_price, tsec_index_price
            FROM market_regime
            WHERE trade_date <= $1
            ORDER BY trade_date DESC LIMIT 1
        """, trade_date)

        current_vix = vix or (
            float(regime_row['vix_level']) if regime_row and regime_row['vix_level'] else 20.0
        )

        if current_basis is None and regime_row:
            tx   = regime_row.get('tx_futures_price')
            tsec = regime_row.get('tsec_index_price')
            if tx and tsec and float(tsec) > 0:
                current_basis = (float(tx) - float(tsec)) / float(tsec)

        for scenario in BLACK_SWAN_SCENARIOS:
            trigger        = scenario['trigger']
            is_triggered   = False
            trigger_detail = {}

            # ── VIX spike（帶 market_drop 雙重確認）────────────────────────
            if 'vix_spike' in trigger and current_vix >= trigger['vix_spike']:
                # [BUG 1 修正] 若 trigger 有 market_drop，必須同時確認跌幅
                if 'market_drop' in trigger:
                    market_drop = await self._get_market_daily_return(trade_date)
                    if market_drop is not None and abs(market_drop) >= trigger['market_drop']:
                        is_triggered   = True
                        trigger_detail = {
                            'current_vix':  current_vix,
                            'market_drop':  round(market_drop, 4),
                        }
                    else:
                        # VIX 超標但跌幅不足，不觸發
                        logger.info(
                            "CRASH_20PCT：VIX=%.1f 超標，但跌幅=%s 未達門檻 %.1f%%，不觸發",
                            current_vix,
                            f"{market_drop*100:.2f}%" if market_drop is not None else "N/A",
                            trigger['market_drop'] * 100,
                        )
                else:
                    is_triggered   = True
                    trigger_detail = {'current_vix': current_vix}

            elif 'vix_level' in trigger and current_vix >= trigger['vix_level']:
                duration = trigger.get('duration_days', 1)
                if duration <= 1:
                    is_triggered   = True
                    trigger_detail = {'current_vix': current_vix}
                else:
                    vix_rows = await self.hub.fetch("""
                        SELECT vix_level FROM market_regime
                        WHERE trade_date <= $1
                        ORDER BY trade_date DESC LIMIT $2
                    """, trade_date, duration)

                    if len(vix_rows) >= duration:
                        all_high = all(
                            float(r['vix_level'] or 0) >= trigger['vix_level']
                            for r in vix_rows
                        )
                        if all_high:
                            is_triggered   = True
                            trigger_detail = {
                                'current_vix':       current_vix,
                                'duration_confirmed': duration,
                            }

            elif 'basis_threshold' in trigger:
                required_min = trigger.get('basis_duration_min', 30)
                is_trig, duration_min = await self._check_basis_duration(
                    current_basis,
                    basis_threshold=trigger['basis_threshold'],
                    required_minutes=required_min,
                )
                if is_trig:
                    is_triggered   = True
                    trigger_detail = {
                        'current_basis': current_basis,
                        'duration_min':  duration_min,
                    }

            elif 'volume_ratio' in trigger:
                is_trig, ratio = await self._check_volume_ratio(
                    trade_date, trigger['volume_ratio']
                )
                if is_trig:
                    is_triggered   = True
                    trigger_detail = {'volume_ratio_5_20': ratio}

            if is_triggered:
                triggered.append({
                    'scenario_id':   scenario['id'],
                    'scenario_name': scenario['name'],
                    'action':        scenario['action'],
                    'description':   scenario['description'],
                    'current_vix':   current_vix,
                    **trigger_detail,
                })
                logger.warning("黑天鵝情境觸發：%s", scenario['name'])

        return triggered

    # =========================================================================
    # 緊急減倉執行
    # =========================================================================

    async def _execute_emergency_reduce(
        self,
        target_exposure: float,
        scenario_name: str,
        trade_date: date,
    ) -> dict:
        from modules.position_manager import PositionManager

        pm        = PositionManager(self.hub)
        positions = await pm.get_open_positions()
        nav_data  = await pm.calc_nav(positions)
        nav       = nav_data['nav']

        if nav <= 0:
            return {'reduced_count': 0, 'total_reduced_value': 0, 'error': 'NAV=0'}

        current_market_value = nav_data['stock_market_value']
        target_market_value  = nav * target_exposure
        need_to_reduce       = current_market_value - target_market_value

        if need_to_reduce <= 0:
            return {
                'reduced_count':        0,
                'total_reduced_value':  0,
                'error':                None,
                'detail':               [],
                'already_below_target': True,
            }

        logger.warning(
            "緊急減倉執行：目標曝險=%.0f%% NAV=%.0f 需減市值=%.0f",
            target_exposure * 100, nav, need_to_reduce,
        )

        sorted_positions = sorted(
            positions,
            key=lambda p: float(p.get('unrealized_pnl') or 0),
        )

        reduced_count       = 0
        total_reduced_value = 0.0
        detail              = []

        for pos in sorted_positions:
            if total_reduced_value >= need_to_reduce:
                break

            ticker        = pos['ticker']
            shares        = int(pos.get('shares', 0))
            entry_price   = float(pos.get('entry_price', 0))
            pos_value     = shares * entry_price
            pos_id        = pos['id']

            remaining_need  = need_to_reduce - total_reduced_value
            shares_to_sell  = min(shares, max(1, int(remaining_need / max(entry_price, 1))))
            sold_value      = shares_to_sell * entry_price
            avg_cost        = entry_price

            try:
                result = await pm.partial_exit(
                    pos_id, shares_to_sell, entry_price, avg_cost, sold_value
                )
                total_reduced_value += sold_value
                reduced_count       += 1
                detail.append({
                    'ticker':      ticker,
                    'shares_sold': shares_to_sell,
                    'value':       sold_value,
                })

                await self.hub.execute("""
                    INSERT INTO decision_log (
                        trade_date, ticker, decision_type,
                        signal_source, position_id, notes
                    ) VALUES ($1, $2, 'EXIT_EXECUTED', 'BLACK_SWAN', $3, $4)
                """,
                    trade_date,
                    ticker,
                    pos_id,
                    f"黑天鵝緊急減倉：{scenario_name} 目標曝險={target_exposure*100:.0f}%",
                )
            except Exception as e:
                logger.error("緊急減倉失敗 %s：%s", ticker, e)
                detail.append({'ticker': ticker, 'error': str(e)})

        logger.warning("緊急減倉完成：減倉 %d 檔，合計減少市值 %.0f", reduced_count, total_reduced_value)
        return {
            'reduced_count':       reduced_count,
            'total_reduced_value': round(total_reduced_value, 0),
            'detail':              detail,
            'error':               None,
        }

    # =========================================================================
    # execute_action
    # =========================================================================

    async def execute_action(
        self,
        scenario: dict,
        trade_date: Optional[date] = None,
    ) -> dict:
        if trade_date is None:
            trade_date = date.today()

        action        = scenario['action']
        scenario_name = scenario['scenario_name']
        target_exposure = EMERGENCY_EXPOSURE_TARGETS.get(action)

        if action == 'HALT_NEW_ENTRIES':
            msg = (f"⚫ 黑天鵝警報：{scenario_name}\n"
                   f"停止新進場，維持現有部位\n"
                   f"原因：{scenario.get('description', '')}")
        elif action == 'REDUCE_EXPOSURE_50PCT':
            msg = (f"🔴 黑天鵝警報：{scenario_name}\n"
                   f"緊急減倉至 50% 曝險\n"
                   f"原因：{scenario.get('description', '')}")
        elif action == 'REDUCE_EXPOSURE_30PCT':
            msg = (f"🔴 黑天鵝警報：{scenario_name}\n"
                   f"緊急減倉至 30% 曝險\n"
                   f"原因：{scenario.get('description', '')}")
        elif action == 'EMERGENCY_EXIT':
            msg = (f"🚨 緊急黑天鵝：{scenario_name}\n"
                   f"緊急出清所有部位！\n"
                   f"原因：{scenario.get('description', '')}")
        else:
            msg = f"⚫ 黑天鵝情境：{scenario_name}"

        await self.telegram.send_alert("黑天鵝情境觸發", msg, "ERROR")
        logger.warning("黑天鵝動作執行：%s → %s", scenario_name, action)

        reduction_result = None
        if target_exposure is not None:
            try:
                reduction_result = await self._execute_emergency_reduce(
                    target_exposure=target_exposure,
                    scenario_name=scenario_name,
                    trade_date=trade_date,
                )
                if reduction_result.get('reduced_count', 0) > 0:
                    result_msg = (
                        f"✅ 緊急減倉執行完成\n"
                        f"減倉標的：{reduction_result['reduced_count']} 檔\n"
                        f"合計減少市值：{reduction_result['total_reduced_value']:,.0f} 元\n"
                        f"目標曝險：{target_exposure*100:.0f}%"
                    )
                    await self.telegram.send_alert("緊急減倉完成", result_msg, "WARNING")
            except Exception as e:
                logger.error("緊急減倉執行失敗：%s", e)
                reduction_result = {'error': str(e)}

        return {
            'message':          msg,
            'action':           action,
            'scenario_name':    scenario_name,
            'target_exposure':  target_exposure,
            'reduction_result': reduction_result,
        }

    # =========================================================================
    # run
    # =========================================================================

    async def run(
        self,
        trade_date: Optional[date] = None,
        current_basis: Optional[float] = None,
    ) -> dict:
        if trade_date is None:
            trade_date = date.today()

        triggered = await self.check_triggers(
            trade_date=trade_date,
            current_basis=current_basis,
        )

        actions = []
        for scenario in triggered:
            result = await self.execute_action(scenario, trade_date)
            actions.append(result)

        return {
            'trade_date':    str(trade_date),
            'triggered':     len(triggered),
            'scenarios':     triggered,
            'actions_taken': actions,
        }