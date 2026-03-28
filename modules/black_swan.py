# modules/black_swan.py
# ═══════════════════════════════════════════════════════════════════════════════
# 變更紀錄（v3）：
# - [流動性偵測] 新增兩項流動性預警指標：
#     基差背離：台指期逆價差 > -1.2% 且持續 30 分鐘 → 系統性恐慌
#     成交量衰退：5日均量 < 20日均量 40% → 買盤枯竭警示
# - [實際減倉執行] execute_action() 不再只發 Telegram 通知，
#     現在連動 PositionManager.partial_exit() 真正執行緊急減倉至 30%：
#     REDUCE_EXPOSURE_50PCT → 減至 NAV 的 50%（賣出超出部分）
#     REDUCE_EXPOSURE_30PCT → 減至 NAV 的 30%
#     EMERGENCY_EXIT        → 強制全數平倉所有部位
# - [新增情境] LIQUIDITY_VOLUME 流動性（成交量枯竭）→ 減倉 30%
# ═══════════════════════════════════════════════════════════════════════════════

import logging
from datetime import date, timedelta
from typing import Optional
from datahub.data_hub import DataHub
from datahub.api_telegram import TelegramBot
from datahub import redis_keys as rk

logger = logging.getLogger(__name__)

# ── 黑天鵝情境定義（v3 擴充）────────────────────────────────────────────────
BLACK_SWAN_SCENARIOS = [
    {
        'id':          'CRASH_20PCT',
        'name':        '市場閃崩 -20%',
        'trigger':     {'vix_spike': 40, 'market_drop': 0.20},
        'action':      'REDUCE_EXPOSURE_50PCT',
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
    # ── [v3 新增] 流動性偵測情境 ──────────────────────────────────────────
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
        'trigger':     {'volume_ratio': 0.40},   # 5MA / 20MA < 40%
        'action':      'REDUCE_EXPOSURE_30PCT',
        'description': '5日均量 < 20日均量 40%（買盤嚴重萎縮）',
    },
]

# ── [v3] 緊急減倉目標曝險比例 ─────────────────────────────────────────────────
EMERGENCY_EXPOSURE_TARGETS = {
    'REDUCE_EXPOSURE_50PCT': 0.50,   # 減至 NAV 的 50%
    'REDUCE_EXPOSURE_30PCT': 0.30,   # 減至 NAV 的 30%
    'EMERGENCY_EXIT':        0.00,   # 全數清倉
    'HALT_NEW_ENTRIES':      None,   # 不減倉，只停止新進場
}

# Redis key：記錄基差持續低點的開始時間
BASIS_PANIC_START_KEY = "blackswan:basis_panic_start"


class BlackSwan:
    """模組⑨ 黑天鵝情境腳本：異常市場自動應對（v3：流動性偵測 + 實際減倉執行）

    變更紀錄（v3）：
    - check_triggers() 新增流動性偵測（基差持續偏離 + 成交量萎縮）
    - execute_action() 連動 PositionManager 真正執行減倉，不再只是 Telegram 通知
    - 新增 _check_basis_duration()：追蹤基差低於門檻的持續時間
    - 新增 _check_volume_ratio()：計算 5MA/20MA 成交量比率
    - 新增 _execute_emergency_reduce()：按目標曝險比例執行減倉
    """

    def __init__(self, hub: DataHub, telegram: TelegramBot):
        self.hub      = hub
        self.telegram = telegram

    # =========================================================================
    # [v3 新增] 流動性偵測輔助函式
    # =========================================================================

    async def _check_basis_duration(
        self,
        current_basis: Optional[float],
        basis_threshold: float = -0.012,
        required_minutes: int = 30,
    ) -> tuple[bool, int]:
        """
        檢查台指期基差是否連續偏離超過指定時間。

        流程：
        1. 若 basis < basis_threshold，記錄開始時間至 Redis
        2. 計算已持續分鐘數
        3. 若持續 >= required_minutes → 觸發

        回傳：(is_triggered, duration_minutes)
        """
        from datetime import datetime

        if current_basis is None:
            return False, 0

        if current_basis >= basis_threshold:
            # 基差已恢復，清除計時
            await self.hub.cache.delete(BASIS_PANIC_START_KEY)
            return False, 0

        # 基差仍低於門檻
        cached = await self.hub.cache.get(BASIS_PANIC_START_KEY)
        now    = datetime.utcnow()

        if cached is None:
            # 第一次偵測到，記錄開始時間
            await self.hub.cache.set(
                BASIS_PANIC_START_KEY,
                {'start': now.isoformat(), 'basis': current_basis},
                ttl=7200,   # 2 小時後自動清除
            )
            return False, 0

        try:
            start_time = datetime.fromisoformat(cached['start'])
            duration   = int((now - start_time).total_seconds() / 60)
        except (KeyError, ValueError):
            duration = 0

        triggered = duration >= required_minutes
        if triggered:
            logger.warning(
                "基差背離警報：basis=%.2f%% 持續 %d 分鐘（門檻：%d 分鐘）",
                current_basis * 100, duration, required_minutes
            )
        return triggered, duration

    async def _check_volume_ratio(
        self,
        trade_date: Optional[date] = None,
        volume_ratio_threshold: float = 0.40,
    ) -> tuple[bool, float]:
        """
        計算大盤成交量 5MA / 20MA 比率。

        資料來源：market_regime 表的 market_volume 欄位（若存在）
        若無資料，從 chip_monitor 聚合各股成交量作為代理指標。

        回傳：(is_triggered, ratio)
        """
        if trade_date is None:
            trade_date = date.today()

        # ① 嘗試從 market_regime 取得大盤成交量
        rows = await self.hub.fetch("""
            SELECT trade_date, market_volume
            FROM market_regime
            WHERE trade_date <= $1
              AND market_volume IS NOT NULL
            ORDER BY trade_date DESC
            LIMIT 20
        """, trade_date)

        if len(rows) >= 10:
            volumes = [float(r['market_volume']) for r in rows]
            ma5  = sum(volumes[:5])  / 5
            ma20 = sum(volumes[:20]) / 20
            ratio = ma5 / ma20 if ma20 > 0 else 1.0
            triggered = ratio < volume_ratio_threshold

            if triggered:
                logger.warning(
                    "成交量萎縮警報：5MA/20MA=%.2f < 門檻 %.2f",
                    ratio, volume_ratio_threshold
                )
            return triggered, round(ratio, 3)

        # ② 無大盤資料：使用籌碼監控的個股成交量聚合（代理指標）
        chip_rows = await self.hub.fetch("""
            SELECT trade_date, SUM(volume_ratio_5d20d) / COUNT(*) AS avg_ratio
            FROM chip_monitor
            WHERE trade_date <= $1
            GROUP BY trade_date
            ORDER BY trade_date DESC
            LIMIT 5
        """, trade_date)

        if chip_rows:
            avg_ratio = sum(
                float(r['avg_ratio'] or 1.0) for r in chip_rows
            ) / len(chip_rows)
            triggered = avg_ratio < volume_ratio_threshold
            return triggered, round(avg_ratio, 3)

        # ③ 無任何資料：不觸發
        logger.debug("無足夠成交量資料，跳過流動性成交量檢查")
        return False, 1.0

    # =========================================================================
    # check_triggers（v3 擴充）
    # =========================================================================

    async def check_triggers(
        self,
        vix: Optional[float] = None,
        trade_date: Optional[date] = None,
        current_basis: Optional[float] = None,   # ← v3 新增
    ) -> list[dict]:
        """檢查是否觸發黑天鵝情境（v3：新增流動性偵測）"""
        if trade_date is None:
            trade_date = date.today()

        triggered = []

        # ── 從 DB 取得最新宏觀數據 ───────────────────────────────────────
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

        # 若未傳入基差，嘗試從 DB 計算
        if current_basis is None and regime_row:
            tx   = regime_row.get('tx_futures_price')
            tsec = regime_row.get('tsec_index_price')
            if tx and tsec and float(tsec) > 0:
                current_basis = (float(tx) - float(tsec)) / float(tsec)

        # ── 逐一評估情境 ─────────────────────────────────────────────────
        for scenario in BLACK_SWAN_SCENARIOS:
            trigger       = scenario['trigger']
            is_triggered  = False
            trigger_detail = {}

            # 原有 VIX 觸發
            if 'vix_spike' in trigger and current_vix >= trigger['vix_spike']:
                is_triggered   = True
                trigger_detail = {'current_vix': current_vix}

            elif 'vix_level' in trigger and current_vix >= trigger['vix_level']:
                # 連續 duration_days 天 VIX 超標才觸發
                duration = trigger.get('duration_days', 1)
                if duration <= 1:
                    is_triggered = True
                    trigger_detail = {'current_vix': current_vix}
                else:
                    # 查詢連續天數
                    vix_rows = await self.hub.fetch("""
                        SELECT vix_level FROM market_regime
                        WHERE trade_date <= $1
                        ORDER BY trade_date DESC
                        LIMIT $2
                    """, trade_date, duration)

                    if len(vix_rows) >= duration:
                        all_high = all(
                            float(r['vix_level'] or 0) >= trigger['vix_level']
                            for r in vix_rows
                        )
                        if all_high:
                            is_triggered   = True
                            trigger_detail = {
                                'current_vix': current_vix,
                                'duration_confirmed': duration,
                            }

            # [v3] 基差持續偏離觸發
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

            # [v3] 成交量枯竭觸發
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
    # [v3 擴充] execute_action：連動 PositionManager 實際執行減倉
    # =========================================================================

    async def _execute_emergency_reduce(
        self,
        target_exposure: float,
        scenario_name: str,
        trade_date: date,
    ) -> dict:
        """
        緊急減倉執行器：按目標曝險比例實際減倉。

        策略：
        1. 取得當前組合 NAV 與所有開倉部位
        2. 計算需減少的市值
        3. 依「損益最差優先」排序（先賣最虧的）
        4. 對每個部位呼叫 PositionManager.partial_exit()

        target_exposure：目標曝險比例（0.0–1.0），0.0 = 全清倉

        回傳：
            {'reduced_count': int, 'total_reduced_value': float,
             'detail': [...], 'error': str or None}
        """
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
            logger.info(
                "緊急減倉：當前市值 %.0f 已低於目標 %.0f，無需操作",
                current_market_value, target_market_value
            )
            return {
                'reduced_count':       0,
                'total_reduced_value': 0,
                'error':               None,
                'detail':              [],
                'already_below_target': True,
            }

        logger.warning(
            "緊急減倉執行：目標曝險=%.0f%% NAV=%.0f 需減市值=%.0f",
            target_exposure * 100, nav, need_to_reduce
        )

        # 依損益最差排序（先賣虧最多的部位）
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

            pos_id     = str(pos['id'])
            ticker     = str(pos['ticker']).strip()
            avg_cost   = float(pos['avg_cost'] or pos['entry_price'])
            shares     = int(pos['current_shares'])
            pos_value  = avg_cost * shares

            if pos_value <= 0 or shares <= 0:
                continue

            # 計算需賣出的股數
            remaining_reduce = need_to_reduce - total_reduced_value
            shares_to_sell   = min(
                shares,
                int(remaining_reduce / avg_cost) + 1,
            )

            if target_exposure == 0.0:
                shares_to_sell = shares   # 全清倉

            try:
                result = await pm.partial_exit(
                    position_id=pos_id,
                    exit_shares=shares_to_sell,
                    exit_price=avg_cost,   # 使用成本價（緊急情況無法取即時報價）
                    trade_date=trade_date,
                )
                sold_value          = avg_cost * shares_to_sell
                total_reduced_value += sold_value
                reduced_count       += 1

                detail.append({
                    'ticker':        ticker,
                    'shares_sold':   shares_to_sell,
                    'price':         avg_cost,
                    'value_reduced': round(sold_value, 0),
                    'fully_closed':  result.get('is_fully_closed', False),
                })

                logger.info(
                    "緊急減倉：%s 賣出 %d 股 @ %.2f = %.0f元",
                    ticker, shares_to_sell, avg_cost, sold_value
                )

                # 記錄 decision_log
                await self.hub.execute("""
                    INSERT INTO decision_log (
                        trade_date, ticker, decision_type,
                        signal_source, position_id, notes
                    ) VALUES ($1, $2, 'EXIT_EXECUTED', 'BLACK_SWAN', $3, $4)
                """,
                    trade_date,
                    pos['ticker'],
                    pos_id,
                    f"黑天鵝緊急減倉：{scenario_name} 目標曝險={target_exposure*100:.0f}%",
                )

            except Exception as e:
                logger.error("緊急減倉失敗 %s：%s", ticker, e)
                detail.append({'ticker': ticker, 'error': str(e)})

        logger.warning(
            "緊急減倉完成：減倉 %d 檔，合計減少市值 %.0f",
            reduced_count, total_reduced_value
        )
        return {
            'reduced_count':       reduced_count,
            'total_reduced_value': round(total_reduced_value, 0),
            'detail':              detail,
            'error':               None,
        }

    async def execute_action(
        self,
        scenario: dict,
        trade_date: Optional[date] = None,
    ) -> dict:
        """
        執行黑天鵝應對動作（v3：實際執行減倉）

        回傳：
            {'message': str, 'action': str, 'reduction_result': dict or None}
        """
        if trade_date is None:
            trade_date = date.today()

        action       = scenario['action']
        scenario_name = scenario['scenario_name']
        target_exposure = EMERGENCY_EXPOSURE_TARGETS.get(action)

        # ── 1. Telegram 通知（先發警報）────────────────────────────────
        if action == 'HALT_NEW_ENTRIES':
            msg = (
                f"⚫ 黑天鵝警報：{scenario_name}\n"
                f"停止新進場，維持現有部位\n"
                f"原因：{scenario.get('description', '')}"
            )

        elif action == 'REDUCE_EXPOSURE_50PCT':
            msg = (
                f"🔴 黑天鵝警報：{scenario_name}\n"
                f"緊急減倉至 50% 曝險\n"
                f"原因：{scenario.get('description', '')}"
            )

        elif action == 'REDUCE_EXPOSURE_30PCT':
            msg = (
                f"🔴 黑天鵝警報：{scenario_name}\n"
                f"緊急減倉至 30% 曝險\n"
                f"原因：{scenario.get('description', '')}"
            )

        elif action == 'EMERGENCY_EXIT':
            msg = (
                f"🚨 緊急黑天鵝：{scenario_name}\n"
                f"緊急出清所有部位！\n"
                f"原因：{scenario.get('description', '')}"
            )

        else:
            msg = f"⚫ 黑天鵝情境：{scenario_name}"

        await self.telegram.send_alert("黑天鵝情境觸發", msg, "ERROR")
        logger.warning("黑天鵝動作執行：%s → %s", scenario_name, action)

        # ── 2. 實際執行減倉（v3 新增）───────────────────────────────────
        reduction_result = None
        if target_exposure is not None:   # HALT_NEW_ENTRIES 不需要減倉
            try:
                reduction_result = await self._execute_emergency_reduce(
                    target_exposure=target_exposure,
                    scenario_name=scenario_name,
                    trade_date=trade_date,
                )

                # 減倉完成後補發一條結果通知
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
    # run（v3 擴充）
    # =========================================================================

    async def run(
        self,
        trade_date: Optional[date] = None,
        current_basis: Optional[float] = None,   # ← v3 新增：可由外部傳入基差
    ) -> dict:
        """執行黑天鵝情境檢查與應對（v3：流動性偵測 + 實際減倉）"""
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