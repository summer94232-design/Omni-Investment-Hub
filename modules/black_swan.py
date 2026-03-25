# modules/black_swan.py
import logging
from datetime import date
from typing import Optional
from datahub.data_hub import DataHub
from datahub.api_telegram import TelegramBot

logger = logging.getLogger(__name__)

# 黑天鵝情境定義
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
        'name':        '流動性危機',
        'trigger':     {'spread_widen': 200},
        'action':      'EMERGENCY_EXIT',
        'description': '信用利差擴大超過 200bp',
    },
]


class BlackSwan:
    """模組⑨ 黑天鵝情境腳本：異常市場自動應對"""

    def __init__(self, hub: DataHub, telegram: TelegramBot):
        self.hub      = hub
        self.telegram = telegram

    async def check_triggers(
        self,
        vix: Optional[float] = None,
        trade_date: Optional[date] = None,
    ) -> list[dict]:
        """檢查是否觸發黑天鵝情境"""
        if trade_date is None:
            trade_date = date.today()

        triggered = []

        # 從 DB 取得最新宏觀數據
        regime_row = await self.hub.fetchrow("""
            SELECT vix_level, mrs_score, regime
            FROM market_regime
            WHERE trade_date <= $1
            ORDER BY trade_date DESC LIMIT 1
        """, trade_date)

        current_vix = vix or (float(regime_row['vix_level']) if regime_row and regime_row['vix_level'] else 20.0)

        for scenario in BLACK_SWAN_SCENARIOS:
            trigger = scenario['trigger']
            is_triggered = False

            if 'vix_spike' in trigger and current_vix >= trigger['vix_spike']:
                is_triggered = True
            elif 'vix_level' in trigger and current_vix >= trigger['vix_level']:
                is_triggered = True

            if is_triggered:
                triggered.append({
                    'scenario_id':  scenario['id'],
                    'scenario_name': scenario['name'],
                    'action':        scenario['action'],
                    'description':   scenario['description'],
                    'current_vix':   current_vix,
                })
                logger.warning("黑天鵝情境觸發：%s", scenario['name'])

        return triggered

    async def execute_action(self, scenario: dict) -> str:
        """執行黑天鵝應對動作"""
        action = scenario['action']

        if action == 'HALT_NEW_ENTRIES':
            msg = f"⚫ 黑天鵝警報：{scenario['scenario_name']}\n停止新進場，維持現有部位"

        elif action == 'REDUCE_EXPOSURE_50PCT':
            msg = f"⚫ 黑天鵝警報：{scenario['scenario_name']}\n建議減少曝險 50%"

        elif action == 'REDUCE_EXPOSURE_30PCT':
            msg = f"⚫ 黑天鵝警報：{scenario['scenario_name']}\n建議減少曝險 30%"

        elif action == 'EMERGENCY_EXIT':
            msg = f"🚨 緊急黑天鵝：{scenario['scenario_name']}\n建議緊急出清所有部位"

        else:
            msg = f"⚫ 黑天鵝情境：{scenario['scenario_name']}"

        await self.telegram.send_alert("黑天鵝情境觸發", msg, "ERROR")
        logger.warning("黑天鵝動作執行：%s → %s", scenario['scenario_name'], action)
        return msg

    async def run(self, trade_date: Optional[date] = None) -> dict:
        triggered = await self.check_triggers(trade_date=trade_date)
        actions   = []
        for scenario in triggered:
            msg = await self.execute_action(scenario)
            actions.append(msg)

        return {
            'trade_date':    str(trade_date or date.today()),
            'triggered':     len(triggered),
            'scenarios':     triggered,
            'actions_taken': actions,
        }