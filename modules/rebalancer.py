# modules/rebalancer.py
import logging
from datetime import date
from typing import Optional
from datahub.data_hub import DataHub
from datahub.api_telegram import TelegramBot
from datahub import redis_keys as rk

logger = logging.getLogger(__name__)

REBALANCE_TRIGGERS = {
    'REGIME_CHANGE':     '市場環境切換',
    'EXPOSURE_BREACH':   '曝險超出上限',
    'WEIGHT_DRIFT':      '因子權重漂移',
    'MONTHLY_REVIEW':    '月度定期審查',
}


class Rebalancer:
    """模組⑬ 組合再平衡觸發器"""

    def __init__(self, hub: DataHub, telegram: TelegramBot):
        self.hub      = hub
        self.telegram = telegram

    async def check_regime_change(self, trade_date: date) -> Optional[dict]:
        """檢查市場環境是否切換"""
        rows = await self.hub.fetch("""
            SELECT trade_date, regime, mrs_score
            FROM market_regime
            WHERE trade_date <= $1
            ORDER BY trade_date DESC LIMIT 2
        """, trade_date)

        if len(rows) < 2:
            return None

        current = dict(rows[0])
        prev    = dict(rows[1])

        if current['regime'] != prev['regime']:
            return {
                'trigger':      'REGIME_CHANGE',
                'old_regime':   prev['regime'],
                'new_regime':   current['regime'],
                'old_mrs':      float(prev['mrs_score']),
                'new_mrs':      float(current['mrs_score']),
            }
        return None

    async def check_exposure_breach(self, trade_date: date) -> Optional[dict]:
        """檢查曝險是否超出當前環境上限"""
        health_row = await self.hub.fetchrow("""
            SELECT gross_exposure_pct, regime_snapshot
            FROM portfolio_health
            WHERE snapshot_date <= $1
            ORDER BY snapshot_date DESC LIMIT 1
        """, trade_date)

        if not health_row:
            return None

        from modules.position_manager import EXPOSURE_LEVELS
        regime      = health_row['regime_snapshot'] or 'CHOPPY'
        max_exp     = EXPOSURE_LEVELS.get(regime, 0.40)
        current_exp = float(health_row['gross_exposure_pct'])

        if current_exp > max_exp * 1.10:
            return {
                'trigger':        'EXPOSURE_BREACH',
                'current_exposure': current_exp,
                'max_exposure':   max_exp,
                'regime':         regime,
            }
        return None

    async def run(self, trade_date: Optional[date] = None) -> dict:
        """執行再平衡檢查"""
        if trade_date is None:
            trade_date = date.today()

        triggers = []

        regime_trigger   = await self.check_regime_change(trade_date)
        exposure_trigger = await self.check_exposure_breach(trade_date)

        if regime_trigger:
            triggers.append(regime_trigger)
        if exposure_trigger:
            triggers.append(exposure_trigger)

        # 月度審查（每月第一個交易日）
        if trade_date.day <= 3:
            triggers.append({'trigger': 'MONTHLY_REVIEW', 'date': str(trade_date)})

        if triggers:
            msg = "🔄 <b>組合再平衡觸發</b>\n\n"
            for t in triggers:
                name = REBALANCE_TRIGGERS.get(t['trigger'], t['trigger'])
                msg += f"• {name}\n"
                if t['trigger'] == 'REGIME_CHANGE':
                    msg += f"  {t['old_regime']} → {t['new_regime']}\n"
                elif t['trigger'] == 'EXPOSURE_BREACH':
                    msg += f"  曝險 {t['current_exposure']*100:.1f}% > 上限 {t['max_exposure']*100:.0f}%\n"
            await self.telegram.send(msg)

        logger.info("再平衡檢查完成：%d 個觸發條件", len(triggers))
        return {
            'trade_date':    str(trade_date),
            'trigger_count': len(triggers),
            'triggers':      triggers,
        }