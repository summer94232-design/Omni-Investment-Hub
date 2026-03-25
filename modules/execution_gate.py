# modules/execution_gate.py
import logging
import asyncio
from datetime import date
from typing import Optional
from datahub.data_hub import DataHub
from datahub.api_fugle import FugleAPI
from datahub.api_telegram import TelegramBot
from datahub import redis_keys as rk

logger = logging.getLogger(__name__)

HUMAN_APPROVAL_TIMEOUT = 30  # 秒


class ExecutionGate:
    """模組⑪ 自動化執行閘門：三層審核 + 安全機制"""

    def __init__(self, hub: DataHub, fugle: FugleAPI, telegram: TelegramBot):
        self.hub      = hub
        self.fugle    = fugle
        self.telegram = telegram

    # ── 層①：市場環境檢查 ──
    async def gate1_regime_check(self, side: str, regime: str) -> tuple[bool, str]:
        if regime == 'BEAR_TREND' and side == 'BUY':
            return False, "熊市環境禁止新進場"
        return True, "通過"

    # ── 層②：部位風控檢查 ──
    async def gate2_risk_check(
        self, ticker: str, quantity: int, price: float
    ) -> tuple[bool, str]:
        health = await self.hub.fetchrow("""
            SELECT gross_exposure_pct, circuit_breaker_triggered, regime_snapshot
            FROM portfolio_health
            ORDER BY snapshot_date DESC LIMIT 1
        """)
        if not health:
            return True, "無歷史快照，允許通過"

        if health['circuit_breaker_triggered']:
            return False, "熔斷觸發中，禁止交易"

        from modules.position_manager import EXPOSURE_LEVELS
        regime    = health['regime_snapshot'] or 'CHOPPY'
        max_exp   = EXPOSURE_LEVELS.get(regime, 0.40)
        cur_exp   = float(health['gross_exposure_pct'])
        if cur_exp >= max_exp:
            return False, f"曝險已達上限 {max_exp*100:.0f}%（目前 {cur_exp*100:.1f}%）"

        return True, "通過"

    # ── 層③：人工確認（大單） ──
    async def gate3_human_approval(
        self, ticker: str, quantity: int, price: float
    ) -> tuple[bool, str]:
        value = quantity * price
        if value < 100_000:
            return True, "小單自動通過"

        order_id = f"{ticker}_{date.today()}"
        cache_key = rk.key_gate_pending(order_id)

        await self.telegram.send_alert(
            "大單需人工確認",
            f"股票：{ticker}\n數量：{quantity}\n價格：{price}\n金額：{value:,.0f}\n"
            f"請在 {HUMAN_APPROVAL_TIMEOUT} 秒內確認",
            "WARN"
        )

        # 寫入 Redis 等待確認
        await self.hub.cache.set(cache_key, {'status': 'PENDING'}, ttl=HUMAN_APPROVAL_TIMEOUT)

        # 等待確認（實際環境應監聽 Telegram webhook）
        await asyncio.sleep(3)
        result = await self.hub.cache.get(cache_key)

        if result and result.get('status') == 'APPROVED':
            return True, "人工確認通過"

        # 逾時預設拒絕
        return False, f"人工確認逾時（{HUMAN_APPROVAL_TIMEOUT}s）"

    async def execute(
        self,
        ticker: str,
        side: str,
        quantity: int,
        price: float,
        signal_source: str,
        exec_mode: str = 'PAPER',
        trade_date: Optional[date] = None,
    ) -> dict:
        """執行三層閘門審核並下單"""
        if trade_date is None:
            trade_date = date.today()

        # 取得市場環境
        regime_row = await self.hub.fetchrow("""
            SELECT regime FROM market_regime
            ORDER BY trade_date DESC LIMIT 1
        """)
        regime = regime_row['regime'] if regime_row else 'CHOPPY'

        # 層① 市場環境
        g1_pass, g1_msg = await self.gate1_regime_check(side, regime)

        # 層② 風控
        g2_pass, g2_msg = (True, "跳過") if not g1_pass else \
            await self.gate2_risk_check(ticker, quantity, price)

        # 層③ 人工確認（大單）
        g3_pass, g3_msg = (True, "跳過") if not g1_pass or not g2_pass else \
            await self.gate3_human_approval(ticker, quantity, price)

        all_pass   = g1_pass and g2_pass and g3_pass
        block_reason = None if all_pass else (
            g1_msg if not g1_pass else
            g2_msg if not g2_pass else g3_msg
        )

        # 寫入訂單記錄
        order_row = await self.hub.fetchrow("""
            INSERT INTO orders (
                order_date, ticker, side, quantity,
                order_type, limit_price,
                status, signal_source, exec_mode,
                gate1_result, gate2_result, gate3_result,
                block_reason
            ) VALUES ($1,$2,$3,$4,'LIMIT',$5,$6,$7,$8,$9,$10,$11,$12)
            RETURNING id
        """,
            trade_date, ticker[:6], side, quantity, price,
            'FILLED' if all_pass else 'REJECTED',
            signal_source, exec_mode,
            g1_msg, g2_msg, g3_msg,
            block_reason,
        )
        order_id = str(order_row['id'])

        # 實際下單
        fill_price = None
        if all_pass and exec_mode != 'ALERT_ONLY':
            order_result = await self.fugle.place_order(
                ticker.strip(), side, quantity, price
            )
            fill_price = price
            logger.info("下單完成：%s %s x%d @ %.2f", side, ticker, quantity, price)

        result = {
            'order_id':     order_id,
            'ticker':       ticker,
            'side':         side,
            'quantity':     quantity,
            'price':        price,
            'executed':     all_pass,
            'fill_price':   fill_price,
            'block_reason': block_reason,
            'gate1':        g1_msg,
            'gate2':        g2_msg,
            'gate3':        g3_msg,
        }

        logger.info(
            "執行閘門：%s %s x%d 結果=%s 原因=%s",
            side, ticker, quantity,
            "通過" if all_pass else "拒絕",
            block_reason or "無"
        )
        return result