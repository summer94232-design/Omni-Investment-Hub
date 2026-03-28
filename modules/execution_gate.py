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

        order_id  = f"{ticker}_{date.today()}"
        cache_key = rk.key_gate_pending(order_id)

        await self.telegram.send_alert(
            "大單需人工確認",
            f"股票：{ticker}\n數量：{quantity}\n價格：{price}\n金額：{value:,.0f}\n"
            f"請在 {HUMAN_APPROVAL_TIMEOUT} 秒內確認",
            "WARN"
        )

        await self.hub.cache.set(cache_key, {'status': 'PENDING'}, ttl=HUMAN_APPROVAL_TIMEOUT)
        await asyncio.sleep(3)
        result = await self.hub.cache.get(cache_key)

        if result and result.get('status') == 'APPROVED':
            return True, "人工確認通過"

        return False, f"人工確認逾時（{HUMAN_APPROVAL_TIMEOUT}s）"

    # ──────────────────────────────────────────────
    # [新增] 持倉合併：同一 ticker 加碼時更新 avg_cost / current_shares
    # ──────────────────────────────────────────────
    async def _merge_or_create_position(
        self,
        ticker: str,
        quantity: int,
        price: float,
        signal_source: str,
        strategy_tag: Optional[str],
        trade_date: date,
        atr_at_entry: float = 0.0,
        atr_multiplier: float = 3.0,
        initial_stop_price: Optional[float] = None,
        r_amount: Optional[float] = None,
        mrs_at_entry: Optional[float] = None,
        regime_at_entry: Optional[str] = None,
        ev_at_entry: Optional[float] = None,
        crs_at_entry: Optional[float] = None,
    ) -> dict:
        """
        若同一 ticker 已有開倉 → 合併加碼（更新 avg_cost / current_shares）
        若無既有開倉 → 建立新持倉列

        [修復] 加權平均成本公式：
            new_avg_cost = (舊股數 × 舊avg_cost + 新股數 × 新price) / (舊股數 + 新股數)
        """
        ticker_padded = ticker.ljust(6)[:6]  # 補齊 CHAR(6)

        existing = await self.hub.fetchrow("""
            SELECT id, current_shares, avg_cost, addon_shares, realized_pnl
            FROM positions
            WHERE ticker = $1
              AND is_open = TRUE
            ORDER BY entry_date DESC
            LIMIT 1
        """, ticker_padded)

        if existing:
            # ── 加碼路徑：重算加權平均成本 ──
            old_shares   = int(existing['current_shares'])
            old_avg_cost = float(existing['avg_cost'] or price)
            new_shares   = old_shares + quantity
            new_avg_cost = round(
                (old_shares * old_avg_cost + quantity * price) / new_shares, 4
            )

            await self.hub.execute("""
                UPDATE positions SET
                    current_shares = $1,
                    avg_cost       = $2,
                    addon_done     = TRUE,
                    addon_shares   = addon_shares + $3,
                    addon_price    = $4
                WHERE id = $5
            """,
                new_shares,
                new_avg_cost,
                quantity,
                price,
                str(existing['id']),
            )

            logger.info(
                "加碼合併：%s  +%d股 @ %.2f  avg_cost: %.4f → %.4f  總持股: %d→%d",
                ticker, quantity, price,
                old_avg_cost, new_avg_cost,
                old_shares, new_shares,
            )
            return {
                'action':       'ADDON',
                'position_id':  str(existing['id']),
                'ticker':       ticker,
                'new_shares':   new_shares,
                'new_avg_cost': new_avg_cost,
            }

        else:
            # ── 新開倉路徑 ──
            if initial_stop_price is None:
                initial_stop_price = round(price - atr_at_entry * atr_multiplier, 4)
            if r_amount is None:
                r_amount = round(atr_at_entry * atr_multiplier, 4)

            row = await self.hub.fetchrow("""
                INSERT INTO positions (
                    ticker, is_open, state,
                    entry_date, entry_price,
                    initial_shares, current_shares,
                    signal_source, strategy_tag,
                    atr_at_entry, atr_multiplier,
                    initial_stop_price, current_stop_price,
                    r_amount, avg_cost,
                    realized_pnl,
                    mrs_at_entry, regime_at_entry,
                    ev_at_entry, crs_at_entry
                ) VALUES (
                    $1, TRUE, 'S1_INITIAL_DEFENSE',
                    $2, $3,
                    $4, $4,
                    $5, $6,
                    $7, $8,
                    $9, $9,
                    $10, $3,
                    0,
                    $11, $12,
                    $13, $14
                )
                RETURNING id
            """,
                ticker_padded, trade_date, price,
                quantity,
                signal_source, strategy_tag,
                atr_at_entry, atr_multiplier,
                initial_stop_price,
                r_amount,
                mrs_at_entry, regime_at_entry,
                ev_at_entry, crs_at_entry,
            )

            logger.info(
                "新開倉：%s  %d股 @ %.2f  stop=%.2f  R=%.2f",
                ticker, quantity, price, initial_stop_price, r_amount
            )
            return {
                'action':      'NEW_POSITION',
                'position_id': str(row['id']),
                'ticker':      ticker,
                'new_shares':  quantity,
                'new_avg_cost': price,
            }

    async def execute(
        self,
        ticker: str,
        side: str,
        quantity: int,
        price: float,
        signal_source: str,
        exec_mode: str = 'PAPER',
        trade_date: Optional[date] = None,
        strategy_tag: Optional[str] = None,
        atr_at_entry: float = 0.0,
        atr_multiplier: float = 3.0,
        initial_stop_price: Optional[float] = None,
        r_amount: Optional[float] = None,
        mrs_at_entry: Optional[float] = None,
        regime_at_entry: Optional[str] = None,
        ev_at_entry: Optional[float] = None,
        crs_at_entry: Optional[float] = None,
    ) -> dict:
        """執行三層閘門審核、下單，並更新持倉記錄（含加碼合併）"""
        if trade_date is None:
            trade_date = date.today()

        regime_row = await self.hub.fetchrow("""
            SELECT regime FROM market_regime
            ORDER BY trade_date DESC LIMIT 1
        """)
        regime = regime_row['regime'] if regime_row else 'CHOPPY'

        g1_pass, g1_msg = await self.gate1_regime_check(side, regime)

        g2_pass, g2_msg = (True, "跳過") if not g1_pass else \
            await self.gate2_risk_check(ticker, quantity, price)

        g3_pass, g3_msg = (True, "跳過") if not g1_pass or not g2_pass else \
            await self.gate3_human_approval(ticker, quantity, price)

        all_pass     = g1_pass and g2_pass and g3_pass
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

        fill_price      = None
        position_result = None

        if all_pass and exec_mode != 'ALERT_ONLY':
            # 實際下單
            await self.fugle.place_order(
                ticker.strip(), side, quantity, price
            )
            fill_price = price
            logger.info("下單完成：%s %s x%d @ %.2f", side, ticker, quantity, price)

            # [修復] BUY → 合併持倉 / SELL → 由 ExitEngine 處理
            if side == 'BUY':
                position_result = await self._merge_or_create_position(
                    ticker=ticker,
                    quantity=quantity,
                    price=price,
                    signal_source=signal_source,
                    strategy_tag=strategy_tag,
                    trade_date=trade_date,
                    atr_at_entry=atr_at_entry,
                    atr_multiplier=atr_multiplier,
                    initial_stop_price=initial_stop_price,
                    r_amount=r_amount,
                    mrs_at_entry=mrs_at_entry,
                    regime_at_entry=regime_at_entry,
                    ev_at_entry=ev_at_entry,
                    crs_at_entry=crs_at_entry,
                )

        result = {
            'order_id':       order_id,
            'ticker':         ticker,
            'side':           side,
            'quantity':       quantity,
            'price':          price,
            'executed':       all_pass,
            'fill_price':     fill_price,
            'block_reason':   block_reason,
            'gate1':          g1_msg,
            'gate2':          g2_msg,
            'gate3':          g3_msg,
            'position_result': position_result,
        }

        logger.info(
            "執行閘門：%s %s x%d 結果=%s 原因=%s",
            side, ticker, quantity,
            "通過" if all_pass else "拒絕",
            block_reason or "無"
        )
        return result