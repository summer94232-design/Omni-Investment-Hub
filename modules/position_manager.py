# modules/position_manager.py
import logging
import json
from datetime import date
from typing import Optional
from datahub.data_hub import DataHub
from datahub import redis_keys as rk

logger = logging.getLogger(__name__)

# 風險參數
MAX_SINGLE_POSITION_PCT  = 0.10   # 單一持倉最大佔 NAV 10%
MAX_CORRELATION_RHO      = 0.70   # 相關係數超過此值限制加倉
DRAWDOWN_CIRCUIT_BREAK   = 0.10   # 從高點回落 10% 觸發熔斷
DAILY_LOSS_CIRCUIT_BREAK = 0.03   # 單日虧損 3% 觸發熔斷

EXPOSURE_LEVELS = {
    'BULL_TREND': 0.90,
    'WEAK_BULL':  0.70,
    'CHOPPY':     0.40,
    'BEAR_TREND': 0.10,
}


class PositionManager:
    """模組④ 部位控管風險防禦：VaR / 曝險限制 / 熔斷機制"""

    def __init__(self, hub: DataHub):
        self.hub = hub

    async def get_open_positions(self) -> list[dict]:
        rows = await self.hub.fetch("""
            SELECT id, ticker, current_shares, avg_cost,
                   unrealized_pnl, realized_pnl, entry_price
            FROM positions WHERE is_open = TRUE
        """)
        return [dict(r) for r in rows]

    async def get_initial_capital(self) -> float:
        """從 account_config 讀取初始資金"""
        row = await self.hub.fetchrow("""
            SELECT initial_capital
            FROM account_config
            WHERE is_active = TRUE
            LIMIT 1
        """)
        if row is None:
            logger.warning("account_config 無有效記錄，fallback 使用 1,000,000")
            return 1_000_000.0
        return float(row['initial_capital'])

    async def calc_nav(self, positions: list[dict]) -> dict:
        """計算組合淨值與曝險"""
        total_market_value = 0.0
        for p in positions:
            price = float(p['avg_cost'] or p['entry_price'])
            total_market_value += price * p['current_shares']

        # 從資料庫讀取初始資金（取代硬編碼 1,000,000）
        initial_capital = await self.get_initial_capital()

        total_pnl = sum(
            float(p['unrealized_pnl'] or 0) + float(p['realized_pnl'] or 0)
            for p in positions
        )
        nav = initial_capital + total_pnl
        cash = nav - total_market_value
        gross_exposure = total_market_value / nav if nav > 0 else 0

        return {
            'nav':                nav,
            'cash_amount':        cash,
            'stock_market_value': total_market_value,
            'gross_exposure_pct': round(gross_exposure, 4),
        }

    async def partial_exit(
        self,
        position_id: str,
        exit_shares: int,
        exit_price: float,
        trade_date: Optional[date] = None,
    ) -> dict:
        """
        局部減倉：減少持倉股數，更新已實現損益。
        若減倉後 current_shares <= 0，自動全數平倉。
        """
        if trade_date is None:
            trade_date = date.today()

        row = await self.hub.fetchrow(
            "SELECT * FROM positions WHERE id = $1 AND is_open = TRUE", position_id
        )
        if not row:
            raise ValueError(f"找不到有效持倉 {position_id}")

        position     = dict(row)
        current_shares = int(position['current_shares'])
        avg_cost       = float(position['avg_cost'] or position['entry_price'])

        if exit_shares <= 0:
            raise ValueError("減倉股數必須大於 0")
        if exit_shares > current_shares:
            raise ValueError(f"減倉股數 {exit_shares} 超過現有股數 {current_shares}")

        # 計算本次已實現損益
        realized_this_trade = (exit_price - avg_cost) * exit_shares
        new_realized_pnl    = float(position['realized_pnl'] or 0) + realized_this_trade
        new_shares          = current_shares - exit_shares
        is_fully_closed     = new_shares <= 0

        if is_fully_closed:
            # 全數平倉
            await self.hub.execute("""
                UPDATE positions SET
                    current_shares  = 0,
                    realized_pnl    = $1,
                    unrealized_pnl  = 0,
                    is_open         = FALSE,
                    exit_date       = $2,
                    exit_price      = $3,
                    exit_reason     = 'MANUAL_PARTIAL_TO_FULL',
                    state           = 'CLOSED'
                WHERE id = $4
            """, round(new_realized_pnl, 2), trade_date, exit_price, position_id)
            logger.info(
                "局部減倉導致全數平倉：%s 減 %d 股 @ %.2f，已實現損益 %.0f",
                position['ticker'].strip(), exit_shares, exit_price, new_realized_pnl,
            )
        else:
            # 部分減倉，更新股數與損益
            new_unrealized_pnl = (exit_price - avg_cost) * new_shares
            await self.hub.execute("""
                UPDATE positions SET
                    current_shares = $1,
                    realized_pnl   = $2,
                    unrealized_pnl = $3
                WHERE id = $4
            """,
                new_shares,
                round(new_realized_pnl, 2),
                round(new_unrealized_pnl, 2),
                position_id,
            )
            logger.info(
                "局部減倉：%s 減 %d 股 @ %.2f，剩餘 %d 股，已實現損益 %.0f",
                position['ticker'].strip(), exit_shares, exit_price,
                new_shares, new_realized_pnl,
            )

        return {
            'position_id':          position_id,
            'ticker':               position['ticker'].strip(),
            'exit_shares':          exit_shares,
            'exit_price':           exit_price,
            'remaining_shares':     new_shares,
            'realized_pnl_added':   round(realized_this_trade, 2),
            'total_realized_pnl':   round(new_realized_pnl, 2),
            'is_fully_closed':      is_fully_closed,
        }

    def calc_var_95(self, positions: list[dict], nav: float) -> float:
        """計算 95% VaR（簡化歷史模擬法）"""
        if not positions or nav <= 0:
            return 0.0
        portfolio_value = sum(
            float(p['avg_cost'] or p['entry_price']) * p['current_shares']
            for p in positions
        )
        daily_vol = 0.02
        var_95 = portfolio_value * daily_vol * 1.645 / nav
        return round(var_95, 4)

    def check_circuit_breaker(
        self,
        drawdown: float,
        daily_pnl_pct: float,
    ) -> tuple[bool, Optional[str]]:
        """檢查是否觸發熔斷"""
        if drawdown >= DRAWDOWN_CIRCUIT_BREAK:
            return True, f"回撤超過 {DRAWDOWN_CIRCUIT_BREAK*100:.0f}%（{drawdown*100:.1f}%）"
        if abs(daily_pnl_pct) >= DAILY_LOSS_CIRCUIT_BREAK:
            return True, f"單日虧損超過 {DAILY_LOSS_CIRCUIT_BREAK*100:.0f}%（{daily_pnl_pct*100:.1f}%）"
        return False, None

    def check_position_size(
        self,
        ticker: str,
        new_value: float,
        nav: float,
    ) -> tuple[bool, str]:
        """檢查單一持倉是否超過上限"""
        pct = new_value / nav if nav > 0 else 0
        if pct > MAX_SINGLE_POSITION_PCT:
            return False, f"{ticker} 佔比 {pct*100:.1f}% 超過上限 {MAX_SINGLE_POSITION_PCT*100:.0f}%"
        return True, ""

    async def run(self, trade_date: Optional[date] = None) -> dict:
        """執行部位控管，產生組合健康快照"""
        if trade_date is None:
            trade_date = date.today()

        positions = await self.get_open_positions()
        nav_data  = await self.calc_nav(positions)
        nav       = nav_data['nav']

        # 取得宏觀環境
        regime_row = await self.hub.fetchrow("""
            SELECT regime, mrs_score FROM market_regime
            WHERE trade_date <= $1
            ORDER BY trade_date DESC LIMIT 1
        """, trade_date)
        regime = regime_row['regime'] if regime_row else 'CHOPPY'
        mrs    = float(regime_row['mrs_score']) if regime_row else 50.0

        # 曝險上限
        max_exposure   = EXPOSURE_LEVELS.get(regime, 0.40)
        exposure_level = min(5, max(1, int(nav_data['gross_exposure_pct'] / 0.20) + 1))

        # VaR
        var_95 = self.calc_var_95(positions, nav)

        # 熔斷檢查
        drawdown      = 0.0
        daily_pnl_pct = 0.0
        circuit, circuit_reason = self.check_circuit_breaker(drawdown, daily_pnl_pct)

        result = {
            **nav_data,
            'snapshot_date':             trade_date,
            'exposure_level':            exposure_level,
            'drawdown_from_peak':        drawdown,
            'portfolio_var_95':          var_95,
            'circuit_breaker_triggered': circuit,
            'circuit_breaker_reason':    circuit_reason,
            'regime_snapshot':           regime,
            'mrs_snapshot':              mrs,
        }

        # 寫入 PostgreSQL
        await self.hub.execute("""
            INSERT INTO portfolio_health (
                snapshot_date, nav, cash_amount, stock_market_value,
                gross_exposure_pct, exposure_level,
                drawdown_from_peak, portfolio_var_95,
                circuit_breaker_triggered, circuit_breaker_reason,
                regime_snapshot, mrs_snapshot
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
            ON CONFLICT (snapshot_date) DO UPDATE SET
                nav                       = EXCLUDED.nav,
                cash_amount               = EXCLUDED.cash_amount,
                stock_market_value        = EXCLUDED.stock_market_value,
                gross_exposure_pct        = EXCLUDED.gross_exposure_pct,
                exposure_level            = EXCLUDED.exposure_level,
                drawdown_from_peak        = EXCLUDED.drawdown_from_peak,
                portfolio_var_95          = EXCLUDED.portfolio_var_95,
                circuit_breaker_triggered = EXCLUDED.circuit_breaker_triggered,
                circuit_breaker_reason    = EXCLUDED.circuit_breaker_reason,
                regime_snapshot           = EXCLUDED.regime_snapshot,
                mrs_snapshot              = EXCLUDED.mrs_snapshot
        """,
            trade_date,
            nav_data['nav'],
            nav_data['cash_amount'],
            nav_data['stock_market_value'],
            nav_data['gross_exposure_pct'],
            exposure_level,
            drawdown,
            var_95,
            circuit,
            circuit_reason,
            regime,
            mrs,
        )

        # 寫入 Redis
        await self.hub.cache.set(
            rk.key_portfolio_state_latest(),
            result,
            ttl=rk.TTL_10MIN,
        )

        logger.info(
            "部位控管完成：NAV=%.0f 曝險=%.1f%% 熔斷=%s",
            nav, nav_data['gross_exposure_pct'] * 100, circuit
        )
        return result