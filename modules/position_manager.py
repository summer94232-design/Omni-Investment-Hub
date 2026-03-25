# modules/position_manager.py
import logging
import json
from datetime import date
from typing import Optional
from datahub.data_hub import DataHub
from datahub import redis_keys as rk

logger = logging.getLogger(__name__)

# 風險參數
MAX_SINGLE_POSITION_PCT = 0.10   # 單一持倉最大佔 NAV 10%
MAX_CORRELATION_RHO     = 0.70   # 相關係數超過此值限制加倉
DRAWDOWN_CIRCUIT_BREAK  = 0.10   # 從高點回落 10% 觸發熔斷
DAILY_LOSS_CIRCUIT_BREAK= 0.03   # 單日虧損 3% 觸發熔斷

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

    async def calc_nav(self, positions: list[dict]) -> dict:
        """計算組合淨值與曝險"""
        total_market_value = 0.0
        for p in positions:
            price = float(p['avg_cost'] or p['entry_price'])
            total_market_value += price * p['current_shares']

        # 假設初始資金 1,000,000（實際應從帳戶 API 取得）
        initial_capital = 1_000_000.0
        total_pnl = sum(float(p['unrealized_pnl'] or 0) + float(p['realized_pnl'] or 0)
                       for p in positions)
        nav = initial_capital + total_pnl
        cash = nav - total_market_value
        gross_exposure = total_market_value / nav if nav > 0 else 0

        return {
            'nav':                nav,
            'cash_amount':        cash,
            'stock_market_value': total_market_value,
            'gross_exposure_pct': round(gross_exposure, 4),
        }

    def calc_var_95(self, positions: list[dict], nav: float) -> float:
        """計算 95% VaR（簡化歷史模擬法）"""
        if not positions or nav <= 0:
            return 0.0
        # 假設每日波動率 2%（實際應用歷史報酬計算）
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
        nav_data = await self.calc_nav(positions)
        nav = nav_data['nav']

        # 取得宏觀環境
        regime_row = await self.hub.fetchrow("""
            SELECT regime, mrs_score FROM market_regime
            WHERE trade_date <= $1
            ORDER BY trade_date DESC LIMIT 1
        """, trade_date)
        regime = regime_row['regime'] if regime_row else 'CHOPPY'
        mrs    = float(regime_row['mrs_score']) if regime_row else 50.0

        # 曝險上限
        max_exposure = EXPOSURE_LEVELS.get(regime, 0.40)
        exposure_level = min(5, max(1, int(nav_data['gross_exposure_pct'] / 0.20) + 1))

        # VaR
        var_95 = self.calc_var_95(positions, nav)

        # 熔斷檢查（簡化：假設 drawdown = 0, daily_pnl = 0）
        drawdown = 0.0
        daily_pnl_pct = 0.0
        circuit, circuit_reason = self.check_circuit_breaker(drawdown, daily_pnl_pct)

        result = {
            **nav_data,
            'snapshot_date':           trade_date,
            'exposure_level':          exposure_level,
            'drawdown_from_peak':      drawdown,
            'portfolio_var_95':        var_95,
            'circuit_breaker_triggered': circuit,
            'circuit_breaker_reason':  circuit_reason,
            'regime_snapshot':         regime,
            'mrs_snapshot':            mrs,
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