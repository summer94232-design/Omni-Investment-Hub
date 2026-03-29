# modules/position_manager.py
# ═══════════════════════════════════════════════════════════════════════════════
# Bug 修正（v3.1）：
# - [BUG 3]  INSERT portfolio_health 補上 daily_pnl_pct（$13）
# - [BUG A]  drawdown_from_peak 和 daily_pnl_pct 從硬編碼 0.0 改為實際計算
#            修正前熔斷機制永遠不會觸發，有實際資金風險
# ═══════════════════════════════════════════════════════════════════════════════

import logging
from datetime import date
from typing import Optional
from datahub.data_hub import DataHub
from datahub import redis_keys as rk

logger = logging.getLogger(__name__)

# ── 熔斷門檻 ─────────────────────────────────────────────────────────────────
DRAWDOWN_CIRCUIT_BREAK    = 0.15   # 回撤超過 15% 觸發熔斷
DAILY_LOSS_CIRCUIT_BREAK  = 0.05   # 單日虧損超過 5% 觸發熔斷
MAX_SINGLE_POSITION_PCT   = 0.20   # 單一持倉不超過 NAV 20%

# ── 各 Regime 曝險上限 ────────────────────────────────────────────────────────
EXPOSURE_LEVELS = {
    'BULL_TREND': 0.90,
    'WEAK_BULL':  0.70,
    'CHOPPY':     0.40,
    'BEAR_TREND': 0.10,
}


class PositionManager:
    """部位管理模組：NAV 計算、曝險控管、熔斷機制"""

    def __init__(self, hub: DataHub):
        self.hub = hub

    async def get_open_positions(self) -> list[dict]:
        """取得所有開倉部位"""
        rows = await self.hub.fetch("""
            SELECT id, ticker, entry_price, shares, atr, atr_multiplier,
                   stop_loss, state, entry_date, unrealized_pnl,
                   r_multiple_current
            FROM positions
            WHERE is_open = TRUE
            ORDER BY entry_date
        """)
        return [dict(r) for r in rows]

    async def calc_nav(self, positions: list[dict]) -> dict:
        """計算組合淨值（NAV）與相關指標"""
        row = await self.hub.fetchrow("""
            SELECT nav, cash_amount FROM portfolio_health
            ORDER BY snapshot_date DESC LIMIT 1
        """)

        cash       = float(row['cash_amount']) if row else 1_000_000.0
        stock_value = sum(
            float(p.get('entry_price', 0)) * int(p.get('shares', 0))
            for p in positions
        )

        nav = cash + stock_value
        gross_exposure_pct = stock_value / nav if nav > 0 else 0.0

        return {
            'nav':                round(nav, 2),
            'cash_amount':        round(cash, 2),
            'stock_market_value': round(stock_value, 2),
            'gross_exposure_pct': round(gross_exposure_pct, 4),
        }

    def calc_var_95(self, positions: list[dict], nav: float, confidence: float = 0.95) -> float:
        """簡化版 VaR（歷史模擬法替代：以 ATR 估算）"""
        if not positions or nav <= 0:
            return 0.0
        total_risk = sum(
            float(p.get('atr', 0)) * int(p.get('shares', 0)) * float(p.get('atr_multiplier', 3))
            for p in positions
        )
        return round(total_risk / nav, 4)

    def check_circuit_breaker(
        self,
        drawdown: float,
        daily_pnl_pct: float,
    ) -> tuple[bool, Optional[str]]:
        """熔斷檢查"""
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

        regime_row = await self.hub.fetchrow("""
            SELECT regime, mrs_score FROM market_regime
            WHERE trade_date <= $1
            ORDER BY trade_date DESC LIMIT 1
        """, trade_date)
        regime = regime_row['regime']         if regime_row else 'CHOPPY'
        mrs    = float(regime_row['mrs_score']) if regime_row else 50.0

        max_exposure   = EXPOSURE_LEVELS.get(regime, 0.40)
        exposure_level = min(5, max(1, int(nav_data['gross_exposure_pct'] / 0.20) + 1))

        var_95 = self.calc_var_95(positions, nav)

        # ── [BUG A 修正] 計算真實回撤（原本硬編碼為 0.0）────────────────────
        peak_row = await self.hub.fetchrow("""
            SELECT MAX(nav) AS peak_nav FROM portfolio_health
            WHERE snapshot_date >= CURRENT_DATE - INTERVAL '252 days'
        """)
        peak_nav = float(peak_row['peak_nav']) if peak_row and peak_row['peak_nav'] else nav
        drawdown = max(0.0, (peak_nav - nav) / peak_nav) if peak_nav > 0 else 0.0

        # ── [BUG A 修正] 計算真實單日損益（原本硬編碼為 0.0）───────────────
        yesterday_row = await self.hub.fetchrow("""
            SELECT nav FROM portfolio_health
            WHERE snapshot_date < $1
            ORDER BY snapshot_date DESC LIMIT 1
        """, trade_date)
        yesterday_nav = float(yesterday_row['nav']) if yesterday_row else nav
        daily_pnl_pct = (nav - yesterday_nav) / yesterday_nav if yesterday_nav > 0 else 0.0

        circuit, circuit_reason = self.check_circuit_breaker(drawdown, daily_pnl_pct)

        if circuit:
            logger.warning(
                "熔斷觸發：%s  drawdown=%.2f%%  daily_pnl=%.2f%%",
                circuit_reason, drawdown * 100, daily_pnl_pct * 100,
            )

        result = {
            **nav_data,
            'snapshot_date':             trade_date,
            'exposure_level':            exposure_level,
            'drawdown_from_peak':        round(drawdown, 4),
            'portfolio_var_95':          var_95,
            'circuit_breaker_triggered': circuit,
            'circuit_breaker_reason':    circuit_reason,
            'regime_snapshot':           regime,
            'mrs_snapshot':              mrs,
            'daily_pnl_pct':             round(daily_pnl_pct, 4),
        }

        # [BUG 3 修正] INSERT 欄位清單末尾加入 daily_pnl_pct（$13）
        await self.hub.execute("""
            INSERT INTO portfolio_health (
                snapshot_date, nav, cash_amount, stock_market_value,
                gross_exposure_pct, exposure_level,
                drawdown_from_peak, portfolio_var_95,
                circuit_breaker_triggered, circuit_breaker_reason,
                regime_snapshot, mrs_snapshot,
                daily_pnl_pct
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
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
                mrs_snapshot              = EXCLUDED.mrs_snapshot,
                daily_pnl_pct             = EXCLUDED.daily_pnl_pct
        """,
            trade_date,
            nav_data['nav'],
            nav_data['cash_amount'],
            nav_data['stock_market_value'],
            nav_data['gross_exposure_pct'],
            exposure_level,
            round(drawdown, 4),
            var_95,
            circuit,
            circuit_reason,
            regime,
            mrs,
            round(daily_pnl_pct, 4),
        )

        await self.hub.cache.set(
            rk.key_portfolio_state_latest(),
            result,
            ttl=rk.TTL_10MIN,
        )

        logger.info(
            "部位控管完成：NAV=%.0f 曝險=%.1f%% 回撤=%.2f%% 熔斷=%s daily_pnl=%.2f%%",
            nav, nav_data['gross_exposure_pct'] * 100,
            drawdown * 100, circuit, daily_pnl_pct * 100,
        )
        return result

    async def partial_exit(
        self,
        position_id: int,
        exit_shares: int,
        exit_price: float,
        avg_cost: Optional[float] = None,
        sold_value: Optional[float] = None,
    ) -> dict:
        """部分出場"""
        row = await self.hub.fetchrow(
            "SELECT ticker, current_shares, avg_cost, entry_price FROM positions WHERE id=$1",
            position_id,
        )
        if not row:
            raise ValueError(f"找不到部位 id={position_id}")

        ticker         = row['ticker']
        current_shares = int(row['current_shares'])
        cost_basis     = float(avg_cost or row['avg_cost'] or row['entry_price'])

        if exit_shares >= current_shares:
            await self.hub.execute("""
                UPDATE positions SET is_open=FALSE, exit_date=$1, exit_price=$2,
                exit_reason='PARTIAL_EXIT', state='CLOSED', current_shares=0
                WHERE id=$3
            """, date.today(), exit_price, position_id)
        else:
            new_shares = current_shares - exit_shares
            await self.hub.execute("""
                UPDATE positions SET current_shares=$1 WHERE id=$2
            """, new_shares, position_id)

        pnl = (exit_price - cost_basis) * exit_shares
        logger.info(
            "部分出場：%s id=%d 出場%d股 @%.2f 成本%.2f PnL=%.0f",
            ticker, position_id, exit_shares, exit_price, cost_basis, pnl,
        )
        return {
            'ticker':       ticker,
            'exit_shares':  exit_shares,
            'exit_price':   exit_price,
            'realized_pnl': round(pnl, 2),
        }