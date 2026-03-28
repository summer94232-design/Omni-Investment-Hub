# modules/exit_engine.py
import logging
from datetime import date
from typing import Optional
from datahub.data_hub import DataHub
from datahub.api_finmind import FinMindAPI

logger = logging.getLogger(__name__)

STATE_TRANSITIONS = {
    'S1_INITIAL_DEFENSE':  ['S2_BREAKOUT_CONFIRM', 'STOPPED_OUT'],
    'S2_BREAKOUT_CONFIRM': ['S3_PROFIT_PROTECT', 'STOPPED_OUT'],
    'S3_PROFIT_PROTECT':   ['S4_TRAILING_STOP', 'STOPPED_OUT'],
    'S4_TRAILING_STOP':    ['S5_ACTIVE_EXIT', 'STOPPED_OUT'],
    'S5_ACTIVE_EXIT':      ['CLOSED'],
}

# ── ATR 動態停損：依 ATR 百分位數決定 trail_pct ──────────────────────────
# ATR 愈高（波動大）→ 停損寬一些，避免被洗出場
# ATR 愈低（波動小）→ 停損緊一些，鎖住更多獲利
ATR_TRAIL_MAP = [
    # (atr_pct_of_price 上限,  trail_pct)
    (0.02,  0.08),   # ATR/價格 ≤ 2%  → 8% trailing
    (0.04,  0.12),   # ATR/價格 ≤ 4%  → 12% trailing
    (0.06,  0.15),   # ATR/價格 ≤ 6%  → 15% trailing（原預設）
    (0.09,  0.18),   # ATR/價格 ≤ 9%  → 18% trailing
    (float('inf'), 0.22),  # ATR/價格 > 9%  → 22% trailing（高波動保護）
]


def _calc_dynamic_trail_pct(atr: float, price: float) -> float:
    """依 ATR/Price 比值動態計算 trailing stop 百分比"""
    if price <= 0:
        return 0.15
    ratio = atr / price
    for threshold, pct in ATR_TRAIL_MAP:
        if ratio <= threshold:
            return pct
    return 0.22


class ExitEngine:
    """模組⑦ 主動出場管理引擎：五段式狀態機
    
    變更紀錄（v2）：
    - [ATR動態停損] trail_pct 不再固定 0.15，改由 _calc_dynamic_trail_pct()
      根據當前 ATR/Price 比值動態決定（範圍 8%–22%）
    - 每日更新 trail_pct 至 positions 表，保持歸因可查
    """

    def __init__(self, hub: DataHub, finmind: FinMindAPI):
        self.hub = hub
        self.finmind = finmind

    def _calc_atr(self, price_df, period: int = 20) -> Optional[float]:
        if price_df.empty or len(price_df) < period:
            return None
        df = price_df.sort_values('date').tail(period + 1)
        highs  = df['max'].astype(float)
        lows   = df['min'].astype(float)
        closes = df['close'].astype(float).shift(1)
        tr = (highs - lows).combine(
            (highs - closes).abs(), max
        ).combine(
            (lows - closes).abs(), max
        )
        return round(float(tr.tail(period).mean()), 4)

    def _eval_state_transition(
        self,
        state: str,
        current_price: float,
        position: dict,
        dynamic_trail_pct: float,   # ← 新增：由外部傳入動態值
    ) -> tuple[str, str]:
        entry   = float(position['entry_price'])
        stop    = float(position['current_stop_price'] or position['initial_stop_price'])
        r       = float(position['r_amount'])
        highest = float(position['highest_price_seen'] or current_price)
        trail   = position['trailing_stop_price']

        # 停損觸發
        if current_price <= stop:
            return 'STOPPED_OUT', 'STOP_LOSS'

        if state == 'S1_INITIAL_DEFENSE':
            if current_price >= entry + r:
                return 'S2_BREAKOUT_CONFIRM', ''
            return state, ''

        if state == 'S2_BREAKOUT_CONFIRM':
            if current_price >= entry + 2 * r:
                return 'S3_PROFIT_PROTECT', ''
            return state, ''

        if state == 'S3_PROFIT_PROTECT':
            if current_price >= entry + 3 * r:
                return 'S4_TRAILING_STOP', ''
            return state, ''

        if state == 'S4_TRAILING_STOP':
            # ── 使用動態 trail_pct（非固定 0.15）────────────────────────
            new_trail = highest * (1 - dynamic_trail_pct)
            if trail and current_price <= float(trail):
                return 'S5_ACTIVE_EXIT', 'TRAILING_STOP'
            return state, ''

        if state == 'S5_ACTIVE_EXIT':
            return 'CLOSED', 'ACTIVE_EXIT'

        return state, ''

    async def run(self, position_id: str, trade_date: Optional[date] = None) -> dict:
        if trade_date is None:
            trade_date = date.today()

        row = await self.hub.fetchrow(
            "SELECT * FROM positions WHERE id = $1", position_id
        )
        if not row:
            raise ValueError(f"找不到持倉 {position_id}")

        position = dict(row)
        ticker = position['ticker'].strip()

        price_df = await self.finmind.get_stock_price(
            ticker, str(trade_date.replace(day=1))
        )
        if price_df.empty:
            logger.warning("無法取得 %s 價格", ticker)
            return {'position_id': position_id, 'action': 'NO_DATA'}

        price_df  = price_df.sort_values('date')
        cur_price = float(price_df['close'].iloc[-1])

        # ── ATR 動態停損計算 ─────────────────────────────────────────────
        current_atr = self._calc_atr(price_df, period=20)
        if current_atr and cur_price > 0:
            dynamic_trail_pct = _calc_dynamic_trail_pct(current_atr, cur_price)
        else:
            # fallback：使用進場時記錄的 trail_pct
            dynamic_trail_pct = float(position.get('trail_pct') or 0.15)

        logger.debug(
            "%s ATR=%.4f Price=%.2f → dynamic_trail_pct=%.2f%%",
            ticker, current_atr or 0, cur_price, dynamic_trail_pct * 100
        )

        new_state, exit_reason = self._eval_state_transition(
            position['state'], cur_price, position, dynamic_trail_pct
        )

        highest   = max(float(position.get('highest_price_seen') or cur_price), cur_price)
        new_trail = round(highest * (1 - dynamic_trail_pct), 4)

        avg_cost       = float(position['avg_cost'] or position['entry_price'])
        unrealized_pnl = (cur_price - avg_cost) * position['current_shares']
        r_multiple     = (cur_price - avg_cost) / float(position['r_amount']) if position['r_amount'] else 0

        is_closed = new_state in ('STOPPED_OUT', 'CLOSED')

        # ── 寫回 positions（含更新後的 trail_pct）──────────────────────
        await self.hub.execute("""
            UPDATE positions SET
                state               = $1,
                trailing_stop_price = $2,
                highest_price_seen  = $3,
                unrealized_pnl      = $4,
                r_multiple_current  = $5,
                exit_date           = $6,
                exit_price          = $7,
                exit_reason         = $8,
                is_open             = $9,
                trail_pct           = $10
            WHERE id = $11
        """,
            new_state,
            new_trail,
            highest,
            round(unrealized_pnl, 2),
            round(r_multiple, 3),
            trade_date if is_closed else None,
            cur_price  if is_closed else None,
            exit_reason or None,
            not is_closed,
            dynamic_trail_pct,   # ← 動態 trail_pct 寫回 DB
            position_id,
        )

        return {
            'position_id':       position_id,
            'ticker':            ticker,
            'state':             new_state,
            'prev_state':        position['state'],
            'current_price':     cur_price,
            'trailing_stop':     new_trail,
            'dynamic_trail_pct': round(dynamic_trail_pct * 100, 1),  # % 格式
            'current_atr':       current_atr,
            'unrealized_pnl':    round(unrealized_pnl, 2),
            'r_multiple':        round(r_multiple, 3),
            'exit_reason':       exit_reason or None,
            'is_closed':         is_closed,
        }
