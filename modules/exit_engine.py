# modules/exit_engine.py
# ═══════════════════════════════════════════════════════════════════════════════
# 變更紀錄（v3）：
# - [動態 ATR 停損] S1–S4 的 ATR 倍數改為可接收 MacroFilter Regime 訊號的動態值：
#     BULL_TREND：S1 停損 3.0 ATR，洗盤空間充足
#     CHOPPY    ：S1 縮緊至 2.0 ATR，S2 迅速移至成本價
#     BEAR_TREND：S1 極端縮緊 1.5 ATR，獲利 0.3R 即進入 S3
# - [S4 Trailing Stop 改進] 改用「最高價 - (ATR20 × K)」公式取代固定百分比：
#     當獲利超過 2R 時，K 自動調降 0.5 以鎖定末端利潤
#     同時保留原 ATR/Price 百分比計算作為備援（無最高價資料時）
# - [Regime 注入] run() 新增可選參數 regime，由 scheduler 傳入最新宏觀環境
# ═══════════════════════════════════════════════════════════════════════════════

import logging
from datetime import date
from typing import Optional
from datahub.data_hub import DataHub
from datahub.api_finmind import FinMindAPI
from datahub import redis_keys as rk

logger = logging.getLogger(__name__)

STATE_TRANSITIONS = {
    'S1_INITIAL_DEFENSE':  ['S2_BREAKOUT_CONFIRM', 'STOPPED_OUT'],
    'S2_BREAKOUT_CONFIRM': ['S3_PROFIT_PROTECT', 'STOPPED_OUT'],
    'S3_PROFIT_PROTECT':   ['S4_TRAILING_STOP', 'STOPPED_OUT'],
    'S4_TRAILING_STOP':    ['S5_ACTIVE_EXIT', 'STOPPED_OUT'],
    'S5_ACTIVE_EXIT':      ['CLOSED'],
}

# ── [v3] Regime 動態 ATR 倍數設定 ─────────────────────────────────────────────
# 格式：regime → {S1_atr_mult, S2_to_cost_at_r, S3_early_trigger_r}
#   S1_atr_mult        : S1 初始停損距離（ATR 的倍數）
#   S2_to_cost_at_r    : 進入 S2 後，當獲利達此 R 值時立即把停損移至成本（None=維持原邏輯）
#   S3_early_trigger_r : 幾 R 觸發 S3（原本是 2R，BEAR 模式提前到 0.3R）
REGIME_ATR_CONFIG: dict[str, dict] = {
    'BULL_TREND': {
        'S1_atr_mult':         3.0,   # 寬鬆：讓趨勢有洗盤空間
        'S2_to_cost_at_r':     None,  # 不強制移至成本，自然跑
        'S3_early_trigger_r':  2.0,   # 原始設定：2R 進 S3
    },
    'WEAK_BULL': {
        'S1_atr_mult':         2.5,
        'S2_to_cost_at_r':     1.0,   # 1R 時停損移至成本
        'S3_early_trigger_r':  2.0,
    },
    'CHOPPY': {
        'S1_atr_mult':         2.0,   # 縮緊：震盪市不給太多虧損空間
        'S2_to_cost_at_r':     0.5,   # 0.5R 就移至成本，快速保護
        'S3_early_trigger_r':  1.5,   # 更早進入獲利保護
    },
    'BEAR_TREND': {
        'S1_atr_mult':         1.5,   # 極度縮緊
        'S2_to_cost_at_r':     0.3,   # 幾乎立即移至成本
        'S3_early_trigger_r':  0.3,   # 0.3R 就觸發 S3 獲利保護
    },
}

# 預設配置（Regime 未知時使用）
DEFAULT_ATR_CONFIG = REGIME_ATR_CONFIG['CHOPPY']

# ── [v3] S4 Trailing Stop 公式參數 ────────────────────────────────────────────
# Stop = Highest_Price - (ATR20 × K)
# 獲利 >= 2R 時，K 自動降低 0.5 以鎖定末端利潤
S4_ATR_K_BASE      = 2.5   # 基準倍數（獲利 < 2R 時使用）
S4_ATR_K_LOCKED    = 2.0   # 鎖利倍數（獲利 >= 2R 時使用）
S4_LOCK_PROFIT_R   = 2.0   # 觸發鎖利的 R 閾值

# ── 原有 ATR Trail 百分比（備援用，無 ATR 資料時使用）──────────────────────────
ATR_TRAIL_MAP = [
    (0.02,  0.08),
    (0.04,  0.12),
    (0.06,  0.15),
    (0.09,  0.18),
    (float('inf'), 0.22),
]


def _calc_dynamic_trail_pct(atr: float, price: float) -> float:
    """依 ATR/Price 比值動態計算 trailing stop 百分比（備援）"""
    if price <= 0:
        return 0.15
    ratio = atr / price
    for threshold, pct in ATR_TRAIL_MAP:
        if ratio <= threshold:
            return pct
    return 0.22


def _get_regime_config(regime: Optional[str]) -> dict:
    """取得當前 Regime 對應的 ATR 參數配置"""
    if regime and regime in REGIME_ATR_CONFIG:
        return REGIME_ATR_CONFIG[regime]
    return DEFAULT_ATR_CONFIG


class ExitEngine:
    """模組⑦ 主動出場管理引擎：五段式狀態機

    變更紀錄（v3）：
    - [動態 ATR] S1 停損倍數由 Regime 決定（BULL=3.0、CHOPPY=2.0、BEAR=1.5）
    - [快速保本] CHOPPY/BEAR 模式在 S2 達到特定 R 值時立即移損至成本
    - [S4 公式] 改用 Highest - (ATR × K)，獲利超 2R 時 K 縮小以鎖定利潤
    - [Regime 參數] run() 可接收外部傳入 regime 字串，也可自動從 DB 查詢
    """

    def __init__(self, hub: DataHub, finmind: FinMindAPI):
        self.hub     = hub
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

    def _calc_s4_trailing_stop(
        self,
        highest_price: float,
        current_atr: float,
        r_multiple: float,
    ) -> float:
        """
        [v3] S4 移動止損公式：Highest - (ATR × K)

        當獲利超過 S4_LOCK_PROFIT_R（2R）時，K 從 2.5 縮至 2.0，
        鎖定更多末端利潤（止損線更貼近高點）。

        若 ATR 為 0 或 highest_price 為 0，回傳 0.0（由 caller 使用備援方案）
        """
        if current_atr <= 0 or highest_price <= 0:
            return 0.0

        k = S4_ATR_K_LOCKED if r_multiple >= S4_LOCK_PROFIT_R else S4_ATR_K_BASE
        stop = highest_price - (current_atr * k)
        logger.debug(
            "S4 Trailing Stop：Highest=%.2f ATR=%.4f K=%.1f (R=%.2f) → Stop=%.4f",
            highest_price, current_atr, k, r_multiple, stop
        )
        return round(stop, 4)

    def _eval_state_transition(
        self,
        state: str,
        current_price: float,
        position: dict,
        dynamic_trail_pct: float,
        regime_config: dict,        # ← v3 新增：動態 Regime 配置
        current_atr: Optional[float],
    ) -> tuple[str, str]:
        """
        評估狀態轉換，回傳 (new_state, exit_reason)

        v3 變更：
        - S1：停損距離改用 regime_config['S1_atr_mult'] × atr_at_entry
          若無 ATR 資料，fallback 使用原 initial_stop_price
        - S2：若 regime_config['S2_to_cost_at_r'] 不為 None，在達到指定 R 值
          時立即把 current_stop_price 移至成本（entry_price）
        - S3：觸發 S4 的門檻改為 regime_config['S3_early_trigger_r']
        - S4：優先使用 _calc_s4_trailing_stop()，備援使用固定百分比
        """
        entry       = float(position['entry_price'])
        r           = float(position['r_amount'])
        highest     = float(position['highest_price_seen'] or current_price)
        trail_price = position.get('trailing_stop_price')
        avg_cost    = float(position['avg_cost'] or entry)
        atr_entry   = float(position.get('atr_at_entry') or 0)

        # ── 計算動態 S1 停損線 ───────────────────────────────────────────
        s1_mult = regime_config['S1_atr_mult']
        if atr_entry > 0:
            dynamic_stop = entry - atr_entry * s1_mult
        else:
            dynamic_stop = float(position['initial_stop_price'])   # fallback

        # 取較高的停損線（不能比原始停損更寬鬆）
        effective_stop = max(
            dynamic_stop,
            float(position['current_stop_price'] or position['initial_stop_price'])
        )

        # ── 停損觸發檢查 ─────────────────────────────────────────────────
        if current_price <= effective_stop:
            return 'STOPPED_OUT', 'STOP_LOSS'

        r_current = (current_price - avg_cost) / r if r > 0 else 0

        # ── S1：初始防守 ─────────────────────────────────────────────────
        if state == 'S1_INITIAL_DEFENSE':
            if r > 0 and current_price >= entry + r:
                return 'S2_BREAKOUT_CONFIRM', ''
            return state, ''

        # ── S2：突破確認 ─────────────────────────────────────────────────
        if state == 'S2_BREAKOUT_CONFIRM':
            # [v3] 快速保本：在達到指定 R 值時立即把停損移至成本
            move_to_cost_r = regime_config.get('S2_to_cost_at_r')
            if move_to_cost_r is not None and r_current >= move_to_cost_r:
                # 停損移至成本：update current_stop_price（在 run() 中處理）
                logger.info(
                    "快速保本觸發（Regime=%s）：%s 獲利=%.2fR ≥ %.1fR，停損移至成本 %.2f",
                    regime_config.get('_regime', '?'), '?', r_current, move_to_cost_r, avg_cost
                )
                # 標記需要移動停損（由 run() 執行 DB 更新）
                position['_move_stop_to_cost'] = avg_cost

            if r > 0 and current_price >= entry + 2 * r:
                return 'S3_PROFIT_PROTECT', ''
            return state, ''

        # ── S3：獲利保護 ─────────────────────────────────────────────────
        if state == 'S3_PROFIT_PROTECT':
            s3_trigger_r = regime_config.get('S3_early_trigger_r', 2.0)
            # [v3] 動態觸發門檻（BEAR = 0.3R，BULL = 2.0R）
            if r > 0 and r_current >= s3_trigger_r + 1:
                return 'S4_TRAILING_STOP', ''
            return state, ''

        # ── S4：移動止損 ─────────────────────────────────────────────────
        if state == 'S4_TRAILING_STOP':
            # [v3] 優先使用 ATR 公式計算移動止損
            if current_atr and current_atr > 0:
                new_trail = self._calc_s4_trailing_stop(highest, current_atr, r_current)
            else:
                # 備援：使用固定百分比
                new_trail = highest * (1 - dynamic_trail_pct)

            if trail_price and current_price <= float(trail_price):
                return 'S5_ACTIVE_EXIT', 'TRAILING_STOP'
            return state, ''

        # ── S5：主動出場 ─────────────────────────────────────────────────
        if state == 'S5_ACTIVE_EXIT':
            return 'CLOSED', 'ACTIVE_EXIT'

        return state, ''

    async def _get_current_regime(self, trade_date: date) -> str:
        """從 Redis 或 DB 取得最新 Regime"""
        cached = await self.hub.cache.get(rk.key_market_regime_latest())
        if cached and cached.get('regime'):
            return cached['regime']
        row = await self.hub.fetchrow("""
            SELECT regime FROM market_regime
            WHERE trade_date <= $1
            ORDER BY trade_date DESC LIMIT 1
        """, trade_date)
        return row['regime'] if row else 'CHOPPY'

    async def run(
        self,
        position_id: str,
        trade_date: Optional[date] = None,
        regime: Optional[str] = None,   # ← v3 新增：可由 scheduler 傳入
    ) -> dict:
        """執行出場引擎（v3：Regime 感知動態 ATR）"""
        if trade_date is None:
            trade_date = date.today()

        row = await self.hub.fetchrow(
            "SELECT * FROM positions WHERE id = $1", position_id
        )
        if not row:
            raise ValueError(f"找不到持倉 {position_id}")

        position = dict(row)
        ticker   = position['ticker'].strip()

        # ── 取得當前 Regime（v3）────────────────────────────────────────
        if regime is None:
            regime = await self._get_current_regime(trade_date)
        regime_config = _get_regime_config(regime)
        regime_config['_regime'] = regime   # 帶入 config 供 log 使用

        logger.info(
            "出場引擎（v3）：%s Regime=%s S1_ATR=%.1fx S2_cost_at=%.1fR",
            ticker, regime,
            regime_config['S1_atr_mult'],
            regime_config.get('S2_to_cost_at_r') or -1,
        )

        # ── 取得價格資料 ─────────────────────────────────────────────────
        price_df = await self.finmind.get_stock_price(
            ticker, str(trade_date.replace(day=1))
        )
        if price_df.empty:
            logger.warning("無法取得 %s 價格", ticker)
            return {'position_id': position_id, 'action': 'NO_DATA'}

        price_df  = price_df.sort_values('date')
        cur_price = float(price_df['close'].iloc[-1])

        # ── ATR 計算 ─────────────────────────────────────────────────────
        current_atr = self._calc_atr(price_df, period=20)

        # 備援 trail_pct（無 ATR 資料時使用）
        if current_atr and cur_price > 0:
            dynamic_trail_pct = _calc_dynamic_trail_pct(current_atr, cur_price)
        else:
            dynamic_trail_pct = float(position.get('trail_pct') or 0.15)

        # ── 狀態轉換評估（v3）────────────────────────────────────────────
        new_state, exit_reason = self._eval_state_transition(
            position['state'],
            cur_price,
            position,
            dynamic_trail_pct,
            regime_config,
            current_atr,
        )

        # ── 計算 S4 Trailing Stop（v3 公式）──────────────────────────────
        highest       = max(float(position.get('highest_price_seen') or cur_price), cur_price)
        avg_cost      = float(position['avg_cost'] or position['entry_price'])
        r             = float(position['r_amount'] or 1)
        r_multiple    = (cur_price - avg_cost) / r if r > 0 else 0

        if position['state'] in ('S4_TRAILING_STOP', 'S3_PROFIT_PROTECT') and current_atr:
            new_trail = self._calc_s4_trailing_stop(highest, current_atr, r_multiple)
            if new_trail <= 0:  # ATR 公式失效，備援
                new_trail = round(highest * (1 - dynamic_trail_pct), 4)
        else:
            new_trail = round(highest * (1 - dynamic_trail_pct), 4)

        unrealized_pnl = (cur_price - avg_cost) * float(position['current_shares'])
        is_closed      = new_state in ('STOPPED_OUT', 'CLOSED')

        # ── 快速保本停損更新（S2 Regime 觸發）───────────────────────────
        new_stop = float(position.get('current_stop_price') or position['initial_stop_price'])
        if '_move_stop_to_cost' in position:
            new_stop = max(new_stop, position['_move_stop_to_cost'])
            logger.info("快速保本：%s 停損更新至成本 %.2f", ticker, new_stop)

        # ── 動態 S1 停損更新（Regime 收緊後調整現有持倉）────────────────
        atr_entry = float(position.get('atr_at_entry') or 0)
        if position['state'] == 'S1_INITIAL_DEFENSE' and atr_entry > 0:
            regime_stop = float(position['entry_price']) - atr_entry * regime_config['S1_atr_mult']
            # 只允許「往上移動」（收緊），不允許把停損移得比原始更遠
            if regime_stop > new_stop:
                new_stop = round(regime_stop, 4)
                logger.info(
                    "Regime 停損收緊：%s [%s] S1 停損 %.2f → %.2f (%.1f ATR)",
                    ticker, regime, float(position['current_stop_price'] or 0),
                    new_stop, regime_config['S1_atr_mult']
                )

        # ── 寫回 positions ────────────────────────────────────────────────
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
                trail_pct           = $10,
                current_stop_price  = $11
            WHERE id = $12
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
            dynamic_trail_pct,
            new_stop,
            position_id,
        )

        # ── 記錄狀態變化 decision_log ────────────────────────────────────
        if new_state != position['state']:
            await self.hub.execute("""
                INSERT INTO decision_log (
                    trade_date, ticker, decision_type, signal_source,
                    position_id, notes
                ) VALUES ($1,$2,'STATE_CHANGE','EXIT_ENGINE',$3,$4)
            """,
                trade_date, position['ticker'],
                position_id,
                f"{position['state']} → {new_state} [Regime={regime} "
                f"S1_ATR={regime_config['S1_atr_mult']}x]",
            )

        return {
            'position_id':         position_id,
            'ticker':              ticker,
            'state':               new_state,
            'prev_state':          position['state'],
            'current_price':       cur_price,
            'trailing_stop':       new_trail,
            'current_stop_price':  new_stop,
            'dynamic_trail_pct':   round(dynamic_trail_pct * 100, 1),
            'current_atr':         current_atr,
            'regime':              regime,
            's1_atr_mult':         regime_config['S1_atr_mult'],
            'r_multiple':          round(r_multiple, 3),
            'unrealized_pnl':      round(unrealized_pnl, 2),
            'exit_reason':         exit_reason or None,
            'is_closed':           is_closed,
        }