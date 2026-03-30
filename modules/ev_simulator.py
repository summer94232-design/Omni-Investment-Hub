# modules/ev_simulator.py
# ═══════════════════════════════════════════════════════════════════════════════
# 變更紀錄（v3.3.1）：
# - [🟢 建議修復] _simulate() 新增 seed 參數：
#     日常執行：seed=None（保留隨機性）
#     回測對比：傳入固定 seed（確保結果可重現）
#     WalkForward 呼叫時可傳入 seed=42 進行歷史對比
# ═══════════════════════════════════════════════════════════════════════════════

import logging
import numpy as np
from datetime import date
from typing import Optional
from datahub.data_hub import DataHub
from datahub import redis_keys as rk

logger = logging.getLogger(__name__)

# 情境機率（依市場環境調整）
SCENARIO_PROBS = {
    'BULL_TREND': {'bull': 0.55, 'base': 0.30, 'bear': 0.15},
    'WEAK_BULL':  {'bull': 0.40, 'base': 0.35, 'bear': 0.25},
    'CHOPPY':     {'bull': 0.30, 'base': 0.35, 'bear': 0.35},
    'BEAR_TREND': {'bull': 0.15, 'base': 0.30, 'bear': 0.55},
}

# 各情境報酬假設（R-Multiple）
SCENARIO_RETURNS = {
    'bull': {'mean': 3.0,  'std': 1.0},
    'base': {'mean': 1.0,  'std': 0.5},
    'bear': {'mean': -1.0, 'std': 0.3},
}

N_SIMULATIONS = 10000


class EVSimulator:
    """模組③ 預期落差EV模擬：蒙地卡羅情境模擬"""

    def __init__(self, hub: DataHub):
        self.hub = hub

    def _simulate(
        self,
        probs:  dict,
        score:  float,
        regime: str,
        seed:   Optional[int] = None,
    ) -> dict:
        """
        執行蒙地卡羅模擬，計算 EV。

        🟢 建議修復：新增 seed 參數。
          - seed=None（預設）：每次執行使用不同種子，保留日常隨機性
          - seed=整數（如 seed=42）：固定種子，供回測對比確保結果可重現

        使用方式：
          日常執行：ev._simulate(probs, score, regime)
          回測對比：ev._simulate(probs, score, regime, seed=42)
        """
        rng = np.random.default_rng(seed)  # seed=None 時行為與原本相同

        rand      = rng.random(N_SIMULATIONS)
        score_adj = (score - 50) / 100
        results   = np.zeros(N_SIMULATIONS)

        bull_mask = rand < probs['bull']
        n_bull    = bull_mask.sum()
        if n_bull > 0:
            mean = SCENARIO_RETURNS['bull']['mean'] * (1 + score_adj * 0.5)
            results[bull_mask] = rng.normal(mean, SCENARIO_RETURNS['bull']['std'], n_bull)

        base_mask = (rand >= probs['bull']) & (rand < probs['bull'] + probs['base'])
        n_base    = base_mask.sum()
        if n_base > 0:
            mean = SCENARIO_RETURNS['base']['mean'] * (1 + score_adj * 0.5)
            results[base_mask] = rng.normal(mean, SCENARIO_RETURNS['base']['std'], n_base)

        bear_mask = rand >= probs['bull'] + probs['base']
        n_bear    = bear_mask.sum()
        if n_bear > 0:
            mean = SCENARIO_RETURNS['bear']['mean'] * (1 + score_adj * 0.5)
            results[bear_mask] = rng.normal(mean, SCENARIO_RETURNS['bear']['std'], n_bear)

        results.sort()

        bull_r  = results[results > 1.5]
        base_r  = results[(results >= -0.5) & (results <= 1.5)]
        bear_r  = results[results < -0.5]
        var_idx = int(N_SIMULATIONS * 0.05)

        return {
            'ev_total': round(float(results.mean()), 4),
            'ev_bull':  round(float(bull_r.mean()) if len(bull_r) > 0 else 0.0, 4),
            'ev_base':  round(float(base_r.mean()) if len(base_r) > 0 else 0.0, 4),
            'ev_bear':  round(float(bear_r.mean()) if len(bear_r) > 0 else 0.0, 4),
            'win_rate': round(float((results > 0).sum()) / N_SIMULATIONS, 4),
            'var_95':   round(float(results[var_idx]), 4),
            'cvar_95':  round(float(results[:var_idx].mean()) if var_idx > 0 else 0.0, 4),
        }

    def is_entry_valid(self, ev: float, win_rate: float) -> tuple[bool, str]:
        """判斷是否符合進場條件"""
        if ev < 0.5:
            return False, f"EV={ev:.2f} 低於門檻 0.5R"
        if win_rate < 0.40:
            return False, f"勝率={win_rate*100:.0f}% 低於門檻 40%"
        return True, "符合進場條件"

    async def run(
        self,
        ticker:      str,
        total_score: float,
        trade_date:  Optional[date] = None,
        seed:        Optional[int]  = None,   # 新增：回測時可傳入固定種子
    ) -> dict:
        """
        執行 EV 模擬。
        seed 參數說明：
          日常執行不傳（隨機），Walk-Forward 回測比較時傳入 seed=42 確保可重現。
        """
        if trade_date is None:
            trade_date = date.today()

        regime_row = await self.hub.fetchrow("""
            SELECT regime, mrs_score FROM market_regime
            WHERE trade_date <= $1
            ORDER BY trade_date DESC LIMIT 1
        """, trade_date)
        regime = regime_row['regime'] if regime_row else 'CHOPPY'

        probs = SCENARIO_PROBS.get(regime, SCENARIO_PROBS['CHOPPY'])
        sim   = self._simulate(probs, total_score, regime, seed=seed)
        valid, reason = self.is_entry_valid(sim['ev_total'], sim['win_rate'])

        result = {
            'ticker':      ticker,
            'trade_date':  trade_date,
            'regime':      regime,
            'total_score': total_score,
            'entry_valid': valid,
            'reason':      reason,
            'seed_used':   seed,  # 方便 debug 確認是否使用固定種子
            **sim,
        }

        logger.info(
            "EV模擬完成：%s EV=%.2f 勝率=%.0f%% 進場=%s seed=%s",
            ticker, sim['ev_total'], sim['win_rate'] * 100, valid, seed,
        )
        return result