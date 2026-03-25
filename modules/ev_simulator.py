# modules/ev_simulator.py
import logging
import random
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

N_SIMULATIONS = 1000


class EVSimulator:
    """模組③ 預期落差EV模擬：蒙地卡羅情境模擬"""

    def __init__(self, hub: DataHub):
        self.hub = hub

    def _simulate(
        self,
        probs: dict,
        score: float,
        regime: str,
    ) -> dict:
        """執行蒙地卡羅模擬，計算 EV"""
        results = []
        for _ in range(N_SIMULATIONS):
            rand = random.random()
            if rand < probs['bull']:
                scenario = 'bull'
            elif rand < probs['bull'] + probs['base']:
                scenario = 'base'
            else:
                scenario = 'bear'

            r = SCENARIO_RETURNS[scenario]
            # 依選股分數調整報酬（高分加成）
            score_adj = (score - 50) / 100
            ret = random.gauss(
                r['mean'] * (1 + score_adj * 0.5),
                r['std']
            )
            results.append(ret)

        results.sort()
        ev = sum(results) / len(results)
        # 各情境平均
        bull_results = [r for r in results if r > 1.5]
        base_results = [r for r in results if -0.5 <= r <= 1.5]
        bear_results = [r for r in results if r < -0.5]

        return {
            'ev_total':  round(ev, 4),
            'ev_bull':   round(sum(bull_results) / max(len(bull_results), 1), 4),
            'ev_base':   round(sum(base_results) / max(len(base_results), 1), 4),
            'ev_bear':   round(sum(bear_results) / max(len(bear_results), 1), 4),
            'win_rate':  round(len([r for r in results if r > 0]) / N_SIMULATIONS, 4),
            'var_95':    round(results[int(N_SIMULATIONS * 0.05)], 4),
            'cvar_95':   round(sum(results[:int(N_SIMULATIONS * 0.05)]) / max(int(N_SIMULATIONS * 0.05), 1), 4),
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
        ticker: str,
        total_score: float,
        trade_date: Optional[date] = None,
    ) -> dict:
        """執行 EV 模擬"""
        if trade_date is None:
            trade_date = date.today()

        # 取得市場環境
        regime_row = await self.hub.fetchrow("""
            SELECT regime, mrs_score FROM market_regime
            WHERE trade_date <= $1
            ORDER BY trade_date DESC LIMIT 1
        """, trade_date)
        regime = regime_row['regime'] if regime_row else 'CHOPPY'

        probs = SCENARIO_PROBS.get(regime, SCENARIO_PROBS['CHOPPY'])
        sim   = self._simulate(probs, total_score, regime)
        valid, reason = self.is_entry_valid(sim['ev_total'], sim['win_rate'])

        result = {
            'ticker':      ticker,
            'trade_date':  trade_date,
            'regime':      regime,
            'total_score': total_score,
            'entry_valid': valid,
            'reason':      reason,
            **sim,
        }

        logger.info(
            "EV模擬完成：%s EV=%.2f 勝率=%.0f%% 進場=%s",
            ticker, sim['ev_total'], sim['win_rate']*100, valid
        )
        return result