# modules/topic_radar.py
import logging
from datetime import date
from typing import Optional
from datahub.data_hub import DataHub

logger = logging.getLogger(__name__)

DEFAULT_TOPICS = [
    'AI算力', 'CoWoS封裝', '車用電子', 'DRAM',
    '伺服器', '散熱模組', '電動車', '儲能',
    '生技醫療', '軍工國防',
]


class TopicRadar:
    """模組① 題材熱度雷達：三週期情緒模型"""

    def __init__(self, hub: DataHub):
        self.hub = hub

    def _calc_zscore(self, values: list[float]) -> float:
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        std  = (sum((v - mean) ** 2 for v in values) / len(values)) ** 0.5
        if std == 0:
            return 0.0
        return round((values[-1] - mean) / std, 4)

    def _calc_percentile(self, values: list[float], current: float) -> float:
        if not values:
            return 50.0
        below = sum(1 for v in values if v <= current)
        return round(below / len(values) * 100, 2)

    def _simulate_raw_count(self, topic: str) -> int:
        import random
        base = {
            'AI算力': 150, 'CoWoS封裝': 80, '車用電子': 60,
            'DRAM': 70, '伺服器': 120, '散熱模組': 55,
            '電動車': 90, '儲能': 65, '生技醫療': 75, '軍工國防': 45,
        }.get(topic, 50)
        return max(0, int(random.gauss(base, base * 0.3)))

    async def get_topic_history(self, topic: str, days: int = 60) -> list[dict]:
        rows = await self.hub.fetch("""
            SELECT trade_date, raw_count FROM topic_heat
            WHERE topic = $1
            ORDER BY trade_date DESC LIMIT $2
        """, topic, days)
        return [dict(r) for r in rows]

    async def run(
        self,
        trade_date: Optional[date] = None,
        topics: Optional[list] = None,
    ) -> list[dict]:
        if trade_date is None:
            trade_date = date.today()
        if topics is None:
            topics = DEFAULT_TOPICS

        results = []
        for topic in topics:
            history   = await self.get_topic_history(topic, 60)
            raw_count = self._simulate_raw_count(topic)
            counts    = [h['raw_count'] for h in history if h['raw_count']] + [raw_count]

            zscore_5d  = self._calc_zscore(counts[-5:])  if len(counts) >= 5  else 0.0
            zscore_20d = self._calc_zscore(counts[-20:]) if len(counts) >= 20 else 0.0
            pct_60d    = self._calc_percentile(counts[:-1], raw_count)
            ma_5d      = round(sum(counts[-5:])  / min(5,  len(counts)), 2)
            ma_20d     = round(sum(counts[-20:]) / min(20, len(counts)), 2)

            alert_type = 'NORMAL'
            if pct_60d > 95:
                alert_type = 'OVERHEATED'
            elif pct_60d < 20:
                alert_type = 'LATENT'

            result = {
                'topic':          topic,
                'trade_date':     trade_date,
                'raw_count':      raw_count,
                'zscore_5d':      zscore_5d,
                'zscore_20d':     zscore_20d,
                'percentile_60d': pct_60d,
                'ma_5d':          ma_5d,
                'ma_20d':         ma_20d,
                'alert_type':     alert_type,
            }
            results.append(result)

            try:
                await self.hub.execute("""
                    INSERT INTO topic_heat (
                        trade_date, topic, raw_count,
                        zscore_5d, zscore_20d, percentile_60d,
                        ma_5d, ma_20d, alert_type
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                    ON CONFLICT (id, trade_date) DO NOTHING
                """,
                    trade_date, topic, raw_count,
                    zscore_5d, zscore_20d, pct_60d,
                    ma_5d, ma_20d, alert_type,
                )
            except Exception as e:
                logger.warning("topic_heat 寫入失敗 %s：%s", topic, e)

        hot    = [r for r in results if r['alert_type'] == 'OVERHEATED']
        latent = [r for r in results if r['alert_type'] == 'LATENT']
        logger.info("題材熱度掃描完成：%d 個題材，過熱=%d 潛伏=%d",
                    len(results), len(hot), len(latent))
        return results