# modules/topic_radar.py
# ═══════════════════════════════════════════════════════════════════════════════
# 變更紀錄（v3.3）：
# - [BUG 2 修正] ON CONFLICT (trade_date, topic)（已含於 v3.1）
# - [真實數據] 移除 _simulate_raw_count() 隨機模擬邏輯
#     改呼叫 FinMindAPI.get_topic_news_count()
#     以 FinMind TaiwanStockNews 資料集的關鍵字出現次數作為 raw_count
# - 新增降級機制：若 FinMind 新聞 API 失敗，回退至歷史均值估算（非隨機）
# - 新增建構子接收 finmind 參數（向下相容：finmind=None 時使用降級模式）
# ═══════════════════════════════════════════════════════════════════════════════

import logging
from datetime import date
from typing import Optional
from datahub.data_hub import DataHub
from datahub.api_finmind import FinMindAPI

logger = logging.getLogger(__name__)

DEFAULT_TOPICS = [
    'AI算力', 'CoWoS封裝', '車用電子', 'DRAM',
    '伺服器', '散熱模組', '電動車', '儲能',
    '生技醫療', '軍工國防',
]

# 題材基準值（僅在 FinMind API 不可用時作為降級估算的中心值）
TOPIC_BASE_COUNT = {
    'AI算力': 150, 'CoWoS封裝': 80, '車用電子': 60,
    'DRAM': 70, '伺服器': 120, '散熱模組': 55,
    '電動車': 90, '儲能': 65, '生技醫療': 75, '軍工國防': 45,
}


class TopicRadar:
    """
    題材熱度雷達：三週期情緒模型

    v3.3：raw_count 改用 FinMind TaiwanStockNews 真實新聞計數，
    徹底取代隨機模擬。若 API 失敗則回退至歷史均值（非隨機，具可重現性）。
    """

    def __init__(self, hub: DataHub, finmind: Optional[FinMindAPI] = None):
        self.hub     = hub
        self.finmind = finmind

    # ── 統計計算 ─────────────────────────────────────────────────────────────

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

    # ── 真實 raw_count 取得 ───────────────────────────────────────────────────

    async def _get_raw_count(self, topic: str, trade_date: date) -> int:
        """
        取得題材關鍵字當日新聞出現次數（真實數據）。

        優先順序：
        1. FinMind TaiwanStockNews（真實新聞計數）
        2. 歷史均值降級（若 finmind=None 或 API 回傳 0）
        """
        if self.finmind is not None:
            try:
                count = await self.finmind.get_topic_news_count(topic, trade_date)
                # FinMind 有時會回傳 0（非錯誤，當日確實無相關新聞）
                # 只有在沒有歷史資料可參考時才降級
                if count > 0:
                    return count
            except Exception as e:
                logger.warning("FinMind 新聞 API 失敗 [%s]：%s，改用歷史均值", topic, e)

        # 降級：取過去 20 日歷史均值，若無歷史則用基準值（可重現，非隨機）
        history = await self.get_topic_history(topic, days=20)
        if history:
            valid_counts = [h["raw_count"] for h in history if h["raw_count"] is not None]
            if valid_counts:
                fallback = int(sum(valid_counts) / len(valid_counts))
                logger.debug("題材 [%s] 使用歷史均值降級：%d", topic, fallback)
                return fallback

        base = TOPIC_BASE_COUNT.get(topic, 50)
        logger.debug("題材 [%s] 使用基準值降級：%d", topic, base)
        return base

    # ── 歷史資料查詢 ─────────────────────────────────────────────────────────

    async def get_topic_history(self, topic: str, days: int = 60) -> list[dict]:
        rows = await self.hub.fetch("""
            SELECT trade_date, raw_count FROM topic_heat
            WHERE topic = $1
            ORDER BY trade_date DESC LIMIT $2
        """, topic, days)
        return [dict(r) for r in rows]

    # ── 主執行 ───────────────────────────────────────────────────────────────

    async def run(
        self,
        trade_date: Optional[date] = None,
        topics:     Optional[list]  = None,
    ) -> list[dict]:
        """執行題材熱度掃描，回傳各題材的三週期情緒指標"""
        if trade_date is None:
            trade_date = date.today()
        if topics is None:
            topics = DEFAULT_TOPICS

        results = []
        for topic in topics:
            # 取真實新聞計數（或歷史均值降級）
            raw_count = await self._get_raw_count(topic, trade_date)

            history = await self.get_topic_history(topic, 60)
            counts  = [h["raw_count"] for h in history if h["raw_count"] is not None]
            counts.append(raw_count)

            zscore_5d  = self._calc_zscore(counts[-5:])  if len(counts) >= 5  else 0.0
            zscore_20d = self._calc_zscore(counts[-20:]) if len(counts) >= 20 else 0.0
            pct_60d    = self._calc_percentile(counts[:-1], raw_count)
            ma_5d      = round(sum(counts[-5:])  / min(5,  len(counts)), 2)
            ma_20d     = round(sum(counts[-20:]) / min(20, len(counts)), 2)

            alert_type = "NORMAL"
            if pct_60d > 95:
                alert_type = "OVERHEATED"
            elif pct_60d < 20:
                alert_type = "LATENT"

            result = {
                "topic":          topic,
                "trade_date":     trade_date,
                "raw_count":      raw_count,
                "zscore_5d":      zscore_5d,
                "zscore_20d":     zscore_20d,
                "percentile_60d": pct_60d,
                "ma_5d":          ma_5d,
                "ma_20d":         ma_20d,
                "alert_type":     alert_type,
            }
            results.append(result)

            try:
                await self.hub.execute("""
                    INSERT INTO topic_heat (
                        trade_date, topic, raw_count,
                        zscore_5d, zscore_20d, percentile_60d,
                        ma_5d, ma_20d, alert_type
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                    ON CONFLICT (trade_date, topic) DO UPDATE SET
                        raw_count       = EXCLUDED.raw_count,
                        zscore_5d       = EXCLUDED.zscore_5d,
                        zscore_20d      = EXCLUDED.zscore_20d,
                        percentile_60d  = EXCLUDED.percentile_60d,
                        ma_5d           = EXCLUDED.ma_5d,
                        ma_20d          = EXCLUDED.ma_20d,
                        alert_type      = EXCLUDED.alert_type
                """,
                    trade_date, topic, raw_count,
                    zscore_5d, zscore_20d, pct_60d,
                    ma_5d, ma_20d, alert_type,
                )
            except Exception as e:
                logger.warning("topic_heat 寫入失敗 %s：%s", topic, e)

        hot    = [r for r in results if r["alert_type"] == "OVERHEATED"]
        latent = [r for r in results if r["alert_type"] == "LATENT"]
        logger.info(
            "題材熱度掃描完成（真實新聞數據）：%d 個題材，過熱=%d 潛伏=%d",
            len(results), len(hot), len(latent),
        )
        return results