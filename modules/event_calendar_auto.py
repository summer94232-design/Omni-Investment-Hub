# modules/event_calendar_auto.py
# ═══════════════════════════════════════════════════════════════════════════════
# 財經事件自動同步器（EventCalendarAutoSync）
#
# 說明：
#   原 EventCalendar 的 calendar_events 只能透過儀表板手動新增，
#   沒有自動拉取 Fed / CPI / PMI 等重要發布日的機制。
#   本模組填補此缺口，每週一（或手動觸發）自動從 FRED 拉取未來 14 天
#   的重大數據發布日，寫入 calendar_events 資料表。
#
# 整合說明：
#   在 scheduler.py 加入 Step 0（MacroFilter 之前）：
#       sync = EventCalendarAutoSync(hub, fred)
#       await sync.run()
#   或在 dashboard.py 的 /api/run-scheduler 路由加入週一判斷自動觸發。
# ═══════════════════════════════════════════════════════════════════════════════

import logging
from datetime import date, timedelta
from typing import Optional
from datahub.data_hub import DataHub
from datahub.api_fred import FredAPI

logger = logging.getLogger(__name__)

# 事件類型對照（FRED release name → calendar_events.event_type）
RELEASE_TYPE_MAP = {
    "Employment Situation (NFP)":  "NFP",
    "Consumer Price Index (CPI)":  "CPI",
    "GDP":                         "GDP",
    "Producer Price Index (PPI)":  "PPI",
    "FOMC Meeting":                "FED",
    "ISM Manufacturing PMI":       "PMI",
    "Retail Sales":                "OTHER",
    "Housing Starts":              "OTHER",
}


class EventCalendarAutoSync:
    """
    自動從 FRED 拉取財經日曆，寫入 calendar_events。
    避免手動維護財經事件，確保 EventCalendar 模組有完整的前置資料。
    """

    def __init__(self, hub: DataHub, fred: FredAPI):
        self.hub  = hub
        self.fred = fred

    async def run(
        self,
        days_ahead: int = 14,
        force:      bool = False,
    ) -> dict:
        """
        自動同步財經日曆。

        參數：
            days_ahead: 拉取未來幾天的發布計畫（預設 14 天）
            force:      True = 無論星期幾都執行；False = 只在週一執行

        回傳：
            {'synced': int, 'skipped': int, 'errors': int, 'events': list}
        """
        today = date.today()

        # 預設只在週一執行（避免每日重複呼叫 FRED API）
        if not force and today.weekday() != 0:
            logger.info("財經日曆自動同步：今日非週一（%s），跳過", today.strftime("%A"))
            return {"synced": 0, "skipped": 0, "errors": 0, "events": [], "reason": "not_monday"}

        logger.info("財經日曆自動同步開始：未來 %d 天", days_ahead)

        try:
            releases = await self.fred.get_upcoming_releases(days_ahead=days_ahead)
        except Exception as e:
            logger.error("FRED 財經日曆 API 失敗：%s", e)
            return {"synced": 0, "skipped": 0, "errors": 1, "events": [], "reason": str(e)}

        synced = 0
        skipped = 0
        errors  = 0
        events  = []

        for rel in releases:
            try:
                event_date   = date.fromisoformat(rel["release_date"])
                event_name   = rel["release_name"]
                event_type   = RELEASE_TYPE_MAP.get(event_name, "OTHER")
                impact_level = rel["impact_level"]

                # 檢查是否已存在（避免重複寫入）
                existing = await self.hub.fetchrow("""
                    SELECT id FROM calendar_events
                    WHERE event_date = $1 AND name = $2
                """, event_date, event_name)

                if existing:
                    skipped += 1
                    continue

                # 寫入
                await self.hub.execute("""
                    INSERT INTO calendar_events (
                        event_date, event_type, name, impact_level
                    ) VALUES ($1, $2, $3, $4)
                """, event_date, event_type, event_name, impact_level)

                synced += 1
                events.append({
                    "date":         str(event_date),
                    "name":         event_name,
                    "type":         event_type,
                    "impact_level": impact_level,
                })
                logger.info("財經事件已寫入：%s %s（影響等級 %d）", event_date, event_name, impact_level)

            except Exception as e:
                errors += 1
                logger.warning("財經事件寫入失敗 [%s]：%s", rel.get("release_name"), e)

        logger.info(
            "財經日曆自動同步完成：新增=%d 已存在=%d 錯誤=%d",
            synced, skipped, errors,
        )
        return {
            "synced":  synced,
            "skipped": skipped,
            "errors":  errors,
            "events":  events,
        }