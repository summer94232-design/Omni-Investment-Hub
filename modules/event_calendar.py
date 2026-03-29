# modules/event_calendar.py
# ═══════════════════════════════════════════════════════════════════════════════
# Bug 修正（v3.1）：
# - [BUG E] get_active_rules() 時間窗口邊界修正
#           原本 BETWEEN 條件語義混淆（兩個重複條件），三個窗口邊界不清
#           修正後改用精確三窗口邊界：
#             事前窗口（PRE）  ：event_date - 3 ≤ trade_date < event_date
#             事中（DURING）   ：trade_date = event_date
#             事後窗口（POST） ：event_date < trade_date ≤ event_date + 3
# ═══════════════════════════════════════════════════════════════════════════════

import logging
from datetime import date, timedelta
from typing import Optional
from datahub.data_hub import DataHub
from datahub import redis_keys as rk

logger = logging.getLogger(__name__)


class EventCalendar:
    """模組⑩ 事件驅動日曆：三窗口規則管理"""

    def __init__(self, hub: DataHub):
        self.hub = hub

    async def get_upcoming_events(
        self,
        days_ahead: int = 7,
        trade_date: Optional[date] = None,
    ) -> list[dict]:
        """取得未來 N 日的高影響力事件"""
        if trade_date is None:
            trade_date = date.today()
        end_date = trade_date + timedelta(days=days_ahead)

        rows = await self.hub.fetch("""
            SELECT id, event_date, event_type, name,
                   impact_level, affected_tickers,
                   pre_rule, during_rule, post_rule,
                   consensus_estimate
            FROM calendar_events
            WHERE event_date BETWEEN $1 AND $2
              AND impact_level >= 3
            ORDER BY event_date, impact_level DESC
        """, trade_date, end_date)

        return [dict(r) for r in rows]

    async def get_active_rules(
        self,
        ticker: Optional[str] = None,
        trade_date: Optional[date] = None,
    ) -> list[dict]:
        """取得當前生效的事件規則（事前/事中/事後）

        三窗口邊界定義（[BUG E 修正]）：
          PRE    事前窗口：event_date - 3 ≤ trade_date < event_date
          DURING 事中    ：trade_date = event_date
          POST   事後窗口：event_date < trade_date ≤ event_date + 3
        """
        if trade_date is None:
            trade_date = date.today()

        cache_key = rk.key_event_rules(str(trade_date))
        cached = await self.hub.cache.get(cache_key)
        if cached:
            return cached

        # [BUG E 修正] 用精確的三窗口邊界取代原本語義混淆的 BETWEEN
        # 完整覆蓋範圍：trade_date 位於 [event_date-3, event_date+3] 的所有事件
        window_start = trade_date - timedelta(days=3)   # 事後窗口最遠覆蓋：+3 天前
        window_end   = trade_date + timedelta(days=3)   # 事前窗口最遠覆蓋：-3 天後

        rows = await self.hub.fetch("""
            SELECT event_date, event_type, name, impact_level,
                   affected_tickers, pre_rule, during_rule, post_rule
            FROM calendar_events
            WHERE event_date BETWEEN $1 AND $2
              AND impact_level >= 3
        """, window_start, window_end)

        active_rules = []
        for row in rows:
            r             = dict(row)
            event_date    = r['event_date']
            days_to_event = (event_date - trade_date).days

            # [BUG E 修正] 精確三窗口分類
            if days_to_event > 0:
                # 事前窗口：1 ≤ days_to_event ≤ 3（event_date 在 trade_date 之後）
                window = 'PRE'
                rule   = r['pre_rule']
            elif days_to_event == 0:
                # 事中：當天
                window = 'DURING'
                rule   = r['during_rule']
            else:
                # 事後窗口：-3 ≤ days_to_event ≤ -1（event_date 在 trade_date 之前）
                window = 'POST'
                rule   = r['post_rule']

            # 跳過無規則的窗口（rule 為 None 表示該窗口無需處理）
            if rule is None:
                continue

            active_rules.append({
                'event_name':    r['name'],
                'event_type':    r['event_type'],
                'event_date':    str(event_date),
                'days_to_event': days_to_event,
                'window':        window,
                'rule':          rule,
                'impact_level':  r['impact_level'],
            })

        await self.hub.cache.set(cache_key, active_rules, ttl=rk.TTL_24H)
        return active_rules

    async def add_event(
        self,
        event_date: date,
        event_type: str,
        name: str,
        impact_level: int,
        affected_tickers: Optional[list] = None,
        pre_rule: Optional[dict] = None,
    ) -> int:
        """新增財經事件"""
        import json
        row = await self.hub.fetchrow("""
            INSERT INTO calendar_events (
                event_date, event_type, name,
                impact_level, affected_tickers, pre_rule
            ) VALUES ($1,$2,$3,$4,$5,$6)
            RETURNING id
        """,
            event_date,
            event_type,
            name,
            impact_level,
            affected_tickers or [],
            json.dumps(pre_rule) if pre_rule else None,
        )
        logger.info("新增事件：%s %s", event_date, name)
        return row['id']

    async def run(self, trade_date: Optional[date] = None) -> dict:
        """執行事件日曆掃描"""
        if trade_date is None:
            trade_date = date.today()

        upcoming    = await self.get_upcoming_events(7, trade_date)
        active      = await self.get_active_rules(trade_date=trade_date)
        high_impact = [e for e in upcoming if e['impact_level'] >= 4]

        logger.info(
            "事件日曆：未來7日 %d 個事件，%d 個高影響力",
            len(upcoming), len(high_impact),
        )
        return {
            'trade_date':        str(trade_date),
            'upcoming_count':    len(upcoming),
            'high_impact_count': len(high_impact),
            'active_rules':      len(active),
            'upcoming_events':   upcoming,
        }