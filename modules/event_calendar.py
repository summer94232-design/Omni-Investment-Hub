# modules/event_calendar.py
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
        """取得當前生效的事件規則（事前/事中/事後）"""
        if trade_date is None:
            trade_date = date.today()

        cache_key = rk.key_event_rules(str(trade_date))
        cached = await self.hub.cache.get(cache_key)
        if cached:
            return cached

        # [BUG FIX] 原本寫了兩個 BETWEEN 條件，語義重疊且容易造成漏抓或重複。
        # 三個窗口可用一個連續區間完整覆蓋：
        #   事後窗口起點 = trade_date - 2 days
        #   事前窗口終點 = trade_date + 5 days
        # 因此只需一個 BETWEEN $1 AND $2 即可，再由 Python 端依 days_to_event 分類窗口。
        window_start = trade_date - timedelta(days=2)   # 事後：-2 天
        window_end   = trade_date + timedelta(days=5)   # 事前：+5 天

        rows = await self.hub.fetch("""
            SELECT event_date, event_type, name, impact_level,
                   affected_tickers, pre_rule, during_rule, post_rule
            FROM calendar_events
            WHERE event_date BETWEEN $1 AND $2
              AND impact_level >= 3
        """, window_start, window_end)

        active_rules = []
        for row in rows:
            r              = dict(row)
            days_to_event  = (r['event_date'] - trade_date).days

            if days_to_event > 0:
                window = 'PRE'
                rule   = r['pre_rule']
            elif days_to_event == 0:
                window = 'DURING'
                rule   = r['during_rule']
            else:
                window = 'POST'
                rule   = r['post_rule']

            active_rules.append({
                'event_name':    r['name'],
                'event_type':    r['event_type'],
                'event_date':    str(r['event_date']),
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

        upcoming     = await self.get_upcoming_events(7, trade_date)
        active       = await self.get_active_rules(trade_date=trade_date)
        high_impact  = [e for e in upcoming if e['impact_level'] >= 4]

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