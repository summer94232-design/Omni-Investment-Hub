# modules/signal_bus.py
import logging
import asyncio
from datetime import date
from typing import Any, Callable, Optional
from datahub.data_hub import DataHub
from datahub import redis_keys as rk

logger = logging.getLogger(__name__)

# 訊號類型定義
SIGNAL_TYPES = {
    'ENTRY':         'entry_signal',
    'EXIT':          'exit_signal',
    'ADDON':         'addon_signal',
    'REGIME_CHANGE': 'regime_change',
    'ALERT':         'alert',
    'CIRCUIT_BREAK': 'circuit_breaker',
}


class SignalBus:
    """C2 訊號匯流排：所有模組透過此類發布/訂閱訊號"""

    def __init__(self, hub: DataHub):
        self.hub = hub
        self._subscribers: dict[str, list[Callable]] = {}
        self._signal_queue: asyncio.Queue = asyncio.Queue()
        self._running = False

    # -------------------------------------------------------------------------
    # 發布訊號
    # -------------------------------------------------------------------------
    async def publish(
        self,
        signal_type: str,
        ticker: Optional[str],
        payload: dict,
        source: str = 'SYSTEM',
        trade_date: Optional[date] = None,
    ) -> None:
        """發布訊號到匯流排"""
        if trade_date is None:
            trade_date = date.today()

        signal = {
            'type':       signal_type,
            'ticker':     ticker,
            'payload':    payload,
            'source':     source,
            'trade_date': str(trade_date),
        }

        # 寫入 Redis（供其他服務消費）
        cache_key = rk.key_signal(ticker or 'MARKET', str(trade_date))
        existing = await self.hub.cache.get(cache_key) or []
        existing.append(signal)
        await self.hub.cache.set(cache_key, existing, ttl=rk.TTL_24H)

        # 寫入記憶體 Queue（供本進程消費）
        await self._signal_queue.put(signal)

        # 寫入 decision_log
        await self._log_signal(signal, trade_date)

        logger.info("訊號發布：%s %s from %s", signal_type, ticker, source)

    async def _log_signal(self, signal: dict, trade_date: date) -> None:
        """記錄訊號到 decision_log"""
        try:
            await self.hub.execute("""
                INSERT INTO decision_log (
                    trade_date, ticker, decision_type,
                    signal_source, regime_at_log, raw_context
                ) VALUES ($1, $2, $3, $4, $5, $6)
            """,
                trade_date,
                signal.get('ticker', '')[:6] if signal.get('ticker') else None,
                self._map_signal_type(signal['type']),
                signal['source'],
                signal['payload'].get('regime'),
                __import__('json').dumps(signal['payload'], ensure_ascii=False, default=str),
            )
        except Exception as e:
            logger.warning("decision_log 寫入失敗：%s", e)

    def _map_signal_type(self, signal_type: str) -> str:
        mapping = {
            'ENTRY':         'ENTRY_SIGNAL',
            'EXIT':          'EXIT_SIGNAL',
            'ADDON':         'ADDON_SIGNAL',
            'REGIME_CHANGE': 'REGIME_CHANGE',
            'ALERT':         'CIRCUIT_BREAKER',
            'CIRCUIT_BREAK': 'CIRCUIT_BREAKER',
        }
        return mapping.get(signal_type, 'MANUAL_OVERRIDE')

    # -------------------------------------------------------------------------
    # 訂閱訊號
    # -------------------------------------------------------------------------
    def subscribe(self, signal_type: str, handler: Callable) -> None:
        """訂閱指定類型的訊號"""
        if signal_type not in self._subscribers:
            self._subscribers[signal_type] = []
        self._subscribers[signal_type].append(handler)
        logger.info("訂閱訊號：%s → %s", signal_type, handler.__name__)

    # -------------------------------------------------------------------------
    # 訊號處理迴圈
    # -------------------------------------------------------------------------
    async def start(self) -> None:
        """啟動訊號處理迴圈"""
        self._running = True
        logger.info("訊號匯流排啟動")
        while self._running:
            try:
                signal = await asyncio.wait_for(
                    self._signal_queue.get(), timeout=1.0
                )
                await self._dispatch(signal)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error("訊號處理錯誤：%s", e)

    async def stop(self) -> None:
        """停止訊號處理迴圈"""
        self._running = False
        logger.info("訊號匯流排停止")

    async def _dispatch(self, signal: dict) -> None:
        """派發訊號給訂閱者"""
        signal_type = signal['type']
        handlers = self._subscribers.get(signal_type, [])
        handlers += self._subscribers.get('*', [])  # 萬用訂閱

        for handler in handlers:
            try:
                await handler(signal)
            except Exception as e:
                logger.error("訊號處理器錯誤 %s: %s", handler.__name__, e)

    # -------------------------------------------------------------------------
    # 便捷方法
    # -------------------------------------------------------------------------
    async def emit_entry(self, ticker: str, score: float, regime: str, source: str) -> None:
        await self.publish('ENTRY', ticker, {
            'total_score': score,
            'regime': regime,
        }, source)

    async def emit_exit(self, ticker: str, reason: str, r_multiple: float, source: str) -> None:
        await self.publish('EXIT', ticker, {
            'exit_reason': reason,
            'r_multiple': r_multiple,
        }, source)

    async def emit_regime_change(self, old_regime: str, new_regime: str, mrs: float) -> None:
        await self.publish('REGIME_CHANGE', None, {
            'old_regime': old_regime,
            'new_regime': new_regime,
            'mrs_score': mrs,
        }, 'MACRO_FILTER')

    async def emit_alert(self, message: str, level: str = 'WARN') -> None:
        await self.publish('ALERT', None, {
            'message': message,
            'level': level,
        }, 'SYSTEM')