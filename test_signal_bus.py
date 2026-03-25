# test_signal_bus.py
import asyncio
import yaml
from datetime import date
from datahub.data_hub import DataHub
from modules.signal_bus import SignalBus

async def main():
    with open('config.yaml', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)

    hub = DataHub('config.yaml')
    await hub.connect()

    bus = SignalBus(hub)

    # 測試訂閱
    received = []
    async def on_entry(signal):
        received.append(signal)
        print(f"  收到進場訊號：{signal['ticker']} 分數={signal['payload']['total_score']}")

    async def on_regime(signal):
        received.append(signal)
        print(f"  收到環境變化：{signal['payload']['old_regime']} → {signal['payload']['new_regime']}")

    bus.subscribe('ENTRY', on_entry)
    bus.subscribe('REGIME_CHANGE', on_regime)

    # 發布測試訊號
    print("=== 測試訊號發布 ===")
    await bus.emit_entry('2330', 72.5, 'CHOPPY', 'SELECTION_ENGINE')
    await bus.emit_entry('2412', 68.3, 'CHOPPY', 'SELECTION_ENGINE')
    await bus.emit_regime_change('CHOPPY', 'WEAK_BULL', 65.0)
    await bus.emit_alert('測試警告訊息', 'WARN')

    # 處理 Queue 裡的訊號
    print("\n=== 訊號派發 ===")
    while not bus._signal_queue.empty():
        signal = await bus._signal_queue.get()
        await bus._dispatch(signal)

    print(f"\n共收到 {len(received)} 個訂閱訊號")

    # 確認 decision_log
    rows = await hub.fetch("""
        SELECT decision_type, ticker, signal_source
        FROM decision_log
        WHERE trade_date = $1
        ORDER BY logged_at DESC
        LIMIT 5
    """, date.today())

    print("\n=== decision_log 最新 5 筆 ===")
    for r in rows:
        print(f"  {r['decision_type']} | {r['ticker']} | {r['signal_source']}")

    await hub.close()

asyncio.run(main())