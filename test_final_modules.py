# test_final_modules.py
import asyncio
import yaml
from datetime import date
from datahub.data_hub import DataHub
from datahub.api_fugle import FugleAPI
from datahub.api_telegram import TelegramBot
from modules.execution_gate import ExecutionGate
from modules.topic_radar import TopicRadar

async def main():
    with open('config.yaml', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)

    hub      = DataHub('config.yaml')
    await hub.connect()
    telegram = TelegramBot(cfg['telegram']['bot_token'], cfg['telegram']['chat_id'])
    fugle    = FugleAPI(
        cfg['fugle']['api_key'],
        cfg['fugle']['trade_token'],
        cfg['fugle']['account'],
        paper_trading=True,
    )
    td = date(2026, 2, 28)

    print("=== ① 題材熱度雷達 ===")
    radar   = TopicRadar(hub)
    results = await radar.run(td)
    for r in sorted(results, key=lambda x: x['percentile_60d'], reverse=True)[:3]:
        print(f"  {r['topic']}：計數={r['raw_count']} 百分位={r['percentile_60d']:.0f}% 狀態={r['alert_type']}")

    print("\n=== ⑪ 執行閘門（紙上交易）===")
    gate   = ExecutionGate(hub, fugle, telegram)
    result = await gate.execute(
        ticker='2330', side='BUY', quantity=1,
        price=850.0, signal_source='TEST',
        exec_mode='PAPER', trade_date=td,
    )
    print(f"  執行={result['executed']} 拒絕原因={result['block_reason']}")
    print(f"  層①={result['gate1']} 層②={result['gate2']} 層③={result['gate3']}")

    await fugle.close()
    await telegram.close()
    await hub.close()
    print("\n✅ 所有模組完成！")

asyncio.run(main())