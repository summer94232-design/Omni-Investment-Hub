# test_new_modules.py
import asyncio
import yaml
from datetime import date
from datahub.data_hub import DataHub
from datahub.api_telegram import TelegramBot
from modules.position_manager import PositionManager
from modules.ev_simulator import EVSimulator
from modules.attribution import Attribution
from modules.black_swan import BlackSwan
from modules.event_calendar import EventCalendar
from modules.walk_forward import WalkForward
from modules.rebalancer import Rebalancer

async def main():
    with open('config.yaml', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)

    hub      = DataHub('config.yaml')
    await hub.connect()
    telegram = TelegramBot(cfg['telegram']['bot_token'], cfg['telegram']['chat_id'])
    td       = date(2026, 2, 28)

    print("=== ④ 部位控管 ===")
    pm = PositionManager(hub)
    r  = await pm.run(td)
    print(f"NAV={r['nav']:,.0f} 曝險={r['gross_exposure_pct']*100:.1f}% 熔斷={r['circuit_breaker_triggered']}")

    print("\n=== ③ EV模擬 ===")
    ev = EVSimulator(hub)
    r  = await ev.run('2330', 65.0, td)
    print(f"EV={r['ev_total']:.2f} 勝率={r['win_rate']*100:.0f}% 進場={r['entry_valid']} ({r['reason']})")

    print("\n=== ⑤ 歸因分析 ===")
    attr = Attribution(hub)
    r    = await attr.analyze_closed_positions(30, td)
    print(f"期間={r['period']} 交易數={r['total_trades']}")

    print("\n=== ⑨ 黑天鵝 ===")
    bs = BlackSwan(hub, telegram)
    r  = await bs.run(td)
    print(f"觸發數={r['triggered']} 情境={[s['scenario_name'] for s in r['scenarios']]}")

    print("\n=== ⑩ 事件日曆 ===")
    ec = EventCalendar(hub)
    r  = await ec.run(td)
    print(f"未來7日事件={r['upcoming_count']} 高影響={r['high_impact_count']}")

    print("\n=== ⑫ Walk-Forward ===")
    wf = WalkForward(hub)
    r  = await wf.run(td)
    print(f"狀態={r['status']} 建議={r.get('recommendation')} 權重={r.get('recommended_weights')}")

    print("\n=== ⑬ 再平衡 ===")
    rb = Rebalancer(hub, telegram)
    r  = await rb.run(td)
    print(f"觸發數={r['trigger_count']} 條件={[t['trigger'] for t in r['triggers']]}")

    await telegram.close()
    await hub.close()
    print("\n✅ 所有模組測試完成！")

asyncio.run(main())