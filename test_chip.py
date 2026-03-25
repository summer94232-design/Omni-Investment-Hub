# test_chip.py
import asyncio
import yaml
from datahub.data_hub import DataHub
from datahub.api_finmind import FinMindAPI
from modules.chip_monitor import ChipMonitor

async def main():
    with open('config.yaml', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)

    hub = DataHub('config.yaml')
    await hub.connect()
    finmind = FinMindAPI(cfg['finmind']['token'])

    chip = ChipMonitor(hub, finmind)

    for ticker in ['2330', '2303', '2454']:
        result = await chip.run(ticker)
        print(f"\n=== {ticker} 籌碼監控 ===")
        print(f"CRS 總分：{result['crs_total']}")
        print(f"外資淨買超：{result['fini_net_buy_bn']} 億")
        print(f"投信淨買超：{result['it_net_buy_bn']} 億")
        print(f"三大法人同向：{result['three_way_resonance']}")
        print(f"券資比：{result['sr_ratio']}")
        print(f"警告：{result['alerts']}")

    await finmind.close()
    await hub.close()

asyncio.run(main())