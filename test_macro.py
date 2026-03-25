# test_macro.py
import asyncio
import yaml
import asyncpg
from datahub.data_hub import DataHub
from datahub.api_fred import FredAPI
from modules.macro_filter import MacroFilter

async def main():
    with open('config.yaml', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)

    # 建立連線
    hub = DataHub('config.yaml')
    await hub.connect()

    fred = FredAPI(cfg['fred']['api_key'])

    # 執行宏觀濾網
    macro = MacroFilter(hub, fred)
    result = await macro.run()

    print(f"\n=== 宏觀濾網結果 ===")
    print(f"市場環境：{result['regime']}")
    print(f"MRS 分數：{result['mrs_score']}")
    print(f"最大曝險：{result['max_exposure']*100:.0f}%")
    print(f"VIX：{result['vix_level']}")
    print(f"Fed Funds Rate：{result['fed_funds_rate']}%")
    print(f"因子權重：{result['factor_weights']}")

    await fred.close()
    await hub.close()

asyncio.run(main())