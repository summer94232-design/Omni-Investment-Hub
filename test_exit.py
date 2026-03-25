# test_exit.py
import asyncio
import yaml
from datetime import date
from datahub.data_hub import DataHub
from datahub.api_finmind import FinMindAPI
from modules.exit_engine import ExitEngine

async def main():
    with open('config.yaml', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)

    hub = DataHub('config.yaml')
    await hub.connect()
    finmind = FinMindAPI(cfg['finmind']['token'])

    # 用上個月日期建立測試持倉
    test_date = date(2026, 2, 28)

    position_id = await hub.fetchrow("""
        INSERT INTO positions (
            ticker, state, entry_date, entry_price,
            initial_shares, current_shares,
            signal_source, atr_at_entry, atr_multiplier,
            initial_stop_price, r_amount,
            current_stop_price, trail_pct, avg_cost
        ) VALUES (
            '2330  ', 'S1_INITIAL_DEFENSE', $1, 850.0,
            1000, 1000,
            'TEST', 15.0, 3.0,
            805.0, 45.0,
            805.0, 0.15, 850.0
        ) RETURNING id
    """, test_date)

    pid = str(position_id['id'])
    print(f"建立測試持倉：{pid}")

    engine = ExitEngine(hub, finmind)
    result = await engine.run(pid, test_date)

    print(f"\n=== 出場引擎結果 ===")
    if result.get('action') == 'NO_DATA':
        print("無法取得價格資料")
    else:
        print(f"股票：{result['ticker']}")
        print(f"狀態：{result['prev_state']} → {result['state']}")
        print(f"當前價格：{result['current_price']}")
        print(f"移動止損：{result['trailing_stop']}")
        print(f"未實現損益：{result['unrealized_pnl']}")
        print(f"R-Multiple：{result['r_multiple']}")

    await hub.execute("DELETE FROM positions WHERE id = $1", pid)
    print("\n測試持倉已清除")

    await finmind.close()
    await hub.close()

asyncio.run(main())