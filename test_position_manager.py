# test_position_manager.py
import asyncio
from datetime import date
from datahub.data_hub import DataHub
from modules.position_manager import PositionManager

async def main():
    hub = DataHub('config.yaml')
    await hub.connect()

    pm = PositionManager(hub)
    result = await pm.run(date(2026, 2, 28))

    print(f"\n=== 部位控管結果 ===")
    print(f"NAV：{result['nav']:,.0f} 元")
    print(f"現金：{result['cash_amount']:,.0f} 元")
    print(f"股票市值：{result['stock_market_value']:,.0f} 元")
    print(f"曝險比例：{result['gross_exposure_pct']*100:.1f}%")
    print(f"VaR 95%：{result['portfolio_var_95']*100:.2f}%")
    print(f"熔斷觸發：{result['circuit_breaker_triggered']}")
    print(f"市場環境：{result['regime_snapshot']} MRS={result['mrs_snapshot']}")

    await hub.close()

asyncio.run(main())