# test_scheduler.py
import asyncio
import logging
from datetime import date
from modules.scheduler import Scheduler

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

async def main():
    scheduler = Scheduler()
    await scheduler.connect()

    print("=== 執行每日排程 ===")
    results = await scheduler.run_daily(date(2026, 2, 28))

    print(f"\n=== 排程結果 ===")
    print(f"日期：{results['date']}")
    for step, data in results['steps'].items():
        print(f"{step}：{data}")

    await scheduler.close()

asyncio.run(main())