# modules/scheduler.py
import asyncio
import logging
import yaml
from datetime import date
from datahub.data_hub import DataHub
from modules.macro_filter import MacroFilter
from modules.selection_engine import SelectionEngine
from datahub.api_finmind import FinMindAPI
from datahub.api_fred import FredAPI

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger("Scheduler")


class Scheduler:
    def __init__(self, config_path='config.yaml'):
        # 編碼修復：指定 utf-8
        with open(config_path, 'r', encoding='utf-8') as f:
            self.cfg = yaml.safe_load(f)
        self.hub = DataHub(config_path)

    async def run_daily_pipeline(self):
        await self.hub.connect()
        logger.info("=== 開始每日自動化交易排程 ===")

        try:
            # 1. 宏觀濾網
            fred = FredAPI(self.cfg['fred']['api_key'])
            macro = MacroFilter(self.hub, fred)
            regime_data = await macro.run()
            await fred.close()
            logger.info(f"當前市場環境: {regime_data['regime']}")

            # 2. 選股引擎（針對觀察清單）
            finmind = FinMindAPI(self.cfg['finmind']['token'])
            engine = SelectionEngine(self.hub, finmind)
            watchlist = ['2330', '2317', '2454', '2303', '2382']
            for ticker in watchlist:
                await engine.run(ticker)
            await finmind.close()

            # 3. 自動化資料庫維護（技術債修復：月底自動分區）
            if date.today().day >= 25:
                logger.info("檢測到月底，執行自動分區維護...")
                try:
                    from modules.maintenance import create_monthly_partitions
                    for table in ['topic_heat', 'stock_diagnostic', 'decision_log']:
                        result = await create_monthly_partitions(
                            self.hub, table, months_ahead=2,
                        )
                        logger.info(
                            "分區維護 %s — 建立 %d 個，跳過 %d 個，錯誤 %d 個",
                            table,
                            len(result['created']),
                            len(result['skipped']),
                            len(result['errors']),
                        )
                    logger.info("分區維護完成")
                except Exception as e:
                    logger.error(f"維護執行失敗: {e}")

            logger.info("=== 每日排程執行完畢 ===")

        finally:
            # Bug #B 修正：DataHub 只有 close()，不存在 disconnect()
            await self.hub.close()


if __name__ == "__main__":
    scheduler = Scheduler()
    asyncio.run(scheduler.run_daily_pipeline())