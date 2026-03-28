# scheduler.py
import asyncio
import logging
import yaml
from datetime import date
from datahub.data_hub import DataHub
from modules.macro_filter import MacroFilter
from modules.selection_engine import SelectionEngine
from datahub.api_finmind import FinMindAPI
from datahub.api_fred import FredAPI

# 設定日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Scheduler")

class Scheduler:
    def __init__(self, config_path='config.yaml'):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.cfg = yaml.safe_load(f)
        self.hub = DataHub(config_path)

    async def run_daily_pipeline(self):
        await self.hub.connect()
        logger.info("=== 開始每日自動化交易排程 ===")

        try:
            # 1. 宏觀濾網 (FRED API)
            fred_key = self.cfg.get('fred', {}).get('api_key')
            if fred_key:
                fred = FredAPI(fred_key)
                macro = MacroFilter(self.hub, fred)
                regime_data = await macro.run()
                await fred.close()
                logger.info(f"當前市場環境: {regime_data['regime']}")
            else:
                logger.error("找不到 FRED API Key")

            # 2. 選股引擎 (FinMind API)
            fm_cfg = self.cfg.get('finmind', {})
            token = fm_cfg.get('api_token') or fm_cfg.get('token')

            if token:
                finmind = FinMindAPI(token)
                engine = SelectionEngine(self.hub, finmind)
                watchlist = ['2330', '2317', '2454', '2303', '2382']
                for ticker in watchlist:
                    await engine.run(ticker)
                await finmind.close()
            else:
                logger.error("找不到 FinMind API Token")

            # 3. 自動化資料庫維護 (月底自動分區)
            if date.today().day >= 25:
                logger.info("檢測到月底，執行自動分區維護...")
                try:
                    # [BUG FIX] 修正 import 路徑：maintenance.py 位於 modules/ 目錄下
                    from modules.maintenance import create_monthly_partitions
                    tables = ['topic_heat', 'stock_diagnostic', 'decision_log']
                    for table in tables:
                        await create_monthly_partitions(self.hub, table, months_ahead=2)
                    logger.info("分區維護完成")
                except Exception as e:
                    logger.error(f"維護執行失敗: {e}")

        except Exception as e:
            logger.error(f"排程執行中發生未預期錯誤: {e}")
        finally:
            logger.info("=== 每日排程執行完畢 ===")
            # [BUG FIX] 移除錯誤的 # 號註解，確保連線池正確關閉，避免資源洩漏
            await self.hub.close()

if __name__ == "__main__":
    scheduler = Scheduler()
    asyncio.run(scheduler.run_daily_pipeline())