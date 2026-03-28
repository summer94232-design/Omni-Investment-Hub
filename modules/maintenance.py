# modules/maintenance.py
"""
自動分區維護模組
每月 25 日後由 scheduler.py 觸發，預建未來 2 個月的分區表。
目標資料表：topic_heat, stock_diagnostic, decision_log
"""

import logging
from datetime import date
from datahub.data_hub import DataHub

logger = logging.getLogger(__name__)

PARTITIONED_TABLES = [
    'topic_heat',
    'stock_diagnostic',
    'decision_log',
]


async def create_monthly_partitions(
    hub: DataHub,
    base_table: str,
    months_ahead: int = 1,
) -> dict:
    """
    為指定資料表預建未來 months_ahead 個月的分區。
    若分區已存在則跳過。
    回傳 {'created': [...], 'skipped': [...], 'errors': [...]}
    """
    results = {'created': [], 'skipped': [], 'errors': []}

    for i in range(1, months_ahead + 1):
        # 計算目標月份第一天
        today = date.today()
        month = today.month + i
        year = today.year
        if month > 12:
            month -= 12
            year += 1
        target_date = date(year, month, 1)

        # 計算分區起訖
        start_date = target_date
        if start_date.month == 12:
            end_date = start_date.replace(year=start_date.year + 1, month=1)
        else:
            end_date = start_date.replace(month=start_date.month + 1)

        partition_name = f"{base_table}_{start_date.strftime('%Y_%m')}"

        try:
            # 檢查分區是否已存在（使用 fetchrow + pg_tables）
            row = await hub.fetchrow("""
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public'
                AND tablename = $1
            """, partition_name)

            exists = row is not None

            if exists:
                logger.debug("分區已存在，跳過：%s", partition_name)
                results['skipped'].append(partition_name)
                continue

            # 建立分區
            await hub.execute(
                f"""
                CREATE TABLE {partition_name}
                PARTITION OF {base_table}
                FOR VALUES FROM ('{start_date}') TO ('{end_date}')
                """
            )
            logger.info("已建立分區：%s（%s ~ %s）", partition_name, start_date, end_date)
            results['created'].append(partition_name)

        except Exception as e:
            logger.error("建立分區失敗 %s：%s", partition_name, e)
            results['errors'].append({'partition': partition_name, 'error': str(e)})

    return results


async def run_maintenance(hub: DataHub, reference_date: date = None) -> dict:
    """
    主執行入口：對所有分區表執行預建作業。
    通常由 scheduler.py 在每月 25 日後呼叫。
    """
    if reference_date is None:
        reference_date = date.today()

    summary = {'reference_date': str(reference_date), 'tables': {}}

    for table in PARTITIONED_TABLES:
        result = await create_monthly_partitions(hub, table, months_ahead=2)
        summary['tables'][table] = result
        logger.info(
            "%s — 建立 %d 個，跳過 %d 個，錯誤 %d 個",
            table,
            len(result['created']),
            len(result['skipped']),
            len(result['errors']),
        )

    return summary