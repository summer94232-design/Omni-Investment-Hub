# scheduler_v3_patch.py
# =============================================================================
# Scheduler v3 整合補丁
# 說明 scheduler.py 需要新增的呼叫點，請手動合併至 modules/scheduler.py
# =============================================================================
#
# 四個主要修改點：
#
#   [A] SelectionEngine 建構時傳入 FugleAPI（基差過濾需要）
#   [B] Step 5 新增：ExitEngine 批次執行後呼叫 check_sector_exit_cascade()
#   [C] Step 3 的 run_daily() 傳入 regime 至 ExitEngine.run()
#   [D] Step 6 新增：Attribution 歸因分析（含 Alpha 拆解 + 環境回饋）
# =============================================================================

import asyncio
import logging
import yaml
from datetime import date, datetime
from datahub.data_hub import DataHub
from datahub.api_fred import FredAPI
from datahub.api_finmind import FinMindAPI
from datahub.api_fugle import FugleAPI            # ← [A] 新增
from datahub.api_telegram import TelegramBot
from modules.macro_filter import MacroFilter
from modules.chip_monitor import ChipMonitor
from modules.selection_engine import (
    SelectionEngine, apply_sector_cluster_bonus,
    check_sector_exit_cascade,                    # ← [B] 新增
)
from modules.exit_engine import ExitEngine
from modules.attribution import Attribution
from modules.signal_bus import SignalBus

logger = logging.getLogger(__name__)

WATCHLIST = ['2330', '2303', '2454', '2412', '2317', '2382', '3711', '6669']
ENTRY_SCORE_THRESHOLD = 60.0


class Scheduler:
    def __init__(self, config_path: str = 'config.yaml'):
        with open(config_path, encoding='utf-8') as f:
            self.cfg = yaml.safe_load(f)
        self.hub      = DataHub(config_path)
        self.fred     = FredAPI(self.cfg['fred']['api_key'])
        self.finmind  = FinMindAPI(self.cfg['finmind']['token'])
        self.telegram = TelegramBot(
            self.cfg['telegram']['bot_token'],
            self.cfg['telegram']['chat_id'],
        )
        # [A] 建立 FugleAPI 實例（基差過濾使用）
        fugle_cfg     = self.cfg.get('fugle', {})
        self.fugle    = FugleAPI(
            api_key=fugle_cfg.get('api_key', ''),
            trade_token=fugle_cfg.get('trade_token', ''),
            account=fugle_cfg.get('account', ''),
            paper_trading=fugle_cfg.get('paper_trading', True),
        )
        self.bus = None

    async def connect(self):
        await self.hub.connect()
        self.bus = SignalBus(self.hub)
        logger.info("排程器（v3）連線完成")

    async def close(self):
        await self.fred.close()
        await self.finmind.close()
        await self.fugle.close()
        await self.telegram.close()
        await self.hub.close()

    async def run_daily(self, trade_date: date = None) -> dict:
        if trade_date is None:
            trade_date = date.today()

        logger.info("=== 每日排程（v3）開始：%s ===", trade_date)
        results = {'date': str(trade_date), 'steps': {}}

        # ── Step 1：宏觀濾網 ──────────────────────────────────────────────
        logger.info("Step 1：執行宏觀濾網")
        try:
            macro = MacroFilter(self.hub, self.fred)
            regime_result = await macro.run(trade_date)
            current_regime = regime_result['regime']
            results['steps']['macro'] = {
                'status': 'OK',
                'regime': current_regime,
                'mrs':    regime_result['mrs_score'],
            }
        except Exception as e:
            logger.error("宏觀濾網失敗：%s", e)
            results['steps']['macro'] = {'status': 'ERROR', 'error': str(e)}
            current_regime = 'CHOPPY'

        # ── Step 2：籌碼監控 ──────────────────────────────────────────────
        logger.info("Step 2：執行籌碼監控（%d 檔）", len(WATCHLIST))
        chip_ok      = 0
        chip_monitor = ChipMonitor(self.hub, self.finmind)
        for ticker in WATCHLIST:
            try:
                await chip_monitor.run(ticker, trade_date)
                chip_ok += 1
            except Exception as e:
                logger.warning("籌碼監控失敗 %s：%s", ticker, e)
        results['steps']['chip'] = {'status': 'OK', 'processed': chip_ok}

        # ── Step 3：選股引擎（[A] v3：傳入 fugle 啟用基差過濾）──────────
        logger.info("Step 3：執行選股引擎（v3 含基差過濾）")
        # [A] SelectionEngine 現在接收 fugle 參數
        engine     = SelectionEngine(self.hub, self.finmind, fugle=self.fugle)
        raw_signals: list[dict] = []
        for ticker in WATCHLIST:
            try:
                result = await engine.run(ticker, trade_date)
                raw_signals.append(result)
                # 記錄基差過濾狀況
                if result.get('basis_filter_reason'):
                    logger.warning(
                        "基差過濾：%s final_score=%.1f 原因=%s",
                        ticker, result['final_score'], result['basis_filter_reason']
                    )
            except Exception as e:
                logger.warning("選股引擎失敗 %s：%s", ticker, e)

        # ── Step 3a：計畫外交易懲罰 ───────────────────────────────────────
        attribution = Attribution(self.hub)
        try:
            penalty_map = await attribution.get_unplanned_penalty_map(trade_date)
            for r in raw_signals:
                factor = penalty_map.get(r['ticker'].strip(), 1.0)
                if factor < 1.0:
                    r['final_score'] = round(r['final_score'] * factor, 2)
        except Exception as e:
            logger.warning("計畫外懲罰失敗：%s", e)

        # ── Step 3b：產業群聚加分（v3 差異化 Beta 倍率）────────────────
        raw_signals = apply_sector_cluster_bonus(raw_signals, ENTRY_SCORE_THRESHOLD)

        # ── Step 4：出場引擎（[C] v3：傳入 regime）──────────────────────
        logger.info("Step 4：執行出場引擎（v3 Regime=%s）", current_regime)
        exit_engine  = ExitEngine(self.hub, self.finmind)
        exit_signals: list[dict] = []

        open_positions = await self.hub.fetch("""
            SELECT id FROM positions WHERE is_open = TRUE
        """)
        for pos in open_positions:
            try:
                # [C] 傳入 regime 讓 ExitEngine 使用動態 ATR 倍數
                result = await exit_engine.run(
                    str(pos['id']),
                    trade_date,
                    regime=current_regime,    # ← v3 新增
                )
                if result.get('action') != 'NO_DATA':
                    exit_signals.append(result)
            except Exception as e:
                logger.warning("出場引擎失敗 %s：%s", pos['id'], e)

        results['steps']['exit'] = {'status': 'OK', 'processed': len(exit_signals)}

        # ── Step 4a：[B] 負向群聚出場檢查（v3 新增）──────────────────────
        try:
            # 為 exit_signals 補上 sector 資訊
            for sig in exit_signals:
                from modules.selection_engine import get_sector
                sig['sector'] = get_sector(sig.get('ticker', ''))

            cascade_list = await check_sector_exit_cascade(
                self.hub, exit_signals, trade_date
            )
            if cascade_list:
                logger.warning(
                    "負向群聚連帶出場：%d 個部位被強制進入 S5",
                    len(cascade_list)
                )
                results['steps']['cascade_exit'] = {
                    'status': 'OK',
                    'triggered': len(cascade_list),
                    'detail': [c['ticker'] for c in cascade_list],
                }
        except Exception as e:
            logger.warning("負向群聚出場檢查失敗：%s", e)

        # ── Step 5：訊號篩選與派送 ─────────────────────────────────────
        # 取得本次 Regime 的進場門檻（可能被環境回饋覆蓋）
        try:
            effective_threshold = await attribution.get_regime_score_threshold(
                current_regime, default=ENTRY_SCORE_THRESHOLD
            )
        except Exception:
            effective_threshold = ENTRY_SCORE_THRESHOLD

        entry_signals = [
            r for r in raw_signals
            if r.get('final_score', 0) >= effective_threshold
        ]

        if entry_signals:
            logger.info(
                "Step 5：篩選出 %d/%d 檔進場訊號（門檻=%.0f）",
                len(entry_signals), len(raw_signals), effective_threshold
            )
            for sig in entry_signals:
                try:
                    await self.bus.dispatch_entry_signal(sig, trade_date)
                except Exception as e:
                    logger.warning("訊號派送失敗 %s：%s", sig['ticker'], e)
        else:
            logger.info("Step 5：無符合門檻的進場訊號（門檻=%.0f）", effective_threshold)

        results['steps']['signals'] = {
            'status':             'OK',
            'entry_count':        len(entry_signals),
            'score_threshold':    effective_threshold,
            'basis_blocked':      sum(
                1 for r in raw_signals if 'BASIS_HARD_BLOCK' in str(r.get('basis_filter_reason', ''))
            ),
        }

        # ── Step 6：[D] 歸因分析（v3 新增，含 Alpha 拆解 + 環境回饋）──
        logger.info("Step 6：執行歸因分析（v3）")
        try:
            attr_result = await attribution.analyze_closed_positions(30, trade_date)
            regime_fb = attr_result.get('regime_ev_feedback', {})
            triggered_fb = [r for r, fb in regime_fb.items() if fb.get('triggered')]
            if triggered_fb:
                logger.warning(
                    "環境回饋觸發：%s 的進場門檻已自動提高", triggered_fb
                )
            results['steps']['attribution'] = {
                'status':          'OK',
                'total_trades':    attr_result.get('total_trades', 0),
                'avg_r':           attr_result.get('avg_r', 0),
                'regime_feedback': triggered_fb,
            }
        except Exception as e:
            logger.warning("歸因分析失敗：%s", e)
            results['steps']['attribution'] = {'status': 'ERROR', 'error': str(e)}

        # ── 月底自動分區維護 ──────────────────────────────────────────────
        if trade_date.day >= 25:
            try:
                from modules.maintenance import create_monthly_partitions
                for table in ['topic_heat', 'stock_diagnostic', 'decision_log']:
                    await create_monthly_partitions(self.hub, table, months_ahead=2)
            except Exception as e:
                logger.error("分區維護失敗：%s", e)

        logger.info("=== 每日排程（v3）完成：%s ===", trade_date)
        return results


if __name__ == "__main__":
    async def main():
        scheduler = Scheduler()
        await scheduler.connect()
        try:
            result = await scheduler.run_daily()
            print("排程完成：", result)
        finally:
            await scheduler.close()

    asyncio.run(main())