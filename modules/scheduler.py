# modules/scheduler.py
# =============================================================================
# v3 完整版 — 可直接取代舊的 scheduler.py
#
# 每日排程執行順序：
#   Step 1  宏觀濾網        MacroFilter（含基差、成交量寫入）
#   Step 2  籌碼監控        ChipMonitor（全 Watchlist）
#   Step 3  選股引擎        SelectionEngine（含基差過濾）
#   Step 3a 計畫外懲罰      Attribution.get_unplanned_penalty_map()
#   Step 3b 群聚加分        apply_sector_cluster_bonus()（Beta 差異化）
#   Step 4  出場引擎        ExitEngine（Regime 動態 ATR）
#   Step 4a 負向群聚出場    check_sector_exit_cascade()
#   Step 5  訊號篩選派送    SignalBus.emit_entry()
#   Step 6  歸因分析        Attribution.analyze_closed_positions()
#   月底     DB 分區維護    create_monthly_partitions()
#
# Bug 修正（v3.1）：
# - [BUG B] __init__ config_path 預設值改用 __file__ 計算絕對路徑，
#           避免從非根目錄直接執行 `python modules/scheduler.py` 時 FileNotFoundError
# - [BUG D] run_daily() 開頭加入 self.bus is None 防護，
#           避免呼叫方未呼叫 connect() 就執行排程時靜默失敗
# =============================================================================

import asyncio
import logging
import os
import yaml
from datetime import date
from datahub.data_hub import DataHub
from datahub.api_fred import FredAPI
from datahub.api_finmind import FinMindAPI
from datahub.api_fugle import FugleAPI
from datahub.api_telegram import TelegramBot
from modules.macro_filter import MacroFilter
from modules.chip_monitor import ChipMonitor
from modules.selection_engine import (
    SelectionEngine,
    apply_sector_cluster_bonus,
    check_sector_exit_cascade,
)
from modules.exit_engine import ExitEngine
from modules.attribution import Attribution
from modules.signal_bus import SignalBus

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger('Scheduler')

# 每日監控的股票清單
WATCHLIST = ['2330', '2303', '2454', '2412', '2317', '2382', '3711', '6669']

# 預設進場門檻（可被 Attribution 環境回饋覆蓋）
ENTRY_SCORE_THRESHOLD = 60.0


class Scheduler:
    """每日自動化交易排程器（v3 完整版）"""

    def __init__(self, config_path: str = None):
        # [BUG B 修正] 預設使用 scheduler.py 所在目錄的上一層（專案根目錄）
        # 原本預設 'config.yaml' 相對路徑，從非根目錄執行會 FileNotFoundError
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                '..', 'config.yaml'
            )

        with open(config_path, encoding='utf-8') as f:
            self.cfg = yaml.safe_load(f)

        self.hub      = DataHub(config_path)
        self.fred     = FredAPI(self.cfg['fred']['api_key'])
        self.finmind  = FinMindAPI(self.cfg['finmind']['token'])
        self.telegram = TelegramBot(
            self.cfg['telegram']['bot_token'],
            self.cfg['telegram']['chat_id'],
        )

        # Fugle API（基差計算用，無設定時優雅降級）
        fugle_cfg  = self.cfg.get('fugle', {})
        self.fugle = FugleAPI(
            api_key=fugle_cfg.get('api_key', ''),
            trade_token=fugle_cfg.get('trade_token', ''),
            account=fugle_cfg.get('account', ''),
            paper_trading=fugle_cfg.get('paper_trading', True),
        )

        self.bus = None  # connect() 後才初始化

    async def connect(self):
        await self.hub.connect()
        self.bus = SignalBus(self.hub)
        logger.info('排程器（v3）連線完成')

    async def close(self):
        await self.fred.close()
        await self.finmind.close()
        await self.fugle.close()
        await self.telegram.close()
        await self.hub.close()

    # =========================================================================
    # 主排程（每日收盤後執行）
    # =========================================================================

    async def run_daily(self, trade_date: date = None) -> dict:
        # [BUG D 修正] 防護：確保 connect() 已被呼叫
        if self.bus is None:
            raise RuntimeError("請先呼叫 await sched.connect() 再執行 run_daily()")

        if trade_date is None:
            trade_date = date.today()

        logger.info('=== 每日排程（v3）開始：%s ===', trade_date)
        results = {'date': str(trade_date), 'steps': {}}

        # ── Step 1：宏觀濾網（含基差與成交量寫入）───────────────────────
        logger.info('Step 1：執行宏觀濾網')
        current_regime = 'CHOPPY'
        try:
            macro = MacroFilter(
                self.hub,
                self.fred,
                fugle=self.fugle,
                finmind=self.finmind,
                watchlist=WATCHLIST,
            )
            regime_result  = await macro.run(trade_date)
            current_regime = regime_result['regime']

            basis = regime_result.get('normalized_basis')
            if basis is not None:
                if basis < -0.015:
                    logger.warning('基差 HARD BLOCK 生效：%.2f%%', basis * 100)
                elif basis < -0.008:
                    logger.warning('基差 SOFT PENALTY 生效：%.2f%%', basis * 100)
                else:
                    logger.info('基差正常：%.2f%%', basis * 100)

            results['steps']['macro'] = {
                'status': 'OK',
                'regime': current_regime,
                'mrs':    regime_result['mrs_score'],
                'basis':  basis,
                'volume': regime_result.get('market_volume'),
            }
            logger.info('宏觀濾網完成：%s MRS=%.1f', current_regime, regime_result['mrs_score'])

        except Exception as e:
            logger.error('宏觀濾網失敗：%s', e)
            results['steps']['macro'] = {'status': 'ERROR', 'error': str(e)}

        # ── Step 2：籌碼監控（全 Watchlist）─────────────────────────────
        logger.info('Step 2：執行籌碼監控（%d 檔）', len(WATCHLIST))
        chip_ok      = 0
        chip_monitor = ChipMonitor(self.hub, self.finmind)
        for ticker in WATCHLIST:
            try:
                await chip_monitor.run(ticker, trade_date)
                chip_ok += 1
            except Exception as e:
                logger.warning('籌碼監控失敗 %s：%s', ticker, e)
        results['steps']['chip'] = {'status': 'OK', 'processed': chip_ok}

        # ── Step 3：選股引擎（含基差過濾）───────────────────────────────
        logger.info('Step 3：執行選股引擎（含基差過濾）')
        engine      = SelectionEngine(self.hub, self.finmind, fugle=self.fugle)
        raw_signals: list[dict] = []
        for ticker in WATCHLIST:
            try:
                result = await engine.run(ticker, trade_date)
                raw_signals.append(result)
                if result.get('basis_filter_reason'):
                    logger.warning(
                        '基差過濾：%s final_score=%.1f 原因=%s',
                        ticker, result['final_score'], result['basis_filter_reason'],
                    )
            except Exception as e:
                logger.warning('選股引擎失敗 %s：%s', ticker, e)

        # ── Step 3a：計畫外交易懲罰 ──────────────────────────────────────
        attribution = Attribution(self.hub)
        try:
            penalty_map = await attribution.get_unplanned_penalty_map(trade_date)
            for r in raw_signals:
                factor = penalty_map.get(r['ticker'].strip(), 1.0)
                if factor < 1.0:
                    r['final_score'] = round(r['final_score'] * factor, 2)
                    logger.info('計畫外懲罰：%s final_score × %.2f', r['ticker'].strip(), factor)
        except Exception as e:
            logger.warning('計畫外懲罰失敗：%s', e)

        # ── Step 3b：產業群聚加分（Beta 差異化）─────────────────────────
        raw_signals = apply_sector_cluster_bonus(raw_signals, ENTRY_SCORE_THRESHOLD)
        results['steps']['selection'] = {
            'status':       'OK',
            'processed':    len(raw_signals),
            'basis_blocked': sum(
                1 for r in raw_signals
                if 'BASIS_HARD_BLOCK' in str(r.get('basis_filter_reason', ''))
            ),
        }

        # ── Step 4：出場引擎（Regime 動態 ATR）──────────────────────────
        logger.info('Step 4：執行出場引擎（Regime=%s）', current_regime)
        exit_engine  = ExitEngine(self.hub, self.finmind)
        exit_signals: list[dict] = []

        open_positions = await self.hub.fetch(
            'SELECT id FROM positions WHERE is_open = TRUE'
        )
        for pos in open_positions:
            try:
                result = await exit_engine.run(
                    str(pos['id']),
                    trade_date,
                    regime=current_regime,
                )
                if result.get('action') != 'NO_DATA':
                    exit_signals.append(result)
            except Exception as e:
                logger.warning('出場引擎失敗 %s：%s', pos['id'], e)

        results['steps']['exit'] = {'status': 'OK', 'processed': len(exit_signals)}

        # ── Step 4a：負向群聚出場檢查 ────────────────────────────────────
        try:
            from modules.selection_engine import get_sector
            for sig in exit_signals:
                sig['sector'] = get_sector(sig.get('ticker', ''))

            cascade_list = await check_sector_exit_cascade(
                self.hub, exit_signals, trade_date
            )
            if cascade_list:
                logger.warning(
                    '負向群聚連帶出場：%d 個部位強制進入 S5', len(cascade_list)
                )
                results['steps']['cascade_exit'] = {
                    'status':    'OK',
                    'triggered': len(cascade_list),
                    'detail':    [c['ticker'] for c in cascade_list],
                }
        except Exception as e:
            logger.warning('負向群聚出場檢查失敗：%s', e)

        # ── Step 5：訊號篩選與派送 ───────────────────────────────────────
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

        dispatched = 0
        if entry_signals:
            logger.info(
                'Step 5：篩選出 %d/%d 檔進場訊號（門檻=%.0f）',
                len(entry_signals), len(raw_signals), effective_threshold,
            )
            for sig in entry_signals:
                try:
                    await self.bus.emit_entry(
                        ticker=sig['ticker'].strip(),
                        score=sig['final_score'],
                        regime=current_regime,
                        source='SELECTION_ENGINE',
                    )
                    dispatched += 1
                except Exception as e:
                    logger.warning('訊號派送失敗 %s：%s', sig['ticker'], e)
        else:
            logger.info('Step 5：無符合門檻的進場訊號（門檻=%.0f）', effective_threshold)

        results['steps']['signals'] = {
            'status':          'OK',
            'entry_count':     len(entry_signals),
            'dispatched':      dispatched,
            'score_threshold': effective_threshold,
        }

        # ── Step 6：歸因分析（含 Alpha 拆解 + 環境回饋）─────────────────
        logger.info('Step 6：執行歸因分析')
        try:
            attr_result  = await attribution.analyze_closed_positions(30, trade_date)
            regime_fb    = attr_result.get('regime_ev_feedback', {})
            triggered_fb = [r for r, fb in regime_fb.items() if fb.get('triggered')]
            if triggered_fb:
                logger.warning('環境回饋觸發：%s 進場門檻已自動提高', triggered_fb)
            results['steps']['attribution'] = {
                'status':          'OK',
                'total_trades':    attr_result.get('total_trades', 0),
                'avg_r':           attr_result.get('avg_r', 0),
                'regime_feedback': triggered_fb,
            }
        except Exception as e:
            logger.warning('歸因分析失敗：%s', e)
            results['steps']['attribution'] = {'status': 'ERROR', 'error': str(e)}

        # ── 月底自動分區維護 ──────────────────────────────────────────────
        if trade_date.day >= 25:
            logger.info('月底分區維護開始')
            try:
                from modules.maintenance import create_monthly_partitions
                for table in ['topic_heat', 'stock_diagnostic', 'decision_log']:
                    r = await create_monthly_partitions(self.hub, table, months_ahead=2)
                    logger.info(
                        '分區維護 %s — 建立 %d 個，跳過 %d 個，錯誤 %d 個',
                        table, len(r['created']), len(r['skipped']), len(r['errors']),
                    )
            except Exception as e:
                logger.error('分區維護失敗：%s', e)

        logger.info('=== 每日排程（v3）完成：%s ===', trade_date)
        return results

    # =========================================================================
    # 向下相容：保留舊的 run_daily_pipeline() 入口
    # =========================================================================

    async def run_daily_pipeline(self):
        """向下相容舊介面，內部呼叫 run_daily()"""
        await self.connect()
        try:
            await self.run_daily()
        finally:
            await self.close()


# =============================================================================
# 直接執行入口
# =============================================================================

if __name__ == '__main__':
    async def main():
        # [BUG B 修正] 不傳入 config_path，讓 __init__ 自動用絕對路徑定位
        scheduler = Scheduler()
        await scheduler.connect()
        try:
            result = await scheduler.run_daily()
            import json
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        finally:
            await scheduler.close()

    asyncio.run(main())