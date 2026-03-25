# modules/scheduler.py
import asyncio
import logging
import yaml
from datetime import date, datetime
from datahub.data_hub import DataHub
from datahub.api_fred import FredAPI
from datahub.api_finmind import FinMindAPI
from datahub.api_telegram import TelegramBot
from modules.macro_filter import MacroFilter
from modules.chip_monitor import ChipMonitor
from modules.selection_engine import SelectionEngine
from modules.signal_bus import SignalBus

logger = logging.getLogger(__name__)

# 每日監控的股票清單（可擴充）
WATCHLIST = ['2330', '2303', '2454', '2412', '2317', '2382', '3711', '6669']

# 選股門檻
ENTRY_SCORE_THRESHOLD = 60.0


class Scheduler:
    """C1 主排程器：每日自動執行所有模組"""

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
        self.bus = None

    async def connect(self):
        await self.hub.connect()
        self.bus = SignalBus(self.hub)
        logger.info("排程器連線完成")

    async def close(self):
        await self.fred.close()
        await self.finmind.close()
        await self.telegram.close()
        await self.hub.close()

    # -------------------------------------------------------------------------
    # 每日收盤後執行（主流程）
    # -------------------------------------------------------------------------
    async def run_daily(self, trade_date: date = None) -> dict:
        if trade_date is None:
            trade_date = date.today()

        logger.info("=== 每日排程開始：%s ===", trade_date)
        results = {'date': str(trade_date), 'steps': {}}

        # Step 1：宏觀濾網
        logger.info("Step 1：執行宏觀濾網")
        try:
            macro = MacroFilter(self.hub, self.fred)
            regime_result = await macro.run(trade_date)
            results['steps']['macro'] = {
                'status': 'OK',
                'regime': regime_result['regime'],
                'mrs': regime_result['mrs_score'],
            }
            logger.info("宏觀濾網完成：%s MRS=%.1f",
                       regime_result['regime'], regime_result['mrs_score'])
        except Exception as e:
            logger.error("宏觀濾網失敗：%s", e)
            results['steps']['macro'] = {'status': 'ERROR', 'error': str(e)}
            regime_result = {'regime': 'CHOPPY', 'mrs_score': 50.0}

        # Step 2：籌碼監控（全部觀察清單）
        logger.info("Step 2：執行籌碼監控（%d 檔）", len(WATCHLIST))
        chip_ok = 0
        chip_monitor = ChipMonitor(self.hub, self.finmind)
        for ticker in WATCHLIST:
            try:
                await chip_monitor.run(ticker, trade_date)
                chip_ok += 1
            except Exception as e:
                logger.warning("籌碼監控失敗 %s：%s", ticker, e)
        results['steps']['chip'] = {'status': 'OK', 'processed': chip_ok}

        # Step 3：選股引擎（全部觀察清單）
        logger.info("Step 3：執行選股引擎（%d 檔）", len(WATCHLIST))
        engine = SelectionEngine(self.hub, self.finmind)
        entry_signals = []
        for ticker in WATCHLIST:
            try:
                result = await engine.run(ticker, trade_date)
                if result['total_score'] >= ENTRY_SCORE_THRESHOLD:
                    entry_signals.append(result)
                    await self.bus.emit_entry(
                        ticker,
                        result['total_score'],
                        result['regime_at_calc'] or '',
                        'SELECTION_ENGINE',
                    )
            except Exception as e:
                logger.warning("選股引擎失敗 %s：%s", ticker, e)

        results['steps']['selection'] = {
            'status': 'OK',
            'entry_signals': len(entry_signals),
            'candidates': [
                {'ticker': r['ticker'], 'score': r['total_score']}
                for r in sorted(entry_signals, key=lambda x: x['total_score'], reverse=True)
            ],
        }

        # Step 4：發送 Telegram 日報
        await self._send_daily_report(trade_date, regime_result, entry_signals)

        logger.info("=== 每日排程完成 ===")
        return results

    async def _send_daily_report(
        self, trade_date: date, regime: dict, candidates: list
    ) -> None:
        """發送每日 Telegram 日報"""
        regime_emoji = {
            'BULL_TREND': '🟢', 'WEAK_BULL': '🟡',
            'CHOPPY': '🟠', 'BEAR_TREND': '🔴'
        }.get(regime['regime'], '⚪')

        msg = f"📊 <b>Omni-Investment 日報</b>\n"
        msg += f"📅 {trade_date}\n\n"
        msg += f"{regime_emoji} <b>市場環境：{regime['regime']}</b>\n"
        msg += f"MRS 分數：{regime['mrs_score']}\n\n"

        if candidates:
            msg += f"🎯 <b>選股訊號（{len(candidates)} 檔）</b>\n"
            for c in sorted(candidates, key=lambda x: x['total_score'], reverse=True)[:5]:
                msg += f"  • {c['ticker'].strip()}  分數：{c['total_score']}\n"
        else:
            msg += "📭 今日無符合條件的選股訊號\n"

        await self.telegram.send(msg)
        logger.info("Telegram 日報發送完成")