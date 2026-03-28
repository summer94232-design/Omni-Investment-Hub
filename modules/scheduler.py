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
from modules.selection_engine import SelectionEngine, apply_sector_cluster_bonus
from modules.attribution import Attribution
from modules.signal_bus import SignalBus

logger = logging.getLogger(__name__)

# 每日監控的股票清單（可擴充）
WATCHLIST = ['2330', '2303', '2454', '2412', '2317', '2382', '3711', '6669']

# 選股門檻（套用產業群聚加分後的 final_score 使用此門檻）
ENTRY_SCORE_THRESHOLD = 60.0


class Scheduler:
    """C1 主排程器：每日自動執行所有模組

    變更紀錄（v2）：
    - [ATR動態停損]    exit_engine.run() 已自動處理，scheduler 無需額外調用
    - [產業群聚加權]   Step 3 後呼叫 apply_sector_cluster_bonus()，以 final_score 篩選訊號
    - [計畫外懲罰]     Step 3 後呼叫 attribution.get_unplanned_penalty_map()，
                       將懲罰係數套入 final_score
    - [Alpha SECTOR_BETA] 歸因分析已在 attribution.py 自動分類，scheduler 日報新增顯示
    """

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

        # ── Step 1：宏觀濾網 ──────────────────────────────────────────────
        logger.info("Step 1：執行宏觀濾網")
        try:
            macro = MacroFilter(self.hub, self.fred)
            regime_result = await macro.run(trade_date)
            results['steps']['macro'] = {
                'status': 'OK',
                'regime': regime_result['regime'],
                'mrs':    regime_result['mrs_score'],
            }
            logger.info("宏觀濾網完成：%s MRS=%.1f",
                        regime_result['regime'], regime_result['mrs_score'])
        except Exception as e:
            logger.error("宏觀濾網失敗：%s", e)
            results['steps']['macro'] = {'status': 'ERROR', 'error': str(e)}
            regime_result = {'regime': 'CHOPPY', 'mrs_score': 50.0}

        # ── Step 2：籌碼監控（全部觀察清單）─────────────────────────────
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

        # ── Step 3：選股引擎（全部觀察清單）─────────────────────────────
        logger.info("Step 3：執行選股引擎（%d 檔）", len(WATCHLIST))
        engine = SelectionEngine(self.hub, self.finmind)
        raw_signals: list[dict] = []
        for ticker in WATCHLIST:
            try:
                result = await engine.run(ticker, trade_date)
                raw_signals.append(result)
            except Exception as e:
                logger.warning("選股引擎失敗 %s：%s", ticker, e)

        # ── Step 3a：計畫外交易懲罰（套入 final_score）──────────────────
        # [計畫外交易懲罰] 由歸因分析模組計算各 ticker 的懲罰係數
        attribution = Attribution(self.hub)
        try:
            penalty_map = await attribution.get_unplanned_penalty_map(trade_date)
            if penalty_map:
                logger.info("計畫外懲罰觸發：%s", list(penalty_map.keys()))
            for r in raw_signals:
                ticker_clean = r['ticker'].strip()
                factor = penalty_map.get(ticker_clean, 1.0)
                if factor < 1.0:
                    original = r['final_score']
                    r['final_score'] = round(original * factor, 2)
                    logger.info(
                        "懲罰套用：%s %.1f → %.1f（×%.2f）",
                        ticker_clean, original, r['final_score'], factor
                    )
        except Exception as e:
            logger.warning("計畫外懲罰計算失敗（跳過）：%s", e)

        # ── Step 3b：產業群聚效應加權 ─────────────────────────────────
        # [產業群聚] 批次加分（修改 final_score，不改 total_score）
        try:
            raw_signals = apply_sector_cluster_bonus(
                raw_signals,
                score_threshold=ENTRY_SCORE_THRESHOLD - 5.0,  # 略低門檻才能觸發加分
            )
        except Exception as e:
            logger.warning("產業群聚加分計算失敗（跳過）：%s", e)

        # ── Step 3c：依 final_score 篩選進場訊號 ────────────────────────
        entry_signals = [
            r for r in raw_signals
            if r.get('final_score', r.get('total_score', 0)) >= ENTRY_SCORE_THRESHOLD
        ]

        # 發訊號至 SignalBus
        for result in entry_signals:
            try:
                await self.bus.emit_entry(
                    result['ticker'],
                    result['final_score'],        # 使用 final_score（含加分後）
                    result['regime_at_calc'] or '',
                    'SELECTION_ENGINE',
                )
            except Exception as e:
                logger.warning("訊號發送失敗 %s：%s", result['ticker'], e)

        results['steps']['selection'] = {
            'status':        'OK',
            'entry_signals': len(entry_signals),
            'candidates': [
                {
                    'ticker':       r['ticker'],
                    'total_score':  r['total_score'],
                    'sector_bonus': r.get('sector_bonus', 0),
                    'final_score':  r.get('final_score', r['total_score']),
                    'sector':       r.get('sector', 'OTHER'),
                }
                for r in sorted(
                    entry_signals,
                    key=lambda x: x.get('final_score', x['total_score']),
                    reverse=True,
                )
            ],
        }

        # ── Step 4：發送 Telegram 日報 ────────────────────────────────────
        await self._send_daily_report(trade_date, regime_result, entry_signals)

        logger.info("=== 每日排程完成 ===")
        return results

    # -------------------------------------------------------------------------
    # Telegram 日報
    # -------------------------------------------------------------------------
    async def _send_daily_report(
        self, trade_date: date, regime: dict, candidates: list
    ) -> None:
        """發送每日 Telegram 日報（v2：新增產業群聚與懲罰資訊）"""
        regime_emoji = {
            'BULL_TREND': '🟢', 'WEAK_BULL': '🟡',
            'CHOPPY': '🟠', 'BEAR_TREND': '🔴'
        }.get(regime['regime'], '⚪')

        msg  = f"📊 <b>Omni-Investment 日報</b>\n"
        msg += f"📅 {trade_date}\n\n"
        msg += f"{regime_emoji} <b>市場環境：{regime['regime']}</b>\n"
        msg += f"MRS 分數：{regime['mrs_score']}\n\n"

        if candidates:
            msg += f"🎯 <b>選股訊號（{len(candidates)} 檔）</b>\n"
            sorted_cands = sorted(
                candidates,
                key=lambda x: x.get('final_score', x.get('total_score', 0)),
                reverse=True,
            )
            for c in sorted_cands[:5]:
                ticker_str = c['ticker'].strip()
                score_str  = f"{c.get('total_score', 0):.1f}"
                bonus      = c.get('sector_bonus', 0)
                final      = c.get('final_score', c.get('total_score', 0))
                sector     = c.get('sector', 'OTHER')
                bonus_str  = f" <i>(+{bonus:.1f} 族群)</i>" if bonus > 0 else ""
                msg += f"  • <b>{ticker_str}</b> [{sector}]  {score_str}{bonus_str} → <b>{final:.1f}</b>\n"
        else:
            msg += "📭 今日無符合條件的選股訊號\n"

        await self.telegram.send(msg)
        logger.info("Telegram 日報發送完成")
