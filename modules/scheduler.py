# modules/scheduler.py
# ═══════════════════════════════════════════════════════════════════════════════
# 變更紀錄（v3.4）：
# - [v3.3.1] Watchlist 動態載入（已含）
# - [v3.4 新增] 初始化 YFinanceAPI（美股大盤，無需金鑰）
# - [v3.4 新增] 初始化 TWSEApi（台指期備援，無需金鑰）
# - [v3.4 新增] MacroFilter 傳入 yfinance_api / twse_api
# - [v3.4 新增] 啟動時呼叫 FinMind 全量產業分類預熱快取
# ═══════════════════════════════════════════════════════════════════════════════

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
from datahub.api_twse import TWSEApi
from datahub.api_yfinance import YFinanceAPI
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

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("Scheduler")

WATCHLIST_FALLBACK    = ["2330", "2303", "2454", "2412", "2317", "2382", "3711", "6669"]
ENTRY_SCORE_THRESHOLD = 60.0


class Scheduler:
    """每日自動化交易排程器（v3.4）"""

    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "..", "config.yaml"
            )
        with open(config_path, encoding="utf-8") as f:
            self.cfg = yaml.safe_load(f)

        self.hub      = DataHub(config_path)
        self.fred     = FredAPI(self.cfg["fred"]["api_key"])
        self.finmind  = FinMindAPI(self.cfg["finmind"]["token"])
        self.telegram = TelegramBot(
            self.cfg["telegram"]["bot_token"],
            self.cfg["telegram"]["chat_id"],
        )
        fugle_cfg  = self.cfg.get("fugle", {})
        self.fugle = FugleAPI(
            api_key=fugle_cfg.get("api_key", ""),
            trade_token=fugle_cfg.get("trade_token", ""),
            account=fugle_cfg.get("account", ""),
            paper_trading=fugle_cfg.get("paper_trading", True),
        )
        self.yfinance_api = YFinanceAPI()
        self.twse_api     = TWSEApi()
        self.bus          = None
        self.watchlist    = list(WATCHLIST_FALLBACK)

    async def connect(self):
        await self.hub.connect()
        self.bus       = SignalBus(self.hub)
        self.watchlist = await self._load_watchlist_from_db()
        await self._warmup_sector_cache()
        logger.info("排程器（v3.4）連線完成，Watchlist=%s", self.watchlist)

    async def _load_watchlist_from_db(self) -> list:
        try:
            rows = await self.hub.fetch(
                "SELECT ticker FROM watchlist WHERE is_active=TRUE ORDER BY added_at ASC"
            )
            if rows:
                tickers = [str(r["ticker"]).strip() for r in rows]
                logger.info("Watchlist 從 DB 載入：%d 檔", len(tickers))
                return tickers
        except Exception as e:
            logger.warning("無法從 DB 載入 Watchlist（%s），使用預設", e)
        return list(WATCHLIST_FALLBACK)

    async def _warmup_sector_cache(self):
        """v3.4：啟動時預熱 FinMind 全量產業分類，避免每檔都打 API"""
        try:
            sector_map = await self.finmind.get_all_stock_industries()
            if sector_map:
                logger.info("FinMind 產業分類預熱：%d 筆", len(sector_map))
            else:
                twse_map = await self.twse_api.get_all_stock_industries()
                logger.info("TWSE 產業分類預熱（備援）：%d 筆", len(twse_map))
        except Exception as e:
            logger.warning("產業分類預熱失敗（不影響執行）：%s", e)

    async def close(self):
        await self.fred.close()
        await self.finmind.close()
        await self.fugle.close()
        await self.telegram.close()
        await self.twse_api.close()
        await self.hub.close()

    async def run_daily(self, trade_date: date = None) -> dict:
        if self.bus is None:
            raise RuntimeError("請先呼叫 await sched.connect() 再執行 run_daily()")
        if trade_date is None:
            trade_date = date.today()

        logger.info("=== 每日排程（v3.4）開始：%s ===", trade_date)
        results = {"date": str(trade_date), "steps": {}}

        # Step 0：財經日曆自動同步
        try:
            from modules.event_calendar_auto import EventCalendarAutoSync
            cal_sync   = EventCalendarAutoSync(self.hub, self.fred)
            cal_result = await cal_sync.run(days_ahead=14, force=False)
            results["steps"]["calendar_sync"] = {
                "status": "OK", "synced": cal_result["synced"], "skipped": cal_result["skipped"],
            }
        except Exception as e:
            results["steps"]["calendar_sync"] = {"status": "ERROR", "error": str(e)}

        # Step 1：宏觀濾網（v3.4：含 YFinance + TWSE 備援）
        current_regime = "CHOPPY"
        vcp_enabled    = False
        try:
            macro = MacroFilter(
                self.hub, self.fred,
                fugle=self.fugle, finmind=self.finmind,
                yfinance_api=self.yfinance_api,
                twse_api=self.twse_api,
                watchlist=self.watchlist,
            )
            regime_result  = await macro.run(trade_date)
            current_regime = regime_result["regime"]
            vcp_enabled    = regime_result.get("vcp_enabled", False)
            results["steps"]["macro"] = {
                "status":            "OK",
                "regime":            current_regime,
                "mrs":               regime_result["mrs_score"],
                "basis":             regime_result.get("normalized_basis"),
                "yield_curve_shape": regime_result.get("yield_curve_shape"),
                "spx_close":         regime_result.get("spx_close"),
                "credit_spread_bp":  round(regime_result["credit_spread"] * 100, 1)
                                     if regime_result.get("credit_spread") else None,
            }
        except Exception as e:
            logger.error("宏觀濾網失敗：%s", e)
            results["steps"]["macro"] = {"status": "ERROR", "error": str(e)}

        # Step 1a：VCP 批量掃描
        if vcp_enabled:
            try:
                from modules.vcp_scanner import VCPScanner
                vcp_scanner = VCPScanner(self.hub, self.finmind)
                vcp_results = await vcp_scanner.run_batch(self.watchlist, trade_date, min_score=50)
                vcp_tickers = [r["ticker"] for r in vcp_results if r["is_vcp"]]
                for r in vcp_results:
                    await self.hub.execute("""
                        UPDATE stock_diagnostic
                        SET vcp_score=$1, vcp_confirmed=$2, vcp_contractions=$3, vcp_final_range=$4
                        WHERE trade_date=$5 AND ticker=$6
                    """, r["score"], r["is_vcp"], r["contractions"], r["final_range"],
                        trade_date, r["ticker"])
                results["steps"]["vcp"] = {"status": "OK", "confirmed_count": len(vcp_tickers)}
            except Exception as e:
                results["steps"]["vcp"] = {"status": "ERROR", "error": str(e)}
        else:
            results["steps"]["vcp"] = {"status": "SKIPPED", "reason": f"Regime={current_regime}"}

        # Step 2：籌碼監控
        try:
            chip_monitor = ChipMonitor(self.hub, self.finmind)
            chip_results = []
            for ticker in self.watchlist:
                try:
                    chip_results.append(await chip_monitor.run(ticker, trade_date))
                except Exception as e:
                    logger.warning("籌碼監控失敗 %s：%s", ticker, e)
            results["steps"]["chip"] = {"status": "OK", "count": len(chip_results)}
        except Exception as e:
            results["steps"]["chip"] = {"status": "ERROR", "error": str(e)}

        # Step 2a：題材熱度
        try:
            from modules.topic_radar import TopicRadar
            radar         = TopicRadar(self.hub, finmind=self.finmind)
            radar_results = await radar.run(trade_date)
            overheated    = [r["topic"] for r in radar_results if r["alert_type"] == "OVERHEATED"]
            results["steps"]["topic_radar"] = {
                "status": "OK", "count": len(radar_results), "overheated": overheated,
            }
        except Exception as e:
            results["steps"]["topic_radar"] = {"status": "ERROR", "error": str(e)}

        # Step 3：選股引擎
        selection_results = []
        try:
            engine = SelectionEngine(self.hub, self.finmind)
            for ticker in self.watchlist:
                try:
                    selection_results.append(await engine.run(ticker, trade_date))
                except Exception as e:
                    logger.warning("選股引擎失敗 %s：%s", ticker, e)
            results["steps"]["selection"] = {
                "status": "OK", "count": len(selection_results),
                "top3": [{"ticker": r["ticker"], "score": r["final_score"]}
                         for r in sorted(selection_results,
                                         key=lambda x: x["final_score"], reverse=True)[:3]],
            }
        except Exception as e:
            results["steps"]["selection"] = {"status": "ERROR", "error": str(e)}

        # Step 3a：計畫外懲罰
        try:
            attr        = Attribution(self.hub)
            penalty_map = await attr.get_unplanned_penalty_map(trade_date)
            for r in selection_results:
                if penalty_map.get(r["ticker"], 0) > 0:
                    r["final_score"] = max(0, r["final_score"] - penalty_map[r["ticker"]])
        except Exception as e:
            logger.warning("計畫外懲罰失敗：%s", e)

        # Step 3b：產業群聚加分
        if selection_results:
            try:
                selection_results = await apply_sector_cluster_bonus(
                    self.hub, selection_results, trade_date, threshold=ENTRY_SCORE_THRESHOLD
                )
            except Exception as e:
                logger.warning("群聚加分失敗：%s", e)

        # Step 4：出場引擎
        exit_tickers = []
        try:
            exit_engine  = ExitEngine(self.hub, self.finmind)
            exit_result  = await exit_engine.run(trade_date, regime=current_regime)
            exit_tickers = exit_result.get("exit_tickers", [])
            results["steps"]["exit"] = {
                "status": "OK", "updated": exit_result.get("updated", 0),
                "exit_tickers": exit_tickers,
            }
        except Exception as e:
            results["steps"]["exit"] = {"status": "ERROR", "error": str(e)}

        if exit_tickers:
            try:
                forced = await check_sector_exit_cascade(self.hub, exit_tickers, trade_date)
                if forced:
                    logger.warning("負向群聚出場：%s", forced)
            except Exception as e:
                logger.warning("負向群聚出場失敗：%s", e)

        # Step 5：訊號派送
        try:
            threshold_override = await self.hub.cache.get(
                f"regime:score_threshold_override:{current_regime}"
            )
            effective_threshold = (
                float(threshold_override.get("threshold", ENTRY_SCORE_THRESHOLD))
                if threshold_override else ENTRY_SCORE_THRESHOLD
            )
            entry_count = 0
            for r in selection_results:
                if r["final_score"] >= effective_threshold:
                    await self.bus.emit_entry(
                        r["ticker"], r["final_score"], current_regime, "SELECTION_ENGINE"
                    )
                    entry_count += 1
            results["steps"]["signals"] = {
                "status": "OK", "threshold": effective_threshold, "entries": entry_count,
            }
        except Exception as e:
            results["steps"]["signals"] = {"status": "ERROR", "error": str(e)}

        # Step 5a：加碼評估
        try:
            from modules.addon_engine import AddonEngine
            addon  = AddonEngine(self.hub)
            addons = await addon.run(trade_date)
            for a in addons:
                await self.bus.emit(signal_type="ADDON_SIGNAL", ticker=a["ticker"],
                                    payload=a, source="ADDON")
            results["steps"]["addon"] = {"status": "OK", "candidates": len(addons)}
        except Exception as e:
            results["steps"]["addon"] = {"status": "ERROR", "error": str(e)}

        # Step 5b：部位控管
        try:
            from modules.position_manager import PositionManager
            pm     = PositionManager(self.hub)
            pm_res = await pm.run(trade_date)
            if pm_res.get("circuit_breaker_triggered"):
                await self.telegram.send_message(
                    f"🚨 熔斷觸發：{pm_res.get('circuit_breaker_reason')}\n"
                    f"NAV={pm_res.get('nav', 0):,.0f} 回撤={pm_res.get('drawdown_from_peak', 0)*100:.1f}%"
                )
            results["steps"]["position"] = {
                "status": "OK", "nav": pm_res.get("nav"),
                "circuit": pm_res.get("circuit_breaker_triggered"),
            }
        except Exception as e:
            results["steps"]["position"] = {"status": "ERROR", "error": str(e)}

        # Step 6：歸因分析
        try:
            attr         = Attribution(self.hub)
            attr_result  = await attr.analyze_closed_positions(30, trade_date)
            triggered_fb = await attr.apply_regime_feedback(attr_result, current_regime, self.hub)
            results["steps"]["attribution"] = {
                "status": "OK", "total_trades": attr_result.get("total_trades", 0),
                "avg_r": attr_result.get("avg_r", 0), "regime_feedback": triggered_fb,
            }
        except Exception as e:
            results["steps"]["attribution"] = {"status": "ERROR", "error": str(e)}

        # Step 7（月底）：DB 分區維護
        if trade_date.day >= 25:
            try:
                from modules.maintenance import create_monthly_partitions
                for table in ["topic_heat", "stock_diagnostic", "decision_log"]:
                    await create_monthly_partitions(self.hub, table, months_ahead=2)
            except Exception as e:
                logger.error("分區維護失敗：%s", e)

        logger.info("=== 每日排程（v3.4）完成：%s ===", trade_date)
        return results

    async def run_daily_pipeline(self):
        await self.connect()
        try:
            await self.run_daily()
        finally:
            await self.close()


if __name__ == "__main__":
    async def main():
        scheduler = Scheduler()
        await scheduler.connect()
        try:
            import json
            result = await scheduler.run_daily()
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        finally:
            await scheduler.close()

    asyncio.run(main())