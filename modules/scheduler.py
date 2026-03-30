# modules/scheduler.py
# ═══════════════════════════════════════════════════════════════════════════════
# 變更紀錄（v3.3）：
# - [BUG B] config_path 絕對路徑（已含於 v3.1）
# - [BUG D] self.bus None 防護（已含於 v3.1）
# - [Step 0] 新增財經日曆自動同步（EventCalendarAutoSync，週一執行）
# - [Step 1a] MacroFilter 完成後同步執行 VCP 批量掃描（Regime=BULL/WEAK_BULL 時）
# - [Step 2a] TopicRadar 改傳入 finmind，使用真實新聞計數
# - [Step 5a] PositionManager 執行後，AddonEngine 評估加碼機會
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
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("Scheduler")

WATCHLIST            = ["2330", "2303", "2454", "2412", "2317", "2382", "3711", "6669"]
ENTRY_SCORE_THRESHOLD = 60.0


class Scheduler:
    """每日自動化交易排程器（v3.3 完整版）"""

    def __init__(self, config_path: str = None):
        # [BUG B] 預設使用 scheduler.py 所在目錄的上一層（專案根目錄）
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

        self.bus = None  # connect() 後才初始化

    async def connect(self):
        await self.hub.connect()
        self.bus = SignalBus(self.hub)
        logger.info("排程器（v3.3）連線完成")

    async def close(self):
        await self.fred.close()
        await self.finmind.close()
        await self.fugle.close()
        await self.telegram.close()
        await self.hub.close()

    # =========================================================================
    # 主排程
    # =========================================================================

    async def run_daily(self, trade_date: date = None) -> dict:
        # [BUG D] 防護
        if self.bus is None:
            raise RuntimeError("請先呼叫 await sched.connect() 再執行 run_daily()")

        if trade_date is None:
            trade_date = date.today()

        logger.info("=== 每日排程（v3.3）開始：%s ===", trade_date)
        results = {"date": str(trade_date), "steps": {}}

        # ── Step 0：財經日曆自動同步（週一執行）──────────────────────────────
        logger.info("Step 0：財經日曆自動同步")
        try:
            from modules.event_calendar_auto import EventCalendarAutoSync
            cal_sync = EventCalendarAutoSync(self.hub, self.fred)
            cal_result = await cal_sync.run(days_ahead=14, force=False)
            results["steps"]["calendar_sync"] = {
                "status": "OK",
                "synced": cal_result["synced"],
                "skipped": cal_result["skipped"],
            }
        except Exception as e:
            logger.warning("財經日曆自動同步失敗：%s", e)
            results["steps"]["calendar_sync"] = {"status": "ERROR", "error": str(e)}

        # ── Step 1：宏觀濾網 ─────────────────────────────────────────────────
        logger.info("Step 1：執行宏觀濾網")
        current_regime  = "CHOPPY"
        vcp_enabled     = False
        try:
            macro = MacroFilter(
                self.hub, self.fred,
                fugle=self.fugle, finmind=self.finmind, watchlist=WATCHLIST,
            )
            regime_result  = await macro.run(trade_date)
            current_regime = regime_result["regime"]
            vcp_enabled    = regime_result.get("vcp_enabled", False)
            results["steps"]["macro"] = {
                "status": "OK",
                "regime": current_regime,
                "mrs":    regime_result["mrs_score"],
                "basis":  regime_result.get("normalized_basis"),
                "credit_spread_bp": (
                    round(regime_result["credit_spread"] * 100, 1)
                    if regime_result.get("credit_spread") else None
                ),
            }
        except Exception as e:
            logger.error("宏觀濾網失敗：%s", e)
            results["steps"]["macro"] = {"status": "ERROR", "error": str(e)}

        # ── Step 1a：VCP 批量掃描（僅 BULL / WEAK_BULL）────────────────────
        if vcp_enabled:
            logger.info("Step 1a：VCP 收縮形態批量掃描（Regime=%s）", current_regime)
            try:
                from modules.vcp_scanner import VCPScanner
                vcp_scanner  = VCPScanner(self.hub, self.finmind)
                vcp_results  = await vcp_scanner.run_batch(WATCHLIST, trade_date, min_score=50)
                vcp_tickers  = [r["ticker"] for r in vcp_results if r["is_vcp"]]

                # 將 VCP 結果寫入 stock_diagnostic
                for r in vcp_results:
                    await self.hub.execute("""
                        UPDATE stock_diagnostic
                        SET vcp_score        = $1,
                            vcp_confirmed    = $2,
                            vcp_contractions = $3,
                            vcp_final_range  = $4
                        WHERE trade_date = $5 AND ticker = $6
                    """, r["score"], r["is_vcp"], r["contractions"],
                        r["final_range"], trade_date, r["ticker"])

                results["steps"]["vcp"] = {
                    "status":         "OK",
                    "confirmed_count": len(vcp_tickers),
                    "tickers":        vcp_tickers,
                }
                logger.info("VCP 掃描完成：%d 檔確認形態", len(vcp_tickers))
            except Exception as e:
                logger.warning("VCP 掃描失敗：%s", e)
                results["steps"]["vcp"] = {"status": "ERROR", "error": str(e)}
        else:
            results["steps"]["vcp"] = {"status": "SKIPPED", "reason": f"Regime={current_regime}"}

        # ── Step 2：籌碼監控 ─────────────────────────────────────────────────
        logger.info("Step 2：籌碼監控（%d 檔）", len(WATCHLIST))
        try:
            chip_monitor = ChipMonitor(self.hub, self.finmind)
            chip_results = []
            for ticker in WATCHLIST:
                try:
                    r = await chip_monitor.run(ticker, trade_date)
                    chip_results.append(r)
                except Exception as e:
                    logger.warning("籌碼監控失敗 %s：%s", ticker, e)
            results["steps"]["chip"] = {"status": "OK", "count": len(chip_results)}
        except Exception as e:
            logger.error("籌碼監控整體失敗：%s", e)
            results["steps"]["chip"] = {"status": "ERROR", "error": str(e)}

        # ── Step 2a：題材熱度（v3.3：傳入 finmind 使用真實新聞數據）──────────
        logger.info("Step 2a：題材熱度掃描（真實新聞數據）")
        try:
            from modules.topic_radar import TopicRadar
            radar = TopicRadar(self.hub, finmind=self.finmind)  # v3.3: 傳入 finmind
            radar_results = await radar.run(trade_date)
            overheated = [r["topic"] for r in radar_results if r["alert_type"] == "OVERHEATED"]
            results["steps"]["topic_radar"] = {
                "status":    "OK",
                "count":     len(radar_results),
                "overheated": overheated,
            }
        except Exception as e:
            logger.warning("題材熱度掃描失敗：%s", e)
            results["steps"]["topic_radar"] = {"status": "ERROR", "error": str(e)}

        # ── Step 3：選股引擎（含基差過濾）────────────────────────────────────
        logger.info("Step 3：選股引擎（%d 檔）", len(WATCHLIST))
        selection_results = []
        try:
            engine = SelectionEngine(self.hub, self.finmind)
            for ticker in WATCHLIST:
                try:
                    r = await engine.run(ticker, trade_date)
                    selection_results.append(r)
                except Exception as e:
                    logger.warning("選股引擎失敗 %s：%s", ticker, e)
            results["steps"]["selection"] = {
                "status": "OK",
                "count":  len(selection_results),
                "top3":   [
                    {"ticker": r["ticker"], "score": r["final_score"]}
                    for r in sorted(selection_results, key=lambda x: x["final_score"], reverse=True)[:3]
                ],
            }
        except Exception as e:
            logger.error("選股引擎整體失敗：%s", e)
            results["steps"]["selection"] = {"status": "ERROR", "error": str(e)}

        # ── Step 3a：計畫外懲罰 ───────────────────────────────────────────────
        try:
            attr      = Attribution(self.hub)
            penalty_map = await attr.get_unplanned_penalty_map(trade_date)
            for r in selection_results:
                penalty = penalty_map.get(r["ticker"], 0)
                if penalty > 0:
                    r["final_score"] = max(0, r["final_score"] - penalty)
        except Exception as e:
            logger.warning("計畫外懲罰計算失敗：%s", e)

        # ── Step 3b：產業群聚加分 ─────────────────────────────────────────────
        if selection_results:
            try:
                selection_results = await apply_sector_cluster_bonus(
                    self.hub, selection_results, trade_date, threshold=ENTRY_SCORE_THRESHOLD
                )
            except Exception as e:
                logger.warning("群聚加分失敗：%s", e)

        # ── Step 4：出場引擎 ─────────────────────────────────────────────────
        logger.info("Step 4：出場引擎")
        exit_tickers = []
        try:
            exit_engine  = ExitEngine(self.hub, self.finmind)
            exit_result  = await exit_engine.run(trade_date, regime=current_regime)
            exit_tickers = exit_result.get("exit_tickers", [])
            results["steps"]["exit"] = {
                "status":       "OK",
                "updated":      exit_result.get("updated", 0),
                "exit_tickers": exit_tickers,
            }
        except Exception as e:
            logger.error("出場引擎失敗：%s", e)
            results["steps"]["exit"] = {"status": "ERROR", "error": str(e)}

        # ── Step 4a：負向群聚出場 ─────────────────────────────────────────────
        if exit_tickers:
            try:
                forced = await check_sector_exit_cascade(self.hub, exit_tickers, trade_date)
                if forced:
                    logger.warning("負向群聚出場強制觸發：%s", forced)
            except Exception as e:
                logger.warning("負向群聚出場失敗：%s", e)

        # ── Step 5：訊號派送（進場）──────────────────────────────────────────
        logger.info("Step 5：訊號篩選與派送")
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
                "status":    "OK",
                "threshold": effective_threshold,
                "entries":   entry_count,
            }
        except Exception as e:
            logger.error("訊號派送失敗：%s", e)
            results["steps"]["signals"] = {"status": "ERROR", "error": str(e)}

        # ── Step 5a：加碼評估（v3.3 新增）──────────────────────────────────
        logger.info("Step 5a：加碼引擎評估")
        try:
            from modules.addon_engine import AddonEngine
            addon   = AddonEngine(self.hub)
            addons  = await addon.run(trade_date)
            for a in addons:
                await self.bus.emit(
                    signal_type="ADDON_SIGNAL",
                    ticker=a["ticker"],
                    payload=a,
                    source="ADDON",
                )
            results["steps"]["addon"] = {"status": "OK", "candidates": len(addons)}
        except Exception as e:
            logger.warning("加碼引擎失敗：%s", e)
            results["steps"]["addon"] = {"status": "ERROR", "error": str(e)}

        # ── Step 6：歸因分析 ─────────────────────────────────────────────────
        logger.info("Step 6：歸因分析")
        try:
            attr        = Attribution(self.hub)
            attr_result = await attr.analyze_closed_positions(30, trade_date)
            triggered_fb = await attr.apply_regime_feedback(attr_result, current_regime, self.hub)
            if triggered_fb:
                logger.warning("環境回饋觸發：%s 進場門檻已自動提高", triggered_fb)
            results["steps"]["attribution"] = {
                "status":          "OK",
                "total_trades":    attr_result.get("total_trades", 0),
                "avg_r":           attr_result.get("avg_r", 0),
                "regime_feedback": triggered_fb,
            }
        except Exception as e:
            logger.warning("歸因分析失敗：%s", e)
            results["steps"]["attribution"] = {"status": "ERROR", "error": str(e)}

        # ── Step 7（月底）：DB 分區維護 ──────────────────────────────────────
        if trade_date.day >= 25:
            logger.info("月底分區維護開始")
            try:
                from modules.maintenance import create_monthly_partitions
                for table in ["topic_heat", "stock_diagnostic", "decision_log"]:
                    r = await create_monthly_partitions(self.hub, table, months_ahead=2)
                    logger.info(
                        "分區維護 %s — 建立 %d 個，跳過 %d 個，錯誤 %d 個",
                        table, len(r["created"]), len(r["skipped"]), len(r["errors"]),
                    )
            except Exception as e:
                logger.error("分區維護失敗：%s", e)

        logger.info("=== 每日排程（v3.3）完成：%s ===", trade_date)
        return results

    async def run_daily_pipeline(self):
        """向下相容舊介面"""
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