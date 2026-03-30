# modules/black_swan.py
# ═══════════════════════════════════════════════════════════════════════════════
# 變更紀錄（v3.3.1）：
# - [BUG 1 修正] CRASH_20PCT market_drop 0.20 → 0.05，補雙重確認（已含於 v3.1）
# - [流動性偵測] 新增 LIQUIDITY_BASIS / LIQUIDITY_VOLUME（已含於 v3.1）
# - [LIQUIDITY_CRISIS 修正] 補上信用利差真實觸發邏輯（已含於 v3.3）
# - [🟡 缺漏修復] _check_liquidity_crisis() 增加 hy_spread 雙指標判斷：
#     credit_spread > 200bp OR hy_spread > 500bp 均可觸發 EMERGENCY_EXIT
#     原本只判斷 credit_spread，hy_spread 欄位完全閒置
# ═══════════════════════════════════════════════════════════════════════════════

import logging
from datetime import date, timedelta
from typing import Optional
from datahub.data_hub import DataHub
from datahub.api_telegram import TelegramBot
from datahub import redis_keys as rk

logger = logging.getLogger(__name__)

BLACK_SWAN_SCENARIOS = [
    {
        "id":          "CRASH_20PCT",
        "name":        "市場閃崩",
        "trigger":     {"vix_spike": 40, "market_drop": 0.05},
        "action":      "REDUCE_EXPOSURE_50PCT",
        "description": "VIX 突破 40 且大盤單日跌幅 > 5%",
    },
    {
        "id":          "RATE_SHOCK",
        "name":        "利率衝擊",
        "trigger":     {"fed_rate_change": 0.75},
        "action":      "HALT_NEW_ENTRIES",
        "description": "Fed 單次升息超過 75bp",
    },
    {
        "id":          "GEOPOLITICAL",
        "name":        "地緣政治危機",
        "trigger":     {"vix_level": 35, "duration_days": 3},
        "action":      "REDUCE_EXPOSURE_30PCT",
        "description": "VIX 連續 3 日高於 35",
    },
    {
        "id":          "LIQUIDITY_CRISIS",
        "name":        "流動性危機（信用利差）",
        "trigger":     {"spread_widen": 200, "hy_spread_widen": 500},
        "action":      "EMERGENCY_EXIT",
        "description": "信用利差（BAA10Y）> 200bp 或高收益債 OAS（BAMLH0A0HYM2）> 500bp",
    },
    {
        "id":          "LIQUIDITY_BASIS",
        "name":        "基差背離恐慌",
        "trigger":     {"basis_threshold": -0.012, "basis_duration_min": 30},
        "action":      "REDUCE_EXPOSURE_30PCT",
        "description": "台指期逆價差 > -1.2% 且持續 30 分鐘（大戶系統性避險）",
    },
    {
        "id":          "LIQUIDITY_VOLUME",
        "name":        "成交量枯竭",
        "trigger":     {"volume_ratio": 0.40},
        "action":      "REDUCE_EXPOSURE_30PCT",
        "description": "5日均量 < 20日均量 40%",
    },
]


class BlackSwan:
    """黑天鵝防護模組：監控 6 種系統性風險情境，自動執行緊急減倉"""

    def __init__(self, hub: DataHub, telegram: Optional[TelegramBot] = None):
        self.hub      = hub
        self.telegram = telegram

    # ── 觸發條件檢測 ─────────────────────────────────────────────────────────

    async def _check_crash(self, scenario: dict, vix: float, trade_date: date) -> tuple[bool, dict]:
        vix_triggered  = vix is not None and vix > scenario["trigger"]["vix_spike"]
        daily_return   = await self._get_market_daily_return(trade_date)
        drop_triggered = daily_return is not None and daily_return < -scenario["trigger"]["market_drop"]
        triggered      = vix_triggered and drop_triggered
        return triggered, {
            "vix": vix,
            "market_drop_pct": round(daily_return * 100, 2) if daily_return is not None else None,
        }

    async def _check_rate_shock(self, scenario: dict, trade_date: date) -> tuple[bool, dict]:
        rows = await self.hub.fetch("""
            SELECT trade_date, fed_funds_rate FROM market_regime
            WHERE fed_funds_rate IS NOT NULL
            ORDER BY trade_date DESC LIMIT 2
        """)
        if len(rows) < 2:
            return False, {}
        delta = abs(float(rows[0]["fed_funds_rate"]) - float(rows[1]["fed_funds_rate"]))
        triggered = delta >= scenario["trigger"]["fed_rate_change"] / 100
        return triggered, {"fed_rate_change_bp": round(delta * 100, 1)}

    async def _check_geopolitical(self, scenario: dict, vix: float, trade_date: date) -> tuple[bool, dict]:
        if vix is None:
            return False, {}
        rows = await self.hub.fetch("""
            SELECT vix_level FROM market_regime
            WHERE trade_date <= $1 AND vix_level IS NOT NULL
            ORDER BY trade_date DESC LIMIT $2
        """, trade_date, scenario["trigger"]["duration_days"])
        if len(rows) < scenario["trigger"]["duration_days"]:
            return False, {}
        triggered = all(float(r["vix_level"]) > scenario["trigger"]["vix_level"] for r in rows)
        return triggered, {"vix_consecutive_days": len(rows), "vix_threshold": scenario["trigger"]["vix_level"]}

    async def _check_liquidity_crisis(self, scenario: dict) -> tuple[bool, dict]:
        """
        v3.3.1 修復：雙指標流動性危機偵測。
        原本只判斷 credit_spread，hy_spread 欄位完全閒置。

        觸發條件（OR 邏輯，任一滿足即觸發 EMERGENCY_EXIT）：
          - credit_spread（BAA10Y）> 200bp：系統性信用壓力
          - hy_spread（BAMLH0A0HYM2）> 500bp：高收益債流動性崩潰（更靈敏的前兆）

        FRED 資料單位為 %（例如 2.0 = 200bp），乘以 100 轉換為 bp。
        """
        row = await self.hub.fetchrow("""
            SELECT credit_spread, hy_spread, trade_date
            FROM market_regime
            WHERE credit_spread IS NOT NULL OR hy_spread IS NOT NULL
            ORDER BY trade_date DESC LIMIT 1
        """)
        if row is None:
            logger.debug("無信用利差資料，跳過 LIQUIDITY_CRISIS 檢查")
            return False, {"reason": "no_data"}

        credit_spread_pct = float(row["credit_spread"]) if row["credit_spread"] is not None else None
        hy_spread_pct     = float(row["hy_spread"])     if row["hy_spread"]     is not None else None

        credit_spread_bp = credit_spread_pct * 100 if credit_spread_pct is not None else None
        hy_spread_bp     = hy_spread_pct     * 100 if hy_spread_pct     is not None else None

        cs_threshold = scenario["trigger"]["spread_widen"]        # 200 bp
        hy_threshold = scenario["trigger"].get("hy_spread_widen", 500)  # 500 bp

        cs_triggered = credit_spread_bp is not None and credit_spread_bp > cs_threshold
        hy_triggered = hy_spread_bp     is not None and hy_spread_bp     > hy_threshold
        triggered    = cs_triggered or hy_triggered

        details = {
            "credit_spread_bp":    round(credit_spread_bp, 1) if credit_spread_bp is not None else None,
            "hy_spread_bp":        round(hy_spread_bp,     1) if hy_spread_bp     is not None else None,
            "cs_threshold_bp":     cs_threshold,
            "hy_threshold_bp":     hy_threshold,
            "cs_triggered":        cs_triggered,
            "hy_triggered":        hy_triggered,
            "data_date":           str(row["trade_date"]),
        }

        if triggered:
            trigger_src = []
            if cs_triggered:
                trigger_src.append(f"credit_spread {credit_spread_bp:.1f}bp > {cs_threshold}bp")
            if hy_triggered:
                trigger_src.append(f"hy_spread {hy_spread_bp:.1f}bp > {hy_threshold}bp")
            logger.warning("流動性危機警報：%s（資料日期：%s）", "，".join(trigger_src), row["trade_date"])

        return triggered, details

    async def _check_liquidity_basis(
        self,
        scenario: dict,
        current_basis: Optional[float],
    ) -> tuple[bool, dict]:
        if current_basis is None:
            return False, {}
        threshold = scenario["trigger"]["basis_threshold"]
        triggered = current_basis < threshold
        if triggered:
            logger.warning("基差背離警報：basis=%.4f < 門檻 %.4f", current_basis, threshold)
        return triggered, {"current_basis": round(current_basis, 4), "threshold": threshold}

    async def _check_liquidity_volume(
        self,
        scenario: dict,
        trade_date: date,
    ) -> tuple[bool, dict]:
        volume_ratio_threshold = scenario["trigger"]["volume_ratio"]

        rows = await self.hub.fetch("""
            SELECT trade_date, market_volume FROM market_regime
            WHERE market_volume IS NOT NULL AND trade_date <= $1
            ORDER BY trade_date DESC LIMIT 25
        """, trade_date)

        if len(rows) >= 20:
            volumes = [float(r["market_volume"]) for r in rows]
            ma5  = sum(volumes[:5])  / 5
            ma20 = sum(volumes[:20]) / 20
            ratio = ma5 / ma20 if ma20 > 0 else 1.0
            triggered = ratio < volume_ratio_threshold
            if triggered:
                logger.warning("成交量萎縮警報：5MA/20MA=%.2f < 門檻 %.2f", ratio, volume_ratio_threshold)
            return triggered, {"volume_5ma_20ma_ratio": round(ratio, 4), "threshold": volume_ratio_threshold}

        return False, {"reason": "insufficient_data", "rows": len(rows)}

    async def _get_market_daily_return(self, trade_date: date) -> Optional[float]:
        rows = await self.hub.fetch("""
            SELECT trade_date, tsec_index_price FROM market_regime
            WHERE tsec_index_price IS NOT NULL AND trade_date <= $1
            ORDER BY trade_date DESC LIMIT 2
        """, trade_date)
        if len(rows) < 2:
            return None
        today_p = float(rows[0]["tsec_index_price"])
        prev_p  = float(rows[1]["tsec_index_price"])
        if prev_p == 0:
            return None
        return (today_p - prev_p) / prev_p

    # ── 動作執行 ─────────────────────────────────────────────────────────────

    async def execute_action(
        self,
        scenario: dict,
        trade_date: date,
        details: dict,
    ) -> None:
        action = scenario["action"]
        logger.warning("執行黑天鵝動作：%s → %s", scenario["name"], action)

        from modules.position_manager import PositionManager
        pm = PositionManager(self.hub)

        if action == "REDUCE_EXPOSURE_50PCT":
            positions = await self.hub.fetch(
                "SELECT id, ticker, current_shares FROM positions WHERE is_open = TRUE"
            )
            for pos in positions:
                reduce_shares = int(pos["current_shares"] * 0.5)
                if reduce_shares > 0:
                    await pm.partial_exit(pos["id"], reduce_shares, reason="BLACK_SWAN_50PCT")

        elif action == "REDUCE_EXPOSURE_30PCT":
            positions = await self.hub.fetch(
                "SELECT id, ticker, current_shares FROM positions WHERE is_open = TRUE"
            )
            for pos in positions:
                reduce_shares = int(pos["current_shares"] * 0.7)
                if reduce_shares > 0:
                    await pm.partial_exit(pos["id"], reduce_shares, reason="BLACK_SWAN_30PCT")

        elif action == "EMERGENCY_EXIT":
            await self.hub.execute("""
                UPDATE positions SET state = 'S5_ACTIVE_EXIT'
                WHERE is_open = TRUE AND state NOT IN ('S5_ACTIVE_EXIT','CLOSED','STOPPED_OUT')
            """)
            logger.warning("緊急全數平倉已觸發（LIQUIDITY_CRISIS）")

        elif action == "HALT_NEW_ENTRIES":
            await self.hub.cache.set(
                rk.key_market_regime_latest(),
                {"halt_new_entries": True},
                ttl=rk.TTL_24H,
            )

        import json
        await self.hub.execute("""
            INSERT INTO decision_log (
                trade_date, decision_type, ticker, signal_source, notes
            ) VALUES ($1, 'SCENARIO_TRIGGER', NULL, 'BLACK_SWAN', $2)
        """, trade_date, json.dumps({
            "scenario_id":   scenario["id"],
            "scenario_name": scenario["name"],
            "action":        action,
            "details":       details,
        }, ensure_ascii=False))

        if self.telegram:
            try:
                await self.telegram.send_message(
                    f"🦢 黑天鵝觸發：{scenario['name']}\n"
                    f"動作：{action}\n"
                    f"說明：{scenario['description']}\n"
                    f"詳情：{details}"
                )
            except Exception as e:
                logger.warning("Telegram 通知失敗：%s", e)

    # ── 主執行 ───────────────────────────────────────────────────────────────

    async def check_triggers(
        self,
        vix:           Optional[float] = None,
        trade_date:    Optional[date]  = None,
        current_basis: Optional[float] = None,
    ) -> list[dict]:
        if trade_date is None:
            trade_date = date.today()

        if vix is None:
            row = await self.hub.fetchrow("""
                SELECT vix_level FROM market_regime
                ORDER BY trade_date DESC LIMIT 1
            """)
            if row:
                vix = float(row["vix_level"]) if row["vix_level"] else None

        triggered_scenarios = []

        for scenario in BLACK_SWAN_SCENARIOS:
            sid = scenario["id"]
            triggered = False
            details   = {}

            try:
                if sid == "CRASH_20PCT":
                    triggered, details = await self._check_crash(scenario, vix, trade_date)

                elif sid == "RATE_SHOCK":
                    triggered, details = await self._check_rate_shock(scenario, trade_date)

                elif sid == "GEOPOLITICAL":
                    triggered, details = await self._check_geopolitical(scenario, vix, trade_date)

                elif sid == "LIQUIDITY_CRISIS":
                    triggered, details = await self._check_liquidity_crisis(scenario)

                elif sid == "LIQUIDITY_BASIS":
                    triggered, details = await self._check_liquidity_basis(scenario, current_basis)

                elif sid == "LIQUIDITY_VOLUME":
                    triggered, details = await self._check_liquidity_volume(scenario, trade_date)

            except Exception as e:
                logger.error("黑天鵝情境檢測失敗 [%s]：%s", sid, e)
                continue

            if triggered:
                triggered_scenarios.append({
                    "scenario_id":   sid,
                    "scenario_name": scenario["name"],
                    "action":        scenario["action"],
                    "description":   scenario["description"],
                    "details":       details,
                })

        return triggered_scenarios

    async def run(self, trade_date: Optional[date] = None) -> dict:
        if trade_date is None:
            trade_date = date.today()

        triggered = await self.check_triggers(trade_date=trade_date)

        for scenario_info in triggered:
            matching = next(
                (s for s in BLACK_SWAN_SCENARIOS if s["id"] == scenario_info["scenario_id"]),
                None,
            )
            if matching:
                await self.execute_action(matching, trade_date, scenario_info["details"])

        return {
            "trade_date": str(trade_date),
            "triggered":  len(triggered),
            "scenarios":  triggered,
        }