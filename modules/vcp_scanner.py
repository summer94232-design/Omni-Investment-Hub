# modules/vcp_scanner.py
# ═══════════════════════════════════════════════════════════════════════════════
# VCP（Volatility Contraction Pattern）收縮形態掃描器
#
# 說明：
#   VCP 是 Mark Minervini 提出的選股形態，核心邏輯為：
#   股價在修正後出現連續的波動收縮（每次高低差 < 前次），
#   配合成交量遞減，最後在低量中醞釀突破，為強勢進場點。
#
# 評分邏輯（0-100 分）：
#   - 每發現一次有效收縮 +25 分（最多 4 次）
#   - 最終收縮幅度 < 5% +10 分（高品質緊密盤整）
#   - 成交量隨收縮同步遞減 +10 分
#   - 目前股價在 MA20 和 MA60 之上 +10 分（必要條件）
#   - 最終 stage 跌幅 < 3% +5 分（超緊密收縮）
#
# 使用時機：
#   MacroFilter Regime 為 BULL_TREND 或 WEAK_BULL（vcp_enabled=True）時，
#   SelectionEngine 呼叫此模組為候選股加分
# ═══════════════════════════════════════════════════════════════════════════════

import logging
from datetime import date, timedelta
from typing import Optional
import pandas as pd
from datahub.data_hub import DataHub
from datahub.api_finmind import FinMindAPI

logger = logging.getLogger(__name__)

# VCP 判斷參數
VCP_LOOKBACK_DAYS  = 120     # 回看天數
VCP_MIN_BARS       = 60      # 最少所需 K 棒數
VCP_STAGE_DAYS     = 10      # 每個收縮週期的 K 棒數
VCP_CONTRACT_RATIO = 0.85    # 每次收縮幅度需 < 前次 × 0.85
VCP_TIGHT_THRESHOLD = 0.05   # 最終收縮幅度 < 5% 為高品質
VCP_ULTRA_TIGHT    = 0.03    # 超緊密收縮 < 3%
VCP_MIN_MA_SCORE   = 40      # 在 MA 之上才算有效 VCP


class VCPScanner:
    """
    VCP 收縮形態掃描器。

    用法：
        scanner = VCPScanner(hub, finmind)
        result  = await scanner.scan('2330', date.today())
        if result['is_vcp']:
            print(f"VCP 確認，品質分數 {result['score']}")
    """

    def __init__(self, hub: DataHub, finmind: FinMindAPI):
        self.hub     = hub
        self.finmind = finmind

    def _calc_stage_range(self, price_df: pd.DataFrame, start: int, length: int) -> float:
        """計算某段 K 棒的高低差比例（振幅）"""
        chunk = price_df.iloc[start: start + length]
        if len(chunk) < 3:
            return 1.0
        high  = chunk["max"].astype(float).max()
        low   = chunk["min"].astype(float).min()
        mid   = (high + low) / 2
        if mid <= 0:
            return 1.0
        return (high - low) / mid

    def _calc_stage_volume(self, price_df: pd.DataFrame, start: int, length: int) -> float:
        """計算某段 K 棒的平均成交量"""
        chunk = price_df.iloc[start: start + length]
        if chunk.empty or "volume" not in chunk.columns:
            return 0.0
        return chunk["volume"].astype(float).mean()

    def _is_above_ma(self, price_df: pd.DataFrame) -> bool:
        """目前股價是否在 MA20 和 MA60 之上"""
        if len(price_df) < 20:
            return False
        closes = price_df["close"].astype(float)
        latest = float(closes.iloc[-1])
        ma20   = closes.tail(20).mean()
        ma60   = closes.tail(60).mean() if len(closes) >= 60 else ma20
        return latest > ma20 and latest > ma60

    async def scan(
        self,
        ticker:     str,
        trade_date: Optional[date] = None,
    ) -> dict:
        """
        掃描個股是否呈現 VCP 形態。

        回傳：
        {
            'ticker':       str,
            'is_vcp':       bool,
            'score':        int,          # 0-100
            'contractions': int,          # 有效收縮次數
            'final_range':  float,        # 最後一段振幅（越小越好）
            'above_ma':     bool,
            'volume_shrink':bool,
            'detail':       str,
        }
        """
        if trade_date is None:
            trade_date = date.today()

        start_date = str(trade_date - timedelta(days=VCP_LOOKBACK_DAYS + 20))
        try:
            price_df = await self.finmind.get_stock_price(
                ticker,
                start_date=start_date,
                end_date=str(trade_date),
            )
        except Exception as e:
            logger.warning("VCP 掃描：無法取得 %s 股價：%s", ticker, e)
            return self._empty_result(ticker, "api_error")

        if price_df.empty or len(price_df) < VCP_MIN_BARS:
            return self._empty_result(ticker, "insufficient_data")

        price_df = price_df.sort_values("date").reset_index(drop=True)

        # 取最後 VCP_LOOKBACK_DAYS 筆
        df = price_df.tail(VCP_LOOKBACK_DAYS).reset_index(drop=True)
        n  = len(df)

        # ── 計算各 stage 振幅與成交量 ──────────────────────────────────────
        stages        = []
        stage_volumes = []
        step          = VCP_STAGE_DAYS

        pos = max(0, n - step * 5)  # 從後 5 個 stage 開始看
        while pos + step <= n:
            rng = self._calc_stage_range(df, pos, step)
            vol = self._calc_stage_volume(df, pos, step)
            stages.append(rng)
            stage_volumes.append(vol)
            pos += step

        if len(stages) < 2:
            return self._empty_result(ticker, "too_few_stages")

        # ── 計算有效收縮次數 ───────────────────────────────────────────────
        contractions = 0
        for i in range(1, len(stages)):
            if stages[i] < stages[i - 1] * VCP_CONTRACT_RATIO:
                contractions += 1

        # ── 成交量是否同步遞減 ─────────────────────────────────────────────
        volume_shrink = all(
            stage_volumes[i] <= stage_volumes[i - 1] * 1.05
            for i in range(1, len(stage_volumes))
            if stage_volumes[i - 1] > 0
        )

        # ── 最後一段振幅 ───────────────────────────────────────────────────
        final_range = stages[-1] if stages else 1.0
        above_ma    = self._is_above_ma(df)

        # ── 計分 ──────────────────────────────────────────────────────────
        score = 0
        score += min(contractions, 4) * 25        # 最多 4 次收縮 × 25分
        if final_range < VCP_ULTRA_TIGHT:  score += 15
        elif final_range < VCP_TIGHT_THRESHOLD: score += 10
        if volume_shrink:                  score += 10
        if above_ma:                       score += 10
        score = min(100, score)

        # ── 判斷是否為有效 VCP ─────────────────────────────────────────────
        is_vcp = (
            contractions >= 2
            and above_ma
            and final_range < VCP_TIGHT_THRESHOLD * 2
        )

        detail_parts = [
            f"收縮次數={contractions}",
            f"最終振幅={final_range*100:.1f}%",
            f"量縮={'是' if volume_shrink else '否'}",
            f"均線之上={'是' if above_ma else '否'}",
        ]

        result = {
            "ticker":        ticker,
            "trade_date":    str(trade_date),
            "is_vcp":        is_vcp,
            "score":         score,
            "contractions":  contractions,
            "final_range":   round(final_range, 4),
            "above_ma":      above_ma,
            "volume_shrink": volume_shrink,
            "detail":        " | ".join(detail_parts),
        }

        if is_vcp:
            logger.info("VCP 確認：%s 分數=%d %s", ticker, score, result["detail"])
        else:
            logger.debug("VCP 不成立：%s %s", ticker, result["detail"])

        return result

    def _empty_result(self, ticker: str, reason: str) -> dict:
        return {
            "ticker":        ticker,
            "trade_date":    str(date.today()),
            "is_vcp":        False,
            "score":         0,
            "contractions":  0,
            "final_range":   1.0,
            "above_ma":      False,
            "volume_shrink": False,
            "detail":        reason,
        }

    async def run_batch(
        self,
        tickers:    list[str],
        trade_date: Optional[date] = None,
        min_score:  int = 50,
    ) -> list[dict]:
        """批量掃描多檔，回傳達到門檻的結果（依分數降序）"""
        if trade_date is None:
            trade_date = date.today()

        results = []
        for ticker in tickers:
            r = await self.scan(ticker, trade_date)
            if r["score"] >= min_score:
                results.append(r)

        results.sort(key=lambda x: x["score"], reverse=True)
        logger.info(
            "VCP 批量掃描完成：%d 檔中共 %d 檔達到門檻（score ≥ %d）",
            len(tickers), len(results), min_score,
        )
        return results