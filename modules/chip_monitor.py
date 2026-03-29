# modules/chip_monitor.py
import logging
from datetime import date, timedelta
from typing import Optional
import pandas as pd
from datahub.data_hub import DataHub
from datahub.api_finmind import FinMindAPI
from datahub import redis_keys as rk

logger = logging.getLogger(__name__)


class ChipMonitor:
    """模組⑧ 籌碼異動監控：計算三層 CRS 分數"""

    def __init__(self, hub: DataHub, finmind: FinMindAPI):
        self.hub = hub
        self.finmind = finmind

    def _clamp(self, value, min_val, max_val):
        """限制數值在範圍內"""
        if value is None:
            return None
        return max(min_val, min(max_val, value))

    def _calc_volume_ratio_5d20d(self, price_df: pd.DataFrame) -> Optional[float]:
        """
        計算個股 5 日均量 / 20 日均量比率。

        [BUG 5 修正] 原本 _calc_crs() 完全未計算此值，
        導致 volume_ratio_5d20d 永遠為 NULL，
        進而使 LIQUIDITY_VOLUME 黑天鵝情境失效。

        Args:
            price_df: 包含 volume 欄位的 OHLCV DataFrame，按日期升序排列

        Returns:
            5MA / 20MA 比率（保留 3 位小數），資料不足時回傳 None
        """
        if price_df is None or price_df.empty:
            return None

        vol_col = None
        for c in ['Trading_Volume', 'volume', 'Volume']:
            if c in price_df.columns:
                vol_col = c
                break

        if vol_col is None:
            logger.debug("price_df 無成交量欄位，無法計算 volume_ratio_5d20d")
            return None

        volumes = price_df[vol_col].dropna().astype(float).tolist()
        if len(volumes) < 20:
            return None

        ma5  = sum(volumes[-5:])  / 5
        ma20 = sum(volumes[-20:]) / 20

        if ma20 <= 0:
            return None

        ratio = round(ma5 / ma20, 3)
        return ratio

    def _calc_crs(self, inst: pd.DataFrame, margin: pd.DataFrame) -> dict:
        result = {
            'crs_layer1': 50.0,
            'crs_layer2': 50.0,
            'crs_layer3': 50.0,
            'crs_total':  50.0,
            'fini_net_buy_bn': None,
            'it_net_buy_bn': None,
            'three_way_resonance': False,
            'margin_loan_change_pct': None,
            'sr_ratio': None,
            'volume_ratio_5d20d': None,   # [BUG 5 修正] 初始化欄位
            'alerts': [],
        }
        alerts = []

        # ── Layer 1：表層（三大法人）──
        l1 = 50.0
        fini_buy = it_buy = dealer_buy = None

        if not inst.empty:
            inst_latest = inst[inst['date'] == inst['date'].max()]
            for _, row in inst_latest.iterrows():
                name = str(row.get('name', ''))
                buy  = float(row.get('buy', 0) or 0)
                sell = float(row.get('sell', 0) or 0)
                net  = (buy - sell) / 1e8

                if '外資' in name or 'Foreign' in name.title():
                    fini_buy = net
                    result['fini_net_buy_bn'] = round(self._clamp(net, -999.99, 999.99), 2)
                    if net > 5:    l1 += 20
                    elif net > 0:  l1 += 10
                    elif net < -5: l1 -= 20
                    else:          l1 -= 5

                elif '投信' in name:
                    it_buy = net
                    result['it_net_buy_bn'] = round(self._clamp(net, -999.99, 999.99), 2)
                    if net > 1:    l1 += 15
                    elif net > 0:  l1 += 5
                    elif net < -1: l1 -= 10

                elif '自營' in name:
                    dealer_buy = net
                    if net > 0: l1 += 5
                    else:        l1 -= 5

            buys = [x for x in [fini_buy, it_buy, dealer_buy] if x is not None]
            if len(buys) >= 2:
                all_pos = all(x > 0 for x in buys)
                all_neg = all(x < 0 for x in buys)
                result['three_way_resonance'] = all_pos or all_neg
                if all_pos:
                    l1 += 10
                    alerts.append('THREE_WAY_BUY')
                elif all_neg:
                    l1 -= 10
                    alerts.append('THREE_WAY_SELL')

        result['crs_layer1'] = round(max(0, min(100, l1)), 2)

        # ── Layer 2：中層（籌碼結構）──
        l2 = 50.0
        if not margin.empty:
            margin_latest = margin[margin['date'] == margin['date'].max()]
            if not margin_latest.empty:
                row = margin_latest.iloc[0]
                margin_remain = float(row.get('MarginPurchaseRemain', 1) or 1)
                short_remain  = float(row.get('ShortSaleRemain', 1) or 1)
                margin_buy    = float(row.get('MarginPurchaseBuy', 0) or 0)
                margin_sell   = float(row.get('MarginPurchaseSell', 0) or 0)

                # 券資比（限制在 0~9.9999）
                sr = short_remain / max(margin_remain, 1)
                result['sr_ratio'] = round(self._clamp(sr, 0, 9.9999), 4)
                if sr > 0.3:
                    l2 -= 15
                    alerts.append('HIGH_SR_RATIO')
                elif sr < 0.1:
                    l2 += 10

                # 融資增減率（限制在 -99.9999~99.9999）
                if margin_remain > 0:
                    margin_chg = (margin_buy - margin_sell) / margin_remain
                    result['margin_loan_change_pct'] = round(
                        self._clamp(margin_chg, -99.9999, 99.9999), 4
                    )
                    if margin_chg > 0.05:    l2 -= 10
                    elif margin_chg < -0.05: l2 += 5

        result['crs_layer2'] = round(max(0, min(100, l2)), 2)

        # ── Layer 3：深層 ──
        l3 = result['crs_layer1'] * 0.5 + result['crs_layer2'] * 0.5
        result['crs_layer3'] = round(l3, 2)

        total = (
            result['crs_layer1'] * 0.4 +
            result['crs_layer2'] * 0.35 +
            result['crs_layer3'] * 0.25
        )
        result['crs_total'] = round(max(0, min(100, total)), 2)
        result['alerts'] = alerts
        return result

    async def run(self, ticker: str, trade_date: Optional[date] = None) -> dict:
        if trade_date is None:
            trade_date = date.today()

        logger.info("籌碼監控執行中：%s %s", ticker, trade_date)
        start_date = str(trade_date.replace(day=1))

        inst_df   = await self.finmind.get_institutional_investors(ticker, start_date)
        margin_df = await self.finmind.get_margin_trading(ticker, start_date)

        # [BUG 5 修正] 取得個股歷史成交量資料，用於計算 volume_ratio_5d20d
        price_start = str(trade_date - timedelta(days=35))
        price_df = await self.finmind.get_stock_price(ticker, price_start)

        crs = self._calc_crs(inst_df, margin_df)

        # [BUG 5 修正] 計算 volume_ratio_5d20d 並存入 crs dict
        volume_ratio = self._calc_volume_ratio_5d20d(price_df)
        crs['volume_ratio_5d20d'] = volume_ratio
        if volume_ratio is not None:
            logger.debug("volume_ratio_5d20d=%s for %s", volume_ratio, ticker)

        await self.hub.execute("""
            INSERT INTO chip_monitor (
                trade_date, ticker,
                fini_net_buy_bn, it_net_buy_bn,
                three_way_resonance,
                margin_loan_change_pct, sr_ratio,
                crs_layer1, crs_layer2, crs_layer3, crs_total,
                alerts,
                volume_ratio_5d20d
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
            ON CONFLICT (trade_date, ticker) DO UPDATE SET
                fini_net_buy_bn        = EXCLUDED.fini_net_buy_bn,
                it_net_buy_bn          = EXCLUDED.it_net_buy_bn,
                three_way_resonance    = EXCLUDED.three_way_resonance,
                margin_loan_change_pct = EXCLUDED.margin_loan_change_pct,
                sr_ratio               = EXCLUDED.sr_ratio,
                crs_layer1             = EXCLUDED.crs_layer1,
                crs_layer2             = EXCLUDED.crs_layer2,
                crs_layer3             = EXCLUDED.crs_layer3,
                crs_total              = EXCLUDED.crs_total,
                alerts                 = EXCLUDED.alerts,
                volume_ratio_5d20d     = EXCLUDED.volume_ratio_5d20d
        """,
        # [BUG 5 修正] 欄位清單末尾加入 volume_ratio_5d20d，VALUES 加入第 13 個參數
            trade_date, ticker,
            crs['fini_net_buy_bn'], crs['it_net_buy_bn'],
            crs['three_way_resonance'],
            crs['margin_loan_change_pct'], crs['sr_ratio'],
            crs['crs_layer1'], crs['crs_layer2'],
            crs['crs_layer3'], crs['crs_total'],
            crs['alerts'],
            crs['volume_ratio_5d20d'],
        )

        await self.hub.cache.set(
            rk.key_chip_crs_latest(ticker),
            crs,
            ttl=rk.TTL_1H,
        )

        logger.info("籌碼監控完成：%s CRS=%.1f vol_ratio=%s",
                    ticker, crs['crs_total'], crs['volume_ratio_5d20d'])
        return {**crs, 'ticker': ticker, 'trade_date': trade_date}