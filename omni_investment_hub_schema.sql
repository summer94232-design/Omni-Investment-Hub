-- =============================================================================
-- Omni-Investment Hub — PostgreSQL Schema DDL
-- PostgreSQL 15+  |  UTF-8  |  Asia/Taipei
-- =============================================================================
-- 設計原則：
--   1. 月分區（大量寫入表）：topic_heat / stock_diagnostic / decision_log
--   2. 不可變歷史：只 INSERT，用 is_active / version 管理「當前有效」記錄
--   3. JSONB 欄位：彈性儲存向量/矩陣類資料，避免過度正規化
--   4. 索引策略：複合索引覆蓋最常見查詢模式
-- =============================================================================
-- [修正記錄 v9]
--   Bug① audit_log: id BIGSERIAL PRIMARY KEY → id BIGSERIAL NOT NULL +
--                   PRIMARY KEY (id, logged_at)，符合 Postgres 分區表規範
--   Bug② idx_ce_upcoming: 移除 WHERE event_date >= CURRENT_DATE 條件，
--                          改為無條件索引，避免插入歷史資料時索引失效
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 0. 擴充套件與設定
-- -----------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";     -- UUID 主鍵
CREATE EXTENSION IF NOT EXISTS "pg_trgm";       -- 文字模糊搜尋（題材名稱）
CREATE EXTENSION IF NOT EXISTS "btree_gin";     -- GIN 索引支援複合查詢

SET timezone = 'Asia/Taipei';

-- 自動更新 updated_at 的觸發器函式（所有表共用）
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- 1. topic_heat — 題材熱度（月分區）
--    來源：模組① 三週期情緒模型
--    寫入頻率：每日收盤後一次
--    保留期：5 年（自動 DROP 舊分區）
-- =============================================================================
CREATE TABLE topic_heat (
    id              UUID        NOT NULL DEFAULT uuid_generate_v4(),
    trade_date      DATE        NOT NULL,              -- 交易日（分區鍵）
    topic           TEXT        NOT NULL,              -- 題材名稱，e.g. 'AI算力'
    raw_count       INTEGER     NOT NULL DEFAULT 0,    -- 當日原始熱詞出現次數
    zscore_5d       NUMERIC(8,4),                      -- 5日 Z-Score（快線）
    zscore_20d      NUMERIC(8,4),                      -- 20日 Z-Score（慢線）
    percentile_60d  NUMERIC(5,2),                      -- 60日熱度百分位 0-100
    ma_5d           NUMERIC(12,2),                     -- 5日滾動均值
    ma_20d          NUMERIC(12,2),
    ma_60d          NUMERIC(12,2),
    alert_type      TEXT        CHECK (alert_type IN (
                        'OVERHEATED',    -- 60d 分位 > 95%
                        'LATENT',        -- 60d 分位 < 20%
                        'DIVERGENCE',    -- 熱度創高但股價不漲
                        'NORMAL',
                        NULL
                    )),
    price_diverge   BOOLEAN     NOT NULL DEFAULT FALSE, -- 利多不漲背離旗標
    source_counts   JSONB,
    -- e.g. {"ptt": 42, "dcard": 18, "news": 127, "twitter": 55}
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, trade_date)                       -- 分區表主鍵必須含分區鍵
) PARTITION BY RANGE (trade_date);

-- 建立月分區（範例：2024–2027，之後用 maintenance.py 自動滾動）
CREATE TABLE topic_heat_2024_01 PARTITION OF topic_heat
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE topic_heat_2024_02 PARTITION OF topic_heat
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
CREATE TABLE topic_heat_2026_02 PARTITION OF topic_heat
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE topic_heat_2026_03 PARTITION OF topic_heat
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE TABLE topic_heat_2026_04 PARTITION OF topic_heat
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');

CREATE INDEX idx_th_topic_date   ON topic_heat (topic, trade_date DESC);
CREATE INDEX idx_th_date_alert   ON topic_heat (trade_date DESC, alert_type)
    WHERE alert_type IS NOT NULL;
CREATE INDEX idx_th_topic_trgm   ON topic_heat USING GIN (topic gin_trgm_ops);

CREATE TRIGGER trg_th_updated_at
    BEFORE UPDATE ON topic_heat
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMENT ON TABLE  topic_heat             IS '題材熱度三週期情緒模型（模組①輸出）';
COMMENT ON COLUMN topic_heat.zscore_5d   IS '5日快線：捕捉突發動能';
COMMENT ON COLUMN topic_heat.zscore_20d  IS '20日慢線：確認月級別趨勢';
COMMENT ON COLUMN topic_heat.percentile_60d IS '60日基線：>95%=過熱, <20%=潛伏';


-- =============================================================================
-- 2. stock_diagnostic — 個股因子診斷（月分區）
--    來源：模組② 選股引擎 + 模組⑥ 宏觀濾網（動態權重）
--    寫入頻率：每日收盤後
-- =============================================================================
CREATE TABLE stock_diagnostic (
    id                  UUID        NOT NULL DEFAULT uuid_generate_v4(),
    trade_date          DATE        NOT NULL,
    ticker              CHAR(6)     NOT NULL,          -- e.g. '2330  '
    company_name        TEXT,

    -- 四大因子原始分數 (0–100)
    score_momentum      NUMERIC(5,2),   -- 動能因子
    score_chip          NUMERIC(5,2),   -- 籌碼因子（對應 CRS）
    score_fundamental   NUMERIC(5,2),   -- 基本面因子
    score_valuation     NUMERIC(5,2),   -- 估值因子

    -- 動態因子權重（從宏觀濾網取得，非靜態 30/25/25/20）
    weight_momentum     NUMERIC(4,3),
    weight_chip         NUMERIC(4,3),
    weight_fundamental  NUMERIC(4,3),
    weight_valuation    NUMERIC(4,3),

    -- 加權總分
    total_score         NUMERIC(5,2) GENERATED ALWAYS AS (
        COALESCE(score_momentum,0)   * COALESCE(weight_momentum,0.30) +
        COALESCE(score_chip,0)       * COALESCE(weight_chip,0.25)     +
        COALESCE(score_fundamental,0)* COALESCE(weight_fundamental,0.25) +
        COALESCE(score_valuation,0)  * COALESCE(weight_valuation,0.20)
    ) STORED,

    -- VCP 形態
    vcp_detected        BOOLEAN     NOT NULL DEFAULT FALSE,
    vcp_contraction_days INTEGER,               -- 波動率收縮天數
    vcp_stage           SMALLINT,               -- 1–4 收縮階段

    -- 技術指標
    atr_20d             NUMERIC(10,4),           -- 20日 ATR
    rs_rank             NUMERIC(5,2),            -- 相對強度排名（全市場百分位）
    price_vs_52w_high   NUMERIC(6,4),            -- 距 52 週高點比例
    ma20                NUMERIC(12,2),
    ma60                NUMERIC(12,2),
    ma200               NUMERIC(12,2),

    -- 估值
    pe_ratio            NUMERIC(8,2),
    pe_percentile       NUMERIC(5,2),            -- P/E 歷史分位
    marginal_cost_support NUMERIC(12,2),         -- 邊際成本支撐價

    -- 基本面快照
    revenue_yoy         NUMERIC(7,4),            -- 營收 YoY
    gross_margin        NUMERIC(6,4),
    op_margin           NUMERIC(6,4),
    net_margin          NUMERIC(6,4),

    -- 市場環境快照（紀錄當時的宏觀狀態，用於事後歸因）
    mrs_at_calc         NUMERIC(5,2),            -- 計算時的 MRS 分數
    regime_at_calc      TEXT CHECK (regime_at_calc IN (
                            'BULL_TREND','WEAK_BULL','CHOPPY','BEAR_TREND')),

    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, trade_date)
) PARTITION BY RANGE (trade_date);

CREATE TABLE stock_diagnostic_2026_02 PARTITION OF stock_diagnostic
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE stock_diagnostic_2026_03 PARTITION OF stock_diagnostic
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE TABLE stock_diagnostic_2026_04 PARTITION OF stock_diagnostic
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');

CREATE INDEX idx_sd_ticker_date   ON stock_diagnostic (ticker, trade_date DESC);
CREATE INDEX idx_sd_date_score    ON stock_diagnostic (trade_date DESC, total_score DESC);
CREATE INDEX idx_sd_vcp           ON stock_diagnostic (trade_date DESC, vcp_detected)
    WHERE vcp_detected = TRUE;
CREATE INDEX idx_sd_regime        ON stock_diagnostic (trade_date DESC, regime_at_calc);

CREATE TRIGGER trg_sd_updated_at
    BEFORE UPDATE ON stock_diagnostic
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMENT ON TABLE  stock_diagnostic IS '個股多因子診斷日快照（模組②輸出）';
COMMENT ON COLUMN stock_diagnostic.total_score IS '四因子加權總分（0-100），計算欄位自動維護';
COMMENT ON COLUMN stock_diagnostic.mrs_at_calc IS '記錄計算時的宏觀環境，供事後歸因比對';


-- =============================================================================
-- 3. market_regime — 宏觀環境每日快照
--    來源：模組⑥ 宏觀濾網
--    寫入頻率：每日收盤後
-- =============================================================================
CREATE TABLE market_regime (
    id                  SERIAL      PRIMARY KEY,
    trade_date          DATE        NOT NULL UNIQUE,
    regime              TEXT        NOT NULL CHECK (regime IN (
                            'BULL_TREND','WEAK_BULL','CHOPPY','BEAR_TREND')),
    mrs_score           NUMERIC(5,2) NOT NULL,          -- 0–100
    layer1_structure    NUMERIC(5,2),                   -- 結構層分數
    layer2_breadth      NUMERIC(5,2),                   -- 廣度層分數
    layer3_volatility   NUMERIC(5,2),                   -- 波動率層分數

    -- 各指標原始值
    ma200_slope_zscore  NUMERIC(7,4),
    ma_alignment_pct    NUMERIC(5,2),
    nhnl_ratio          NUMERIC(5,4),
    breadth_ma20_pct    NUMERIC(5,2),
    ad_divergence       BOOLEAN,
    hv_percentile       NUMERIC(5,2),
    vix_level           NUMERIC(7,3),

    -- 對應的策略配置
    max_exposure_limit  NUMERIC(4,3) NOT NULL,          -- e.g. 0.90
    vcp_enabled         BOOLEAN     NOT NULL DEFAULT TRUE,
    atr_multiplier      NUMERIC(3,1) NOT NULL DEFAULT 3.0,
    factor_weights      JSONB        NOT NULL,
    -- e.g. {"momentum":0.38,"chip":0.28,"fundamental":0.22,"valuation":0.12}

    alerts              TEXT[],                         -- 觸發的警告列表
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_mr_date ON market_regime (trade_date DESC);

COMMENT ON TABLE market_regime IS '宏觀環境每日快照，是所有選股/倉位邏輯的上游輸入（模組⑥）';
COMMENT ON COLUMN market_regime.factor_weights IS 'JSON格式的動態因子權重，下發給 stock_diagnostic 計算';


-- =============================================================================
-- 4. portfolio_health — 組合健康度日快照
--    來源：模組④ 部位控管
--    寫入頻率：每日收盤後
-- =============================================================================
CREATE TABLE portfolio_health (
    id                  SERIAL      PRIMARY KEY,
    snapshot_date       DATE        NOT NULL UNIQUE,
    nav                 NUMERIC(14,2) NOT NULL,          -- 組合淨值（元）
    cash_amount         NUMERIC(14,2) NOT NULL DEFAULT 0,
    stock_market_value  NUMERIC(14,2) NOT NULL DEFAULT 0,

    -- 曝險統計
    gross_exposure_pct  NUMERIC(5,4) NOT NULL,           -- 股票部位 / NAV
    net_exposure_pct    NUMERIC(5,4),
    exposure_level      SMALLINT CHECK (exposure_level BETWEEN 1 AND 5),

    -- 損益
    daily_pnl_pct       NUMERIC(7,4),
    daily_pnl_abs       NUMERIC(14,2),
    mtd_pnl_pct         NUMERIC(7,4),                   -- 月累計
    ytd_pnl_pct         NUMERIC(7,4),                   -- 年累計
    drawdown_from_peak  NUMERIC(6,4),                   -- 從高點回落

    -- 風險指標
    portfolio_var_95    NUMERIC(7,4),                   -- 95% VaR（日）
    portfolio_cvar_95   NUMERIC(7,4),                   -- CVaR（尾端期望損失）
    portfolio_beta      NUMERIC(5,3),
    portfolio_sharpe_ytd NUMERIC(6,3),

    -- 相關係數矩陣（JSONB，避免動態欄位數問題）
    correlation_matrix  JSONB,
    -- e.g. {"2330":{"2330":1.0,"2303":0.82},"2303":{"2330":0.82,"2303":1.0}}
    high_corr_pairs     JSONB,
    -- e.g. [{"a":"2330","b":"2303","rho":0.82}]
    high_corr_count     SMALLINT    NOT NULL DEFAULT 0, -- ρ>0.7 的持倉對數

    -- 熔斷狀態
    circuit_breaker_triggered BOOLEAN NOT NULL DEFAULT FALSE,
    circuit_breaker_reason TEXT,

    -- 宏觀快照（快取，避免每次 JOIN）
    regime_snapshot     TEXT,
    mrs_snapshot        NUMERIC(5,2),

    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_ph_date ON portfolio_health (snapshot_date DESC);

COMMENT ON TABLE  portfolio_health IS '組合健康度每日快照（模組④輸出），含 VaR/相關係數/熔斷狀態';
COMMENT ON COLUMN portfolio_health.correlation_matrix IS 'JSON 格式相關係數矩陣，ρ>0.7 時限制新增同類部位';


-- =============================================================================
-- 5. positions — 持倉狀態機
--    來源：模組⑦ 主動出場引擎
--    寫入頻率：每次進出場/狀態切換
-- =============================================================================
CREATE TABLE positions (
    id                  UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    ticker              CHAR(6)     NOT NULL,
    is_open             BOOLEAN     NOT NULL DEFAULT TRUE,

    -- 狀態機（對應 S1–S5）
    state               TEXT        NOT NULL CHECK (state IN (
                            'S1_INITIAL_DEFENSE',
                            'S2_BREAKOUT_CONFIRM',
                            'S3_PROFIT_PROTECT',
                            'S4_TRAILING_STOP',
                            'S5_ACTIVE_EXIT',
                            'STOPPED_OUT',
                            'CLOSED'
                        )),

    -- 進場資訊
    entry_date          DATE        NOT NULL,
    entry_price         NUMERIC(12,4) NOT NULL,
    initial_shares      INTEGER     NOT NULL,
    current_shares      INTEGER     NOT NULL,
    signal_source       TEXT        NOT NULL,  -- 'VCP'|'ADDON'|...
    strategy_tag        TEXT,                  -- 'VCP突破'|'贏家跟單'|...

    -- 風控參數（進場時快照，不隨市場變化）
    atr_at_entry        NUMERIC(10,4) NOT NULL,
    atr_multiplier      NUMERIC(3,1)  NOT NULL DEFAULT 3.0,
    initial_stop_price  NUMERIC(12,4) NOT NULL,  -- = entry - R
    r_amount            NUMERIC(12,4) NOT NULL,  -- 每股風險金額

    -- 動態止損（每日更新）
    current_stop_price  NUMERIC(12,4),
    trailing_stop_price NUMERIC(12,4),
    highest_price_seen  NUMERIC(12,4),
    trail_pct           NUMERIC(4,3)  NOT NULL DEFAULT 0.15,

    -- 加碼記錄
    addon_done          BOOLEAN     NOT NULL DEFAULT FALSE,
    addon_shares        INTEGER     NOT NULL DEFAULT 0,
    addon_price         NUMERIC(12,4),
    avg_cost            NUMERIC(12,4),           -- 含加碼後的平均成本

    -- 損益追蹤
    realized_pnl        NUMERIC(14,2) NOT NULL DEFAULT 0,
    unrealized_pnl      NUMERIC(14,2),           -- 每日收盤更新
    r_multiple_current  NUMERIC(6,3),            -- 當前 R-Multiple

    -- 出場記錄
    exit_date           DATE,
    exit_price          NUMERIC(12,4),
    exit_reason         TEXT,                    -- 'STOP_LOSS'|'TRAILING'|...
    exit_signal_source  TEXT,

    -- 進場當時的系統狀態快照（歸因分析用）
    mrs_at_entry        NUMERIC(5,2),
    regime_at_entry     TEXT,
    ev_at_entry         NUMERIC(7,4),            -- 進場時計算的期望值
    crs_at_entry        NUMERIC(5,2),            -- 進場時的籌碼評分

    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_pos_open    ON positions (is_open, ticker) WHERE is_open = TRUE;
CREATE INDEX idx_pos_ticker  ON positions (ticker, entry_date DESC);
CREATE INDEX idx_pos_state   ON positions (state) WHERE is_open = TRUE;

CREATE TRIGGER trg_pos_updated_at
    BEFORE UPDATE ON positions
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMENT ON TABLE  positions IS '持倉狀態機，記錄每筆部位的完整生命週期（模組⑦）';
COMMENT ON COLUMN positions.r_amount IS 'R = ATR × 倍數，是所有出場計算的基準單位';
COMMENT ON COLUMN positions.mrs_at_entry IS '進場時的宏觀環境分數，供事後歸因分析用';


-- =============================================================================
-- 6. decision_log — 決策日誌（月分區）
--    來源：所有模組（訊號 → 決策 → 執行全程記錄）
--    寫入頻率：每次訊號產生時
-- =============================================================================
CREATE TABLE decision_log (
    id              UUID        NOT NULL DEFAULT uuid_generate_v4(),
    logged_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    trade_date      DATE        NOT NULL,

    -- 決策識別
    position_id     UUID        REFERENCES positions(id),
    ticker          CHAR(6),
    decision_type   TEXT        NOT NULL CHECK (decision_type IN (
                        'ENTRY_SIGNAL',     -- 選股引擎選中
                        'ENTRY_EXECUTED',   -- 實際進場
                        'STATE_CHANGE',     -- 持倉狀態切換
                        'EXIT_SIGNAL',      -- 出場訊號觸發
                        'EXIT_EXECUTED',    -- 實際出場
                        'ADDON_SIGNAL',     -- 加碼訊號
                        'ADDON_EXECUTED',
                        'STOP_ADJUSTMENT',  -- 停損線調整
                        'REGIME_CHANGE',    -- 宏觀環境切換
                        'EVENT_RULE',       -- 事件日曆規則觸發
                        'CIRCUIT_BREAKER',  -- 熔斷觸發
                        'MANUAL_OVERRIDE',  -- 人工覆蓋
                        'SCENARIO_TRIGGER'  -- 黑天鵝情境觸發
                    )),

    -- 策略標籤
    strategy_tag    TEXT,        -- 'VCP突破'|'贏家跟單'|'預期落差'|...
    signal_source   TEXT,        -- 訊號來源模組
    regime_at_log   TEXT,        -- 記錄時的市場環境

    -- 決策依據（進場時填充）
    ev_bull         NUMERIC(6,4),   -- Bull 情境期望值
    ev_base         NUMERIC(6,4),   -- Base 情境期望值
    ev_bear         NUMERIC(6,4),   -- Bear 情境期望值
    ev_total        NUMERIC(6,4),   -- EV = Σ(P × R)
    total_score     NUMERIC(5,2),   -- 進場時的選股總分
    factor_weights  JSONB,          -- 進場時使用的因子權重快照

    -- 執行結果（出場/執行後填充）
    actual_return   NUMERIC(7,4),   -- 實際報酬
    r_multiple      NUMERIC(6,3),   -- 最終 R-Multiple
    holding_days    SMALLINT,       -- 持有天數
    alpha_source    TEXT CHECK (alpha_source IN (
                        'ALPHA','BETA','MIXED',NULL)),

    -- 行為標記（歸因閉環用）
    is_planned      BOOLEAN     NOT NULL DEFAULT TRUE,   -- FALSE = 計畫外交易
    is_emotional    BOOLEAN     NOT NULL DEFAULT FALSE,  -- 情緒性操作旗標
    override_reason TEXT,                                -- 人工覆蓋原因

    -- 備註
    notes           TEXT,
    raw_context     JSONB,       -- 儲存觸發時的完整市場快照

    PRIMARY KEY (id, trade_date)
) PARTITION BY RANGE (trade_date);

CREATE TABLE decision_log_2026_02 PARTITION OF decision_log
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE decision_log_2026_03 PARTITION OF decision_log
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE TABLE decision_log_2026_04 PARTITION OF decision_log
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');

CREATE INDEX idx_dl_ticker_date   ON decision_log (ticker, trade_date DESC)
    WHERE ticker IS NOT NULL;
CREATE INDEX idx_dl_type_date     ON decision_log (decision_type, trade_date DESC);
CREATE INDEX idx_dl_position      ON decision_log (position_id)
    WHERE position_id IS NOT NULL;
CREATE INDEX idx_dl_unplanned     ON decision_log (trade_date DESC)
    WHERE is_planned = FALSE;

COMMENT ON TABLE  decision_log IS '完整決策日誌，記錄每個訊號的產生→驗證→執行→結果（模組⑤歸因用）';
COMMENT ON COLUMN decision_log.is_planned IS '計畫外交易旗標，連續為 FALSE 時觸發行為偏差警告';
COMMENT ON COLUMN decision_log.alpha_source IS '歸因分析：ALPHA=選股能力, BETA=大盤紅利, MIXED=混合';


-- =============================================================================
-- 7. orders — 訂單執行記錄
--    來源：模組⑪ 執行閘門
--    寫入頻率：每次下單時
-- =============================================================================
CREATE TABLE orders (
    id              UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_date      DATE        NOT NULL DEFAULT CURRENT_DATE,
    position_id     UUID        REFERENCES positions(id),
    ticker          CHAR(6)     NOT NULL,

    -- 訂單參數
    side            TEXT        NOT NULL CHECK (side IN ('BUY','SELL')),
    quantity        INTEGER     NOT NULL CHECK (quantity > 0),
    order_type      TEXT        NOT NULL CHECK (order_type IN (
                        'LIMIT','MARKET','TWAP','VWAP')),
    limit_price     NUMERIC(12,4),
    twap_slices     SMALLINT    NOT NULL DEFAULT 1,

    -- 執行狀態
    status          TEXT        NOT NULL DEFAULT 'PENDING' CHECK (status IN (
                        'PENDING','SUBMITTED','PARTIAL','FILLED',
                        'CANCELLED','REJECTED','ROLLED_BACK'
                    )),
    filled_quantity INTEGER     NOT NULL DEFAULT 0,
    avg_fill_price  NUMERIC(12,4),
    slippage_pct    NUMERIC(6,4),
    commission      NUMERIC(10,2),
    tax             NUMERIC(10,2),  -- 證交稅 0.3%

    -- 閘門記錄
    signal_source   TEXT        NOT NULL,
    exec_mode       TEXT        NOT NULL CHECK (exec_mode IN (
                        'FULL_AUTO','SEMI_AUTO','PAPER','ALERT_ONLY','HALT'
                    )),
    gate1_result    TEXT,           -- 層①結果
    gate2_result    TEXT,           -- 層②結果
    gate3_result    TEXT,           -- 層③結果
    block_reason    TEXT,           -- 拒絕原因

    -- 安全機制記錄
    human_approved  BOOLEAN,        -- NULL=未需要, TRUE=通過, FALSE=拒絕/逾時
    rollback_done   BOOLEAN     NOT NULL DEFAULT FALSE,

    -- 時間戳
    submitted_at    TIMESTAMPTZ,
    filled_at       TIMESTAMPTZ,
    cancelled_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_ord_ticker_date ON orders (ticker, order_date DESC);
CREATE INDEX idx_ord_status      ON orders (status) WHERE status IN ('PENDING','SUBMITTED');
CREATE INDEX idx_ord_position    ON orders (position_id) WHERE position_id IS NOT NULL;

COMMENT ON TABLE orders IS '訂單執行記錄，含三層閘門結果與安全機制狀態（模組⑪）';


-- =============================================================================
-- 8. chip_monitor — 籌碼監控日快照
--    來源：模組⑧ 籌碼異動監控
--    寫入頻率：每日收盤後
-- =============================================================================
CREATE TABLE chip_monitor (
    id                  SERIAL      PRIMARY KEY,
    trade_date          DATE        NOT NULL,
    ticker              CHAR(6)     NOT NULL,
    UNIQUE (trade_date, ticker),

    -- 三大法人
    fini_net_buy_bn     NUMERIC(10,2),          -- 外資淨買超（億元）
    fini_streak         SMALLINT,               -- 連買(+)/連賣(-)天數
    fini_5d_pct         NUMERIC(7,4),           -- 5日買超佔市值比
    fini_acceleration   NUMERIC(5,2),           -- 加速度（5d/20d 均量比）
    it_net_buy_bn       NUMERIC(10,2),          -- 投信淨買超
    it_streak           SMALLINT,
    it_hold_pct         NUMERIC(5,4),           -- 投信持股比例
    it_at_peak          BOOLEAN,                -- 是否創持股歷史新高
    dealer_net_buy_bn   NUMERIC(10,2),          -- 自營商（自行）
    three_way_resonance BOOLEAN     NOT NULL DEFAULT FALSE, -- 三大法人同向

    -- 籌碼結構
    margin_loan_change_pct NUMERIC(6,4),        -- 融資增減率
    short_sell_change_pct  NUMERIC(6,4),        -- 融券增減率
    sr_ratio            NUMERIC(5,4),           -- 券資比
    margin_utilization  NUMERIC(5,4),           -- 融資使用率
    big_order_ratio     NUMERIC(5,4),           -- 大單成交比例

    -- 深層指標
    hhi_concentration   NUMERIC(6,4),           -- HHI 籌碼集中度
    pcr_ratio           NUMERIC(6,4),           -- Put/Call 未平倉比
    iv_skew             NUMERIC(7,4),           -- IV 偏斜
    inst_hold_slope_20d NUMERIC(8,4),           -- 法人持股20日斜率
    implied_direction   TEXT CHECK (implied_direction IN (
                            'BULLISH','NEUTRAL','BEARISH')),

    -- 綜合評分
    crs_layer1          NUMERIC(5,2),           -- 表層分數
    crs_layer2          NUMERIC(5,2),           -- 中層分數
    crs_layer3          NUMERIC(5,2),           -- 深層分數
    crs_total           NUMERIC(5,2),           -- 綜合 CRS（0-100）

    -- 警告
    alerts              TEXT[],
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_cm_date_crs    ON chip_monitor (trade_date DESC, crs_total DESC);
CREATE INDEX idx_cm_ticker_date ON chip_monitor (ticker, trade_date DESC);
CREATE INDEX idx_cm_resonance   ON chip_monitor (trade_date DESC)
    WHERE three_way_resonance = TRUE;

COMMENT ON TABLE  chip_monitor IS '三層籌碼監控日快照（模組⑧），CRS 下發至選股引擎作為籌碼因子';


-- =============================================================================
-- 9. calendar_events — 財經事件日曆
--    來源：模組⑩ 事件驅動日曆
--    寫入頻率：手動維護 + 自動爬取
-- =============================================================================
CREATE TABLE calendar_events (
    id              SERIAL      PRIMARY KEY,
    event_date      DATE        NOT NULL,
    event_type      TEXT        NOT NULL CHECK (event_type IN (
                        'FED','CPI','GDP','PMI',
                        'EARNINGS','GUIDANCE_CALL',
                        'EX_DIVIDEND','EX_RIGHTS',
                        'INDEX_REBALANCE','TRIPLE_WITCHING',
                        'MONTHLY_REVENUE','EXPORT_STATS',
                        'GEOPOLITICAL','OTHER'
                    )),
    name            TEXT        NOT NULL,
    impact_level    SMALLINT    NOT NULL CHECK (impact_level BETWEEN 1 AND 5),
    affected_tickers TEXT[],                    -- 空陣列 = 全市場
    affected_sectors TEXT[],

    -- 三窗口規則（JSON 儲存，彈性擴充）
    pre_rule        JSONB,
    -- {"days_offset":-5,"action":"RESTRICT","new_position":false,"atr_override":2.0}
    during_rule     JSONB,
    post_rule       JSONB,

    -- 預期值（用於 Surprise 計算）
    consensus_estimate NUMERIC(12,4),
    actual_value    NUMERIC(12,4),
    surprise_pct    NUMERIC(7,4) GENERATED ALWAYS AS (
        CASE WHEN consensus_estimate IS NOT NULL AND consensus_estimate != 0
             THEN (actual_value - consensus_estimate) / ABS(consensus_estimate)
             ELSE NULL END
    ) STORED,
    surprise_direction TEXT CHECK (surprise_direction IN (
                            'POSITIVE','NEUTRAL','NEGATIVE',NULL)),

    is_processed    BOOLEAN     NOT NULL DEFAULT FALSE,  -- 事後已處理
    notes           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_ce_date_type   ON calendar_events (event_date, event_type);
-- [Bug② 修正] 移除 WHERE event_date >= CURRENT_DATE
-- 原本的 partial index 在重建後會遺漏所有歷史日期的資料，改為無條件索引
CREATE INDEX idx_ce_upcoming    ON calendar_events (event_date);
CREATE INDEX idx_ce_tickers     ON calendar_events USING GIN (affected_tickers);

CREATE TRIGGER trg_ce_updated_at
    BEFORE UPDATE ON calendar_events
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMENT ON TABLE  calendar_events IS '財經事件日曆（模組⑩），含三窗口行動規則與 Surprise 自動計算';


-- =============================================================================
-- 10. wf_results — Walk-Forward 驗證報告
--    來源：Walk-Forward 引擎
--    寫入頻率：每季執行一次
-- =============================================================================
CREATE TABLE wf_results (
    id              SERIAL      PRIMARY KEY,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    data_from       DATE        NOT NULL,
    data_to         DATE        NOT NULL,
    train_months    SMALLINT    NOT NULL,
    oos_months      SMALLINT    NOT NULL,
    step_months     SMALLINT    NOT NULL,

    -- 彙總績效
    n_oos_windows       SMALLINT    NOT NULL,
    avg_oos_sharpe      NUMERIC(6,3),
    avg_oos_return      NUMERIC(7,4),
    avg_oos_max_dd      NUMERIC(6,4),
    consistency_rate    NUMERIC(5,4),   -- 正報酬窗口比例
    mc_pvalue           NUMERIC(6,4),
    is_significant      BOOLEAN,

    -- 因子穩定性（完整 JSON）
    factor_stability    JSONB NOT NULL,
    -- e.g. {"momentum":{"oos_mean":0.28,"oos_std":0.05,"is_stable":true}}

    -- 環境分層績效
    regime_breakdown    JSONB,

    -- 各窗口明細
    oos_window_details  JSONB,

    -- 評估結果
    overfitting_score   NUMERIC(4,3),
    recommendation      TEXT NOT NULL CHECK (recommendation IN (
                            'DEPLOY','ADJUST','REJECT')),
    warnings            TEXT[],

    -- 輸出：建議使用的因子權重
    recommended_weights JSONB,
    -- e.g. {"momentum":0.32,"chip":0.27,"fundamental":0.24,"valuation":0.17}

    is_active           BOOLEAN NOT NULL DEFAULT TRUE,  -- 當前生效的版本
    notes               TEXT
);

CREATE INDEX idx_wf_run_at  ON wf_results (run_at DESC);
CREATE INDEX idx_wf_active  ON wf_results (is_active) WHERE is_active = TRUE;

COMMENT ON TABLE wf_results IS 'Walk-Forward 驗證報告（每季），recommendation=DEPLOY 才能更新因子權重';


-- =============================================================================
-- 11. audit_log — 執行閘門審計日誌
--    來源：模組⑪ 執行閘門
--    寫入頻率：每個閘門事件
-- =============================================================================
-- [Bug① 修正] 分區表不可用 BIGSERIAL PRIMARY KEY（主鍵必須含分區鍵 logged_at）
-- 原本：id BIGSERIAL PRIMARY KEY
-- 修正：id BIGSERIAL NOT NULL + PRIMARY KEY (id, logged_at)
CREATE TABLE audit_log (
    id              BIGSERIAL   NOT NULL,                -- [修正] 移除 PRIMARY KEY
    logged_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),  -- 分區鍵
    order_id        UUID        REFERENCES orders(id),
    event_type      TEXT        NOT NULL,
    -- 'SUBMIT'|'EXEC'|'BLOCK'|'ADJUST'|'ROLLBACK'|'TIMEOUT'|'SYSTEM'
    exec_mode       TEXT        NOT NULL,
    ticker          CHAR(6),
    side            TEXT,
    quantity        INTEGER,
    price           NUMERIC(12,4),
    signal_source   TEXT,
    message         TEXT        NOT NULL,
    metadata        JSONB,                               -- 額外上下文
    PRIMARY KEY (id, logged_at)                          -- [修正] 複合主鍵含分區鍵
) PARTITION BY RANGE (logged_at);

CREATE TABLE audit_log_2024 PARTITION OF audit_log
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE audit_log_2025 PARTITION OF audit_log
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE audit_log_2026 PARTITION OF audit_log
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE INDEX idx_al_logged ON audit_log (logged_at DESC);
CREATE INDEX idx_al_order  ON audit_log (order_id) WHERE order_id IS NOT NULL;
CREATE INDEX idx_al_type   ON audit_log (event_type, logged_at DESC);

COMMENT ON TABLE audit_log IS '執行閘門完整審計日誌（模組⑪），按年分區，永久保存不可刪除';


-- =============================================================================
-- 12. 實用 VIEW — 常用查詢封裝
-- =============================================================================

-- 當前持倉綜合視圖
CREATE OR REPLACE VIEW v_open_positions AS
SELECT
    p.id,
    p.ticker,
    p.state,
    p.entry_date,
    p.entry_price,
    p.current_shares,
    p.avg_cost,
    p.current_stop_price,
    p.trailing_stop_price,
    p.r_multiple_current,
    p.realized_pnl,
    p.unrealized_pnl,
    sd.total_score         AS current_score,
    sd.vcp_detected,
    cm.crs_total           AS current_crs,
    cm.three_way_resonance,
    mr.regime              AS current_regime,
    mr.mrs_score           AS current_mrs
FROM positions p
LEFT JOIN LATERAL (
    SELECT total_score, vcp_detected
    FROM stock_diagnostic
    WHERE ticker = p.ticker
    ORDER BY trade_date DESC
    LIMIT 1
) sd ON TRUE
LEFT JOIN LATERAL (
    SELECT crs_total, three_way_resonance
    FROM chip_monitor
    WHERE ticker = p.ticker
    ORDER BY trade_date DESC
    LIMIT 1
) cm ON TRUE
LEFT JOIN LATERAL (
    SELECT regime, mrs_score
    FROM market_regime
    ORDER BY trade_date DESC
    LIMIT 1
) mr ON TRUE
WHERE p.is_open = TRUE;

-- 近30日策略績效摘要
CREATE OR REPLACE VIEW v_strategy_performance_30d AS
SELECT
    strategy_tag,
    signal_source,
    COUNT(*)                              AS total_trades,
    COUNT(*) FILTER (WHERE actual_return > 0) AS winning_trades,
    ROUND(
        COUNT(*) FILTER (WHERE actual_return > 0)::NUMERIC / NULLIF(COUNT(*),0),
        3
    )                                     AS win_rate,
    ROUND(AVG(actual_return), 4)          AS avg_return,
    ROUND(AVG(r_multiple), 3)             AS avg_r_multiple,
    ROUND(AVG(holding_days), 1)           AS avg_holding_days,
    ROUND(
        AVG(CASE WHEN alpha_source = 'ALPHA' THEN actual_return END), 4
    )                                     AS avg_alpha_return,
    COUNT(*) FILTER (WHERE is_planned = FALSE) AS unplanned_trades
FROM decision_log
WHERE trade_date >= CURRENT_DATE - INTERVAL '30 days'
  AND decision_type = 'EXIT_EXECUTED'
  AND actual_return IS NOT NULL
GROUP BY strategy_tag, signal_source
ORDER BY avg_r_multiple DESC NULLS LAST;


-- =============================================================================
-- 13. 分區自動建立函式（SQL 層，供 pg_cron 或手動呼叫）
-- =============================================================================
CREATE OR REPLACE FUNCTION create_monthly_partitions(
    base_table TEXT,
    target_date DATE DEFAULT DATE_TRUNC('month', NOW() + INTERVAL '1 month')
)
RETURNS VOID AS $$
DECLARE
    partition_name TEXT;
    start_date DATE;
    end_date   DATE;
BEGIN
    start_date := DATE_TRUNC('month', target_date);
    end_date   := start_date + INTERVAL '1 month';
    partition_name := base_table || '_' ||
                      TO_CHAR(start_date, 'YYYY_MM');

    IF NOT EXISTS (
        SELECT 1 FROM pg_tables
        WHERE schemaname = 'public'
          AND tablename  = partition_name
    ) THEN
        EXECUTE FORMAT(
            'CREATE TABLE %I PARTITION OF %I
             FOR VALUES FROM (%L) TO (%L)',
            partition_name, base_table, start_date, end_date
        );
        RAISE NOTICE 'Created partition: %', partition_name;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- 使用範例：
-- SELECT create_monthly_partitions('topic_heat');
-- SELECT create_monthly_partitions('stock_diagnostic');
-- SELECT create_monthly_partitions('decision_log');


-- =============================================================================
-- 14. 初始化基準資料
-- =============================================================================

-- 預設 Walk-Forward 種子權重（在第一次 WF 驗證完成前暫用）
INSERT INTO wf_results (
    data_from, data_to, train_months, oos_months, step_months,
    n_oos_windows, avg_oos_sharpe, consistency_rate,
    mc_pvalue, is_significant,
    factor_stability, recommendation,
    recommended_weights, is_active, notes
) VALUES (
    '2020-01-01', '2024-12-31', 12, 3, 3,
    13, NULL, NULL,
    NULL, NULL,
    '{"momentum":{"oos_mean":0.30,"oos_std":null,"is_stable":null},
      "chip":{"oos_mean":0.25,"oos_std":null,"is_stable":null},
      "fundamental":{"oos_mean":0.25,"oos_std":null,"is_stable":null},
      "valuation":{"oos_mean":0.20,"oos_std":null,"is_stable":null}}',
    'ADJUST',
    '{"momentum":0.30,"chip":0.25,"fundamental":0.25,"valuation":0.20}',
    TRUE,
    '初始種子權重（設計值）——尚未執行 Walk-Forward 驗證，部署前必須更新'
);

-- =============================================================================
-- 完成
-- =============================================================================