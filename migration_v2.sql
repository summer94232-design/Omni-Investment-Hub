-- =============================================================================
-- migration_v3.sql
-- 對應 v3 優化升級的資料庫變更
-- 執行環境：PostgreSQL 15+
-- 必須在 migration_v2.sql 之後執行
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. market_regime：新增基差與成交量欄位（供 BasisFilter 與 BlackSwan 使用）
-- -----------------------------------------------------------------------------
ALTER TABLE market_regime
    ADD COLUMN IF NOT EXISTS tx_futures_price    NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS tsec_index_price    NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS dividend_adjustment NUMERIC(8,2) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS market_volume       BIGINT;

COMMENT ON COLUMN market_regime.tx_futures_price    IS '台指期近月合約收盤價';
COMMENT ON COLUMN market_regime.tsec_index_price    IS '加權指數現貨收盤價';
COMMENT ON COLUMN market_regime.dividend_adjustment IS '近月預估除息點數（用於基差計算）';
COMMENT ON COLUMN market_regime.market_volume       IS '大盤成交量（元），供 BlackSwan 流動性偵測使用';

-- -----------------------------------------------------------------------------
-- 2. stock_diagnostic：新增 basis_filter_reason 欄位
--    記錄基差過濾的原因，方便事後歸因查詢
-- -----------------------------------------------------------------------------
ALTER TABLE stock_diagnostic
    ADD COLUMN IF NOT EXISTS basis_filter_reason TEXT;

COMMENT ON COLUMN stock_diagnostic.basis_filter_reason IS
    '基差過濾原因：NULL=未觸發, BASIS_HARD_BLOCK=硬性攔截(basis<-1.5%), BASIS_SOFT_PENALTY=軟性懲罰(basis<-0.8%)';

-- -----------------------------------------------------------------------------
-- 3. stock_diagnostic：新增 sector_beta_mult 欄位
--    記錄群聚加分時使用的 Beta 差異化倍率
-- -----------------------------------------------------------------------------
ALTER TABLE stock_diagnostic
    ADD COLUMN IF NOT EXISTS sector_beta_mult NUMERIC(4,2) DEFAULT 1.0;

COMMENT ON COLUMN stock_diagnostic.sector_beta_mult IS
    '產業連動性倍率（v3 差異化加分）：SEMI/AI_SERVER=1.2, BIOTECH/PROPERTY=0.5, 其餘=1.0';

-- -----------------------------------------------------------------------------
-- 4. positions：新增 s1_atr_mult_applied 欄位
--    記錄進場當下 Regime 決定的 S1 停損倍數（歸因用）
-- -----------------------------------------------------------------------------
ALTER TABLE positions
    ADD COLUMN IF NOT EXISTS s1_atr_mult_applied NUMERIC(4,2) DEFAULT 3.0;

COMMENT ON COLUMN positions.s1_atr_mult_applied IS
    'S1 進場時依 Regime 決定的 ATR 倍數（BULL=3.0, CHOPPY=2.0, BEAR=1.5）';

-- -----------------------------------------------------------------------------
-- 5. wf_results：確保 notes 欄位可存 JSON（Attribution v3 Alpha 拆解）
-- -----------------------------------------------------------------------------
ALTER TABLE wf_results
    ALTER COLUMN notes TYPE TEXT;   -- 確保不是 VARCHAR 限制長度

COMMENT ON COLUMN wf_results.notes IS
    '備註 JSON（v3 起包含 alpha_decomposition、regime_ev_feedback 等結構化資料）';

-- -----------------------------------------------------------------------------
-- 6. chip_monitor：新增 volume_ratio_5d20d 欄位（BlackSwan 流動性代理）
-- -----------------------------------------------------------------------------
ALTER TABLE chip_monitor
    ADD COLUMN IF NOT EXISTS volume_ratio_5d20d NUMERIC(6,3);

COMMENT ON COLUMN chip_monitor.volume_ratio_5d20d IS
    '個股 5日均量 / 20日均量比率，BlackSwan 用來估算大盤流動性萎縮';

-- -----------------------------------------------------------------------------
-- 7. decision_log：新增 individual_alpha 欄位（Attribution v3）
-- -----------------------------------------------------------------------------
ALTER TABLE decision_log
    ADD COLUMN IF NOT EXISTS individual_alpha NUMERIC(8,3);

COMMENT ON COLUMN decision_log.individual_alpha IS
    '個股超額 Alpha：個股 R - 同產業同期平均 R（由 Attribution.analyze_closed_positions 計算）';

-- =============================================================================
-- END OF migration_v3.sql
-- =============================================================================