-- =============================================================================
-- Migration v2：四項優化升級
-- 執行環境：PostgreSQL 15+
-- 執行順序：依序執行，不可跳過
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. decision_log.alpha_source：新增 'SECTOR_BETA' 值
--    原 CHECK: ('ALPHA','BETA','MIXED',NULL)
--    新 CHECK: ('ALPHA','BETA','SECTOR_BETA','MIXED',NULL)
-- -----------------------------------------------------------------------------
ALTER TABLE decision_log
    DROP CONSTRAINT IF EXISTS decision_log_alpha_source_check;

ALTER TABLE decision_log
    ADD CONSTRAINT decision_log_alpha_source_check
    CHECK (alpha_source IN ('ALPHA', 'BETA', 'SECTOR_BETA', 'MIXED', NULL));

COMMENT ON COLUMN decision_log.alpha_source IS
    '歸因分析：ALPHA=選股能力, BETA=大盤紅利, SECTOR_BETA=族群紅利, MIXED=混合';


-- -----------------------------------------------------------------------------
-- 2. positions.trail_pct：允許更廣範圍（原 DEFAULT 0.15，新範圍 0.05–0.30）
--    ATR 動態停損會將此欄每日更新，不再是靜態值
-- -----------------------------------------------------------------------------
COMMENT ON COLUMN positions.trail_pct IS
    'ATR 動態 Trailing Stop 百分比（每日由 exit_engine 依 ATR/Price 動態更新，範圍 0.08–0.22）';

-- 確保範圍合理（防呆）
ALTER TABLE positions
    DROP CONSTRAINT IF EXISTS positions_trail_pct_range;

ALTER TABLE positions
    ADD CONSTRAINT positions_trail_pct_range
    CHECK (trail_pct BETWEEN 0.05 AND 0.30);


-- -----------------------------------------------------------------------------
-- 3. stock_diagnostic：新增 sector 欄位（方便查詢時直接取用，不用每次 JOIN）
--    注意：stock_diagnostic 是月分區表，需對所有現有分區執行
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'stock_diagnostic' AND column_name = 'sector'
    ) THEN
        ALTER TABLE stock_diagnostic ADD COLUMN sector TEXT DEFAULT 'OTHER';
        COMMENT ON COLUMN stock_diagnostic.sector IS '產業分類代碼（SEMI/NETWORK/AI_SERVER/...），由 selection_engine 寫入';
    END IF;
END $$;

-- sector 的索引（產業群聚查詢用）
CREATE INDEX IF NOT EXISTS idx_sd_sector_date
    ON stock_diagnostic (sector, trade_date DESC)
    WHERE sector IS NOT NULL AND sector != 'OTHER';


-- -----------------------------------------------------------------------------
-- 4. decision_log：新增 sector_bonus 欄位，記錄群聚加分量
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'decision_log' AND column_name = 'sector_bonus'
    ) THEN
        ALTER TABLE decision_log ADD COLUMN sector_bonus NUMERIC(5,2) DEFAULT 0;
        ALTER TABLE decision_log ADD COLUMN sector       TEXT         DEFAULT 'OTHER';
        COMMENT ON COLUMN decision_log.sector_bonus IS '進場時產業群聚加分量（0=未加分）';
        COMMENT ON COLUMN decision_log.sector       IS '進場時個股所屬產業代碼';
    END IF;
END $$;


-- -----------------------------------------------------------------------------
-- 5. 驗證
-- -----------------------------------------------------------------------------
DO $$
DECLARE
    v_alpha_ok   BOOLEAN;
    v_trail_ok   BOOLEAN;
    v_sector_ok  BOOLEAN;
    v_bonus_ok   BOOLEAN;
BEGIN
    -- 確認 alpha_source CHECK 已更新
    SELECT EXISTS (
        SELECT 1 FROM information_schema.check_constraints
        WHERE constraint_name = 'decision_log_alpha_source_check'
    ) INTO v_alpha_ok;

    -- 確認 trail_pct range CHECK 已新增
    SELECT EXISTS (
        SELECT 1 FROM information_schema.check_constraints
        WHERE constraint_name = 'positions_trail_pct_range'
    ) INTO v_trail_ok;

    -- 確認 stock_diagnostic.sector 欄位存在
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'stock_diagnostic' AND column_name = 'sector'
    ) INTO v_sector_ok;

    -- 確認 decision_log.sector_bonus 欄位存在
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'decision_log' AND column_name = 'sector_bonus'
    ) INTO v_bonus_ok;

    RAISE NOTICE '=== Migration v2 驗證 ===';
    RAISE NOTICE 'alpha_source CHECK 含 SECTOR_BETA: %', v_alpha_ok;
    RAISE NOTICE 'positions trail_pct 範圍限制:       %', v_trail_ok;
    RAISE NOTICE 'stock_diagnostic.sector 欄位:       %', v_sector_ok;
    RAISE NOTICE 'decision_log.sector_bonus 欄位:     %', v_bonus_ok;

    IF NOT (v_alpha_ok AND v_trail_ok AND v_sector_ok AND v_bonus_ok) THEN
        RAISE WARNING '⚠ 部分 migration 未完成，請檢查上方錯誤';
    ELSE
        RAISE NOTICE '✅ Migration v2 全部完成';
    END IF;
END $$;
