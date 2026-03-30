-- =============================================================================
-- schema_patch_final.sql  ── OmniHub v3.3.1 最終補丁
-- 涵蓋所有尚未執行的 Schema 缺失，全部使用 IF NOT EXISTS / DO $$ 防冪等
--
-- 執行方式（PowerShell）：
--   docker cp schema_patch_final.sql omni-investment-postgres-1:/tmp/patch_final.sql
--   docker exec omni-investment-postgres-1 psql -U omni -d omni_investment_hub -f /tmp/patch_final.sql
-- =============================================================================

\echo ''
\echo '======================================================'
\echo '  OmniHub v3.3.1 最終 Schema 補丁'
\echo '======================================================'
\echo ''

-- =============================================================================
-- PATCH 1：topic_heat — 補上 UNIQUE constraint
-- -----------------------------------------------------------------------------
-- 問題：topic_radar.py 的 INSERT...ON CONFLICT (trade_date, topic) 需要此
--       constraint，否則同天同題材會無限重複寫入，ON CONFLICT 永遠不命中。
-- =============================================================================
\echo '--- PATCH 1: topic_heat UNIQUE constraint ---'

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_topic_heat_date_topic'
    ) THEN
        ALTER TABLE topic_heat
            ADD CONSTRAINT uq_topic_heat_date_topic
            UNIQUE (trade_date, topic);
        RAISE NOTICE '✅ topic_heat UNIQUE constraint 已新增';
    ELSE
        RAISE NOTICE '⏭  topic_heat UNIQUE constraint 已存在，跳過';
    END IF;
END $$;


-- =============================================================================
-- PATCH 2：portfolio_health — 補上 daily_pnl_pct 欄位
-- -----------------------------------------------------------------------------
-- 問題：position_manager.py 修復後寫入 daily_pnl_pct（$13），
--       若欄位不存在 INSERT 會拋出 UndefinedColumnError。
-- =============================================================================
\echo '--- PATCH 2: portfolio_health.daily_pnl_pct ---'

ALTER TABLE portfolio_health
    ADD COLUMN IF NOT EXISTS daily_pnl_pct NUMERIC(7,4);

COMMENT ON COLUMN portfolio_health.daily_pnl_pct IS
    '單日損益率：(今日NAV - 昨日NAV) / 昨日NAV；PositionManager v3.1 寫入';

\echo '✅ portfolio_health.daily_pnl_pct 確認完成'


-- =============================================================================
-- PATCH 3：market_regime — 補上 credit_spread / hy_spread
-- -----------------------------------------------------------------------------
-- 問題：macro_filter.py v3.3 寫入 $16/$17，migration.sql PART 7 漏加這兩欄。
--       首次執行 MacroFilter 即崩潰（UndefinedColumnError）。
-- （若已由 fix_migration_credit_spread.sql 處理，本段為防呆重跑）
-- =============================================================================
\echo '--- PATCH 3: market_regime credit_spread / hy_spread ---'

ALTER TABLE market_regime
    ADD COLUMN IF NOT EXISTS credit_spread NUMERIC(6,3),
    ADD COLUMN IF NOT EXISTS hy_spread     NUMERIC(6,3);

COMMENT ON COLUMN market_regime.credit_spread IS
    'FRED BAA10Y：Baa 公司債與 10 年美債利差（%，×100 = bp）；BlackSwan LIQUIDITY_CRISIS 觸發來源';

COMMENT ON COLUMN market_regime.hy_spread IS
    'FRED BAMLH0A0HYM2：ICE BofA 高收益債 OAS（%）；流動性危機前兆指標（v3.3.1 補入觸發邏輯）';

\echo '✅ market_regime 信用利差欄位確認完成'


-- =============================================================================
-- PATCH 4：watchlist — 建立資料表（全新）
-- -----------------------------------------------------------------------------
-- 問題：scheduler.py / dashboard.py v3.3.1 改為從 DB 動態載入 Watchlist，
--       但整個專案中從未建立過 watchlist 資料表。
--       表不存在時會靜默降級為靜態清單，且 /api/watchlist-add 的 DB 同步失敗。
-- =============================================================================
\echo '--- PATCH 4: 建立 watchlist 資料表 ---'

CREATE TABLE IF NOT EXISTS watchlist (
    id         SERIAL       PRIMARY KEY,
    ticker     VARCHAR(10)  NOT NULL UNIQUE,
    note       TEXT,                          -- 備註，如 '主力標的' / '觀察中'
    is_active  BOOLEAN      NOT NULL DEFAULT TRUE,
    added_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  watchlist           IS 'Scheduler / Dashboard 共用監控清單，取代硬編碼 WATCHLIST 常數';
COMMENT ON COLUMN watchlist.ticker    IS '股票代號（VARCHAR 10，支援 ETF 與較長代號）';
COMMENT ON COLUMN watchlist.is_active IS 'FALSE = 從監控清單移除但保留歷史記錄';

-- 建立索引加速 is_active 篩選
CREATE INDEX IF NOT EXISTS idx_watchlist_active
    ON watchlist (is_active)
    WHERE is_active = TRUE;

-- updated_at 自動更新 trigger（若 set_updated_at() 函式存在）
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_proc WHERE proname = 'set_updated_at'
    ) THEN
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger WHERE tgname = 'trg_watchlist_updated_at'
        ) THEN
            EXECUTE '
                CREATE TRIGGER trg_watchlist_updated_at
                    BEFORE UPDATE ON watchlist
                    FOR EACH ROW EXECUTE FUNCTION set_updated_at()
            ';
            RAISE NOTICE '✅ watchlist updated_at trigger 已建立';
        END IF;
    ELSE
        RAISE NOTICE '⚠  set_updated_at() 函式不存在，跳過 trigger 建立';
    END IF;
END $$;

-- 寫入預設 Watchlist（僅在資料表為空時插入）
INSERT INTO watchlist (ticker, note)
SELECT unnest(ARRAY['2330','2303','2454','2412','2317','2382','3711','6669']),
       unnest(ARRAY['台積電','聯電','聯發科','中華電','鴻海','廣達','日月光投控','緯穎'])
WHERE NOT EXISTS (SELECT 1 FROM watchlist LIMIT 1);

\echo '✅ watchlist 資料表建立完成，預設 8 檔已寫入（若表為空）'


-- =============================================================================
-- PATCH 5：chip_monitor — 補上 volume_ratio_5d20d（防呆確認）
-- -----------------------------------------------------------------------------
-- 問題：migration.sql PART 6 應已處理，此處防呆重跑確認欄位存在。
-- =============================================================================
\echo '--- PATCH 5: chip_monitor.volume_ratio_5d20d ---'

ALTER TABLE chip_monitor
    ADD COLUMN IF NOT EXISTS volume_ratio_5d20d NUMERIC(6,3);

COMMENT ON COLUMN chip_monitor.volume_ratio_5d20d IS
    '個股 5日均量 / 20日均量比率；BlackSwan LIQUIDITY_VOLUME 偵測來源（ChipMonitor v3 寫入）';

\echo '✅ chip_monitor.volume_ratio_5d20d 確認完成'


-- =============================================================================
-- PATCH 6：stock_diagnostic — 補上 basis_filter_reason / sector_beta_mult（防呆）
-- =============================================================================
\echo '--- PATCH 6: stock_diagnostic 欄位防呆確認 ---'

ALTER TABLE stock_diagnostic
    ADD COLUMN IF NOT EXISTS basis_filter_reason TEXT,
    ADD COLUMN IF NOT EXISTS sector_beta_mult    NUMERIC(4,2) DEFAULT 1.0;

\echo '✅ stock_diagnostic 欄位確認完成'


-- =============================================================================
-- PATCH 7：positions — 補上 s1_atr_mult_applied（防呆）
-- =============================================================================
\echo '--- PATCH 7: positions.s1_atr_mult_applied ---'

ALTER TABLE positions
    ADD COLUMN IF NOT EXISTS s1_atr_mult_applied NUMERIC(4,2) DEFAULT 3.0;

\echo '✅ positions.s1_atr_mult_applied 確認完成'


-- =============================================================================
-- PATCH 8：decision_log — 補上 individual_alpha / alpha_source CHECK（防呆）
-- =============================================================================
\echo '--- PATCH 8: decision_log 欄位防呆確認 ---'

ALTER TABLE decision_log
    ADD COLUMN IF NOT EXISTS individual_alpha NUMERIC(8,3);

-- 重建 alpha_source CHECK（含 SECTOR_BETA）
ALTER TABLE decision_log
    DROP CONSTRAINT IF EXISTS decision_log_alpha_source_check;

ALTER TABLE decision_log
    ADD CONSTRAINT decision_log_alpha_source_check
    CHECK (alpha_source IN ('ALPHA', 'BETA', 'SECTOR_BETA', 'MIXED', NULL));

\echo '✅ decision_log 欄位確認完成'


-- =============================================================================
-- PATCH 9：wf_results.notes — 確保為 TEXT（防呆）
-- =============================================================================
\echo '--- PATCH 9: wf_results.notes 型別確認 ---'

DO $$
DECLARE
    col_type TEXT;
BEGIN
    SELECT data_type INTO col_type
    FROM information_schema.columns
    WHERE table_name = 'wf_results' AND column_name = 'notes';

    IF col_type = 'character varying' THEN
        ALTER TABLE wf_results ALTER COLUMN notes TYPE TEXT;
        RAISE NOTICE '✅ wf_results.notes 已從 VARCHAR 改為 TEXT';
    ELSE
        RAISE NOTICE '⏭  wf_results.notes 已是 TEXT（目前：%），跳過', col_type;
    END IF;
END $$;


-- =============================================================================
-- 最終驗證查詢
-- =============================================================================
\echo ''
\echo '======================================================'
\echo '  驗證：確認所有新增欄位與資料表'
\echo '======================================================'

-- 確認欄位
SELECT
    table_name,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
  AND (
      (table_name = 'topic_heat'       AND column_name IN ('trade_date','topic'))
   OR (table_name = 'portfolio_health' AND column_name = 'daily_pnl_pct')
   OR (table_name = 'market_regime'    AND column_name IN ('credit_spread','hy_spread'))
   OR (table_name = 'chip_monitor'     AND column_name = 'volume_ratio_5d20d')
   OR (table_name = 'stock_diagnostic' AND column_name IN ('basis_filter_reason','sector_beta_mult'))
   OR (table_name = 'positions'        AND column_name = 's1_atr_mult_applied')
   OR (table_name = 'decision_log'     AND column_name IN ('individual_alpha','alpha_source'))
   OR (table_name = 'wf_results'       AND column_name = 'notes')
  )
ORDER BY table_name, column_name;

-- 確認 watchlist 資料表與初始資料
\echo ''
\echo '--- watchlist 現有資料 ---'
SELECT ticker, note, is_active, added_at FROM watchlist ORDER BY id;

-- 確認 topic_heat UNIQUE constraint
\echo ''
\echo '--- topic_heat constraints ---'
SELECT conname, contype
FROM pg_constraint
WHERE conrelid = 'topic_heat'::regclass
  AND conname = 'uq_topic_heat_date_topic';

\echo ''
\echo '======================================================'
\echo '  schema_patch_final.sql 執行完畢 ✅'
\echo '======================================================'
