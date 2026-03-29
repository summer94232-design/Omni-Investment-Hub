-- =============================================================
-- schema_patch_v2.sql — stock_diagnostic 欄位補丁
--
-- 問題：stock_diagnostic 缺少以下欄位，造成後端錯誤：
--   column "sector_bonus" does not exist
--   column "total_score" does not exist
--   column "final_score" does not exist
--
-- 執行方式（Docker）：
--   docker exec -i <postgres_container> psql -U omni -d omni_investment_hub \
--     < schema_patch_v2.sql
--
-- 或直接連線執行：
--   psql -h localhost -p 5433 -U omni -d omni_investment_hub \
--     -f schema_patch_v2.sql
--
-- 說明：stock_diagnostic 是月分區表，ALTER TABLE 對母表執行即可，
--       PostgreSQL 會自動套用至所有子分區。
-- =============================================================

\echo '=== schema_patch_v2.sql 開始執行 ==='

-- -------------------------------------------------------------
-- Patch 3：stock_diagnostic — 補上 SelectionEngine 寫入的核心欄位
-- migration.sql PART 2 只補了 basis_filter_reason / sector_beta_mult，
-- 但 total_score、final_score、sector_bonus 從未建立，
-- 導致 selection_engine.py 的 INSERT 拋出 column does not exist。
-- -------------------------------------------------------------

ALTER TABLE stock_diagnostic
    ADD COLUMN IF NOT EXISTS total_score         NUMERIC(6,2),
    ADD COLUMN IF NOT EXISTS final_score         NUMERIC(6,2),
    ADD COLUMN IF NOT EXISTS sector_bonus        NUMERIC(5,2) DEFAULT 0.0;

COMMENT ON COLUMN stock_diagnostic.total_score IS
    '加權總分（動能×W + 籌碼×W + 基本面×W + 估值×W），套用基差過濾前';

COMMENT ON COLUMN stock_diagnostic.final_score IS
    '最終進場分數：套用基差過濾後，再加上群聚加分（由 apply_sector_cluster_bonus 填充）';

COMMENT ON COLUMN stock_diagnostic.sector_bonus IS
    '產業群聚加分（apply_sector_cluster_bonus 填充）：同產業 2/3/4+ 檔 → +3/6/9 × Beta倍率';

\echo 'stock_diagnostic 補丁完成'

-- 確認欄位已存在
SELECT column_name, data_type, column_default
FROM information_schema.columns
WHERE table_name = 'stock_diagnostic'
  AND column_name IN (
    'total_score', 'final_score', 'sector_bonus',
    'basis_filter_reason', 'sector_beta_mult'
  )
ORDER BY column_name;

\echo '=== schema_patch_v2.sql 執行完畢 ==='