-- =============================================================================
-- migration_v3_complete.sql
-- v3 優化升級完整資料庫遷移腳本
-- 執行環境：PostgreSQL 15+
-- 前置條件：migration_v2.sql 已執行完畢
--
-- 執行方式（PowerShell）：
--   psql -h localhost -p 5433 -U omni -d omni_investment_hub -f migration_v3_complete.sql
--
-- 或在 Docker 容器內：
--   docker exec -i omni-investment-postgres-1 psql -U omni -d omni_investment_hub < migration_v3_complete.sql
-- =============================================================================

\echo '=== migration_v3_complete.sql 開始執行 ==='
\echo ''

-- =============================================================================
-- PART 1：market_regime 新增基差與成交量欄位
-- =============================================================================
\echo '--- PART 1: market_regime ---'

ALTER TABLE market_regime
    ADD COLUMN IF NOT EXISTS tx_futures_price    NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS tsec_index_price    NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS dividend_adjustment NUMERIC(8,2) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS market_volume       BIGINT;

COMMENT ON COLUMN market_regime.tx_futures_price    IS '台指期近月合約收盤價（MacroFilter v3 寫入）';
COMMENT ON COLUMN market_regime.tsec_index_price    IS '加權指數現貨收盤價（MacroFilter v3 寫入）';
COMMENT ON COLUMN market_regime.dividend_adjustment IS '近月預估除息點數，用於 Normalized Basis 計算（MacroFilter v3 寫入）';
COMMENT ON COLUMN market_regime.market_volume       IS '大盤成交量（元），供 BlackSwan 流動性偵測使用（MacroFilter v3 寫入）';

\echo 'market_regime 欄位新增完成'

-- =============================================================================
-- PART 2：stock_diagnostic 新增 basis_filter_reason 與 sector_beta_mult
-- =============================================================================
\echo '--- PART 2: stock_diagnostic ---'

-- 注意：stock_diagnostic 是月分區表，ALTER TABLE 會對母表及所有子分區生效
ALTER TABLE stock_diagnostic
    ADD COLUMN IF NOT EXISTS basis_filter_reason TEXT,
    ADD COLUMN IF NOT EXISTS sector_beta_mult    NUMERIC(4,2) DEFAULT 1.0;

COMMENT ON COLUMN stock_diagnostic.basis_filter_reason IS
    '基差過濾原因：NULL=未觸發, BASIS_HARD_BLOCK=硬攔截(basis<-1.5%), BASIS_SOFT_PENALTY=軟懲罰(basis<-0.8%)';

COMMENT ON COLUMN stock_diagnostic.sector_beta_mult IS
    '產業連動性倍率（v3 差異化加分）：SEMI/AI_SERVER=1.2, BIOTECH/PROPERTY=0.5, 其餘=1.0';

\echo 'stock_diagnostic 欄位新增完成'

-- =============================================================================
-- PART 3：positions 新增 s1_atr_mult_applied
-- =============================================================================
\echo '--- PART 3: positions ---'

ALTER TABLE positions
    ADD COLUMN IF NOT EXISTS s1_atr_mult_applied NUMERIC(4,2) DEFAULT 3.0;

COMMENT ON COLUMN positions.s1_atr_mult_applied IS
    'S1 進場時依 Regime 決定的 ATR 倍數（BULL=3.0, CHOPPY=2.0, BEAR=1.5）；ExitEngine v3 寫入';

-- trail_pct 範圍放寬（v2 已做，防呆再確認）
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'positions_trail_pct_range'
          AND table_name = 'positions'
    ) THEN
        ALTER TABLE positions
            ADD CONSTRAINT positions_trail_pct_range
            CHECK (trail_pct BETWEEN 0.05 AND 0.30);
        RAISE NOTICE 'trail_pct 範圍約束已新增';
    ELSE
        RAISE NOTICE 'trail_pct 範圍約束已存在，跳過';
    END IF;
END $$;

\echo 'positions 欄位新增完成'

-- =============================================================================
-- PART 4：decision_log 新增 individual_alpha + alpha_source 新增 SECTOR_BETA
-- =============================================================================
\echo '--- PART 4: decision_log ---'

ALTER TABLE decision_log
    ADD COLUMN IF NOT EXISTS individual_alpha NUMERIC(8,3);

COMMENT ON COLUMN decision_log.individual_alpha IS
    '個股超額 Alpha：個股 R - 同產業同期平均 R（Attribution v3 計算）';

-- alpha_source CHECK 新增 SECTOR_BETA（v2 已做，防呆確認）
ALTER TABLE decision_log
    DROP CONSTRAINT IF EXISTS decision_log_alpha_source_check;

ALTER TABLE decision_log
    ADD CONSTRAINT decision_log_alpha_source_check
    CHECK (alpha_source IN ('ALPHA', 'BETA', 'SECTOR_BETA', 'MIXED', NULL));

COMMENT ON COLUMN decision_log.alpha_source IS
    '歸因分析：ALPHA=選股能力, BETA=大盤紅利, SECTOR_BETA=族群紅利, MIXED=混合/虧損';

\echo 'decision_log 欄位新增完成'

-- =============================================================================
-- PART 5：wf_results.notes 確保為 TEXT（無長度限制）
-- =============================================================================
\echo '--- PART 5: wf_results ---'

-- 將 notes 欄位確保為 TEXT（避免 VARCHAR(N) 截斷 Alpha 拆解 JSON）
DO $$
DECLARE
    col_type TEXT;
BEGIN
    SELECT data_type INTO col_type
    FROM information_schema.columns
    WHERE table_name = 'wf_results' AND column_name = 'notes';

    IF col_type = 'character varying' THEN
        ALTER TABLE wf_results ALTER COLUMN notes TYPE TEXT;
        RAISE NOTICE 'wf_results.notes 已從 VARCHAR 改為 TEXT';
    ELSE
        RAISE NOTICE 'wf_results.notes 已是 TEXT，無需修改（目前類型：%）', col_type;
    END IF;
END $$;

COMMENT ON COLUMN wf_results.notes IS
    '備註 JSON（v3 起包含 alpha_decomposition、regime_ev_feedback、per_trade_alpha 等結構化資料）';

\echo 'wf_results 確認完成'

-- =============================================================================
-- PART 6：chip_monitor 新增 volume_ratio_5d20d
-- =============================================================================
\echo '--- PART 6: chip_monitor ---'

ALTER TABLE chip_monitor
    ADD COLUMN IF NOT EXISTS volume_ratio_5d20d NUMERIC(6,3);

COMMENT ON COLUMN chip_monitor.volume_ratio_5d20d IS
    '個股 5日均量 / 20日均量比率；BlackSwan 在無大盤資料時用此欄作流動性代理指標';

\echo 'chip_monitor 欄位新增完成'

-- =============================================================================
-- PART 7：market_regime 新增 fed_funds_rate / us_10y_yield / us_2y_yield
--         （MacroFilter v3 的 result dict 有這些欄位但原表可能沒有）
-- =============================================================================
\echo '--- PART 7: market_regime 補充 FRED 指標欄位 ---'

ALTER TABLE market_regime
    ADD COLUMN IF NOT EXISTS fed_funds_rate  NUMERIC(6,3),
    ADD COLUMN IF NOT EXISTS us_10y_yield    NUMERIC(6,3),
    ADD COLUMN IF NOT EXISTS us_2y_yield     NUMERIC(6,3);

COMMENT ON COLUMN market_regime.fed_funds_rate IS 'FRED FEDFUNDS：聯邦基金利率（%）';
COMMENT ON COLUMN market_regime.us_10y_yield   IS 'FRED DGS10：10年期美債殖利率（%）';
COMMENT ON COLUMN market_regime.us_2y_yield    IS 'FRED DGS2：2年期美債殖利率（%）';

\echo 'market_regime FRED 指標欄位新增完成'

-- =============================================================================
-- PART 8：驗證查詢（確認所有新欄位都存在）
-- =============================================================================
\echo ''
\echo '=== 驗證查詢 ==='

SELECT
    table_name,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name   IN (
        'market_regime',
        'stock_diagnostic',
        'positions',
        'decision_log',
        'wf_results',
        'chip_monitor'
  )
  AND column_name IN (
        'tx_futures_price', 'tsec_index_price', 'dividend_adjustment', 'market_volume',
        'fed_funds_rate', 'us_10y_yield', 'us_2y_yield',
        'basis_filter_reason', 'sector_beta_mult',
        's1_atr_mult_applied',
        'individual_alpha', 'alpha_source',
        'volume_ratio_5d20d',
        'notes'
  )
ORDER BY table_name, column_name;

\echo ''
\echo '=== migration_v3_complete.sql 執行完畢 ==='