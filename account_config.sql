-- =============================================================================
-- account_config — 帳戶資金設定
-- 用途：提供 PositionManager 讀取初始資金，取代硬編碼的 1,000,000
-- 設計原則：不可變歷史，用 is_active 管理唯一有效記錄
-- =============================================================================

CREATE TABLE account_config (
    id                  SERIAL          PRIMARY KEY,
    initial_capital     NUMERIC(14,2)   NOT NULL,               -- 初始資金（元）
    currency            CHAR(3)         NOT NULL DEFAULT 'TWD', -- 幣別
    note                TEXT,                                   -- 備註，e.g. '2025-01 入金'
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- 確保同一時間只有一筆 is_active = TRUE
CREATE UNIQUE INDEX idx_account_config_active
    ON account_config (is_active)
    WHERE is_active = TRUE;

CREATE TRIGGER trg_account_config_updated_at
    BEFORE UPDATE ON account_config
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMENT ON TABLE  account_config IS '帳戶資金設定，PositionManager 讀取 initial_capital 計算 NAV';
COMMENT ON COLUMN account_config.initial_capital IS '帳戶初始資金（元），是 NAV 計算的基準值';
COMMENT ON COLUMN account_config.is_active       IS '唯一有效記錄旗標；變更資金時停用舊記錄並插入新記錄';

-- =============================================================================
-- 初始資料：填入你的實際初始資金
-- =============================================================================
INSERT INTO account_config (initial_capital, note)
VALUES (1000000.00, '初始資金設定');

-- =============================================================================
-- 日後追加入金 / 變更資金的操作方式（範例，需要時才執行）
-- =============================================================================
-- 1. 停用舊記錄
-- UPDATE account_config SET is_active = FALSE WHERE is_active = TRUE;
--
-- 2. 插入新資金設定
-- INSERT INTO account_config (initial_capital, note)
-- VALUES (1500000.00, '2025-04 追加入金 50 萬');