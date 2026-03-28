-- =============================================================
-- schema_patch.sql — 配合本次 Bug 修復所需的 DB 補丁
-- 執行方式：
--   docker exec omni-investment-postgres-1 psql -U omni -d omni_investment_hub -f schema_patch.sql
-- =============================================================


-- -------------------------------------------------------------
-- Patch 1：topic_heat — 補上 UNIQUE constraint
-- 對應 Bug：topic_radar.py 的 ON CONFLICT (trade_date, topic) 需要此 constraint 才能運作。
-- 原本 ON CONFLICT (id, trade_date) 永遠不命中，同天同題材會無限重複寫入。
-- -------------------------------------------------------------
ALTER TABLE topic_heat
    ADD CONSTRAINT uq_topic_heat_date_topic
    UNIQUE (trade_date, topic);


-- -------------------------------------------------------------
-- Patch 2：portfolio_health — 補上 daily_pnl_pct 欄位
-- 對應 Bug：position_manager.py 修復後會寫入 daily_pnl_pct，
-- 若 schema 缺少此欄位 INSERT 會報錯。
-- （若欄位已存在請跳過此行）
-- -------------------------------------------------------------
ALTER TABLE portfolio_health
    ADD COLUMN IF NOT EXISTS daily_pnl_pct NUMERIC(7,4);


-- =============================================================
-- END OF PATCH
-- =============================================================