-- ============================================================
-- 02_create_schemas.sql
-- Cria os schemas bronze, silver e gold
-- ============================================================

CREATE SCHEMA IF NOT EXISTS MOVIES_DATABASE.BRONZE;
CREATE SCHEMA IF NOT EXISTS MOVIES_DATABASE.SILVER;
CREATE SCHEMA IF NOT EXISTS MOVIES_DATABASE.GOLD;