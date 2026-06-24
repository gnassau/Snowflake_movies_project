-- ============================================================
-- 06_copy_into.sql
-- Carrega dados do S3 para as tabelas Snowflake
-- ============================================================

USE DATABASE MOVIES_DATABASE;
USE SCHEMA BRONZE;

-- ============================================================
-- TABLE MOVIES
-- ============================================================
COPY INTO MOVIES
(
    RAW_DATA,
    INGESTION_TIMESTAMP,
    SOURCE_FILE
)
FROM
(
    SELECT
        $1,
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME
    FROM @aws_ext_stage_integration/movies
)
FILE_FORMAT = (
    TYPE = JSON
)
ON_ERROR = CONTINUE;

-- ============================================================
-- TABLE updated_movie_ids
-- ============================================================

COPY INTO updated_movie_ids
(
    RAW_DATA,
    INGESTION_TIMESTAMP,
    SOURCE_FILE
)
FROM
(
    SELECT
        $1,
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME
    FROM @aws_ext_stage_integration/updated_movie_ids/
)
FILE_FORMAT = (
    TYPE = JSON
)
ON_ERROR = CONTINUE;

-- ============================================================
-- TABLE movies_updates
-- ============================================================

COPY INTO movies_updates
(
    RAW_DATA,
    INGESTION_TIMESTAMP,
    SOURCE_FILE
)
FROM
(
    SELECT
        $1,
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME
    FROM @aws_ext_stage_integration/movies_updates/
)
FILE_FORMAT = (
    TYPE = JSON
)
ON_ERROR = CONTINUE;