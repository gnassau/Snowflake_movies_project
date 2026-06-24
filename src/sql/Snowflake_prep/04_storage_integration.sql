-- ============================================================
-- 04_storage_integration.sql
-- Cria a Storage Integration e o Stage externo no S3
-- Os placeholders são substituídos pelas Variables do Airflow.
-- ============================================================

USE DATABASE MOVIES_DATABASE;
USE SCHEMA BRONZE;

CREATE STORAGE INTEGRATION IF NOT EXISTS s3_movies_int
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = '{STORAGE_AWS_ROLE_ARN}'
    STORAGE_ALLOWED_LOCATIONS = ('{STORAGE_ALLOWED_LOCATIONS}');

CREATE STAGE IF NOT EXISTS aws_ext_stage_integration
    URL = '{S3_BRONZE_URL}'
    STORAGE_INTEGRATION = s3_movies_int;