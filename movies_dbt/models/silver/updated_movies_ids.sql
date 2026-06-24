SELECT
    RAW_DATA:movie_id::NUMBER AS movie_id,

    RAW_DATA:ingestion_timestamp::TIMESTAMP_NTZ AS source_ingestion_timestamp,

    INGESTION_TIMESTAMP AS bronze_ingestion_timestamp

FROM MOVIES_DATABASE.BRONZE.UPDATED_MOVIE_IDS