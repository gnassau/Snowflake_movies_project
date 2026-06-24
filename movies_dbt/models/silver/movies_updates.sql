SELECT
    RAW_DATA:movie_id::NUMBER                      AS movie_id,
    RAW_DATA:title::STRING                         AS title,
    RAW_DATA:overview::STRING                      AS overview,
    RAW_DATA:adult::BOOLEAN                        AS adult,
    RAW_DATA:budget::NUMBER                        AS budget,
    RAW_DATA:homepage::STRING                      AS homepage,
    RAW_DATA:language::STRING                      AS language,
    RAW_DATA:original_language::STRING             AS original_language,
    RAW_DATA:popularity::FLOAT                     AS popularity,
    TRY_TO_DATE(RAW_DATA:release_date::STRING)     AS release_date,
    RAW_DATA:revenue::NUMBER                       AS revenue,
    RAW_DATA:runtime::NUMBER                       AS runtime,
    RAW_DATA:status::STRING                        AS status,
    RAW_DATA:vote_average::FLOAT                   AS vote_average,
    RAW_DATA:vote_count::NUMBER                    AS vote_count,

    -- Mantém o array de gêneros
    RAW_DATA:genres                                AS genres,

    -- Timestamp informado pela API
    RAW_DATA:ingestion_timestamp::TIMESTAMP_NTZ    AS source_ingestion_timestamp,

    -- Timestamp da carga na Bronze
    INGESTION_TIMESTAMP                            AS bronze_ingestion_timestamp

FROM MOVIES_DATABASE.BRONZE.UPDATED_MOVIE_IDS