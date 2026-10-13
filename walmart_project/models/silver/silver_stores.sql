WITH source AS (
    SELECT * FROM {{ source('walmart_databricks', 'bronze_stores') }}
),

cleaned AS (
    SELECT
        store_id,
        TRIM(store_name)                     AS store_name,
        TRIM(city)                           AS store_city,
        TRIM(province)                       AS store_province,
        TRIM(country)                        AS store_country,
        CASE is_active WHEN 'Y' THEN true WHEN 'N' THEN false END AS store_is_active,
        CAST(created_timestamp AS TIMESTAMP) AS store_created_at,
        CAST(updated_timestamp AS TIMESTAMP) AS store_updated_at,
        current_timestamp()                  AS silver_processed_at
    FROM source
)

SELECT * FROM cleaned
