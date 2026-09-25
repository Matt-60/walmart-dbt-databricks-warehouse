SELECT
    store_id,
    store_name,
    city AS store_city,
    province AS store_province,
    country AS store_country,
    is_active AS store_is_active,
    created_timestamp AS store_created_at,
    updated_timestamp AS store_updated_at,
    current_timestamp() AS store_processed_at
FROM {{ source('walmart_databricks', 'stores') }}