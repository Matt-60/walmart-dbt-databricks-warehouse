WITH source AS (
    SELECT * FROM {{ source('walmart_databricks', 'bronze_customers') }}
),

cleaned AS (
    SELECT
        customer_id,
        TRIM(first_name)                                AS customer_first_name,
        TRIM(last_name)                                 AS customer_last_name,
        LOWER(TRIM(email))                              AS customer_email,
        NULLIF(REGEXP_REPLACE(phone, '[^0-9]', ''), '') AS customer_phone,   -- digits only
        TRIM(city)                                      AS customer_city,
        TRIM(province)                                  AS customer_province,
        TRIM(country)                                   AS customer_country,
        CAST(created_timestamp AS TIMESTAMP)            AS customer_created_at,
        CAST(updated_timestamp AS TIMESTAMP)            AS customer_updated_at,
        CASE is_active WHEN 'Y' THEN true WHEN 'N' THEN false END AS customer_is_active,
        current_timestamp()                             AS silver_processed_at
    FROM source
)

SELECT * FROM cleaned
