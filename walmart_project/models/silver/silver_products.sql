WITH source AS (
    SELECT * FROM {{ source('walmart_databricks', 'bronze_products') }}
),

cleaned AS (
    SELECT
        product_id,
        TRIM(product_name)                   AS product_name,
        INITCAP(TRIM(category))              AS product_category,  -- controlled list: Home, Sports, ...
        TRIM(brand)                          AS product_brand,     -- no INITCAP: would turn "LG" into "Lg"
        CAST(price AS DECIMAL(10, 2))        AS product_price,
        CASE is_active WHEN 'Y' THEN true WHEN 'N' THEN false END AS product_is_active,
        CAST(created_timestamp AS TIMESTAMP) AS product_created_at,
        CAST(updated_timestamp AS TIMESTAMP) AS product_updated_at,
        current_timestamp()                  AS silver_processed_at
    FROM source
)

SELECT * FROM cleaned
