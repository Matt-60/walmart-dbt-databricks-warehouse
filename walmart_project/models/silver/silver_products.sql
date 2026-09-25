SELECT
    product_id,
    product_name,
    category as product_category,
    brand as product_brand,
    price as product_price,
    is_active AS product_is_active,
    created_timestamp AS product_created_at,
    updated_timestamp AS product_updated_at,
    current_timestamp() AS silver_processed_at
FROM {{ source('walmart_databricks', 'products') }}
