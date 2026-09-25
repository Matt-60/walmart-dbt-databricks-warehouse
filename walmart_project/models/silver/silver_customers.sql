SELECT
    customer_id,
    first_name AS customer_first_name,
    last_name AS customer_last_name,
    email AS customer_email,
    phone AS customer_phone,
    city AS customer_city,
    province AS customer_province,
    country AS customer_country,
    created_timestamp AS customer_created_at,
    updated_timestamp AS customer_updated_at,
    is_active AS customer_is_active,
    current_timestamp() AS silver_processed_at
FROM {{ source('walmart_databricks', 'customers') }}
