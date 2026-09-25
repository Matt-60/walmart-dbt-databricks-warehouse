{{ config(
    materialized='incremental',
    unique_key='order_id',
) }}


SELECT
    order_id,
    customer_id,
    store_id,
    order_timestamp,
    payment_method as order_payment_method,
    order_status,
    total_amount as order_total_amount,
    is_active AS order_is_active,
    created_timestamp AS order_created_at,
    updated_timestamp AS order_updated_at,
    current_timestamp() AS silver_processed_at
FROM {{ source('walmart_databricks', 'bronze_orders') }}

{% if is_incremental() %}
    WHERE updated_timestamp > (SELECT COALESCE(MAX(order_updated_at), '1900-01-01') FROM {{ this }})
{% endif %}
