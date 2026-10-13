-- SCD Type 2: one row per product VERSION (products_snapshot). product_sk identifies the version,
-- so a sale keeps the price tier / category the product had when it was sold.
SELECT
    dbt_scd_id AS product_sk,
    product_id,
    product_name,
    product_category,
    product_brand,
    product_price,
    CASE
        WHEN product_price < 100 THEN 'Budget'
        WHEN product_price < 300 THEN 'Mid'
        ELSE 'Premium'
    END AS price_tier,
    product_is_active,
    {{ scd2_validity('product_id') }}
FROM {{ ref("products_snapshot") }}
