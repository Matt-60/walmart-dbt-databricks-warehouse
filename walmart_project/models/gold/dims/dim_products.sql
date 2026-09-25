SELECT
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
    product_is_active
FROM {{ ref("products_snapshot") }}
WHERE dbt_valid_to IS NULL
