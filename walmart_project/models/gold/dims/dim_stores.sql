-- SCD Type 2: one row per store VERSION (stores_snapshot). store_sk identifies the version.
SELECT
    dbt_scd_id AS store_sk,
    store_id,
    store_name,
    store_city,
    store_province,
    store_country,
    store_is_active,
    {{ scd2_validity('store_id') }}
FROM {{ ref("stores_snapshot") }}
