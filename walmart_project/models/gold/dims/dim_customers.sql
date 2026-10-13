-- SCD Type 2: one row per customer VERSION (customers_snapshot). customer_sk identifies the version.
SELECT
    dbt_scd_id                                          AS customer_sk,
    customer_id,
    customer_first_name,
    customer_last_name,
    CONCAT(customer_first_name, ' ', customer_last_name) AS full_name,
    customer_email,
    SPLIT_PART(customer_email, '@', 2)                  AS email_domain,
    customer_phone,
    customer_city,
    customer_province,
    customer_country,
    customer_is_active,
    {{ scd2_validity('customer_id') }}
FROM {{ ref("customers_snapshot") }}
