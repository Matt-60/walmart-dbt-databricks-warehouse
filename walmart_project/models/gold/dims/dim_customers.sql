SELECT
    customer_id,
    customer_first_name,
    customer_last_name,
    CONCAT(customer_first_name, ' ', customer_last_name) AS full_name,
    customer_email,
    SPLIT_PART(customer_email, '@', 2) AS email_domain,
    customer_phone,
    customer_city,
    customer_province,
    customer_country,
    customer_is_active
FROM {{ ref("customers_snapshot") }}
WHERE dbt_valid_to IS NULL
