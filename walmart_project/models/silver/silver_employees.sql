WITH source AS (
    SELECT * FROM {{ source('walmart_databricks', 'bronze_employees') }}
),

cleaned AS (
    SELECT
        employee_id,
        store_id,
        TRIM(first_name)                     AS employee_first_name,
        TRIM(last_name)                      AS employee_last_name,
        LOWER(TRIM(email))                   AS employee_email,
        TRIM(job_title)                      AS employee_job_title,
        CAST(salary AS DECIMAL(12, 2))       AS employee_salary,
        CAST(created_timestamp AS TIMESTAMP) AS employee_created_at,
        CAST(updated_timestamp AS TIMESTAMP) AS employee_updated_at,
        CASE is_active WHEN 'Y' THEN true WHEN 'N' THEN false END AS employee_is_active,
        current_timestamp()                  AS silver_processed_at
    FROM source
)

SELECT * FROM cleaned
