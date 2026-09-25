SELECT
    employee_id,
    store_id,
    first_name AS employee_first_name,
    last_name AS employee_last_name,
    email AS employee_email,
    job_title AS employee_job_title,
    salary AS employee_salary,
    created_timestamp AS employee_created_at,
    updated_timestamp AS employee_updated_at,
    is_active AS employee_is_active,
    current_timestamp() AS silver_processed_at
FROM {{ source('walmart_databricks', 'bronze_employees') }}