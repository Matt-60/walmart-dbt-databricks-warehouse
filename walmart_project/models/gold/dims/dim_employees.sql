SELECT
    employee_id,
    employee_first_name,
    employee_last_name,
    employee_email,
    employee_job_title,
    employee_salary,
    CASE 
        WHEN employee_salary < 50000 THEN 'Entry'
        WHEN employee_salary < 80000 THEN 'Mid'
        ELSE 'Senior'
    END AS employee_salary_band,
    employee_is_active
FROM {{ ref("employees_snapshot") }}
WHERE dbt_valid_to IS NULL
