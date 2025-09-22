-- Mart model: Company employee count analysis
-- This model aggregates employee count by company and stores result as parquet

{{ config(
    materialized='table',
    file_format='parquet',
    location='s3a://{{ env_var("STORAGE_BUCKET") }}/dbt/marts/company_employee_count'
) }}

SELECT
    company,
    COUNT(Index) AS number_of_employees
FROM {{ ref('stg_customers') }}
GROUP BY
    company
ORDER BY
    number_of_employees DESC
