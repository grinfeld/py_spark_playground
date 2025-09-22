{{ config(
  materialized='table',
  file_format='iceberg',
  pre_hook="""
    CREATE OR REPLACE TEMP VIEW raw_customers
    USING csv
    OPTIONS (
      path '{{ env_var('RAW_DATA_PATH') }}',
      header 'true',
      inferSchema 'true'
    )
  """
) }}

-- SELECT-only body (no CREATE statements below!)
SELECT
    CAST(`Index` AS int)                        AS index,
    `Company`                                   AS company,
    `First Name`                                AS first_name,
    `Last Name`                                 AS last_name,
    `Email`                                     AS email,
    `City`                                      AS city,
    `Country`                                   AS country,
    `Phone 1`                                   AS phone_1,
    `Phone 2`                                   AS phone_2,
    `Website`                                   AS website,
    TO_DATE(`Subscription Date`)                AS start_date_parsed,
    CASE
        WHEN `Email` IS NULL OR `Email` = '' THEN 'MISSING_EMAIL'
        WHEN `Email` NOT LIKE '%@%'              THEN 'INVALID_EMAIL'
        ELSE 'VALID_EMAIL'
        END                                         AS email_status,
    CONCAT(`First Name`, ' ', `Last Name`)      AS full_name
FROM raw_customers;