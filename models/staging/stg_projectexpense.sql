{{ config(
    materialized = 'view',
    schema = 'staging'
)}}

SELECT
    recordid AS record_id,
    projectid AS project_id,
    amount AS amount,
    TO_DATE(date) AS expense_date,
    is_billable AS is_billable
FROM {{ source('Consulting_Source_Data', 'ProjectExpense') }}