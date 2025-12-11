{{ config(
    materialized = 'view',
    schema = 'staging'
)}}

SELECT 
    consultantid,
    "BILLABLE HOURS" AS billable_hours,
    "TABLE NONBILLABLEHOURS" As non_billable_hours,
    TO_DATE(CONCAT(yearmonth, '01'), 'YYYYMMDD') AS month_start_date
FROM {{ source('Consulting_Source_Data', 'Non_Billable_Hours') }}
