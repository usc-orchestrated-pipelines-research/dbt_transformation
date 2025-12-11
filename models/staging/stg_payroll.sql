{{ config(
    materialized = 'view',
    schema = 'staging'
)}}

SELECT 
    RECORDID AS record_id,
    AMOUNT AS amount,
    CONSULTANTID AS consultant_id,
    payment_date
    
FROM {{source('Consulting_Source_Data','Payroll')}}