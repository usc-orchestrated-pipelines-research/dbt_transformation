{{ config(
    materialized = 'view',
    schema = 'staging'
)}}

SELECT businessunitID, 
    business_unit_name AS businessunitname
FROM {{source('Consulting_Source_Data','Businessunit')}}