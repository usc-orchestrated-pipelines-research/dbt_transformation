{{ config(
    materialized = 'view',
    schema = 'staging'
)}}

SELECT  
    RATE AS rate,
    TITLEID AS title_id,
    PROJECTID AS project_id
    
FROM {{source('Consulting_Source_Data','ProjectBillingRate')}}