{{ config(
    materialized = 'view',
    schema = 'staging'
)}}

SELECT 
    RECORDID AS record_id,
    CONSULTANTID AS consultant_id,
    TITLEID AS title_id,
    TO_DATE(START_DATE) AS start_date,
    TO_DATE(LAST_UPDATE) AS end_date
FROM {{source('Consulting_Source_Data','ConsultantTitleHistory')}}