{{ config(
    materialized = 'view',
    schema = 'staging'
)}}

SELECT 
    RESPONSE_ID as response_id, 
    CLIENT_ID AS client_id,
    PROJECT_ID AS project_id,
    OVERALL_SATISFACTION AS overall_satisfaction
FROM {{source('Consulting_Source_Data','Client_Feedback_Initial')}}