{{ config(
    materialized = 'view',
    schema = 'staging'
)}}

SELECT CLIENTID, 
    CLIENT_NAME AS clientname,
    PHONE_NUMBER,
    EMAIL
FROM {{source('Consulting_Source_Data','Client')}}