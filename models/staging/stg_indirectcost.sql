{{ config(
    materialized = 'view',
    schema = 'staging'
)}}

SELECT *
FROM {{ source('Consulting_Source_Data', 'Indirect_Cost') }}