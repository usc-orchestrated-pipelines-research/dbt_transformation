{{ config(
    materialized = 'view',
    schema = 'staging'
)}}

SELECT
    titleid AS title_id,
    title_name AS title_name
FROM {{ source('Consulting_Source_Data', 'Title') }}


