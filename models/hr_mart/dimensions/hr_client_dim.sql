{{ config(
    materialized='table',
    schema='hr_mart'
) }}

SELECT
    {{ surrogate_key(['CLIENTID', ts_to_key('dbt_valid_from')]) }} AS client_key,
    CLIENTID AS client_id,
    CLIENTNAME AS client_name,
    PHONE_NUMBER AS phone_number,
    EMAIL AS email,
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to
FROM {{ ref('client_snap') }}
