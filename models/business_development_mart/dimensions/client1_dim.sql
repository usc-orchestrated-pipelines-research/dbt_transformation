{{ config(
    		materialized='table',
    		schema='business_development_mart'
)
}}

SELECT
    {{ surrogate_key(['CLIENTID', ts_to_key('dbt_valid_from')]) }} as client_key,
    ClientID AS client_id, 
    ClientName AS client_name,
    Phone_Number AS phone_number,
    Email AS email,
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to
    
FROM {{ ref('client_snap') }}

