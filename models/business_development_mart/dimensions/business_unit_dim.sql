{{ config(
    		materialized='table',
    		schema='Business_Development_Mart'
)}}

SELECT
    {{ surrogate_key(['BusinessUnitID', ts_to_key('dbt_valid_from')]) }} as business_unit_key,
    businessunitID AS unitid, 
    businessunitname AS name,
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to
FROM {{ ref('businessunit_snap') }}

