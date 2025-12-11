{{ config(
    materialized = 'table',
    schema = 'financial_report_mart'
) }}

SELECT
    {{ surrogate_key(["businessunitid", ts_to_key("dbt_valid_from")]) }} AS unitkey,
    businessunitid AS unitid,
    businessunitname AS name,
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to

FROM {{ ref('businessunit_snap') }}

