{{ config(
    materialized = 'table',
    schema = 'project_management_mart'
) }}

SELECT
    {{ surrogate_key(["project_id",ts_to_key("dbt_valid_from")]) }} AS project_key,
    project_id,
    project_type AS type,
    price,
    planned_start_date,
    planned_end_date,
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to

FROM {{ ref('project_snap') }}
