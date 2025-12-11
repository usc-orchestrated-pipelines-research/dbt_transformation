{{ config(
    materialized = 'table',
    schema = 'HR_Mart'
) }}

with d as (
    -- Deliverable snapshot (SCD2)
    select
        DELIVERABLEID,
        PROJECTID,
        NAME,
        DUE_DATE,
        DBT_VALID_FROM,
        DBT_VALID_TO
    from {{ ref('deliverable_snap') }}
),

p as (
    -- Project snapshot used to enrich deliverable with project attributes
    select
        project_id   as projectid,
        project_name as projectname,
        project_type as projecttype,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('project_snap') }}
),

final_dim AS (
    -- natural key + attributes
    SELECT d.DELIVERABLEID            as deliverableid,
        d.PROJECTID                as projectid,
        d.NAME                     as name,
        d.DUE_DATE                 as duedate,
        p.projectname              as projectname,
        p.projecttype              as projecttype,

    -- SCD2 validity
        greatest(d.DBT_VALID_FROM,p.dbt_valid_from) as valid_from,
        least(coalesce(d.DBT_VALID_TO,'9999-12-31'::timestamp),
            coalesce(p.dbt_valid_to, '9999-12-31'::timestamp)) as valid_to_internal

    FROM d 
    LEFT JOIN p
    ON d.PROJECTID = p.projectid
)

SELECT 
    {{ surrogate_key(['fd.deliverableid',ts_to_key('fd.valid_from')])}} AS             deliverable_key,
    fd.deliverableid,
    fd.projectid,
    fd.name,
    fd.duedate,
    fd.projectname,
    fd.projecttype,
    fd.valid_from,
    CASE WHEN fd.valid_to_internal = '9999-12-31' THEN NULL
        ELSE fd.valid_to_internal
    END AS valid_to
FROM final_dim fd