{{ config(
    materialized='table',
    schema='hr_mart'
) }}

with his as (
    select
        RECORD_ID,
        CONSULTANT_ID,
        TITLE_ID,
        START_DATE,
        END_DATE,
        DBT_VALID_FROM,
        DBT_VALID_TO
    from {{ ref('consultanttitlehistory_snap') }}
),

t as (
    select
        TITLE_ID   as titleid,
        TITLE_NAME as titlename,
        DBT_VALID_FROM,
        DBT_VALID_TO
    from {{ ref('title_snap') }}
),

final_dim AS (
    -- natural key + attributes
    SELECT his.RECORD_ID            as ID,
        his.CONSULTANT_ID           as CONSULTANTID,
        t.titlename               as title,
        his.START_DATE              as startdate,
        his.END_DATE                as enddate,


    -- SCD2 validity
        greatest(his.DBT_VALID_FROM,t.DBT_VALID_FROM) as valid_from,
        least(coalesce(his.DBT_VALID_TO,'9999-12-31'::timestamp),
            coalesce(t.DBT_VALID_TO, '9999-12-31'::timestamp)) as valid_to_internal

    FROM his 
    LEFT JOIN t
    ON his.TITLE_ID = t.titleid
)


SELECT 
    {{ surrogate_key(['fd.id',ts_to_key('fd.valid_from')])}} AS TitleHistory_key,
    fd.id,
    fd.CONSULTANTID,
    fd.title,
    fd.startdate,
    fd.enddate,
    fd.valid_from,
    CASE WHEN fd.valid_to_internal = '9999-12-31' THEN NULL
        ELSE fd.valid_to_internal
    END AS valid_to
FROM final_dim fd