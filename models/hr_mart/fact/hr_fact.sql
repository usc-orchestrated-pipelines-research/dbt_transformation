{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    schema='HR_MART',
    unique_key='HR_record_key'
) }}

WITH b_hours AS (
    SELECT
        consultantid              AS consultant_id,
        month_start_date          AS snapshot_month,
        SUM(billable_hours)/60         AS billable_hours,
        SUM(non_billable_hours)/60     AS non_billable_hours
    FROM {{ ref('stg_nonbillablehours') }}
    GROUP BY
        consultantid, month_start_date
),

hours_percent AS (
    SELECT
        consultant_id,
        snapshot_month,
        billable_hours + non_billable_hours AS total_hours,
        billable_hours
            / NULLIF(billable_hours + non_billable_hours, 0)
            AS billable_hours_percentage
    FROM b_hours
),

join_keys AS(
    SELECT bh.consultant_id, 
        cd.deliverableid, 
        c.businessunitid,
        p.client_id
    FROM b_hours bh
    LEFT JOIN {{ ref('stg_consultantdeliverables')}} cd
    ON bh.consultant_id = cd.consultantid
    LEFT JOIN {{ ref('stg_consultant')}} c
    ON bh.consultant_id = c.consultantid
    LEFT JOIN {{ ref('stg_project')}} p 
    ON c.businessunitid = p.unit_id
)

SELECT {{surrogate_key(['bh.consultant_id',ts_to_key('bh.snapshot_month')])}} AS HR_record_key,
    cd.consultantkey AS consultant_key,
    dd.deliverable_key,
    bd.unitkey AS business_unit_key,
    cid.client_key,
    d.date,
    thd.titlehistory_key, 
    bh.billable_hours,
    bh.non_billable_hours,
    hp.total_hours,
    hp.billable_hours_percentage
FROM b_hours bh
INNER JOIN hours_percent hp
ON bh.consultant_id = hp.consultant_id
    AND bh.snapshot_month = hp.snapshot_month
LEFT JOIN {{ref('consultant_dim')}} cd 
ON bh.consultant_id = cd.consultantid
LEFT JOIN join_keys jk
ON bh.consultant_id = jk.consultant_id
LEFT JOIN {{ref('deliverable_dim')}} dd
ON jk.deliverableid = dd.deliverableid
LEFT JOIN {{ ref('businessuni_dim')}} bd 
ON jk.businessunitid = bd.unitid
LEFT JOIN {{ ref('hr_client_dim')}} cid 
ON jk.client_id = cid.client_id
LEFT JOIN {{ ref('hr_date_dim')}} d 
ON bh.snapshot_month = d.date
LEFT JOIN {{ref('title_history_dim')}} thd
ON bh.consultant_id = thd.consultantid

{% if is_incremental() %}
  WHERE d.date >= (
      SELECT dateadd('month', -1, max(date)) FROM {{ this }}
  )
{% endif %}  
    

