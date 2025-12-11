{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  on_schema_change='sync',
  schema='business_development_mart',
  unique_key='client_feedback_key'
) }}

WITH 
fixed_price_revenue AS (
    SELECT
        p.client_id, p.unit_id,
        SUM(de.price) AS total_fp_revenue,
        date_trunc('month', de.due_date)::date AS deliverable_date
    FROM {{ ref('stg_deliverable') }} de
    LEFT JOIN {{ ref('stg_project') }} p
      ON de.projectid = p.project_id
    WHERE p.status = 'Completed'
      AND p.project_type = 'Fixed'
    GROUP BY 
        p.client_id, p.unit_id,
        date_trunc('month', de.due_date)::date
),

tm_expense_revenue AS (
    SELECT
        p.client_id, p.unit_id,
        SUM(pe.amount) AS total_tm_expense_revenue,
        date_trunc('month', pe.expense_date)::date AS expense_date
    FROM {{ ref('stg_projectexpense') }} pe
    LEFT JOIN {{ ref('stg_project')}} p 
    USING(project_id)
    WHERE pe.is_billable = 1
    GROUP BY 
        p.client_id, p.unit_id,
        date_trunc('month', pe.expense_date)::date
),

tm_consultant_revenue AS (
    SELECT
        p.client_id, p.unit_id,
        SUM(cd.hours * pbr.rate) AS total_tm_consultant_revenue,
        date_trunc('month', consultant_deliverable_date)::date AS deliverable_date
    FROM {{ ref('stg_consultantdeliverables') }} cd
    LEFT JOIN {{ ref('stg_deliverable') }} de
        ON cd.DeliverableID = de.DeliverableID
    LEFT JOIN {{ ref('stg_projectbillingrate') }} pbr
        ON de.PROJECTID = pbr.project_id
    LEFT JOIN {{ ref('stg_project')}} p
        USING(project_id)
    GROUP BY 
        p.client_id, p.unit_id,
        date_trunc('month', consultant_deliverable_date)::date
),

satisfaction AS (
    SELECT
        cf.client_id, 
        p.unit_id, 
        date_trunc('month', coalesce(p.actual_start_date,p.planned_start_date))::date  AS feedback_date,
        AVG(cf.overall_satisfaction) AS average_satisfaction
    FROM {{ref('stg_project')}} p
    INNER JOIN {{ ref('stg_clientfeedback') }} cf
    ON p.project_id = cf.project_id
    GROUP BY cf.client_id, 
        p.unit_id, 
        date_trunc('month', coalesce(p.actual_start_date,p.planned_start_date))::date 
),

fixed_price_incomplete AS (
    SELECT 
        p.client_id, p.unit_id,
        sum(de.price) AS incomplete_fp_revenue,
        date_trunc('month', de.due_date)::date AS deliverable_date
    FROM {{ ref('stg_deliverable') }} de
    LEFT JOIN {{ ref('stg_project') }} p 
        ON de.PROJECTID = p.project_id
    WHERE p.status <> 'Completed' AND p.project_type = 'Fixed'
    GROUP BY 
        p.client_id, 
        p.unit_id,
        date_trunc('month', de.due_date)::date
),

TM_price_incomplete AS (
    SELECT p.client_id, p.unit_id,
           date_trunc('month', coalesce(p.actual_start_date,p.planned_start_date))::date  AS TM_date,
           ROUND(SUM(COALESCE(p.price, 0) * (1 - (p.percent_complete/100))), 2) AS incomplete_tm_revenue
    FROM {{ ref('stg_project')}} p
    WHERE p.status <> 'Completed' AND p.project_type = 'Time and Material'
    GROUP BY client_id, unit_id, date_trunc('month', coalesce(p.actual_start_date,p.planned_start_date))::date 
),

all_client_months as (
    select distinct
        client_id, unit_id,
        snapshot_month
    from (
        select client_id,unit_id, deliverable_date AS snapshot_month 
        from  fixed_price_revenue 
        union
        select client_id, unit_id, expense_date AS snapshot_month 
        from tm_expense_revenue 
        union
        select client_id,unit_id, deliverable_date AS snapshot_month 
        from tm_consultant_revenue
        union 
        select client_id, unit_id, feedback_date AS snapshot_month
        from satisfaction
        union
        select client_id, unit_id, deliverable_date AS snapshot_month
        from fixed_price_incomplete
        union 
        select client_id, unit_id, TM_date AS snapshot_month
        from TM_price_incomplete
    )
    where client_id IS NOT NULL
),

forecasted_revenue AS (
    SELECT
        acm.client_id,
        acm.snapshot_month,
        acm.unit_id,
        CASE
            WHEN p.project_type = 'Fixed' 
            THEN COALESCE(fpi.incomplete_fp_revenue, 0) 
            WHEN p.project_type = 'Time and Material' 
            THEN COALESCE(tpi.incomplete_tm_revenue, 0)
        END AS forecasted_future_revenue   
    FROM all_client_months acm
    LEFT JOIN fixed_price_incomplete fpi
        ON acm.client_id = fpi.client_id  AND acm.snapshot_month = fpi.deliverable_date
    LEFT JOIN TM_price_incomplete tpi 
        ON acm.client_id = tpi.client_id AND acm.snapshot_month = tpi.TM_date
    LEFT JOIN {{ ref('stg_project')}} p 
    ON acm.client_id = p.client_id
),

revenue_received_final AS (
    SELECT
        acm.client_id,
        acm.snapshot_month,
        COALESCE(fp.total_fp_revenue, 0) +
        COALESCE(tm_exp.total_tm_expense_revenue, 0) +
        COALESCE(tm_con.total_tm_consultant_revenue, 0) AS revenue_received

    FROM all_client_months acm
    LEFT JOIN fixed_price_revenue fp
        ON acm.client_id = fp.client_id
        AND acm.snapshot_month = fp.deliverable_date

    LEFT JOIN tm_expense_revenue tm_exp
        ON acm.client_id = tm_exp.client_id
        AND acm.snapshot_month = tm_exp.expense_date

    LEFT JOIN tm_consultant_revenue tm_con
        ON acm.client_id = tm_con.client_id
        AND acm.snapshot_month = tm_con.deliverable_date
    -- Removed stg_project join to avoid Double Counting risk
)

SELECT {{ surrogate_key(['acm.client_id', ts_to_key('acm.snapshot_month')]) }} AS client_feedback_key,
    c.client_key,
    bd.business_unit_key,
    dd.date,
    rrf.revenue_received,
    s.average_satisfaction,
    fr.forecasted_future_revenue 
FROM {{ ref('client1_dim')}} c
LEFT JOIN all_client_months acm
ON c.client_id = acm.client_id
INNER JOIN {{ ref('business_development_date_dim')}} dd 
ON dd.date = acm.snapshot_month
LEFT JOIN {{ ref('business_unit_dim')}} bd 
ON acm.unit_id = bd.unitid
LEFT JOIN revenue_received_final rrf 
ON c.client_id = rrf.client_id AND dd.date = rrf.snapshot_month
LEFT JOIN satisfaction s 
ON c.client_id = s.client_id AND dd.date = s.feedback_date
LEFT JOIN forecasted_revenue fr 
ON c.client_id = fr.client_id AND dd.date = fr.snapshot_month
WHERE c.client_key IS NOT NULL

{% if is_incremental() %}
  and dd.date >= (
      select dateadd('month', -1, max(date)) from {{ this }}
  )
{% endif %}
    
    


