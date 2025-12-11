{{ config(
    materialized='incremental',
    schema='project_management_mart',
    incremental_strategy='merge',
    on_schema_change='sync',
    unique_key='row_key'
) }}

WITH src AS (
    SELECT
        project_id,
        client_id,
        unit_id AS business_unit_id,
        project_type AS type,
        price,
        planned_start_date,
        planned_end_date,
        actual_start_date,
        actual_end_date,
        estimated_budget,
        planned_hours,
        percent_complete,
        status,
        last_update,
        created_at
    FROM {{ ref('stg_project') }}

    {% if is_incremental() %}
      WHERE last_update > (
        SELECT COALESCE(MAX(last_update), TO_TIMESTAMP_NTZ('1900-01-01'))
        FROM {{ this }}
      )
    {% endif %}
),

latest AS (
    SELECT
        project_id,
        client_id,
        business_unit_id,
        type,
        price,
        planned_start_date,
        planned_end_date,
        actual_start_date,
        actual_end_date,
        estimated_budget,
        planned_hours,
        percent_complete,
        status,
        last_update,
        created_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY last_update DESC) AS rn
        FROM src
    ) t
    WHERE rn = 1
),

-- 维度表
project_dim       AS (SELECT * FROM {{ ref('project_dim') }}),
client_dim        AS (SELECT * FROM {{ ref('client_dim') }}),
businessunit_dim  AS (SELECT * FROM {{ ref('businessunit_dim') }}),
date_dim          AS (SELECT * FROM {{ ref('project_date_dim') }}),

-- 4. 按项目 + 月汇总的工时（通过 Deliverable 拿到 project_id）
hours_base AS (
    SELECT
        d.PROJECTID AS project_id,
        DATE_TRUNC('month', cd.consultant_deliverable_date)::date AS month,
        SUM(cd.hours) AS hours_spent
    FROM {{ ref('stg_consultantdeliverables') }} cd
    LEFT JOIN {{ ref('stg_deliverable') }} d
        ON cd.deliverableid = d.DELIVERABLEID
    GROUP BY
        d.PROJECTID,
        DATE_TRUNC('month', cd.consultant_deliverable_date)::date
),

-- 5. 按项目 + 月汇总的项目支出
project_expense_base AS (
    SELECT
        pe.project_id,
        DATE_TRUNC('month', pe.expense_date)::date AS month,
        SUM(pe.amount) AS project_expenses_per_month
    FROM {{ ref('stg_projectexpense') }} pe
    GROUP BY
        pe.project_id,
        DATE_TRUNC('month', pe.expense_date)::date
),

-- 6. 按项目 + 月汇总的顾问工时成本（hours * payroll）
consultant_cost_base AS (
    SELECT
        d.PROJECTID AS project_id,
        DATE_TRUNC('month', cd.consultant_deliverable_date)::date AS month,
        SUM(cd.hours * pr.amount) AS consultant_hour_costs_per_month
    FROM {{ ref('stg_consultantdeliverables') }} cd
    LEFT JOIN {{ ref('stg_deliverable') }} d
        ON cd.deliverableid = d.DELIVERABLEID
    LEFT JOIN {{ ref('stg_payroll') }} pr
        ON cd.consultantid = pr.consultant_id
    GROUP BY
        d.PROJECTID,
        DATE_TRUNC('month', cd.consultant_deliverable_date)::date
),

-- 7. 收入：Fixed & T&M（按项目累计到当前）
fixed_price_revenue AS (
    SELECT
        d.PROJECTID AS project_id,
        SUM(d.price) AS total_fp_revenue
    FROM {{ ref('stg_deliverable') }} d
    LEFT JOIN {{ ref('stg_project') }} p
      ON d.PROJECTID = p.project_id
    WHERE p.status = 'Completed'
      AND p.project_type = 'Fixed'
    GROUP BY d.PROJECTID
),

tm_expense_revenue AS (
    SELECT
        project_id,
        SUM(amount) AS total_tm_expense_revenue
    FROM {{ ref('stg_projectexpense') }}
    WHERE is_billable = 1
    GROUP BY project_id
),

tm_consultant_revenue AS (
    SELECT
        d.PROJECTID AS project_id,
        SUM(cd.hours * pbr.rate) AS total_tm_consultant_revenue
    FROM {{ ref('stg_consultantdeliverables') }} cd
    LEFT JOIN {{ ref('stg_deliverable') }} d
        ON cd.deliverableid = d.DELIVERABLEID
    LEFT JOIN {{ ref('stg_projectbillingrate') }} pbr
        ON d.PROJECTID = pbr.project_id
    GROUP BY d.PROJECTID
),

revenue_base AS (
    SELECT
        l.project_id,
        CASE
            WHEN l.type = 'Fixed'
                THEN COALESCE(fp.total_fp_revenue, 0)
            WHEN l.type = 'Time and Material'
                THEN COALESCE(tm_exp.total_tm_expense_revenue, 0)
                     + COALESCE(tm_con.total_tm_consultant_revenue, 0)
            ELSE 0
        END AS revenue_received_to_date
    FROM latest l
    LEFT JOIN fixed_price_revenue    fp     ON l.project_id = fp.project_id
    LEFT JOIN tm_expense_revenue    tm_exp ON l.project_id = tm_exp.project_id
    LEFT JOIN tm_consultant_revenue tm_con ON l.project_id = tm_con.project_id
),

-- 8. 主 join：项目 + 维表 + 月度指标
joined AS (
    SELECT
        l.project_id,

        -- 维度 surrogate keys
        p.project_key,
        c.client_key,
        b.unitkey AS business_unit_key,

        -- 快照月份（project + month 粒度）
        DATE_TRUNC('month', l.last_update)::date AS snapshot_month,

        -- 日期维 key（这里仅保留实际开始/结束）
        das.date AS actual_start_date_key,
        dae.date AS actual_end_date_key,

        -- 原始指标字段（内部使用）
        l.price,
        l.estimated_budget,
        l.planned_hours,
        l.percent_complete,
        l.status,
        l.type,
        l.planned_start_date,
        l.planned_end_date,

        -- 是否晚开工
        CASE
            WHEN l.actual_start_date IS NOT NULL
             AND l.planned_start_date IS NOT NULL
             AND l.actual_start_date > l.planned_start_date
            THEN 1 ELSE 0
        END AS if_late_start_flag,

        -- 工期类指标
        DATEDIFF('day', l.planned_start_date, l.planned_end_date) AS planned_duration_days,
        CASE
            WHEN l.actual_start_date IS NOT NULL
             AND l.actual_end_date IS NOT NULL
            THEN DATEDIFF('day', l.actual_start_date, l.actual_end_date)
        END AS actual_duration_days,

        -- 预计结束日期（基于当前日期和完成百分比）
        CASE
            WHEN l.actual_start_date IS NOT NULL
             AND l.percent_complete IS NOT NULL
             AND l.percent_complete > 0
            THEN DATEADD(
                     'day',
                     ROUND(
                        DATEDIFF('day', l.actual_start_date, CURRENT_DATE)
                        / NULLIF(l.percent_complete, 0)
                     ),
                     l.actual_start_date
                 )
        END AS expected_end_date,

        -- 月度工时、费用、成本
        hb.hours_spent,
        peb.project_expenses_per_month,
        ccb.consultant_hour_costs_per_month,

        -- 收入（累计到当前）
        rb.revenue_received_to_date,

        -- 追踪字段（不在最终输出中选出）
        l.last_update,
        l.created_at,
        CURRENT_TIMESTAMP() AS _loaded_at

    FROM latest l

    LEFT JOIN project_dim      p ON p.project_id = l.project_id
    LEFT JOIN client_dim       c ON c.client_id  = l.client_id
    LEFT JOIN businessunit_dim b ON b.unitid     = l.business_unit_id

    LEFT JOIN date_dim das ON das.date = l.actual_start_date
    LEFT JOIN date_dim dae ON dae.date = l.actual_end_date

    LEFT JOIN hours_base hb
      ON hb.project_id = l.project_id
     AND hb.month      = DATE_TRUNC('month', l.last_update)::date

    LEFT JOIN project_expense_base peb
      ON peb.project_id = l.project_id
     AND peb.month      = DATE_TRUNC('month', l.last_update)::date

    LEFT JOIN consultant_cost_base ccb
      ON ccb.project_id = l.project_id
     AND ccb.month      = DATE_TRUNC('month', l.last_update)::date

    LEFT JOIN revenue_base rb
      ON rb.project_id = l.project_id
),

fact_base AS (
    SELECT
        j.*,
        -- total_expenses：项目费用 + 顾问工时成本
        (j.project_expenses_per_month + j.consultant_hour_costs_per_month) AS total_expenses,
        -- forecasted_cost：total_expenses / percent_complete
        CASE
            WHEN j.percent_complete IS NOT NULL
             AND j.percent_complete > 0
            THEN (j.project_expenses_per_month + j.consultant_hour_costs_per_month)
                 / NULLIF(j.percent_complete, 0)
        END AS forecasted_cost
    FROM joined j
)

SELECT
    -- Row key：不用 surrogate_key 宏，直接用 md5 + concat_ws
    md5(
        concat_ws(
            '|',
            COALESCE(f.project_id::string, ''),
            COALESCE(TO_CHAR(f.snapshot_month, 'YYYYMMDD'), '')
        )
    ) AS row_key,

    f.project_id,
    f.project_key,
    f.client_key,
    f.business_unit_key,

    -- 项目 + 月 的日期
    f.snapshot_month AS date,

    -- 日期维（只保留实际开始/结束）
    f.actual_start_date_key,
    f.actual_end_date_key,

    -- 主要状态与进度指标
    f.percent_complete,
    f.status,
    f.if_late_start_flag,
    f.planned_duration_days,
    f.actual_duration_days,
    f.expected_end_date,

    -- 月度费用与成本
    f.hours_spent,
    f.project_expenses_per_month,
    f.consultant_hour_costs_per_month,
    f.total_expenses,
    f.forecasted_cost,

    -- 收入（累计到当前）
    f.revenue_received_to_date AS revenue_received_per_month,

    -- 潜在问题项目标记
    CASE
        WHEN (
            -- 1. 进度落后：计划结束日 < 预计结束日
            f.planned_end_date IS NOT NULL
            AND f.expected_end_date IS NOT NULL
            AND f.planned_end_date < f.expected_end_date
        )
        OR (
            -- 2. Fixed: forecasted_cost > price
            f.type = 'Fixed'
            AND f.forecasted_cost IS NOT NULL
            AND f.price IS NOT NULL
            AND f.forecasted_cost > f.price
        )
        OR (
            -- 3. T&M: forecasted_cost > estimated_budget
            f.type = 'Time and Material'
            AND f.forecasted_cost IS NOT NULL
            AND f.estimated_budget IS NOT NULL
            AND f.forecasted_cost > f.estimated_budget
        )
        THEN 1 ELSE 0
    END AS potential_problem_project

FROM fact_base f

{% if is_incremental() %}
  and date >= (
      select dateadd('month', -1, max(date)) from {{ this }}
  )
{% endif %}