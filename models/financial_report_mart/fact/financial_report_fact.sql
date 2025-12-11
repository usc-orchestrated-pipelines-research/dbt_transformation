-- CONSULTING_FIRM_DB.C_FIRM_HR_MART.HR_REFER_MODIFYVERSIONCONSULTING_FIRM_DB.C_FIRM_HR_MART.HR_REFER_MODIFYVERSIONCONSULTING_FIRM_DB.C_FIRM_HR_MART.HR_REFER_MODIFYVERSIONCONSULTING_FIRM_DB.C_FIRM_HR_MART.HR_REFER_MODIFYVERSION

{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  on_schema_change='sync',
  schema='financial_report_mart',
  unique_key='row_key'
) }}

with project_expense_base as (
  select 
    SUM(pe.amount) as monthly_amount,
    -- MONTH(pe.expense_date) as expense_month,
    date_trunc('month', pe.expense_date)::date as expense_month,
    p.unit_id as unit_id
  from {{ ref('stg_projectexpense') }} pe
  left join {{ ref('stg_project') }} p
  on pe.project_id = p.project_id
  group by 
    p.unit_id, 
    -- MONTH(pe.expense_date),
    date_trunc('month', pe.expense_date)::date
),

direct_cost_base as (
  select 
    SUM(cd.hours * pr.amount) as total_billable_consultant_cost,
    c.BusinessUnitID as unit_id,
    date_trunc('month', cd.consultant_deliverable_date)::date as expense_month
  from {{ ref('stg_consultantdeliverables') }} cd
  left join {{ ref('stg_payroll') }} pr
  on cd.consultantid = pr.consultant_id
  left join {{ ref('stg_consultant') }} c
  on c.ConsultantID = cd.consultantid
  group by 
    c.BusinessUnitID, 
    date_trunc('month', cd.consultant_deliverable_date)::date
),

indirect_cost_base as (
  select 
    business_unit_id as unit_id,
    SUM(non_proj_labor_costs) as total_non_proj_labor_costs,
    SUM(other_expense_costs) as total_other_expense_costs,
    TO_DATE(TO_VARCHAR(yearmonth), 'YYYYMM') as expense_month
  from {{ ref('stg_indirectcost') }}
  group by
    business_unit_id, 
    TO_DATE(TO_VARCHAR(yearmonth), 'YYYYMM')  
),

fixed_price_revenue as (
    select
        d.projectid as project_id,
        sum(d.price) as total_fp_revenue
    from {{ ref('stg_deliverable') }} d
    join {{ ref('stg_project')}} p
    on d.projectid = p.project_id
    where p.status = 'Completed' and p.project_type = 'Fixed'
    group by d.projectid
),

tm_expense_revenue as (
    select
        project_id,
        sum(amount) as total_tm_expense_revenue
    from {{ ref('stg_projectexpense') }}
    where is_billable = 1
    group by project_id
),

tm_consultant_revenue as (
    select
        d.projectid as project_id,
        sum(cd.hours * pbr.rate) as total_tm_consultant_revenue
    from {{ ref('stg_consultantdeliverables') }} cd
    left join {{ ref('stg_deliverable') }} d
    on cd.deliverableid = d.DeliverableID
    left join {{ ref('stg_projectbillingrate') }} pbr
    on d.projectid = pbr.project_id
    group by d.projectid
),

completed_project_revenue as (
    select 
        p.project_id,
        p.unit_id,
        date_trunc('month', p.planned_start_date)::date AS expense_month,
        case
            when p.project_type = 'Fixed' then coalesce(fp.total_fp_revenue, 0)
            when p.project_type = 'Time and Material' then coalesce (tm_exp.total_tm_expense_revenue, 0) + coalesce(tm_con.total_tm_consultant_revenue, 0)
            else 0
        end as total_project_revenue
    from {{ ref('stg_project') }} p
    left join fixed_price_revenue fp on p.project_id = fp.project_id
    left join tm_expense_revenue tm_exp on p.project_id = tm_exp.project_id
    left join tm_consultant_revenue tm_con on p.project_id = tm_con.project_id
),

total_bu_project_revenue as (
    select 
        unit_id,
        expense_month,
        SUM(total_project_revenue) as total_revenue
    from completed_project_revenue
    group by
        unit_id,
        expense_month
)

{% if is_incremental() %}
, incremental_cutoff as (
    select dateadd('month', -1, max(expense_month)) as cutoff_date
    
    from project_expense_base peb
    left join direct_cost_base dcb
    using (unit_id, expense_month)
    left join indirect_cost_base icb
    using (unit_id, expense_month)
    left join {{ ref('fin_businessunit_dim') }} fbud
    on peb.unit_id = fbud.unitid 
    left join total_bu_project_revenue tbpr
    using (unit_id, expense_month)
    inner join {{ ref('fin_date_dim')}} dd 
    on dd.date = peb.expense_month
        or dd.date = dcb.expense_month
        or dd.date = icb.expense_month
        or dd.date = tbpr.expense_month
    )
{% endif %}

select
  {{ surrogate_key(["peb.unit_id", ts_to_key("peb.expense_month")]) }} as row_key,
  fbud.unitkey as unit_key,
  peb.monthly_amount as direct_project_expenses,
  dcb.total_billable_consultant_cost as direct_billable_consultant_cost,
  (peb.monthly_amount + dcb.total_billable_consultant_cost) as total_direct_cost,
  icb.total_non_proj_labor_costs as indirect_nonbillable_consultant_cost,
  icb.total_other_expense_costs as indirect_other_expense_cost,
  (icb.total_non_proj_labor_costs + icb.total_other_expense_costs) as total_indirect_costs,
  (peb.monthly_amount + dcb.total_billable_consultant_cost + icb.total_non_proj_labor_costs + icb.total_other_expense_costs) as total_expenses,
  tbpr.total_revenue,
  (tbpr.total_revenue - (peb.monthly_amount + dcb.total_billable_consultant_cost + icb.total_non_proj_labor_costs + icb.total_other_expense_costs)) as profit,
  dd.date
  
from project_expense_base peb
left join direct_cost_base dcb
using (unit_id, expense_month)
left join indirect_cost_base icb
using (unit_id, expense_month)
left join {{ ref('fin_businessunit_dim') }} fbud
on peb.unit_id = fbud.unitid 
left join total_bu_project_revenue tbpr
using (unit_id, expense_month)
inner join {{ ref('fin_date_dim')}} dd 
on dd.date = peb.expense_month
    or dd.date = dcb.expense_month
    or dd.date = icb.expense_month
    or dd.date = tbpr.expense_month
-- left join completed_project_revenue cpr
--     on peb.project_id = cpr.project_id         -- peb.project_id needed
{% if is_incremental() %}
left join incremental_cutoff ic on true
where dd.date >= ic.cutoff_date
{% endif %}

-- {% if is_incremental() %}
--   and dd.date >= (
--     select dateadd('month', -1, max(date)) as max_date
--     from {{ this }}
--   )
-- {% endif %}