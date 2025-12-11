{{ config(
    materialized = 'table',
    schema = 'PROJECT_MANAGEMENT_MART'
) }}

select
    calendar_date as date,
    year,
    quarter,
    month,
    month_name,
    day_name,
    day_of_week,
    week,
    is_weekend
from {{ ref('Date') }}
