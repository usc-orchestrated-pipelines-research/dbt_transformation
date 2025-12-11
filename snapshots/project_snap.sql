{% snapshot project_snap %}
{{
    config(
        target_schema='snapshots',
        unique_key='project_id',
        strategy='check',
        check_cols= ['client_id', 'unit_id', 'project_name', 'project_type', 'price', 'planned_start_date', 'planned_end_date']
    )
}}

select project_id, client_id, unit_id, project_name, project_type, price, planned_start_date, planned_end_date
from {{ ref('stg_project') }}

{% endsnapshot %}
