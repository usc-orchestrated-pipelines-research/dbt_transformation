{% snapshot consultanttitlehistory_snap %}

{{
    config(
        target_schema='snapshots',
        unique_key='record_id',
        strategy='check',
        check_cols = ['consultant_id', 'title_id', 'start_date', 'end_date']
    )
}}

select *
from {{ ref('stg_consultanttitlehistory') }}

{% endsnapshot %}