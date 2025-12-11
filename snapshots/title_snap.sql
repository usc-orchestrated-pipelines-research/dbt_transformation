{% snapshot title_snap %}

{{
    config(
        target_schema='snapshots',
        unique_key='title_id',
        strategy='check',
        check_cols = ['title_name']
    )
}}

select *
from {{ ref('stg_title') }}

{% endsnapshot %}