{% snapshot deliverable_snap %}

{{
    config(
        target_schema='snapshots',
        unique_key='DeliverableID',
        strategy='check',
        check_cols = ['PROJECTID','NAME','PRICE','DUE_DATE']
    )
}}

select *
from {{ ref('stg_deliverable') }}

{% endsnapshot %}