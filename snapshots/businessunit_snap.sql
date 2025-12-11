{% snapshot businessunit_snap %}

{{
    config(
        target_schema='snapshots',
        unique_key='businessunitid',
        strategy='check',
        check_cols = ['businessunitname']
    )
}}

select *
from {{ ref('stg_businessunit') }}

{% endsnapshot %}