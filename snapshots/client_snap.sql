{% snapshot client_snap %}

{{
    config(
        target_schema='snapshots',
        unique_key='CLIENTID',
        strategy='check',
        check_cols = ['clientname','PHONE_NUMBER','EMAIL']
    )
}}

select *
from {{ ref('stg_client') }}

{% endsnapshot %}