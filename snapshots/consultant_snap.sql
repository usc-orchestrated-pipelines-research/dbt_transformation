{% snapshot consultant_snap %}

{{
    config(
        target_schema='snapshots',
        unique_key='ConsultantID',
        strategy='check',
        check_cols = ['BusinessUnitID', 'FirstName', 'LastName', 'Contact']  
    )
}}

select *
from {{ ref('stg_consultant') }}

{% endsnapshot %}