{{ config(
    materialized = 'view',
    schema = 'staging'
)}}

SELECT 
    consultantid,
    deliverableid,
    recordid,
    hours,
    to_date("DATE")       as consultant_deliverable_date
FROM {{ source('Consulting_Source_Data', 'ConsultantDeliverable') }}
