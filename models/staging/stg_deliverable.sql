{{ config(
    materialized = 'view',
    schema = 'staging'
)}}

SELECT 
    DELIVERABLEID AS DeliverableID,
    PROJECTID,
    NAME,
    PRICE,
    TO_DATE(DUE_DATE,'YYYY-MM-DD') AS Due_date
FROM {{source('Consulting_Source_Data','Deliverable')}}