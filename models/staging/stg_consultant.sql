{{ config(
    materialized = 'view',
    schema = 'staging'
)}}

SELECT ConsultantID, 
    BusinessUnitID, 
    First_Name AS FirstName, 
    Last_Name AS LastName, 
    Contact
FROM {{source('Consulting_Source_Data','Consultant')}}