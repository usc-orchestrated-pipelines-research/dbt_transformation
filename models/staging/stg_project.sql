{{ config(
    materialized = 'view',
    schema       = 'staging'
) }}

-- Staging view for source: Consulting_Source_Data.Project


with base as (

    select
        projectID,
        created_at,
        clientID,
        unitID,
        name,
        type,
        price,
        estimated_budget,
        planned_hours,
        planned_start_date,
        planned_end_date,
        status,
        actual_start_date,
        actual_end_date,
        progress,
        last_update     
    from {{ source('Consulting_Source_Data', 'Project') }}

),

final as (

    select


        
        projectID                               as project_id,           
        try_to_timestamp_ntz(created_at)        as created_at,
        clientID                                as client_id,
        unitID                                  as unit_id,
        trim(name)                              as project_name,
        trim(type)                              as project_type,
        price                                   as price,

    
        try_to_double(estimated_budget)         as estimated_budget,
        try_to_number(planned_hours)            as planned_hours,

        
        to_date(planned_start_date)             as planned_start_date,
        to_date(planned_end_date)               as planned_end_date,
        upper(trim(status))                     as status,             
        to_date(actual_start_date)              as actual_start_date,
        to_date(actual_end_date)                as actual_end_date,

      
        progress                                as percent_complete,

        try_to_timestamp_ntz(last_update)       as last_update

    from base

)

select *
from final
