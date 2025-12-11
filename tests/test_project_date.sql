-- Test to ensure Planned End Date is after Planned Start Date
-- Failure condition: Returns rows where End Date <= Start Date

SELECT *
FROM {{ ref('project_dim') }}
WHERE planned_end_date IS NOT NULL
  AND planned_end_date <= planned_start_date