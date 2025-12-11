-- Test to ensure Project Price is never negative
-- Failure condition: Returns rows where price < 0

SELECT *
FROM {{ ref('project_dim') }}
WHERE price < 0