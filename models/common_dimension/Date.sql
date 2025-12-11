{{ config(
    materialized='table',
    schema='common_dim'
) }}

WITH date_range AS (
    SELECT
        DATEADD(DAY, seq4(), DATE '2024-01-01') AS calendar_date
    FROM TABLE(GENERATOR(ROWCOUNT => 731)) -- 731 = number of days from Jan 1, 2024 to Dec 31, 2025
)

SELECT
    calendar_date,
    EXTRACT(YEAR FROM calendar_date) AS year,
    EXTRACT(QUARTER FROM calendar_date) AS quarter,
    EXTRACT(MONTH FROM calendar_date) AS month,
    TO_CHAR(calendar_date, 'Month') AS month_name,
    TO_CHAR(calendar_date, 'Day') AS day_name,
    EXTRACT(DAYOFWEEK FROM calendar_date) AS day_of_week,
    EXTRACT(WEEK FROM calendar_date) AS week,
    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM calendar_date) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS is_weekend
FROM date_range