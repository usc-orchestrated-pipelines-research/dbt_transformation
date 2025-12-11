{{ config(
    materialized = 'table',
    schema = 'HR_Mart'
) }}

SELECT
    {{ surrogate_key(["CONSULTANTID", ts_to_key("dbt_valid_from")]) }} AS ConsultantKey,
    CONSULTANTID    AS ConsultantID,
    FIRSTNAME       AS FirstName,
    LASTNAME        AS LastName,
    CONTACT         AS Contact,
    DBT_VALID_FROM  AS valid_from,
    DBT_VALID_TO    AS valid_to
FROM {{ ref('consultant_snap') }}
