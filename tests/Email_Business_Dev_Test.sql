SELECT *
FROM {{ ref('business_development_mart') }}
WHERE email IS NOT NULL
  AND NOT REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')