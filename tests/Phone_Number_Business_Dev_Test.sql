SELECT *
FROM {{ ref(‘business_development_mart) }}
WHERE phone_number IS NOT NULL
  AND NOT REGEXP_LIKE(phone_number, '^\+?[0-9\s\-\(\)]+(x[0-9]+|ext\.?\s?[0-9]+)?$')
