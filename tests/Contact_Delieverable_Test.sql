SELECT *
FROM {{ ref('consultant_dim') }}
WHERE contact IS NOT NULL
  AND NOT REGEXP_LIKE(contact, '^\+?[0-9\s\-\(\)]+(x[0-9]+|ext\.?\s?[0-9]+)?$')

