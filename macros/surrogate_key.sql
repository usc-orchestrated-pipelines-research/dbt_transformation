-- {# Stable string formatter + hash for surrogate keys #}
{% macro surrogate_key(columns) %}
    md5(
      {%- for col in columns -%}
        coalesce(lower(trim(to_varchar({{ col }}))), '') || '|' ||
      {%- endfor -%}
      ''
    )
{% endmacro %}

-- {# Helper to format TIMESTAMP for keying (NTZ-safe, fixed precision) #}
{% macro ts_to_key(column) -%}
  to_char({{ column }}, 'YYYY-MM-DD"T"HH24:MI:SSFF3')
{%- endmacro %}