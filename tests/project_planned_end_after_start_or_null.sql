{% test project_planned_end_after_start_or_null(model, column_name, start_column) %}

    select *
    from {{ model }}
    where {{ column_name }} is not null
      and {{ start_column }} is not null
      and {{ column_name }} < {{ start_column }}

{% endtest %}
