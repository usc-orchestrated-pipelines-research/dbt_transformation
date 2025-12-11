{% test between_0_and_100(model, column_name) %}

    select
        {{ column_name }} as value
    from {{ model }}
    where {{ column_name }} < 0
       or {{ column_name }} > 100

{% endtest %}
