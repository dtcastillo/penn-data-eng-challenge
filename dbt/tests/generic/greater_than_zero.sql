{% test greater_than_zero(model, column_name) %}

select *
from (
    select
      {{ column_name }}    

    from {{ model }}

    where {{ column_name }} <= 0

) validation_errors

{% endtest %}