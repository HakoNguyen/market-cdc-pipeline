{% test unique_combination_of_columns(model, combination_of_columns) %}
-- Không có 2 dòng trùng tổ hợp khoá. Tự viết thay cho dbt_utils để khỏi thêm package + dbt deps.
select
    {{ combination_of_columns | join(', ') }},
    count(*) as n_rows
from {{ model }}
group by {{ combination_of_columns | join(', ') }}
having count(*) > 1
{% endtest %}
