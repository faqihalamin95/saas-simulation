-- tests/generic/assert_accepted_value_pct.sql
-- Fails if percentage of unexpected values exceeds max_pct threshold
-- Usage:
--   - assert_accepted_value_pct:
--       values: ['Growth', 'Starter', 'Enterprise', 'Unknown']
--       max_pct: 10   ← trigger if unexpected values > 10%

{% test assert_accepted_value_pct(model, column_name, values, max_pct) %}

with validation as (
    select
        round(
            count(case when {{ column_name }} not in {{ values | tojson }} then 1 end)::float
            / nullif(count(*), 0) * 100, 2
        ) as unexpected_pct
    from {{ model }}
    where {{ column_name }} is not null
)

select unexpected_pct
from validation
where unexpected_pct > {{ max_pct }}

{% endtest %}