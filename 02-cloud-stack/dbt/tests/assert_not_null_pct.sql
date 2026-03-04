-- tests/generic/assert_not_null_pct.sql
-- Fails if null percentage exceeds max_pct threshold
-- Usage:
--   - assert_not_null_pct:
--       max_pct: 5   ← trigger if null > 5%

{% test assert_not_null_pct(model, column_name, max_pct) %}

with validation as (
    select
        round(
            count(case when {{ column_name }} is null then 1 end)::float
            / nullif(count(*), 0) * 100, 2
        ) as null_pct
    from {{ model }}
)

select null_pct
from validation
where null_pct > {{ max_pct }}

{% endtest %}