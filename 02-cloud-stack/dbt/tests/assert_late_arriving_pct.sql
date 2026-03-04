-- tests/generic/assert_late_arriving_pct.sql
-- Fails if late arriving percentage exceeds max_pct threshold
-- Usage:
--   - assert_late_arriving_pct:
--       max_pct: 10   ← trigger if late arriving > 10%

{% test assert_late_arriving_pct(model, column_name, max_pct) %}

with validation as (
    select
        round(
            count(case when {{ column_name }} = true then 1 end)::float
            / nullif(count(*), 0) * 100, 2
        ) as late_arriving_pct
    from {{ model }}
)

select late_arriving_pct
from validation
where late_arriving_pct > {{ max_pct }}

{% endtest %}