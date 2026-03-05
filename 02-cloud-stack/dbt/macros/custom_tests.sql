-- Test: persentase null tidak boleh melebihi threshold
{% test assert_not_null_pct(model, column_name, max_pct=5) %}
select 1
from (
    select
        sum(case when {{ column_name }} is null then 1 else 0 end) * 1.0
        / nullif(count(*), 0) * 100 as null_pct
    from {{ model }}
) t
where null_pct > {{ max_pct }}
{% endtest %}

-- Test: persentase late arriving tidak boleh melebihi threshold
{% test assert_late_arriving_pct(model, column_name, max_pct=15) %}
select 1
from (
    select
        sum(case when {{ column_name }} = true then 1 else 0 end) * 1.0
        / nullif(count(*), 0) * 100 as late_pct
    from {{ model }}
) t
where late_pct > {{ max_pct }}
{% endtest %}