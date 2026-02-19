with source_plan as (
    select distinct plan from {{ ref('stg_product_events') }}
    ),

source_plan_price as (
    select
        plan,
        case plan
            when 'Free'     then 0.00
            when 'Pro'      then 15.00
            when 'Pro Plus' then 15.00
            when 'Business' then 50.00
        end as price_usd
    from source_plan
),

final as (
    select
        row_number() over (order by price_usd) as plan_key,
        plan,
        price_usd
from source_plan_price
)

select * from final