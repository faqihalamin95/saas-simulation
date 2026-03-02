with product_source as (
    select *
    from {{ref ("stg_product_events")}}
),

final as (
    select 
        row_number() over (order by event_id)   as product_event_key,
        u.user_key,
        p.plan_key,
        d.date_key,
        r.event_id,
        r.event_type,
        r.referral_code_cleaned                 as referral_code,
        r.event_timestamp_local,
        r.event_timestamp_utc
    from product_source r
    left join {{ ref('dim_users') }} u on r.user_id = u.user_id
    left join {{ ref('dim_date') }} d  on r.event_date = d.full_date
    left join {{ ref('dim_plans')}} p on r.plan_cleaned = p.plan
)

select * from final