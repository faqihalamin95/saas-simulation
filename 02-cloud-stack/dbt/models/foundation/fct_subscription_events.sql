with subs_source as (
    select * from {{ ref("stg_subscription_events")}}
),

final as (
    select 
        row_number() over (order by event_id)   as subscription_event_key,
        u.user_key,
        p.plan_key,
        d.date_key,
        s.event_id,
        s.event_type,
        s.subscription_status,
        s.referral_code_cleaned                 as referral_code,
        s.event_timestamp_local,
        s.event_timestamp_utc,
        s.is_late_arriving
    from subs_source s
    left join {{ ref('dim_users') }} u on s.user_id = u.user_id
    left join {{ ref('dim_date') }} d  on s.event_date = d.full_date
    left join {{ ref('dim_plans')}} p on s.plan_cleaned = p.plan
)

select * from final