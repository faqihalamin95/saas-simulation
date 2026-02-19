with payments_source as ( 
    select * 
    from {{ ref('stg_payments') }}
),

final as (
    select
        row_number() over (order by payment_id) as payment_key,
        u.user_key,
        d.date_key,
        p.payment_id,
        p.amount_usd,
        p.status,
        p.attempt_number,
        p.payment_timestamp_local,
        p.payment_timestamp_utc
    from payments_source p
    left join {{ ref('dim_users') }} u on p.user_id = u.user_id
    left join {{ ref('dim_date') }} d  on p.payment_date = d.full_date
)

select * from final