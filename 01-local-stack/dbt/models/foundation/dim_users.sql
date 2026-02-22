with users_source as (
    select *
    from {{ ref('stg_users') }}
),

final as (
    select
        row_number() over (order by user_id) as user_key,
        user_id,
        name,
        email,
        country,
        timezone,
        acquisition_channel
    from users_source
)

select * from final
