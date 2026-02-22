with payment_status as (
	select 
		to_char(dd.full_date, 'YYYY-MM') as month,
		du.acquisition_channel,
		du.user_id,
		fp.amount_usd 
	from {{ ref('fct_payments')}} fp 
	left join {{ ref('dim_users')}} du on fp.user_key = du.user_key 
	left join {{ ref('dim_date')}} dd on fp.date_key = dd.date_key 
	where fp.status = 'success'
),

total_revenue as (
	select 
		month,
		acquisition_channel,
		count(distinct user_id) as user_count,
		sum(amount_usd) as total_revenue
	from payment_status
	group by month, acquisition_channel
),

final as (
    select
        month,
        acquisition_channel,
        user_count,
        total_revenue,
        round(total_revenue / nullif(user_count, 0), 2)                                    as avg_revenue_per_user,
        round(total_revenue * 100.0 / nullif(sum(total_revenue) over (partition by month), 0), 2)  as pct_of_total_revenue
    from total_revenue
)

select * from final