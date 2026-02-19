WITH source AS (
  SELECT * FROM {{ source('raw', 'subscription_events') }}
),

renamed AS (
    SELECT
      event_id,
      user_id,
      event_type,
      
      -- separate plan from status
      case
        when plan in ('Trial', 'Expired', 'Canceled') then null 
        else plan
      end as plan,
      case
          when plan = 'Trial'    then 'trial'
          when plan = 'Expired'  then 'expired'
          when plan = 'Canceled' then 'canceled'
          else 'active'
      end as subscription_status, 

      event_timestamp_local,
      event_timestamp_utc,
      event_timestamp_utc::DATE AS event_date,
      country,
      batch_month,
      promo_code
    FROM source
)

SELECT * FROM renamed
