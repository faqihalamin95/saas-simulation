WITH source AS (
  SELECT * FROM {{ source('raw', 'product_events') }}
),

renamed AS (
    SELECT
      event_id,
      user_id,
      event_type,
      plan,
      event_timestamp_local,
      event_timestamp_utc,
      event_timestamp_utc::DATE AS event_date,
      batch_month,
      promo_code
    FROM source
)

SELECT * FROM renamed
