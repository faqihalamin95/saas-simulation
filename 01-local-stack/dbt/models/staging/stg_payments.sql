WITH source AS (
  SELECT * FROM {{ source('raw', 'payments') }}
),

renamed AS (
    SELECT
        payment_id,
        user_id,
        amount_usd::NUMERIC(10,2) AS amount_usd,
        status,
        attempt_number,
        payment_timestamp_local,
        payment_timestamp_utc,
        payment_timestamp_utc::DATE AS payment_date,
        batch_month,
        ROW_NUMBER() OVER (
            PARTITION BY payment_id 
            ORDER BY payment_timestamp_utc DESC  
        ) AS rn
    FROM source
)

SELECT * FROM renamed
WHERE rn = 1