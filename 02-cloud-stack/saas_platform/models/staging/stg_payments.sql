WITH source AS (
    SELECT * FROM {{ source('raw', 'payments') }}
),

renamed AS (
    SELECT
        raw:payment_id::VARCHAR                                                 AS payment_id,
        raw:user_id::VARCHAR                                                    AS user_id,
        raw:status::VARCHAR                                                     AS status,
        raw:attempt_number::NUMBER                                              AS attempt_number,
        raw:batch_month::VARCHAR                                                AS batch_month,

        -- TRY_TO_NUMBER: handles chaos where amount_usd is cast to string
        TRY_TO_NUMBER(raw:amount_usd::VARCHAR, 10, 2)                           AS amount_usd,

        TO_TIMESTAMP_NTZ(raw:payment_timestamp_utc::NUMBER   / 1000)            AS payment_timestamp_utc,
        TO_TIMESTAMP_NTZ(raw:payment_timestamp_local::NUMBER / 1000)            AS payment_timestamp_local,
        TO_TIMESTAMP_NTZ(raw:payment_timestamp_utc::NUMBER   / 1000)::DATE      AS payment_date,

        -- Late arriving flag
        TO_CHAR(TO_TIMESTAMP_NTZ(raw:payment_timestamp_utc::NUMBER / 1000), 'YYYY-MM') != raw:batch_month::VARCHAR AS is_late_arriving,

        ROW_NUMBER() OVER (
            PARTITION BY raw:payment_id::VARCHAR
            ORDER BY _stg_loaded_at DESC
        ) AS rn

    FROM source
)

SELECT * FROM renamed
WHERE rn = 1