WITH source AS (
    SELECT * FROM {{ source('raw', 'subscription_events') }}
),

renamed AS (
    SELECT
        raw:event_id::VARCHAR                                               AS event_id,
        raw:user_id::VARCHAR                                                AS user_id,
        raw:event_type::VARCHAR                                             AS event_type,
        COALESCE(raw:plan::VARCHAR, 'Unknown')                              AS plan_raw,

        -- standardize plan names with messy data cleaning logic
        CASE
            WHEN UPPER(TRIM(raw:plan::VARCHAR)) IN ('STARTER', 'FREE')
                THEN 'Starter'
            WHEN UPPER(TRIM(raw:plan::VARCHAR)) IN ('GROWTH', 'GROWHT', 'GROWTH_PLAN', 'PRO', 'PRO PLUS')
                THEN 'Growth'
            WHEN UPPER(TRIM(raw:plan::VARCHAR)) IN ('ENTERPRISE', 'ENTERPRISE_V2', 'BUSINESS')
                THEN 'Enterprise'
            WHEN UPPER(TRIM(raw:plan::VARCHAR)) IN ('TRIAL', 'EXPIRED', 'CANCELED')
                THEN NULL
            ELSE 'Unknown'
        END                                                                 AS plan_cleaned,

        -- standardize subscription status with messy data cleaning logic
        CASE
            WHEN UPPER(TRIM(raw:plan::VARCHAR)) IN ('Expired', 'Canceled')
                THEN 'Inactive'
            ELSE 'Active'
        END                                                                 AS subscription_status,

        raw:country::VARCHAR                                                AS country,
        raw:batch_month::VARCHAR                                            AS batch_month,

        TO_TIMESTAMP_NTZ(raw:event_timestamp_utc::NUMBER   / 1000)          AS event_timestamp_utc,
        TO_TIMESTAMP_NTZ(raw:event_timestamp_local::NUMBER / 1000)          AS event_timestamp_local,
        TO_TIMESTAMP_NTZ(raw:event_timestamp_utc::NUMBER   / 1000)::DATE    AS event_date,

        -- standardize referral codes with messy data cleaning logic
        CASE
            WHEN raw:referral_code::VARCHAR IN ('N/A', '', 'ORGANIC', NULL)
                THEN NULL
            ELSE UPPER(TRIM(raw:referral_code::VARCHAR))
        END                                                                 AS referral_code_cleaned,

        -- Late arriving flag
        TO_CHAR(TO_TIMESTAMP_NTZ(raw:event_timestamp_utc::NUMBER / 1000), 'YYYY-MM') != raw:batch_month::VARCHAR AS is_late_arriving,

        ROW_NUMBER() OVER (
            PARTITION BY raw:event_id::VARCHAR
            ORDER BY _stg_loaded_at DESC
        ) AS rn

    FROM source
)

SELECT * FROM renamed
WHERE rn = 1