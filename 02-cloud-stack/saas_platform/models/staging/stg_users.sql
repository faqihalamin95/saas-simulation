WITH source AS (
    SELECT * FROM {{ source('raw', 'users') }}
),

renamed AS (
    SELECT
        raw:user_id::VARCHAR                                    AS user_id,
        raw:name::VARCHAR                                       AS name,
        raw:email::VARCHAR                                      AS email,
        raw:country::VARCHAR                                    AS country,
        raw:timezone::VARCHAR                                   AS timezone,
        raw:acquisition_channel::VARCHAR                        AS acquisition_channel,
        raw:current_plan::VARCHAR                               AS current_plan,
        raw:current_status::VARCHAR                             AS current_status,

        TO_TIMESTAMP_NTZ(raw:created_at_utc::NUMBER / 1000)     AS created_at_utc,

        ROW_NUMBER() OVER (
            PARTITION BY raw:user_id::VARCHAR
            ORDER BY _stg_loaded_at DESC
        ) AS rn

    FROM source
)

SELECT * FROM renamed
WHERE rn = 1