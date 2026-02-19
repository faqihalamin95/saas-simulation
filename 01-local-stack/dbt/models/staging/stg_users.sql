WITH source AS (
  SELECT * FROM {{ source('raw', 'users') }}
),

renamed AS (
    SELECT
        user_id,
        name,
        email,
        acquisition_channel,
        country,
        timezone,
        current_status,
        current_plan,
        created_at_utc,
        created_at_utc::DATE AS created_at_date
    FROM source
)

SELECT * FROM renamed