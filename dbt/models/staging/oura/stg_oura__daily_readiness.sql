with source as (
    select * from {{ source('oura', 'raw_daily_readiness') }}
),

staged as (
    select
        id as readiness_id,
        day,
        score,
        temperature_deviation,
        contributor_activity_balance,
        contributor_body_temperature,
        contributor_hrv_balance,
        contributor_previous_day_activity,
        contributor_previous_night,
        contributor_recovery_index,
        contributor_resting_heart_rate,
        contributor_sleep_balance
    from source
)

select * from staged
