with source as (
    select * from {{ source('oura', 'raw_sleep_sessions') }}
),

staged as (
    select
        id as sleep_session_id,
        day,
        bedtime_start,
        bedtime_end,
        sleep_type,
        total_sleep_duration_seconds,
        time_in_bed_seconds,
        awake_time_seconds,
        light_sleep_duration_seconds,
        deep_sleep_duration_seconds,
        rem_sleep_duration_seconds,
        latency_seconds,
        efficiency as sleep_efficiency,
        average_heart_rate,
        lowest_heart_rate,
        average_hrv,
        restless_periods,
        average_breath,

        -- Derived metrics (convert seconds to hours)
        round(total_sleep_duration_seconds / 3600.0, 2) as total_sleep_hours,
        round(time_in_bed_seconds / 3600.0, 2) as time_in_bed_hours,
        round(deep_sleep_duration_seconds / 3600.0, 2) as deep_sleep_hours,
        round(rem_sleep_duration_seconds / 3600.0, 2) as rem_sleep_hours,
        round(light_sleep_duration_seconds / 3600.0, 2) as light_sleep_hours
    from source
)

select * from staged
