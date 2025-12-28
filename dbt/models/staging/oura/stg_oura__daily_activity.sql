with source as (
    select * from {{ source('oura', 'raw_daily_activity') }}
),

-- Oura API can return multiple records per day (timezone edge cases)
-- Keep the record with the most steps (most complete activity data)
deduplicated as (
    select *,
        row_number() over (partition by day order by steps desc, id desc) as rn
    from source
),

staged as (
    select
        id as activity_id,
        day,
        score,
        active_calories,
        total_calories,
        steps,
        equivalent_walking_distance,
        cast(high_activity_time / 60 as int64) as high_activity_time_minutes,
        cast(medium_activity_time / 60 as int64) as medium_activity_time_minutes,
        cast(low_activity_time / 60 as int64) as low_activity_time_minutes,
        cast(sedentary_time / 60 as int64) as sedentary_time_minutes,
        cast(resting_time / 60 as int64) as resting_time_minutes,
        contributor_meet_daily_targets,
        contributor_move_every_hour,
        contributor_recovery_time,
        contributor_stay_active,
        contributor_training_frequency,
        contributor_training_volume
    from deduplicated
    where rn = 1
)

select * from staged
