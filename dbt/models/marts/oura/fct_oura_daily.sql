with sleep as (
    select * from {{ ref('stg_oura__sleep') }}
),

readiness as (
    select * from {{ ref('stg_oura__daily_readiness') }}
),

activity as (
    select * from {{ ref('stg_oura__daily_activity') }}
),

-- Get all unique days from any source
all_days as (
    select distinct day from sleep
    union distinct
    select distinct day from readiness
    union distinct
    select distinct day from activity
),

final as (
    select
        d.day,

        -- Sleep metrics
        s.sleep_id,
        s.sleep_score,
        s.contributor_deep_sleep as sleep_contributor_deep_sleep,
        s.contributor_efficiency as sleep_contributor_efficiency,
        s.contributor_latency as sleep_contributor_latency,
        s.contributor_rem_sleep as sleep_contributor_rem_sleep,
        s.contributor_restfulness as sleep_contributor_restfulness,
        s.contributor_timing as sleep_contributor_timing,
        s.contributor_total_sleep as sleep_contributor_total_sleep,

        -- Readiness metrics
        r.readiness_id,
        r.score as readiness_score,
        r.temperature_deviation,
        r.contributor_hrv_balance as readiness_hrv_balance,
        r.contributor_resting_heart_rate as readiness_resting_hr,
        r.contributor_recovery_index as readiness_recovery_index,

        -- Activity metrics
        a.activity_id,
        a.score as activity_score,
        a.steps,
        a.active_calories,
        a.total_calories,
        a.equivalent_walking_distance as walking_distance_meters,
        a.high_activity_time_minutes,
        a.medium_activity_time_minutes,
        a.low_activity_time_minutes,
        a.sedentary_time_minutes,

        -- Derived metrics
        case
            when s.sleep_score >= 85 then 'excellent'
            when s.sleep_score >= 70 then 'good'
            when s.sleep_score >= 55 then 'fair'
            else 'poor'
        end as sleep_category,

        case
            when r.score >= 85 then 'optimal'
            when r.score >= 70 then 'good'
            when r.score >= 55 then 'fair'
            else 'poor'
        end as readiness_category,

        case
            when a.steps >= 10000 then 'very_active'
            when a.steps >= 7500 then 'active'
            when a.steps >= 5000 then 'moderate'
            else 'sedentary'
        end as activity_category,

        -- Combined wellness score (average of available scores)
        round(
            (coalesce(s.sleep_score, 0) + coalesce(r.score, 0) + coalesce(a.score, 0))
            / nullif(
                (case when s.sleep_score is not null then 1 else 0 end) +
                (case when r.score is not null then 1 else 0 end) +
                (case when a.score is not null then 1 else 0 end),
                0
            ),
            0
        ) as combined_wellness_score

    from all_days d
    left join sleep s on d.day = s.day
    left join readiness r on d.day = r.day
    left join activity a on d.day = a.day
)

select * from final
order by day desc
