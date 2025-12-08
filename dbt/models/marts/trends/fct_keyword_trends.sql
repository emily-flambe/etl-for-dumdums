{{
    config(
        materialized='table',
        schema='trends',
        tags=['trends']
    )
}}

with trends as (
    select * from {{ ref('stg_trends__interest_over_time') }}
),

with_metrics as (
    select
        trend_id,
        keyword,
        date,
        interest,
        is_partial,
        geo,
        fetched_at,

        -- Rolling averages
        avg(interest) over (
            partition by keyword, geo
            order by date
            rows between 6 preceding and current row
        ) as interest_7d_avg,

        avg(interest) over (
            partition by keyword, geo
            order by date
            rows between 29 preceding and current row
        ) as interest_30d_avg,

        -- Week over week change
        interest - lag(interest, 7) over (
            partition by keyword, geo
            order by date
        ) as interest_wow_change,

        -- Month over month change
        interest - lag(interest, 30) over (
            partition by keyword, geo
            order by date
        ) as interest_mom_change,

        -- Peak detection (is this a local max in 7-day window?)
        case
            when interest >= max(interest) over (
                partition by keyword, geo
                order by date
                rows between 3 preceding and 3 following
            )
            then true
            else false
        end as is_local_peak,

        -- Rank within keyword (most recent = 1)
        row_number() over (
            partition by keyword, geo
            order by date desc
        ) as recency_rank

    from trends
)

select * from with_metrics
