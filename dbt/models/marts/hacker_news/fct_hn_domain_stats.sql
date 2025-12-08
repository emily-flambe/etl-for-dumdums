with stories as (
    select * from {{ ref('stg_hn__stories') }}
),

-- Aggregate by week and domain
domain_weekly as (
    select
        posted_week as week,
        domain,
        count(*) as story_count,
        sum(score) as total_score,
        round(avg(score), 1) as avg_score
    from stories
    where posted_week is not null
      and domain is not null
      and domain != ''
    group by posted_week, domain
),

-- Filter to domains with at least 3 stories per week (reduce noise)
filtered as (
    select *
    from domain_weekly
    where story_count >= 3
)

select * from filtered
order by week desc, story_count desc
