with stories as (
    select * from {{ ref('stg_hn__stories') }}
),

weekly_stats as (
    select
        posted_week as week,
        count(*) as story_count,
        sum(score) as total_score,
        round(avg(score), 1) as avg_score,
        sum(comment_count) as total_comments,
        round(avg(comment_count), 1) as avg_comments,
        count(distinct author) as unique_authors
    from stories
    where posted_week is not null
    group by posted_week
)

select * from weekly_stats
order by week desc
