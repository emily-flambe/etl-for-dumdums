{{
    config(
        materialized='table',
        schema='hacker_news',
        tags=['hacker_news']
    )
}}

with comment_sentiment as (
    select * from {{ ref('int_hn__comment_sentiment') }}
    where posted_day is not null
),

daily_aggregates as (
    select
        keyword,
        posted_day as day,

        -- Counts
        count(*) as comment_count,
        count(distinct story_id) as story_count,

        -- Sentiment scores
        round(avg(sentiment_score), 3) as avg_sentiment,
        round(stddev(sentiment_score), 3) as sentiment_stddev,
        round(avg(sentiment_magnitude), 3) as avg_magnitude,

        -- Sentiment distribution
        round(100.0 * countif(sentiment_category = 'positive') / count(*), 1) as positive_pct,
        round(100.0 * countif(sentiment_category = 'negative') / count(*), 1) as negative_pct,
        round(100.0 * countif(sentiment_category = 'neutral') / count(*), 1) as neutral_pct,

        -- Extremes
        min(sentiment_score) as min_sentiment,
        max(sentiment_score) as max_sentiment

    from comment_sentiment
    group by keyword, posted_day
),

-- Add day-over-day change
with_changes as (
    select
        *,
        avg_sentiment - lag(avg_sentiment) over (
            partition by keyword order by day
        ) as sentiment_dod_change,
        positive_pct - lag(positive_pct) over (
            partition by keyword order by day
        ) as positive_pct_dod_change
    from daily_aggregates
)

select * from with_changes
order by day desc, comment_count desc
