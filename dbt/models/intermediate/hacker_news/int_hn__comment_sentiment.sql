{{
    config(
        materialized='table',
        schema='hacker_news',
        tags=['hacker_news']
    )
}}

with comment_keywords as (
    select * from {{ ref('int_hn__comment_keywords') }}
),

comments as (
    select * from {{ ref('stg_hn__comments') }}
),

-- Join keyword-matched comments with pre-computed sentiment scores
-- Sentiment was computed during ETL via Cloudflare Workers AI
with_sentiment as (
    select
        ck.comment_id,
        ck.story_id,
        ck.keyword,
        ck.posted_month,
        c.sentiment_score,
        abs(c.sentiment_score) as sentiment_magnitude,  -- Use absolute score as magnitude proxy
        c.sentiment_category
    from comment_keywords ck
    join comments c on ck.comment_id = c.comment_id
    where c.sentiment_score is not null
)

select * from with_sentiment
