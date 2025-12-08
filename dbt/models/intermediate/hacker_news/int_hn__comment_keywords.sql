{{
    config(
        materialized='table',
        schema='hacker_news',
        tags=['hacker_news']
    )
}}

with comments as (
    select * from {{ ref('stg_hn__comments') }}
),

stories as (
    select * from {{ ref('stg_hn__stories') }}
),

-- Define keywords to track with regex patterns
-- Using word boundaries (\b) to avoid partial matches
-- Subset of keywords from fct_hn_keyword_trends focused on high-interest topics
keywords as (
    select keyword, pattern from unnest([
        -- AI/ML (high interest for sentiment tracking)
        struct('AI' as keyword, r'\bai\b|\bartificial intelligence\b' as pattern),
        struct('LLM', r'\bllm\b|\blarge language model'),
        struct('GPT', r'\bgpt\b'),
        struct('ChatGPT', r'\bchatgpt\b'),
        struct('OpenAI', r'\bopenai\b'),
        struct('Claude', r'\bclaude\b'),
        struct('Anthropic', r'\banthropic\b'),
        -- Languages (popular/controversial)
        struct('Rust', r'\brust\b'),
        struct('Python', r'\bpython\b'),
        struct('JavaScript', r'\bjavascript\b'),
        -- Crypto (historically polarizing)
        struct('Crypto', r'\bcrypto\b|\bcryptocurrency\b'),
        struct('Bitcoin', r'\bbitcoin\b|\bbtc\b'),
        -- Other high-engagement topics
        struct('Open Source', r'\bopen source\b|\bopen-source\b'),
        struct('Remote Work', r'\bremote work\b|\bwork from home\b|\bwfh\b'),
        struct('Startup', r'\bstartup\b')
    ])
),

-- Match comments to keywords via their parent story
comment_keywords as (
    select
        c.comment_id,
        c.story_id,
        c.comment_text_clean,
        c.posted_month,
        k.keyword
    from comments c
    join stories s on c.story_id = s.story_id
    cross join keywords k
    where regexp_contains(lower(s.title), k.pattern)
)

select * from comment_keywords
