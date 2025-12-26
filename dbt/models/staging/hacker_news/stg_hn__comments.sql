with source as (
    select * from {{ source('hacker_news', 'raw_comments') }}
),

-- Deduplicate comments (in case of overlapping syncs)
deduplicated as (
    select
        *,
        row_number() over (partition by id order by posted_at desc) as rn
    from source
),

staged as (
    select
        id as comment_id,
        parent_id,
        story_id,
        author,
        text as comment_text,
        -- Clean HTML entities and tags from comment text
        regexp_replace(
            regexp_replace(text, r'<[^>]+>', ' '),  -- Remove HTML tags
            r'&[a-z]+;', ' '  -- Remove HTML entities like &amp; &lt; etc.
        ) as comment_text_clean,
        posted_at,
        posted_month,
        posted_day,
        -- Sentiment fields (pre-computed via Cloudflare Workers AI during ETL)
        sentiment_score,
        sentiment_label,
        sentiment_category
    from deduplicated
    where rn = 1
)

select * from staged
