with source as (
    select * from {{ source('hacker_news', 'raw_stories') }}
    where title is not null
),

staged as (
    select
        id as story_id,
        title,
        url,
        domain,
        author,
        score,
        descendants as comment_count,
        posted_at,
        date(posted_at) as posted_date,
        posted_week
    from source
)

select * from staged
