with source as (
    select * from {{ source('trends', 'raw_interest_over_time') }}
),

staged as (
    select
        id as trend_id,
        keyword,
        date,
        interest,
        is_partial,
        geo,
        fetched_at
    from source
)

select * from staged
