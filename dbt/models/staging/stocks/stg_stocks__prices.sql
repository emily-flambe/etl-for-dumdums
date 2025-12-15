with source as (
    select * from {{ source('stocks', 'raw_prices') }}
),

staged as (
    select
        id as price_id,
        ticker,
        sector,
        date as trade_date,
        open as open_price,
        high as high_price,
        low as low_price,
        close as close_price,
        adj_close as adj_close_price,
        volume,
        fetched_at
    from source
)

select * from staged
