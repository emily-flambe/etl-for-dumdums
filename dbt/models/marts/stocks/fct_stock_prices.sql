with prices as (
    select * from {{ ref('stg_stocks__prices') }}
),

with_calculations as (
    select
        price_id,
        ticker,
        sector,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        adj_close_price,
        volume,

        -- Daily price range
        high_price - low_price as daily_range,

        -- Daily change (close vs open)
        close_price - open_price as daily_change,

        -- Daily percent change
        round(
            safe_divide(close_price - open_price, open_price) * 100,
            2
        ) as daily_change_pct,

        -- Previous day's close for calculating overnight gap
        lag(close_price) over (
            partition by ticker
            order by trade_date
        ) as prev_close,

        -- 7-day moving average of close price
        round(
            avg(close_price) over (
                partition by ticker
                order by trade_date
                rows between 6 preceding and current row
            ),
            2
        ) as close_7d_ma,

        -- 30-day moving average of close price
        round(
            avg(close_price) over (
                partition by ticker
                order by trade_date
                rows between 29 preceding and current row
            ),
            2
        ) as close_30d_ma,

        -- 7-day average volume
        round(
            avg(volume) over (
                partition by ticker
                order by trade_date
                rows between 6 preceding and current row
            ),
            0
        ) as volume_7d_avg,

        -- 52-week high (rolling)
        max(high_price) over (
            partition by ticker
            order by trade_date
            rows between 251 preceding and current row
        ) as high_52w,

        -- 52-week low (rolling)
        min(low_price) over (
            partition by ticker
            order by trade_date
            rows between 251 preceding and current row
        ) as low_52w,

        -- Recency rank (1 = most recent)
        row_number() over (
            partition by ticker
            order by trade_date desc
        ) as recency_rank,

        fetched_at

    from prices
),

final as (
    select
        *,

        -- Overnight gap (open vs previous close)
        open_price - prev_close as overnight_gap,

        round(
            safe_divide(open_price - prev_close, prev_close) * 100,
            2
        ) as overnight_gap_pct,

        -- Day-over-day change (close vs prev close)
        close_price - prev_close as close_change,

        round(
            safe_divide(close_price - prev_close, prev_close) * 100,
            2
        ) as close_change_pct,

        -- Position in 52-week range (0 = at low, 100 = at high)
        round(
            safe_divide(close_price - low_52w, high_52w - low_52w) * 100,
            1
        ) as position_in_52w_range,

        -- Golden cross / death cross signals
        case
            when close_7d_ma > close_30d_ma then 'above_30d_ma'
            when close_7d_ma < close_30d_ma then 'below_30d_ma'
            else 'at_30d_ma'
        end as ma_trend,

        -- Volume trend vs 7-day average
        case
            when volume > volume_7d_avg * 1.5 then 'high_volume'
            when volume < volume_7d_avg * 0.5 then 'low_volume'
            else 'normal_volume'
        end as volume_trend

    from with_calculations
)

select * from final
order by trade_date desc, ticker
