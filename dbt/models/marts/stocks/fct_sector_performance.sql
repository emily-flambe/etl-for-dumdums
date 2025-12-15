with prices as (
    select * from {{ ref('fct_stock_prices') }}
    where recency_rank <= 30  -- Last 30 days per ticker
),

-- Get the most recent day's data for each ticker
latest_prices as (
    select *
    from prices
    where recency_rank = 1
),

-- Calculate sector-level metrics for the latest day
sector_latest as (
    select
        sector,
        trade_date,
        count(distinct ticker) as ticker_count,

        -- Average metrics across sector
        round(avg(close_change_pct), 2) as avg_daily_change_pct,
        round(avg(position_in_52w_range), 1) as avg_52w_position,

        -- Count of gainers vs losers
        countif(close_change_pct > 0) as gainers,
        countif(close_change_pct < 0) as losers,
        countif(close_change_pct = 0) as unchanged,

        -- Volume trends
        countif(volume_trend = 'high_volume') as high_volume_count,
        countif(volume_trend = 'low_volume') as low_volume_count,

        -- MA trends
        countif(ma_trend = 'above_30d_ma') as above_ma_count,
        countif(ma_trend = 'below_30d_ma') as below_ma_count,

        -- Best and worst performers
        max(close_change_pct) as best_performer_pct,
        min(close_change_pct) as worst_performer_pct

    from latest_prices
    where sector is not null
    group by sector, trade_date
),

-- Add ticker names for best/worst performers
final as (
    select
        sl.*,

        best.ticker as best_performer_ticker,
        worst.ticker as worst_performer_ticker,

        -- Sector sentiment indicator
        case
            when sl.gainers > sl.losers and sl.avg_daily_change_pct > 0.5 then 'bullish'
            when sl.losers > sl.gainers and sl.avg_daily_change_pct < -0.5 then 'bearish'
            else 'neutral'
        end as sector_sentiment,

        -- MA health (percentage of stocks above 30d MA)
        round(
            safe_divide(sl.above_ma_count, sl.ticker_count) * 100,
            1
        ) as pct_above_30d_ma

    from sector_latest sl
    left join latest_prices best
        on sl.sector = best.sector
        and sl.best_performer_pct = best.close_change_pct
    left join latest_prices worst
        on sl.sector = worst.sector
        and sl.worst_performer_pct = worst.close_change_pct
)

select * from final
order by avg_daily_change_pct desc
