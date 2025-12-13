{{
    config(
        materialized='table',
        schema='iowa_liquor',
        tags=['iowa_liquor']
    )
}}

with sales as (
    select * from {{ ref('stg_iowa_liquor__sales') }}
),

monthly_category_sales as (
    select
        sale_month,
        category_name,

        -- Sales metrics
        round(sum(sale_dollars), 2) as total_sales,
        sum(bottles_sold) as total_bottles,
        round(sum(volume_liters), 2) as total_volume_liters,
        count(*) as transaction_count,

        -- Averages
        round(avg(bottle_retail), 2) as avg_bottle_price,

        -- Store coverage
        count(distinct store_id) as store_count

    from sales
    where category_name is not null
    group by sale_month, category_name
)

select * from monthly_category_sales
order by sale_month desc, total_sales desc
