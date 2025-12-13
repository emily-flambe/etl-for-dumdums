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

vendor_sales as (
    select
        vendor_name,

        -- Sales metrics
        round(sum(sale_dollars), 2) as total_sales,
        sum(bottles_sold) as total_bottles,
        round(sum(volume_liters), 2) as total_volume_liters,

        -- Product/store coverage
        count(distinct item_id) as product_count,
        count(distinct store_id) as store_count,

        -- Averages
        round(avg(bottle_retail), 2) as avg_bottle_price

    from sales
    where vendor_name is not null
    group by vendor_name
),

-- Get top product per vendor
product_by_vendor as (
    select
        vendor_name,
        item_name,
        sum(sale_dollars) as product_sales,
        row_number() over (partition by vendor_name order by sum(sale_dollars) desc) as rank
    from sales
    where vendor_name is not null and item_name is not null
    group by vendor_name, item_name
),

top_products as (
    select vendor_name, item_name as top_product
    from product_by_vendor
    where rank = 1
)

select
    vs.*,
    tp.top_product
from vendor_sales vs
left join top_products tp on vs.vendor_name = tp.vendor_name
order by total_sales desc
