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

county_sales as (
    select
        county,

        -- Sales metrics
        round(sum(sale_dollars), 2) as total_sales,
        sum(bottles_sold) as total_bottles,
        round(sum(volume_liters), 2) as total_volume_liters,
        count(*) as transaction_count,

        -- Store metrics
        count(distinct store_id) as store_count,

        -- Averages
        round(sum(sale_dollars) / nullif(count(*), 0), 2) as avg_transaction_value

    from sales
    where county is not null
    group by county
),

-- Get top category per county
category_by_county as (
    select
        county,
        category_name,
        sum(sale_dollars) as category_sales,
        row_number() over (partition by county order by sum(sale_dollars) desc) as rank
    from sales
    where county is not null and category_name is not null
    group by county, category_name
),

top_categories as (
    select county, category_name as top_category
    from category_by_county
    where rank = 1
)

select
    cs.*,
    tc.top_category
from county_sales cs
left join top_categories tc on cs.county = tc.county
order by total_sales desc
