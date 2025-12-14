{{
    config(
        materialized='view',
        schema='fda_food',
        tags=['fda_food']
    )
}}

-- Staged FDA food adverse events with cleaned column names
-- Source: raw_food_events table synced from bigquery-public-data.fda_food.food_events
-- Deduplicated to one row per report_number, prioritizing "Suspect" products

with source as (
    select * from {{ source('fda_food', 'raw_food_events') }}
),

-- Rank products to get one row per report, preferring Suspect over Concomitant
ranked as (
    select
        *,
        row_number() over (
            partition by report_number
            order by
                case when products_role = 'Suspect' then 0 else 1 end,
                products_brand_name
        ) as product_rank
    from source
    where report_number is not null
),

staged as (
    select
        -- Identifiers
        report_number,

        -- Reactions and outcomes (key fields for analysis)
        reactions,
        outcomes,

        -- Product information (first/suspect product only)
        products_brand_name as brand_name,
        products_industry_code as industry_code,
        products_role as product_role,
        -- Clean up cryptic industry names for readability
        case
            when products_industry_name = 'Vit/Min/Prot/Unconv Diet(Human/Animal)'
                then 'Vitamins & Supplements'
            when products_industry_name = 'Cosmetics'
                then 'Cosmetics'
            when products_industry_name = 'Whole Grain/Milled Grain Prod/Starch'
                then 'Grains & Starches'
            when products_industry_name = 'Soft Drink/Water'
                then 'Beverages (Non-Alcoholic)'
            when products_industry_name = 'Fruit/Fruit Prod'
                then 'Fruits & Fruit Products'
            when products_industry_name = 'Vegetable/Vegetable Products'
                then 'Vegetables & Vegetable Products'
            when products_industry_name = 'Fishery/Seafood Prod'
                then 'Seafood'
            when products_industry_name = 'Nut/Edible Seed'
                then 'Nuts & Seeds'
            when products_industry_name = 'Milk/Butter/Dried Milk Prod'
                then 'Dairy Products'
            when products_industry_name = 'Ice Cream Prod'
                then 'Ice Cream & Frozen Desserts'
            when products_industry_name = 'Bakery Prod/Dough/Mix/Icing'
                then 'Bakery Products'
            when products_industry_name = 'Choc/Cocoa Prod'
                then 'Chocolate & Cocoa'
            when products_industry_name = 'Baby Food Prod'
                then 'Baby Food'
            else products_industry_name
        end as industry_name,

        -- Dates
        date_created as report_date,
        date_started as symptom_start_date,
        coalesce(date_started, date_created) as event_date,

        -- Consumer demographics
        consumer_gender as gender,
        consumer_age as age,
        consumer_age_unit as age_unit,

        -- Derived fields
        extract(year from coalesce(date_started, date_created)) as event_year,
        extract(month from coalesce(date_started, date_created)) as event_month,
        date_trunc(coalesce(date_started, date_created), month) as event_month_start

    from ranked
    where product_rank = 1
)

select * from staged
