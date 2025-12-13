with source as (
    select * from {{ source('iowa_liquor', 'raw_sales') }}
),

staged as (
    select
        -- Identifiers
        invoice_and_item_number as sale_id,
        store_number as store_id,
        category as category_id,
        vendor_number as vendor_id,
        item_number as item_id,

        -- Store attributes
        store_name,
        address as store_address,
        city as store_city,
        zip_code as store_zip,
        county,

        -- Product attributes
        category_name,
        vendor_name,
        item_description as item_name,
        pack as pack_size,
        bottle_volume_ml,

        -- Pricing
        state_bottle_cost as bottle_cost,
        state_bottle_retail as bottle_retail,

        -- Transaction metrics
        bottles_sold,
        sale_dollars,
        volume_sold_liters as volume_liters,
        volume_sold_gallons as volume_gallons,

        -- Dates
        date as sale_date,
        sale_month,
        sale_year

    from source
)

select * from staged
