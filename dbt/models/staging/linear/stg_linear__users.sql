with source as (
    select * from {{ source('linear', 'users') }}
),

staged as (
    select
        id as user_id,
        email,
        display_name,
        name,
        active as is_active
    from source
)

select * from staged
