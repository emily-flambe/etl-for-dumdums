with source as (
    select * from {{ source('linear', 'raw_cycles') }}
),

staged as (
    select
        id as cycle_id,
        number as cycle_number,
        name as cycle_name,
        team_name,
        starts_at,
        ends_at
    from source
)

select * from staged
