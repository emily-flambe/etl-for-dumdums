with linear_users as (
    select * from {{ ref('stg_linear__users') }}
),

final as (
    select
        user_id,
        email,
        display_name,
        name,
        is_active,

        -- Placeholder for future cross-platform fields
        -- These will be populated when we add GitHub users
        cast(null as string) as github_username,
        'linear' as primary_source

    from linear_users
)

select * from final
