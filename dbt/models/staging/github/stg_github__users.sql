with source as (
    select * from {{ source('github', 'raw_users') }}
),

staged as (
    select
        id as user_id,
        login as username,
        email,
        name,
        avatar_url
    from source
)

select * from staged
