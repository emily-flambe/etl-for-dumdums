with linear_users as (
    select * from {{ ref('stg_linear__users') }}
),

github_users as (
    select * from {{ ref('stg_github__users') }}
),

-- Join Linear and GitHub users by email
-- Use Linear as the primary source, enrich with GitHub data
combined as (
    select
        -- Use Linear user_id as primary key when available
        coalesce(l.user_id, 'gh_' || g.user_id) as user_id,

        -- Email is the cross-platform join key
        coalesce(l.email, g.email) as email,

        -- Linear fields
        l.user_id as linear_user_id,
        l.display_name as linear_display_name,
        l.name as linear_name,
        l.is_active as linear_is_active,

        -- GitHub fields
        g.user_id as github_user_id,
        g.username as github_username,
        g.name as github_name,
        g.avatar_url as github_avatar_url,

        -- Unified display name (prefer Linear, fall back to GitHub)
        coalesce(l.display_name, l.name, g.name, g.username) as display_name,

        -- Source tracking
        case
            when l.user_id is not null and g.user_id is not null then 'both'
            when l.user_id is not null then 'linear'
            else 'github'
        end as source

    from linear_users l
    full outer join github_users g on lower(l.email) = lower(g.email)
)

select * from combined
