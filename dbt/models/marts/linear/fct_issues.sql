with issues as (
    select * from {{ ref('stg_linear__issues') }}
),

users as (
    select * from {{ ref('stg_linear__users') }}
),

cycles as (
    select * from {{ ref('stg_linear__cycles') }}
),

-- Count children for each issue
child_counts as (
    select
        parent_id,
        count(*) as child_count
    from issues
    where parent_id is not null
    group by parent_id
),

final as (
    select
        -- Issue attributes
        i.issue_id,
        i.identifier,
        i.title,
        i.state,
        i.priority,
        i.estimate,
        i.labels,
        i.project_name,
        i.created_at,
        i.updated_at,

        -- User attributes (denormalized for easy querying)
        i.assignee_id,
        u.display_name as assignee_name,
        u.email as assignee_email,

        -- Cycle attributes (denormalized for easy querying)
        i.cycle_id,
        c.cycle_number,
        c.cycle_name,
        c.team_name as cycle_team,
        c.starts_at as cycle_starts_at,
        c.ends_at as cycle_ends_at,

        -- Parent/child relationship
        i.parent_id,
        i.parent_identifier,
        parent.title as parent_title,
        i.parent_id is not null as is_child,
        coalesce(cc.child_count, 0) as child_count,
        coalesce(cc.child_count, 0) > 0 as is_parent,

        -- Derived fields
        case
            when c.cycle_id is not null
                and current_timestamp() between c.starts_at and c.ends_at
            then true
            else false
        end as is_in_active_cycle,

        date_diff(current_date(), date(i.created_at), day) as days_since_created

    from issues i
    left join users u on i.assignee_id = u.user_id
    left join cycles c on i.cycle_id = c.cycle_id
    left join issues parent on i.parent_id = parent.issue_id
    left join child_counts cc on i.issue_id = cc.parent_id
)

select * from final
