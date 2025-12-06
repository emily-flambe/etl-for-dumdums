with source as (
    select * from {{ source('linear', 'issues') }}
),

staged as (
    select
        id as issue_id,
        identifier,
        title,
        state,
        priority,
        assignee_id,
        cycle_id,
        project_name,
        labels,
        created_at,
        updated_at
    from source
)

select * from staged
