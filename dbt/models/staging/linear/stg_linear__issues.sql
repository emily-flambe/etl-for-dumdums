with source as (
    select * from {{ source('linear', 'raw_issues') }}
),

staged as (
    select
        id as issue_id,
        identifier,
        title,
        state,
        priority,
        estimate,
        assignee_id,
        cycle_id,
        project_name,
        labels,
        parent_id,
        parent_identifier,
        created_at,
        updated_at
    from source
)

select * from staged
