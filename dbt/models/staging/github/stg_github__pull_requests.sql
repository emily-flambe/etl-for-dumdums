with source as (
    select * from {{ source('github', 'raw_pull_requests') }}
),

staged as (
    select
        id as pull_request_id,
        number as pr_number,
        repo,
        title,
        state,
        merged as is_merged,
        author_id,
        created_at,
        updated_at,
        merged_at,
        closed_at,
        additions,
        deletions,
        changed_files
    from source
)

select * from staged
