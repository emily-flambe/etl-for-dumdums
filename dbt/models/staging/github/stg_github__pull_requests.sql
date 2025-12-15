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
        draft as is_draft,
        author_id,
        created_at,
        updated_at,
        merged_at,
        closed_at,
        -- Use ready_for_review_at if available, otherwise fall back to created_at
        coalesce(ready_for_review_at, created_at) as ready_for_review_at,
        additions,
        deletions,
        changed_files
    from source
)

select * from staged
