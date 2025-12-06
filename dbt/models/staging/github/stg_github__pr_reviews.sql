with source as (
    select * from {{ source('github', 'raw_pr_reviews') }}
),

staged as (
    select
        id as review_id,
        pull_request_id,
        repo,
        author_id as reviewer_id,
        state as review_state,
        submitted_at,
        body as review_body
    from source
)

select * from staged
