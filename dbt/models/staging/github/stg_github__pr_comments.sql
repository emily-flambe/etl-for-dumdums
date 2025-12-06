with source as (
    select * from {{ source('github', 'raw_pr_comments') }}
),

staged as (
    select
        id as comment_id,
        pull_request_id,
        repo,
        author_id,
        created_at,
        updated_at,
        path as file_path,
        body as comment_body
    from source
)

select * from staged
