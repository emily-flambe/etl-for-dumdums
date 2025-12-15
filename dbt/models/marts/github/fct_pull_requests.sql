with pull_requests as (
    select * from {{ ref('stg_github__pull_requests') }}
),

users as (
    select * from {{ ref('stg_github__users') }}
),

reviews as (
    select * from {{ ref('stg_github__pr_reviews') }}
),

comments as (
    select * from {{ ref('stg_github__pr_comments') }}
),

-- Aggregate review stats per PR
review_stats as (
    select
        pull_request_id,
        count(*) as review_count,
        countif(review_state = 'APPROVED') as approval_count,
        countif(review_state = 'CHANGES_REQUESTED') as changes_requested_count,
        min(submitted_at) as first_review_at
    from reviews
    group by 1
),

-- Aggregate comment stats per PR
comment_stats as (
    select
        pull_request_id,
        count(*) as comment_count
    from comments
    group by 1
),

final as (
    select
        -- PR attributes
        pr.pull_request_id,
        pr.pr_number,
        pr.repo,
        pr.title,
        pr.state,
        pr.is_merged,
        pr.is_draft,
        pr.created_at,
        pr.updated_at,
        pr.merged_at,
        pr.closed_at,
        pr.ready_for_review_at,
        pr.additions,
        pr.deletions,
        pr.changed_files,

        -- Author attributes (denormalized)
        pr.author_id,
        u.username as author_username,
        u.name as author_name,
        u.email as author_email,

        -- Review stats
        coalesce(rs.review_count, 0) as review_count,
        coalesce(rs.approval_count, 0) as approval_count,
        coalesce(rs.changes_requested_count, 0) as changes_requested_count,
        rs.first_review_at,

        -- Comment stats
        coalesce(cs.comment_count, 0) as comment_count,

        -- Derived metrics (measured from ready_for_review_at, not created_at)
        -- This avoids penalizing PRs that were in draft state for a long time
        case
            when pr.is_merged then
                timestamp_diff(pr.merged_at, pr.ready_for_review_at, hour)
            else null
        end as cycle_time_hours,

        case
            when rs.first_review_at is not null then
                timestamp_diff(rs.first_review_at, pr.ready_for_review_at, hour)
            else null
        end as time_to_first_review_hours,

        pr.additions + pr.deletions as total_lines_changed,

        case
            when pr.state = 'open' then 'open'
            when pr.is_merged then 'merged'
            else 'closed_without_merge'
        end as pr_outcome

    from pull_requests pr
    left join users u on pr.author_id = u.user_id
    left join review_stats rs on pr.pull_request_id = rs.pull_request_id
    left join comment_stats cs on pr.pull_request_id = cs.pull_request_id
)

select * from final
