-- Reviewer activity metrics: response times for reviews and comments per PR
-- Used for measuring review responsiveness by team member

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

-- Get first review timestamp per reviewer per PR
reviewer_first_review as (
    select
        reviewer_id,
        pull_request_id,
        min(submitted_at) as first_review_at,
        count(*) as review_count
    from reviews
    group by 1, 2
),

-- Get first comment timestamp per commenter per PR
commenter_first_comment as (
    select
        author_id as commenter_id,
        pull_request_id,
        min(created_at) as first_comment_at,
        count(*) as comment_count
    from comments
    group by 1, 2
),

-- Union reviewers and commenters to get all participants
all_participants as (
    select distinct
        coalesce(r.reviewer_id, c.commenter_id) as participant_id,
        coalesce(r.pull_request_id, c.pull_request_id) as pull_request_id,
        r.first_review_at,
        r.review_count,
        c.first_comment_at,
        c.comment_count
    from reviewer_first_review r
    full outer join commenter_first_comment c
        on r.reviewer_id = c.commenter_id
        and r.pull_request_id = c.pull_request_id
),

final as (
    select
        -- Participant info
        ap.participant_id as reviewer_id,
        u.username as reviewer_username,
        u.name as reviewer_name,

        -- PR info
        ap.pull_request_id,
        pr.repo as pr_repo,
        pr.pr_number,
        pr.title as pr_title,
        pr.author_id as pr_author_id,
        pr.created_at as pr_created_at,
        pr.ready_for_review_at as pr_ready_for_review_at,
        pr.merged_at as pr_merged_at,
        pr.state as pr_state,

        -- Activity timestamps
        ap.first_review_at,
        ap.first_comment_at,

        -- Activity counts
        coalesce(ap.review_count, 0) as review_count,
        coalesce(ap.comment_count, 0) as comment_count,

        -- Response time metrics (hours) - measured from ready_for_review_at
        -- This avoids penalizing PRs that were in draft state for a long time
        case
            when ap.first_review_at is not null then
                timestamp_diff(ap.first_review_at, pr.ready_for_review_at, hour)
            else null
        end as time_to_first_review_hours,

        case
            when ap.first_comment_at is not null then
                timestamp_diff(ap.first_comment_at, pr.ready_for_review_at, hour)
            else null
        end as time_to_first_comment_hours,

        -- First response (either review or comment, whichever came first)
        least(
            coalesce(ap.first_review_at, ap.first_comment_at),
            coalesce(ap.first_comment_at, ap.first_review_at)
        ) as first_response_at,

        case
            when ap.first_review_at is not null or ap.first_comment_at is not null then
                timestamp_diff(
                    least(
                        coalesce(ap.first_review_at, ap.first_comment_at),
                        coalesce(ap.first_comment_at, ap.first_review_at)
                    ),
                    pr.ready_for_review_at,
                    hour
                )
            else null
        end as time_to_first_response_hours

    from all_participants ap
    inner join pull_requests pr on ap.pull_request_id = pr.pull_request_id
    left join users u on ap.participant_id = u.user_id
    -- Exclude PR authors reviewing their own PRs
    where ap.participant_id != pr.author_id
)

select * from final
