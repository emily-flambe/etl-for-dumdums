# GitHub Data Source Design

## Overview

Add GitHub ETL source for demexchange organization to sync PR activity data useful for engineering management visibility.

## Scope

**Repositories**:
- demexchange/ddx-data-pipeline
- demexchange/snowflake-queries

**Data Entities**:
- Users (org members)
- Pull Requests
- PR Reviews
- PR Review Comments

## ETL Data Model

### github.raw_users
| Column | Type | Description |
|--------|------|-------------|
| id | STRING (PK) | GitHub user ID |
| login | STRING | GitHub username |
| email | STRING | Email (for Linear join) |
| name | STRING | Display name |
| avatar_url | STRING | Profile image URL |

### github.raw_pull_requests
| Column | Type | Description |
|--------|------|-------------|
| id | STRING (PK) | GitHub PR ID |
| number | INTEGER | PR number (#123) |
| repo | STRING | Repository name |
| title | STRING | PR title |
| state | STRING | open/closed |
| merged | BOOLEAN | Was PR merged |
| author_id | STRING | FK to users |
| created_at | TIMESTAMP | PR opened |
| updated_at | TIMESTAMP | Last activity |
| merged_at | TIMESTAMP | When merged |
| closed_at | TIMESTAMP | When closed |
| additions | INTEGER | Lines added |
| deletions | INTEGER | Lines removed |
| changed_files | INTEGER | Files modified |

### github.raw_pr_reviews
| Column | Type | Description |
|--------|------|-------------|
| id | STRING (PK) | Review ID |
| pull_request_id | STRING | FK to PRs |
| repo | STRING | Repository name |
| author_id | STRING | FK to users |
| state | STRING | APPROVED/CHANGES_REQUESTED/COMMENTED |
| submitted_at | TIMESTAMP | Review submitted |
| body | STRING | Review body text |

### github.raw_pr_comments
| Column | Type | Description |
|--------|------|-------------|
| id | STRING (PK) | Comment ID |
| pull_request_id | STRING | FK to PRs |
| repo | STRING | Repository name |
| author_id | STRING | FK to users |
| created_at | TIMESTAMP | Comment created |
| updated_at | TIMESTAMP | Comment edited |
| path | STRING | File path commented on |
| body | STRING | Comment text |

## dbt Models

### Staging Layer (views)
- `stg_github__users` - Rename id -> user_id
- `stg_github__pull_requests` - Rename id -> pull_request_id
- `stg_github__pr_reviews` - Rename id -> review_id
- `stg_github__pr_comments` - Rename id -> comment_id

### Marts Layer (tables)
- `fct_pull_requests` - PRs enriched with author details, review/comment counts, cycle time
- `dim_users` (updated) - Union of Linear + GitHub users joined by email

## Sync Strategy

1. Fetch org members -> raw_users
2. For each repo:
   - Fetch PRs updated in last N days -> raw_pull_requests
   - For each PR: fetch reviews -> raw_pr_reviews
   - For each PR: fetch review comments -> raw_pr_comments

## Environment Variables

| Variable | Description |
|----------|-------------|
| GITHUB_TOKEN | Personal Access Token with `repo` and `read:org` scopes |

## File Structure

```
sources/
  github.py              # Source classes
scripts/
  sync_github.py         # Entry point
dbt/models/
  staging/github/
    _github__sources.yml
    _github__models.yml
    stg_github__users.sql
    stg_github__pull_requests.sql
    stg_github__pr_reviews.sql
    stg_github__pr_comments.sql
  marts/core/
    fct_pull_requests.sql
    dim_users.sql         # Updated
```
