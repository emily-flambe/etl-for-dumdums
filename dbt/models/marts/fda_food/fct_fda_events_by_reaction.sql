{{
    config(
        materialized='table',
        schema='fda_food',
        tags=['fda_food']
    )
}}

-- Aggregates FDA food adverse events by reaction category
-- Uses rollup flags to prevent double-counting when events have multiple reactions

with events as (
    select * from {{ ref('int_fda__food_event_reactions') }}
),

-- Unnest reaction categories for per-category aggregation
unnested as (
    select
        report_number,
        event_year,
        event_month_start,
        gender,
        outcomes,
        category
    from events,
    unnest(reaction_categories) as category
),

-- Aggregate by reaction category
by_reaction as (
    select
        category as reaction,
        count(distinct report_number) as event_count,
        count(distinct case when gender = 'Female' then report_number end) as female_count,
        count(distinct case when gender = 'Male' then report_number end) as male_count,
        count(distinct case when regexp_contains(outcomes, r'Hospitalization') then report_number end) as hospitalization_count,
        count(distinct case when regexp_contains(outcomes, r'Death') then report_number end) as death_count,
        min(event_year) as first_year,
        max(event_year) as last_year
    from unnested
    group by category
)

select
    reaction,
    event_count,
    female_count,
    male_count,
    hospitalization_count,
    death_count,
    round(hospitalization_count * 100.0 / nullif(event_count, 0), 1) as hospitalization_pct,
    first_year,
    last_year
from by_reaction
order by event_count desc
