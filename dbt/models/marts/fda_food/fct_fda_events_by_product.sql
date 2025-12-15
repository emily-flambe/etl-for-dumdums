{{
    config(
        materialized='table',
        schema='fda_food',
        tags=['fda_food']
    )
}}

-- Aggregates FDA food adverse events by product industry
-- Shows which food categories have the most reported adverse events

with events as (
    select * from {{ ref('int_fda__food_event_reactions') }}
    where upper(product_role) = 'SUSPECT'  -- Only include suspected products
),

-- Unnest reaction categories to count by industry
reaction_counts as (
    select
        industry_name,
        category,
        count(*) as category_count
    from events,
    unnest(reaction_categories) as category
    where industry_name is not null
    group by industry_name, category
),

-- Find top reaction per industry
top_reactions as (
    select
        industry_name,
        category as top_reaction
    from (
        select
            industry_name,
            category,
            row_number() over (partition by industry_name order by category_count desc) as rn
        from reaction_counts
    )
    where rn = 1
),

by_industry as (
    select
        industry_name,
        count(distinct report_number) as event_count,

        -- Reaction category breakdowns
        countif(has_gastrointestinal) as gastrointestinal_count,
        countif(has_allergic) as allergic_count,
        countif(has_respiratory) as respiratory_count,
        countif(has_cardiovascular) as cardiovascular_count,
        countif(has_neurological) as neurological_count,
        countif(has_systemic) as systemic_count,
        countif(has_other) as other_count,

        -- Severity indicators
        count(distinct case when regexp_contains(outcomes, r'Hospitalization') then report_number end) as hospitalization_count,
        count(distinct case when regexp_contains(outcomes, r'Death') then report_number end) as death_count

    from events
    where industry_name is not null
    group by industry_name
)

select
    bi.industry_name,
    bi.event_count,
    bi.gastrointestinal_count,
    bi.allergic_count,
    bi.respiratory_count,
    bi.cardiovascular_count,
    bi.neurological_count,
    bi.systemic_count,
    bi.other_count,
    bi.hospitalization_count,
    bi.death_count,
    round(bi.hospitalization_count * 100.0 / nullif(bi.event_count, 0), 1) as hospitalization_pct,
    tr.top_reaction
from by_industry bi
left join top_reactions tr on bi.industry_name = tr.industry_name
order by bi.event_count desc
