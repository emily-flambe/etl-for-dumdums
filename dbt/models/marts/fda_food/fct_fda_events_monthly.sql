{{
    config(
        materialized='table',
        schema='fda_food',
        tags=['fda_food']
    )
}}

-- Monthly trends for FDA food adverse events
-- Tracks event counts and reaction category breakdowns over time

with events as (
    select * from {{ ref('int_fda__food_event_reactions') }}
),

monthly as (
    select
        event_month_start as month,
        event_year as year,
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
        count(distinct case when regexp_contains(outcomes, r'Death') then report_number end) as death_count,

        -- Demographics
        countif(gender = 'Female') as female_count,
        countif(gender = 'Male') as male_count,

        -- Average reactions per event
        round(avg(reaction_count), 2) as avg_reactions_per_event

    from events
    where event_month_start is not null
    group by event_month_start, event_year
)

select
    month,
    year,
    event_count,
    gastrointestinal_count,
    allergic_count,
    respiratory_count,
    cardiovascular_count,
    neurological_count,
    systemic_count,
    other_count,
    hospitalization_count,
    death_count,
    female_count,
    male_count,
    avg_reactions_per_event,
    round(hospitalization_count * 100.0 / nullif(event_count, 0), 1) as hospitalization_pct
from monthly
order by month desc
