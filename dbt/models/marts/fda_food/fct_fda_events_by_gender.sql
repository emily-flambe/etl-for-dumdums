{{
    config(
        materialized='table',
        schema='fda_food',
        tags=['fda_food']
    )
}}

-- Aggregates FDA food adverse events by gender
-- Shows reaction patterns and severity differences across genders

with events as (
    select * from {{ ref('int_fda__food_event_reactions') }}
    where upper(product_role) = 'SUSPECT'  -- Only include suspected products
),

-- Clean and standardize gender values
cleaned as (
    select
        *,
        case
            when upper(gender) in ('F', 'FEMALE') then 'Female'
            when upper(gender) in ('M', 'MALE') then 'Male'
            when gender is null or trim(gender) = '' then 'Not Reported'
            else 'Other'
        end as gender_clean
    from events
),

by_gender as (
    select
        gender_clean as gender,
        count(distinct report_number) as event_count,

        -- Reaction category breakdowns
        countif(has_gastrointestinal) as gastrointestinal_count,
        countif(has_allergic) as allergic_count,
        countif(has_respiratory) as respiratory_count,
        countif(has_cardiovascular) as cardiovascular_count,
        countif(has_neurological) as neurological_count,
        countif(has_systemic) as systemic_count,

        -- Severity indicators
        count(distinct case when regexp_contains(outcomes, r'Hospitalization') then report_number end) as hospitalization_count,
        count(distinct case when regexp_contains(outcomes, r'Death') then report_number end) as death_count

    from cleaned
    group by gender_clean
)

select
    gender,
    event_count,
    gastrointestinal_count,
    allergic_count,
    respiratory_count,
    cardiovascular_count,
    neurological_count,
    systemic_count,
    hospitalization_count,
    death_count,
    round(hospitalization_count * 100.0 / nullif(event_count, 0), 1) as hospitalization_pct,
    round(gastrointestinal_count * 100.0 / nullif(event_count, 0), 1) as gastrointestinal_pct,
    round(allergic_count * 100.0 / nullif(event_count, 0), 1) as allergic_pct,
    round(respiratory_count * 100.0 / nullif(event_count, 0), 1) as respiratory_pct,
    round(cardiovascular_count * 100.0 / nullif(event_count, 0), 1) as cardiovascular_pct,
    round(neurological_count * 100.0 / nullif(event_count, 0), 1) as neurological_pct,
    round(systemic_count * 100.0 / nullif(event_count, 0), 1) as systemic_pct
from by_gender
order by event_count desc
