{{
    config(
        materialized='view',
        schema='fda_food',
        tags=['fda_food']
    )
}}

with source as (
    select * from {{ source('fda_food', 'raw_recalls') }}
),

-- US state codes for filtering (excludes Canadian provinces, etc.)
us_states as (
    select state_code from unnest([
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
        'DC', 'PR', 'VI', 'GU', 'AS', 'MP'  -- Include US territories
    ]) as state_code
),

staged as (
    select
        recall_number,
        event_id,
        classification,
        -- Parse classification to severity level for easier analysis
        case classification
            when 'Class I' then 1
            when 'Class II' then 2
            when 'Class III' then 3
            else null
        end as classification_severity,
        status,
        voluntary_mandated,
        recalling_firm,
        city,
        state as state_code,
        country,
        postal_code,
        reason_for_recall,
        product_description,
        product_quantity,
        distribution_pattern,
        recall_initiation_date,
        center_classification_date,
        report_date,
        termination_date,
        -- Derived fields
        date_trunc(recall_initiation_date, month) as recall_month,
        date_trunc(recall_initiation_date, week(monday)) as recall_week
    from source
    where country = 'United States'
      and state in (select state_code from us_states)
)

select * from staged
where recall_number is not null
  and recall_number != 'nan'
