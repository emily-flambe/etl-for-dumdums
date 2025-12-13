{{
    config(
        materialized='table',
        schema='fda_food',
        tags=['fda_food']
    )
}}

-- Aggregates FDA food recalls by topic for trend analysis and filtering
-- Uses topics extracted via regex from reason_for_recall text
-- Provides both individual topic counts and rollup categories

with recall_topics as (
    select * from {{ ref('int_fda__recall_topics') }}
),

-- Unnest topics array to get one row per recall-topic combination
topics_exploded as (
    select
        recall_number,
        recall_initiation_date,
        state_code,
        classification,
        recalling_firm,
        reason_for_recall,
        topic,
        has_pathogen,
        has_allergen
    from recall_topics,
    unnest(topics) as topic
),

-- Aggregate by topic
by_topic as (
    select
        topic,
        count(distinct recall_number) as recall_count,
        countif(classification = 'Class I') as class_i_count,
        countif(classification = 'Class II') as class_ii_count,
        countif(classification = 'Class III') as class_iii_count,
        count(distinct state_code) as states_affected,
        count(distinct recalling_firm) as firms_affected,
        min(recall_initiation_date) as earliest_recall,
        max(recall_initiation_date) as latest_recall
    from topics_exploded
    group by topic
),

-- Add topic category for grouping in UI
with_category as (
    select
        topic,
        case
            when topic in ('Listeria', 'Salmonella', 'E. coli', 'Other Pathogen') then 'Pathogen'
            when topic in ('Milk/Dairy', 'Eggs', 'Peanuts', 'Tree Nuts', 'Wheat/Gluten', 'Soy', 'Fish', 'Shellfish', 'Sesame') then 'Allergen'
            when topic = 'Foreign Material' then 'Physical'
            when topic = 'Labeling' then 'Labeling'
            when topic = 'Temperature' then 'Process'
            else 'Other'
        end as topic_category,
        recall_count,
        class_i_count,
        class_ii_count,
        class_iii_count,
        states_affected,
        firms_affected,
        earliest_recall,
        latest_recall
    from by_topic
),

-- Calculate rollup totals (using distinct counts from source to avoid double-counting)
rollup_stats as (
    select
        'Pathogen (Any)' as topic,
        'Pathogen Rollup' as topic_category,
        count(distinct recall_number) as recall_count,
        countif(classification = 'Class I') as class_i_count,
        countif(classification = 'Class II') as class_ii_count,
        countif(classification = 'Class III') as class_iii_count,
        count(distinct state_code) as states_affected,
        count(distinct recalling_firm) as firms_affected,
        min(recall_initiation_date) as earliest_recall,
        max(recall_initiation_date) as latest_recall
    from recall_topics
    where has_pathogen = true

    union all

    select
        'Allergen (Any)' as topic,
        'Allergen Rollup' as topic_category,
        count(distinct recall_number) as recall_count,
        countif(classification = 'Class I') as class_i_count,
        countif(classification = 'Class II') as class_ii_count,
        countif(classification = 'Class III') as class_iii_count,
        count(distinct state_code) as states_affected,
        count(distinct recalling_firm) as firms_affected,
        min(recall_initiation_date) as earliest_recall,
        max(recall_initiation_date) as latest_recall
    from recall_topics
    where has_allergen = true
)

-- Combine individual topics with rollup totals
select * from with_category
union all
select * from rollup_stats
order by recall_count desc
