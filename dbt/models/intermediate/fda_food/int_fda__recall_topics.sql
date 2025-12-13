{{
    config(
        materialized='table',
        schema='fda_food',
        tags=['fda_food']
    )
}}

-- Extracts topic tags from recall reason text using regex pattern matching
-- Each recall can have multiple topics (stored as array)
-- Rollup flags (has_allergen, has_pathogen) prevent double-counting in aggregations

with recalls as (
    select * from {{ ref('stg_fda__recalls') }}
),

-- Apply regex patterns to extract topics
with_topics as (
    select
        recall_number,
        reason_for_recall,
        recall_initiation_date,
        state_code,
        classification,
        recalling_firm,

        -- Pathogen detection
        regexp_contains(lower(reason_for_recall), r'listeria|l\.\s*monocytogenes') as is_listeria,
        regexp_contains(lower(reason_for_recall), r'salmonella') as is_salmonella,
        regexp_contains(lower(reason_for_recall), r'e\.?\s*coli|escherichia') as is_ecoli,
        regexp_contains(lower(reason_for_recall), r'clostridium|botulism|cronobacter|hepatitis|norovirus|cyclospora') as is_other_pathogen,

        -- Allergen detection (looking for undeclared/unlabeled context OR direct allergen contamination)
        regexp_contains(lower(reason_for_recall), r'(undeclared|unlisted|undisclosed|not\s+declar|fail.*declar|without.*list|omitted)[\w\s,]*\b(milk|dairy|cream|butter|cheese|lactose)\b|\bmilk\b.*allergen|allergen.*\bmilk\b') as is_milk,
        regexp_contains(lower(reason_for_recall), r'(undeclared|unlisted|undisclosed|not\s+declar|fail.*declar|without.*list|omitted)[\w\s,]*\beggs?\b|\beggs?\b.*allergen|allergen.*\beggs?\b') as is_eggs,
        regexp_contains(lower(reason_for_recall), r'(undeclared|unlisted|undisclosed|not\s+declar|fail.*declar|without.*list|omitted)[\w\s,]*\bpeanuts?\b|\bpeanuts?\b.*allergen|allergen.*\bpeanuts?\b') as is_peanuts,
        regexp_contains(lower(reason_for_recall), r'(undeclared|unlisted|undisclosed|not\s+declar|fail.*declar|without.*list|omitted)[\w\s,]*\b(almond|walnut|cashew|pecan|pistachio|hazelnut|macadamia|tree\s*nut)|\b(almond|walnut|cashew|pecan|pistachio).*allergen') as is_tree_nuts,
        regexp_contains(lower(reason_for_recall), r'(undeclared|unlisted|undisclosed|not\s+declar|fail.*declar|without.*list|omitted)[\w\s,]*\b(wheat|gluten)\b|\b(wheat|gluten)\b.*allergen|allergen.*\b(wheat|gluten)\b') as is_wheat,
        regexp_contains(lower(reason_for_recall), r'(undeclared|unlisted|undisclosed|not\s+declar|fail.*declar|without.*list|omitted)[\w\s,]*\bsoy(bean)?\b|\bsoy\b.*allergen|allergen.*\bsoy\b') as is_soy,
        regexp_contains(lower(reason_for_recall), r'(undeclared|unlisted|undisclosed|not\s+declar|fail.*declar|without.*list|omitted)[\w\s,]*\b(fish|anchov|cod|salmon|tuna|tilapia)\b') as is_fish,
        regexp_contains(lower(reason_for_recall), r'(undeclared|unlisted|undisclosed|not\s+declar|fail.*declar|without.*list|omitted)[\w\s,]*\b(shellfish|shrimp|crab|lobster|crustacean|crawfish|prawn)\b') as is_shellfish,
        regexp_contains(lower(reason_for_recall), r'(undeclared|unlisted|undisclosed|not\s+declar|fail.*declar|without.*list|omitted)[\w\s,]*\bsesame\b|\bsesame\b.*allergen|allergen.*\bsesame\b') as is_sesame,

        -- Other issues
        regexp_contains(lower(reason_for_recall), r'foreign\s*(material|object|matter|body)|plastic.*(piece|fragment|found|present)|metal.*(piece|fragment|shaving)|glass.*(piece|fragment)|wood.*(particle|chip|piece)') as is_foreign_material,
        regexp_contains(lower(reason_for_recall), r'mislabel|misbranded|incorrect.*label|label.*incorrect|fail.*label|label.*fail|does not (include|declare|list)|not properly.*label') as is_labeling,
        regexp_contains(lower(reason_for_recall), r'temperature\s*(abuse|excursion)|improper.*temperature|cold chain') as is_temperature

    from recalls
),

-- Build topic array and rollup flags
with_arrays as (
    select
        *,
        -- Build array of matched topics
        array(
            select topic from unnest([
                if(is_listeria, 'Listeria', null),
                if(is_salmonella, 'Salmonella', null),
                if(is_ecoli, 'E. coli', null),
                if(is_other_pathogen, 'Other Pathogen', null),
                if(is_milk, 'Milk/Dairy', null),
                if(is_eggs, 'Eggs', null),
                if(is_peanuts, 'Peanuts', null),
                if(is_tree_nuts, 'Tree Nuts', null),
                if(is_wheat, 'Wheat/Gluten', null),
                if(is_soy, 'Soy', null),
                if(is_fish, 'Fish', null),
                if(is_shellfish, 'Shellfish', null),
                if(is_sesame, 'Sesame', null),
                if(is_foreign_material, 'Foreign Material', null),
                if(is_labeling, 'Labeling', null),
                if(is_temperature, 'Temperature', null)
            ]) as topic
            where topic is not null
        ) as topics,

        -- Rollup flags (for aggregation without double-counting)
        (is_listeria or is_salmonella or is_ecoli or is_other_pathogen) as has_pathogen,
        (is_milk or is_eggs or is_peanuts or is_tree_nuts or is_wheat or is_soy or is_fish or is_shellfish or is_sesame) as has_allergen

    from with_topics
)

select
    recall_number,
    reason_for_recall,
    recall_initiation_date,
    state_code,
    classification,
    recalling_firm,
    topics,
    array_length(topics) as topic_count,
    has_pathogen,
    has_allergen,
    -- Individual flags for flexible querying
    is_listeria,
    is_salmonella,
    is_ecoli,
    is_other_pathogen,
    is_milk,
    is_eggs,
    is_peanuts,
    is_tree_nuts,
    is_wheat,
    is_soy,
    is_fish,
    is_shellfish,
    is_sesame,
    is_foreign_material,
    is_labeling,
    is_temperature
from with_arrays
