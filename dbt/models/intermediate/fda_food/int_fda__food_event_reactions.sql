{{
    config(
        materialized='table',
        schema='fda_food',
        tags=['fda_food']
    )
}}

-- Categorizes FDA food adverse events by reaction type using regex pattern matching
-- Each event can have multiple reaction categories (stored as array)
-- Reactions field contains comma-separated symptoms like "DIARRHOEA, VOMITING, NAUSEA"

with events as (
    select * from {{ ref('stg_fda__food_events') }}
),

-- Apply regex patterns to categorize reactions
with_categories as (
    select
        report_number,
        reactions,
        outcomes,
        brand_name,
        industry_name,
        product_role,
        report_date,
        event_date,
        event_year,
        event_month_start,
        gender,
        age,
        age_unit,

        -- Gastrointestinal reactions
        regexp_contains(lower(reactions), r'diarrhoea|diarrhea') as is_diarrhea,
        regexp_contains(lower(reactions), r'\bvomiting\b') as is_vomiting,
        regexp_contains(lower(reactions), r'\bnausea\b') as is_nausea,
        regexp_contains(lower(reactions), r'abdominal\s*pain|stomach\s*pain') as is_abdominal_pain,
        regexp_contains(lower(reactions), r'dyspepsia|indigestion') as is_dyspepsia,
        regexp_contains(lower(reactions), r'abdominal\s*distension|bloating') as is_bloating,
        regexp_contains(lower(reactions), r'\bconstipation\b') as is_constipation,

        -- Allergic/Immune reactions
        regexp_contains(lower(reactions), r'hypersensitivity|allergic\s*reaction') as is_hypersensitivity,
        regexp_contains(lower(reactions), r'\bpruritus\b|itching') as is_itching,
        regexp_contains(lower(reactions), r'\brash\b') as is_rash,
        regexp_contains(lower(reactions), r'\burticaria\b|hives') as is_hives,
        regexp_contains(lower(reactions), r'anaphyla|anaphylactic') as is_anaphylaxis,
        regexp_contains(lower(reactions), r'\bswelling\b|oedema|edema|angioedema') as is_swelling,

        -- Respiratory reactions
        regexp_contains(lower(reactions), r'dyspnoea|dyspnea|shortness\s*of\s*breath|breathing\s*difficult') as is_breathing_difficulty,
        regexp_contains(lower(reactions), r'\bchoking\b') as is_choking,
        regexp_contains(lower(reactions), r'\bdysphagia\b|difficulty\s*swallowing') as is_swallowing_difficulty,
        regexp_contains(lower(reactions), r'\basthma\b|bronchospasm') as is_asthma,
        regexp_contains(lower(reactions), r'\bcough\b|wheezing') as is_cough,

        -- Cardiovascular reactions
        regexp_contains(lower(reactions), r'blood\s*pressure\s*(increased|elevated|high)') as is_high_bp,
        regexp_contains(lower(reactions), r'heart\s*rate\s*(increased|elevated)|tachycardia') as is_high_hr,
        regexp_contains(lower(reactions), r'chest\s*pain') as is_chest_pain,
        regexp_contains(lower(reactions), r'\bpalpitations\b') as is_palpitations,
        regexp_contains(lower(reactions), r'arrhythmia|irregular\s*heart') as is_arrhythmia,

        -- Neurological reactions
        regexp_contains(lower(reactions), r'\bheadache\b') as is_headache,
        regexp_contains(lower(reactions), r'\bdizziness\b|vertigo') as is_dizziness,
        regexp_contains(lower(reactions), r'loss\s*of\s*consciousness|syncope|faint') as is_unconsciousness,
        regexp_contains(lower(reactions), r'\btremor\b') as is_tremor,
        regexp_contains(lower(reactions), r'paraesthesia|paresthesia|tingling|numbness') as is_tingling,
        regexp_contains(lower(reactions), r'\bseizure\b|convulsion') as is_seizure,

        -- Systemic reactions
        regexp_contains(lower(reactions), r'\bmalaise\b') as is_malaise,
        regexp_contains(lower(reactions), r'\bfatigue\b|tiredness') as is_fatigue,
        regexp_contains(lower(reactions), r'\basthenia\b|weakness') as is_weakness,
        regexp_contains(lower(reactions), r'\bpyrexia\b|\bfever\b') as is_fever,
        regexp_contains(lower(reactions), r'\bchills\b') as is_chills,
        regexp_contains(lower(reactions), r'\bdehydration\b') as is_dehydration

    from events
    where reactions is not null
),

-- Build category arrays and rollup flags
with_arrays as (
    select
        *,
        -- Build array of matched categories
        array(
            select category from unnest([
                -- Gastrointestinal
                if(is_diarrhea, 'Diarrhea', null),
                if(is_vomiting, 'Vomiting', null),
                if(is_nausea, 'Nausea', null),
                if(is_abdominal_pain, 'Abdominal Pain', null),
                if(is_dyspepsia, 'Dyspepsia', null),
                if(is_bloating, 'Bloating', null),
                if(is_constipation, 'Constipation', null),
                -- Allergic
                if(is_hypersensitivity, 'Hypersensitivity', null),
                if(is_itching, 'Itching', null),
                if(is_rash, 'Rash', null),
                if(is_hives, 'Hives', null),
                if(is_anaphylaxis, 'Anaphylaxis', null),
                if(is_swelling, 'Swelling', null),
                -- Respiratory
                if(is_breathing_difficulty, 'Breathing Difficulty', null),
                if(is_choking, 'Choking', null),
                if(is_swallowing_difficulty, 'Swallowing Difficulty', null),
                if(is_asthma, 'Asthma', null),
                if(is_cough, 'Cough', null),
                -- Cardiovascular
                if(is_high_bp, 'High Blood Pressure', null),
                if(is_high_hr, 'High Heart Rate', null),
                if(is_chest_pain, 'Chest Pain', null),
                if(is_palpitations, 'Palpitations', null),
                if(is_arrhythmia, 'Arrhythmia', null),
                -- Neurological
                if(is_headache, 'Headache', null),
                if(is_dizziness, 'Dizziness', null),
                if(is_unconsciousness, 'Loss of Consciousness', null),
                if(is_tremor, 'Tremor', null),
                if(is_tingling, 'Tingling/Numbness', null),
                if(is_seizure, 'Seizure', null),
                -- Systemic
                if(is_malaise, 'Malaise', null),
                if(is_fatigue, 'Fatigue', null),
                if(is_weakness, 'Weakness', null),
                if(is_fever, 'Fever', null),
                if(is_chills, 'Chills', null),
                if(is_dehydration, 'Dehydration', null)
            ]) as category
            where category is not null
        ) as reaction_categories,

        -- Rollup flags for category groups (prevents double-counting in aggregations)
        (is_diarrhea or is_vomiting or is_nausea or is_abdominal_pain or is_dyspepsia or is_bloating or is_constipation) as has_gastrointestinal,
        (is_hypersensitivity or is_itching or is_rash or is_hives or is_anaphylaxis or is_swelling) as has_allergic,
        (is_breathing_difficulty or is_choking or is_swallowing_difficulty or is_asthma or is_cough) as has_respiratory,
        (is_high_bp or is_high_hr or is_chest_pain or is_palpitations or is_arrhythmia) as has_cardiovascular,
        (is_headache or is_dizziness or is_unconsciousness or is_tremor or is_tingling or is_seizure) as has_neurological,
        (is_malaise or is_fatigue or is_weakness or is_fever or is_chills or is_dehydration) as has_systemic

    from with_categories
)

select
    report_number,
    reactions,
    outcomes,
    brand_name,
    industry_name,
    product_role,
    report_date,
    event_date,
    event_year,
    event_month_start,
    gender,
    age,
    age_unit,
    reaction_categories,
    array_length(reaction_categories) as reaction_count,
    -- Rollup flags
    has_gastrointestinal,
    has_allergic,
    has_respiratory,
    has_cardiovascular,
    has_neurological,
    has_systemic,
    -- Individual flags for flexible querying
    is_diarrhea,
    is_vomiting,
    is_nausea,
    is_abdominal_pain,
    is_hypersensitivity,
    is_breathing_difficulty,
    is_choking,
    is_headache,
    is_dizziness,
    is_chest_pain,
    is_anaphylaxis,
    is_seizure

from with_arrays
