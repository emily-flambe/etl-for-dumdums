{{
    config(
        materialized='table',
        schema='fda_food',
        tags=['fda_food']
    )
}}

-- Aggregates FDA food recalls by state for geographic visualization
-- Includes counts by classification severity

with recalls as (
    select * from {{ ref('stg_fda__recalls') }}
),

-- State name mapping for display
state_names as (
    select * from unnest([
        struct('AL' as state_code, 'Alabama' as state_name),
        struct('AK', 'Alaska'),
        struct('AZ', 'Arizona'),
        struct('AR', 'Arkansas'),
        struct('CA', 'California'),
        struct('CO', 'Colorado'),
        struct('CT', 'Connecticut'),
        struct('DE', 'Delaware'),
        struct('FL', 'Florida'),
        struct('GA', 'Georgia'),
        struct('HI', 'Hawaii'),
        struct('ID', 'Idaho'),
        struct('IL', 'Illinois'),
        struct('IN', 'Indiana'),
        struct('IA', 'Iowa'),
        struct('KS', 'Kansas'),
        struct('KY', 'Kentucky'),
        struct('LA', 'Louisiana'),
        struct('ME', 'Maine'),
        struct('MD', 'Maryland'),
        struct('MA', 'Massachusetts'),
        struct('MI', 'Michigan'),
        struct('MN', 'Minnesota'),
        struct('MS', 'Mississippi'),
        struct('MO', 'Missouri'),
        struct('MT', 'Montana'),
        struct('NE', 'Nebraska'),
        struct('NV', 'Nevada'),
        struct('NH', 'New Hampshire'),
        struct('NJ', 'New Jersey'),
        struct('NM', 'New Mexico'),
        struct('NY', 'New York'),
        struct('NC', 'North Carolina'),
        struct('ND', 'North Dakota'),
        struct('OH', 'Ohio'),
        struct('OK', 'Oklahoma'),
        struct('OR', 'Oregon'),
        struct('PA', 'Pennsylvania'),
        struct('RI', 'Rhode Island'),
        struct('SC', 'South Carolina'),
        struct('SD', 'South Dakota'),
        struct('TN', 'Tennessee'),
        struct('TX', 'Texas'),
        struct('UT', 'Utah'),
        struct('VT', 'Vermont'),
        struct('VA', 'Virginia'),
        struct('WA', 'Washington'),
        struct('WV', 'West Virginia'),
        struct('WI', 'Wisconsin'),
        struct('WY', 'Wyoming'),
        struct('DC', 'District of Columbia'),
        struct('PR', 'Puerto Rico'),
        struct('VI', 'Virgin Islands'),
        struct('GU', 'Guam'),
        struct('AS', 'American Samoa'),
        struct('MP', 'Northern Mariana Islands')
    ])
),

-- FIPS codes for Altair/Vega geographic mapping
state_fips as (
    select * from unnest([
        struct('AL' as state_code, 1 as fips_code),
        struct('AK', 2),
        struct('AZ', 4),
        struct('AR', 5),
        struct('CA', 6),
        struct('CO', 8),
        struct('CT', 9),
        struct('DE', 10),
        struct('FL', 12),
        struct('GA', 13),
        struct('HI', 15),
        struct('ID', 16),
        struct('IL', 17),
        struct('IN', 18),
        struct('IA', 19),
        struct('KS', 20),
        struct('KY', 21),
        struct('LA', 22),
        struct('ME', 23),
        struct('MD', 24),
        struct('MA', 25),
        struct('MI', 26),
        struct('MN', 27),
        struct('MS', 28),
        struct('MO', 29),
        struct('MT', 30),
        struct('NE', 31),
        struct('NV', 32),
        struct('NH', 33),
        struct('NJ', 34),
        struct('NM', 35),
        struct('NY', 36),
        struct('NC', 37),
        struct('ND', 38),
        struct('OH', 39),
        struct('OK', 40),
        struct('OR', 41),
        struct('PA', 42),
        struct('RI', 44),
        struct('SC', 45),
        struct('SD', 46),
        struct('TN', 47),
        struct('TX', 48),
        struct('UT', 49),
        struct('VT', 50),
        struct('VA', 51),
        struct('WA', 53),
        struct('WV', 54),
        struct('WI', 55),
        struct('WY', 56),
        struct('DC', 11),
        struct('PR', 72),
        struct('VI', 78),
        struct('GU', 66),
        struct('AS', 60),
        struct('MP', 69)
    ])
),

aggregated as (
    select
        r.state_code,
        sn.state_name,
        sf.fips_code,
        count(*) as total_recalls,
        countif(r.classification = 'Class I') as class_i_recalls,
        countif(r.classification = 'Class II') as class_ii_recalls,
        countif(r.classification = 'Class III') as class_iii_recalls,
        countif(r.status = 'Ongoing') as ongoing_recalls,
        countif(r.status = 'Terminated') as terminated_recalls,
        min(r.recall_initiation_date) as earliest_recall,
        max(r.recall_initiation_date) as latest_recall
    from recalls r
    left join state_names sn on r.state_code = sn.state_code
    left join state_fips sf on r.state_code = sf.state_code
    group by r.state_code, sn.state_name, sf.fips_code
)

select * from aggregated
order by total_recalls desc
