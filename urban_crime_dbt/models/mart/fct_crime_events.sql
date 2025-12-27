{{config (
    schema = 'MART',
    materialized = 'table',
    tags = ['mart', 'chicago', 'crime_events']
)}}

-- fct_crime_events
-- Grain: one row per crime incident reported in Chicago.
-- Linked to:
--  - dim_date        (incident date)
--  - dim_location    (district + community_area + ward)
--  - dim_crime_type  (crime category + FBI code)

with src as (
    select
        id,
        incident_date,
        crime_category,
        fbi_code,
        police_district,
        community_area,
        ward,
        location_description,
        latitude,
        longitude,
        was_arrested,
        is_domestic,
        incident_year
    from {{ref ('stg_crime_chicago')}}
),

-- join to date dimension
with_date as (
    select
        s.*,
        d.date_key
    from src as s
    left join {{ref ('dim_date')}} as d
    on s.incident_date::date = d.date
),

-- Join to location dimension
with_location as (
    select 
        wd.*,
        dl.location_key
    from with_date as wd
    left join {{ref ('dim_location')}} as dl
    on wd.police_district = dl.police_district
    and wd.community_area = dl.community_area
    and wd.ward = dl.ward
),
-- Join to crime type dimension
with_crime_type AS (
    SELECT
        wl.*,
        dt.crime_type_key
    FROM with_location AS wl
    LEFT JOIN {{ ref('dim_crime_type') }} AS dt
        ON wl.crime_category = dt.crime_category
       AND wl.fbi_code       = dt.fbi_code
),
final as (
    select 
    -- Surrogate key for the fact row
        -- Surrogate key puramente técnico, garantizado único
        ROW_NUMBER() OVER (
            ORDER BY
                incident_date,
                id,
                crime_category
        ) AS crime_event_key,
        id,
        incident_date,
        incident_year,
        -- foreign keys to dimensions
        date_key,
        location_key,
        crime_type_key,
        -- “degenerate” dimension attributes we queremos cerca del fact
        crime_category,
        fbi_code,
        location_description,
        -- measures / flags
        1 AS incident_count,
        was_arrested,
        is_domestic,
        latitude,
        longitude
    FROM with_crime_type
)

select * from final