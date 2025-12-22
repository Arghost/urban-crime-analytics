{{ config (
    schema = 'STAGE',
    materialized = 'table',
    tags = ['staging', 'chicago'],
    pre_hook = [
        "{{ load_chicago_from_s3() }}"
    ]
)}}

-- model: stg_crime_chicago
-- layer: staging
-- grain: 1 row per crime incident reported in Chicago
-- inputs:
--   - CRIME_ANALYTICS_DEV.RAW.CRIME_EVENTS_CHICAGO_RAW

SELECT
    id,
    case_number,
    TO_TIMESTAMP_NTZ(date)          AS incident_timestamp,
    TO_DATE(date)                   AS incident_date,
    year::NUMBER                    AS incident_year,
    primary_type                    AS crime_category,
    description                     AS crime_description,
    location_description,
    arrest::BOOLEAN                 AS was_arrested,
    domestic::BOOLEAN               AS is_domestic,
    district::NUMBER                AS police_district,
    ward::NUMBER                    AS ward,
    community_area::NUMBER          AS community_area,
    latitude::FLOAT                 AS latitude,
    longitude::FLOAT                AS longitude,
    updated_on::TIMESTAMP_NTZ       AS updated_on,
    beat::NUMBER                    AS beat,
    iucr,
    fbi_code,
    x_coordinate::NUMBER            AS x_coordinate,
    y_coordinate::NUMBER            AS y_coordinate
FROM CRIME_ANALYTICS_DEV.RAW.CRIME_EVENTS_CHICAGO_RAW