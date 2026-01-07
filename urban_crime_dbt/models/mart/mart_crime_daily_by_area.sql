{{config (
    schema = 'MART',
    materialized = 'table',
    tags = ['mart', 'chicago'],

    pre_hook = [
        "insert into crime_analytics_dev.meta.DBT_MODEL_RUN_AUDIT (model_name, run_started_at, status) values ('mart_crime_daily_by_area', CURRENT_TIMESTAMP(), 'STARTED')"
    ],
    post_hook = [
        "update crime_analytics_dev.meta.DBT_MODEL_RUN_AUDIT set run_finished_at = CURRENT_TIMESTAMP(), status = 'SUCCESS' where model_name = 'mart_crime_daily_by_area' and run_finished_at is null"
    ]

)}}

with fact as (
    select
        crime_event_key,
        date_key,
        location_key,
        crime_type_key,
        incident_count,
        is_domestic,
        was_arrested
    from {{ref ('fct_crime_events')}}
),
d_date as (
    select 
        date_key,
        date,
        year,
        month,
        day_of_month,
        year_quarter
    from {{ref ('dim_date')}}
),
d_location as (
    SELECT
        location_key,
        police_district,
        community_area,
        ward
    FROM {{ ref('dim_location') }}
),
d_crtype as (
    SELECT
        crime_type_key,
        crime_category,
        severity_level,
        is_violent_crime,
        is_property_crime
    FROM {{ ref('dim_crime_type') }}
),
joined as (
    SELECT
        f.crime_event_key,
        f.incident_count,
        f.was_arrested,
        f.is_domestic,

        d_date.date_key,
        d_date.date,
        d_date.year,
        d_date.month,
        d_date.day_of_month,
        d_date.year_quarter,

        d_location.location_key,
        d_location.police_district,
        d_location.community_area,
        d_location.ward,

        d_crtype.crime_type_key,
        d_crtype.crime_category,
        d_crtype.severity_level,
        d_crtype.is_violent_crime,
        d_crtype.is_property_crime
    FROM fact f
    LEFT JOIN d_date ON f.date_key = d_date.date_key
    LEFT JOIN d_location ON f.location_key = d_location.location_key
    LEFT JOIN d_crtype ON f.crime_type_key = d_crtype.crime_type_key
),
aggregated as (
    SELECT
        date_key,
        date,
        year,
        month,
        year_quarter,

        police_district,
        community_area,
        ward,

        crime_type_key,
        crime_category,
        severity_level,

        SUM(incident_count) AS total_incidents,
        SUM(CASE WHEN is_violent_crime   THEN incident_count ELSE 0 END) AS violent_incidents,
        SUM(CASE WHEN is_property_crime  THEN incident_count ELSE 0 END) AS property_incidents,
        SUM(CASE WHEN was_arrested       THEN incident_count ELSE 0 END) AS arrests,
        SUM(CASE WHEN is_domestic        THEN incident_count ELSE 0 END) AS domestic_incidents
    FROM joined
    GROUP BY
        date_key,
        date,
        year,
        month,
        year_quarter,
        police_district,
        community_area,
        ward,
        crime_type_key,
        crime_category,
        severity_level
),

final AS (
    SELECT
        date_key,
        date,
        year,
        month,
        year_quarter,

        police_district,
        community_area,
        ward,

        crime_type_key,
        crime_category,
        severity_level,

        total_incidents,
        violent_incidents,
        property_incidents,
        arrests,
        domestic_incidents,

        -- simple rates (avoid divide-by-zero)
        CASE
            WHEN total_incidents > 0
                THEN violent_incidents::FLOAT / total_incidents
            ELSE 0.0
        END AS violent_incident_rate,

        CASE
            WHEN total_incidents > 0
                THEN arrests::FLOAT / total_incidents
            ELSE 0.0
        END AS arrest_rate
    FROM aggregated
)

SELECT *
FROM final