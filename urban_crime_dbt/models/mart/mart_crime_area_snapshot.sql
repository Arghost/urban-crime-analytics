{{config (
    schema = 'MART',
    materialized = 'table',
    tags = ['mart', 'chicago']
)}}
WITH fact AS (
    SELECT
        date_key,
        location_key,
        incident_count,
        was_arrested,
        is_domestic
    FROM {{ ref('fct_crime_events') }}
),

d AS (
    SELECT
        date_key,
        year,
        year_quarter
    FROM {{ ref('dim_date') }}
),

l AS (
    SELECT
        location_key,
        police_district,
        community_area,
        ward
    FROM {{ ref('dim_location') }}
),

joined AS (
    SELECT
        d.year,
        d.year_quarter,
        l.police_district,
        l.community_area,
        l.ward,
        f.incident_count,
        f.was_arrested,
        f.is_domestic
    FROM fact f
    LEFT JOIN d ON f.date_key = d.date_key
    LEFT JOIN l ON f.location_key = l.location_key
),

agg AS (
    SELECT
        year,
        police_district,
        community_area,
        ward,

        COUNT(*)                                        AS total_event_rows,
        SUM(incident_count)                             AS total_incidents,
        SUM(CASE WHEN was_arrested   THEN incident_count ELSE 0 END) AS arrests,
        SUM(CASE WHEN is_domestic THEN incident_count ELSE 0 END) AS domestic_incidents
    FROM joined
    GROUP BY
        year,
        police_district,
        community_area,
        ward
),

final AS (
    SELECT
        year,
        police_district,
        community_area,
        ward,

        total_event_rows,
        total_incidents,
        arrests,
        domestic_incidents,

        CASE
            WHEN total_incidents > 0
                THEN arrests::FLOAT / total_incidents
            ELSE 0.0
        END AS arrest_rate,

        CASE
            WHEN total_incidents > 0
                THEN domestic_incidents::FLOAT / total_incidents
            ELSE 0.0
        END AS domestic_rate
    FROM agg
)

SELECT *
FROM final