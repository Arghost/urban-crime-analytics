{{config (
    schema='CORE',
    materialized='table',
    tags = ['core', 'chicago', 'location']
)}}
-- dim_location:
-- One row per geographic area: district + community_area + ward,
-- with centroid coordinates and simple metadata.

with source AS (
    SELECT
        police_district,
        community_area,
        ward,
        -- Representative description for the area
        MIN(location_description)                   AS location_description,
        -- Centroid approximation for mapping
        AVG(latitude)                               AS avg_latitude,
        AVG(longitude)                              AS avg_longitude,
        COUNT(*)                                    AS total_incidents,
        MIN(incident_date)                          AS first_seen_date,
        MAX(incident_date)                          AS last_seen_date
    FROM {{ ref('stg_crime_chicago') }}
    WHERE police_district IS NOT NULL
      AND community_area  IS NOT NULL
      AND ward            IS NOT NULL
    GROUP BY
        police_district,
        community_area,
        ward
),
final AS (
    SELECT
        -- Surrogate key: hash of the 3 geo identifiers
        HEX_ENCODE(
          MD5(
            COALESCE(TO_CHAR(police_district), '') || '|' ||
            COALESCE(TO_CHAR(community_area), '')  || '|' ||
            COALESCE(TO_CHAR(ward), '')
          )
        ) AS location_key,
        police_district,
        community_area,
        ward,
        location_description,
        avg_latitude,
        avg_longitude,
        total_incidents,
        first_seen_date,
        last_seen_date
    FROM source
)

SELECT *
FROM final