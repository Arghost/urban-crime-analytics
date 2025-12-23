{{ config(
    schema = 'CORE',
    materialized = 'table',
    tags = ['core', 'date', 'calendar', 'chicago']
) }}

-- dim_date:
-- One row per calendar date, from min(incident_date) to max(incident_date)
-- found in stg_crime_chicago. No extra padding dates.

WITH RECURSIVE
bounds AS (
    SELECT
        MIN(incident_date) AS min_date,
        MAX(incident_date) AS max_date
    FROM {{ ref('stg_crime_chicago') }}
),
dates AS (
    -- Anchor: start at the minimum date
    SELECT
        (SELECT min_date FROM bounds) AS date_day

    UNION ALL

    -- Recursive step: add 1 day until we reach max_date
    SELECT
        DATEADD('day', 1, date_day) AS date_day
    FROM dates, bounds
    WHERE DATEADD('day', 1, date_day) <= (SELECT max_date FROM bounds)
),
final AS (
    SELECT
        -- Surrogate key in YYYYMMDD format (still using TO_CHAR but no numeric casts around 'D', 'IYYY', etc.)
        TO_NUMBER(TO_CHAR(date_day, 'YYYYMMDD'))   AS date_key,

        date_day                                   AS date,

        EXTRACT(YEAR  FROM date_day)              AS year,
        EXTRACT(MONTH FROM date_day)              AS month,
        EXTRACT(DAY   FROM date_day)              AS day_of_month,

        TO_CHAR(date_day, 'Month')                AS month_name,
        TO_CHAR(date_day, 'Mon')                  AS month_name_short,

        EXTRACT(QUARTER FROM date_day)            AS quarter,
        TO_CHAR(date_day, 'YYYY-"Q"Q')            AS year_quarter,

        -- Week/year (simple, non-ISO)
        EXTRACT(YEAR FROM date_day)               AS iso_year,
        EXTRACT(WEEK FROM date_day)               AS iso_week,

        -- Numeric day-of-week directly from Snowflake
        DAYOFWEEK(date_day)                       AS day_of_week_us,

        -- Text labels for readability
        TO_CHAR(date_day, 'DY')                   AS day_name_short,
        TO_CHAR(date_day, 'DAY')                  AS day_name,

        -- Weekend flag: Sunday(1) or Saturday(7)
        CASE
            WHEN DAYOFWEEK(date_day) IN (1, 7) THEN TRUE
            ELSE FALSE
        END                                       AS is_weekend,

        -- Month boundaries
        CASE
            WHEN date_day = DATE_TRUNC('month', date_day)
                THEN TRUE
            ELSE FALSE
        END                                       AS is_month_start,

        CASE
            WHEN date_day = DATEADD(
                'day',
                -1,
                DATEADD('month', 1, DATE_TRUNC('month', date_day))
            )
                THEN TRUE
            ELSE FALSE
        END                                       AS is_month_end

    FROM dates
)

SELECT *
FROM final
ORDER BY date