{{ config(
    schema = 'CORE',
    materialized = 'table',
    tags = ['core', 'date', 'calendar']
) }}

-- dim_date:
-- One row per calendar date, covering the full range of incident_date
-- found in stg_crime_chicago.

WITH bounds AS (
    SELECT
        MIN(incident_date) AS min_date,
        MAX(incident_date) AS max_date
    FROM {{ ref('stg_crime_chicago') }}
),

span AS (
    SELECT
        min_date,
        max_date,
        DATEDIFF('day', min_date, max_date) AS num_days
    FROM bounds
),

dates AS (
    SELECT
        DATEADD(
            'day',
            SEQ4(),
            (SELECT min_date FROM span)
        ) AS date_day
    FROM TABLE(
        GENERATOR(
            ROWCOUNT => (SELECT num_days + 1 FROM span)
        )
    )
),

final AS (
    SELECT
        -- Surrogate key in YYYYMMDD format
        TO_NUMBER(TO_CHAR(date_day, 'YYYYMMDD'))   AS date_key,

        date_day                                   AS date,

        EXTRACT(YEAR  FROM date_day)              AS year,
        EXTRACT(MONTH FROM date_day)              AS month,
        EXTRACT(DAY   FROM date_day)              AS day_of_month,

        TO_CHAR(date_day, 'Month')                AS month_name,
        TO_CHAR(date_day, 'Mon')                  AS month_name_short,

        EXTRACT(QUARTER FROM date_day)            AS quarter,
        TO_CHAR(date_day, 'YYYY-"Q"Q')            AS year_quarter,

        -- ISO week/year for consistent week-based analysis
        TO_CHAR(date_day, 'IYYY')::NUMBER         AS iso_year,
        TO_CHAR(date_day, 'IW')::NUMBER           AS iso_week,

        -- Day of week: 1 = Monday, 7 = Sunday (ISO)
        TO_CHAR(date_day, 'D')::NUMBER            AS day_of_week_us,
        TO_CHAR(date_day, 'DY')                   AS day_name_short,
        TO_CHAR(date_day, 'DAY')                  AS day_name,

        CASE
            WHEN DAYOFWEEK(date_day) IN (1, 7)    THEN TRUE  -- Sunday(1) or Saturday(7)
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