{{config (
    materialized = 'table',
    schema = 'CORE',
    tags = ['staging', 'chicago', 'dim'],
)}}
with base as (
    select  
        crime_category,
        fbi_code,
        MIN(crime_description)      AS crime_description,
        COUNT(*)                    AS total_incidents,
        MIN(incident_date)          AS first_seen_date,
        MAX(incident_date)          AS last_seen_date
    from {{ref  ('stg_crime_chicago')}}
    group by crime_category, fbi_code
),
classified as (
    select
        HEX_ENCODE (MD5(crime_category || '|' || COALESCE(fbi_code, ''))) AS crime_type_key,
        crime_category,
        crime_description,
        fbi_code,
        total_incidents,
        first_seen_date,
        last_seen_date,
        -- Flag for violent crime types
        CASE
            WHEN UPPER(crime_category) IN (
                'HOMICIDE',
                'BATTERY',
                'ASSAULT',
                'CRIM SEXUAL ASSAULT',
                'ROBBERY',
                'OFFENSE INVOLVING CHILDREN',
                'KIDNAPPING'
            ) THEN TRUE
            ELSE FALSE
        END AS is_violent_crime,
         -- Flag for property crimes
        CASE
            WHEN UPPER(crime_category) IN (
                'BURGLARY',
                'THEFT',
                'MOTOR VEHICLE THEFT',
                'ROBBERY',
                'ARSON',
                'CRIMINAL DAMAGE',
                'DECEPTIVE PRACTICE'
            ) THEN TRUE
            ELSE FALSE
        END AS is_property_crime,
        -- Simple severity band based on business rules
        CASE
            WHEN UPPER(crime_category) IN ('HOMICIDE', 'CRIM SEXUAL ASSAULT', 'KIDNAPPING')
                THEN 'CRITICAL'
            WHEN UPPER(crime_category) IN ('ROBBERY', 'BATTERY', 'ASSAULT', 'ARSON')
                THEN 'HIGH'
            WHEN UPPER(crime_category) IN (
                'BURGLARY',
                'MOTOR VEHICLE THEFT',
                'CRIMINAL DAMAGE',
                'OFFENSE INVOLVING CHILDREN'
            )
                THEN 'MEDIUM'
            ELSE 'LOW'
        END AS severity_level
    from base
)

SELECT
    crime_type_key,
    crime_category,
    crime_description,
    fbi_code,
    total_incidents,
    first_seen_date,
    last_seen_date,
    is_violent_crime,
    is_property_crime,
    severity_level
FROM classified
