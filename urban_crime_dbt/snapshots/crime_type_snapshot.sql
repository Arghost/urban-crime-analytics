{% snapshot crime_type_snapshot %}
{{ config (
    target_schema = 'CORE_HISTORY',
    unique_key = 'crime_type_key',
    strategy = 'check',
    check_cols = [
        'crime_description',
        'severity_level',
        'is_violent_crime',
        'is_property_crime'
    ]
)}}
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
  FROM {{ ref('dim_crime_type') }}
{% endsnapshot %}