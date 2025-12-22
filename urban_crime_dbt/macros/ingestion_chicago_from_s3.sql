{% macro load_chicago_from_s3 ()%}
    {# 
      Load all Chicago crime CSV files from the external stage
      into the RAW table. Snowflake will not re-load already
    #}

    {% set copy_sql %}
        COPY INTO {{ target.database }}.RAW.CRIME_EVENTS_CHICAGO_RAW
        FROM @{{ target.database }}.RAW.STG_CHICAGO_CRIME_S3
        FILE_FORMAT = (FORMAT_NAME = '{{ target.database }}.RAW.CHICAGO_CSV_FORMAT')
        PATTERN = '.*chicago_crime_.*\\.csv'
        ON_ERROR = 'ABORT_STATEMENT';
    {% endset %}

    {{ log("Loading CRIME_EVENTS_CHICAGO_RAW from S3 via COPY INTO ...", info=True) }}
    {{ log(copy_sql, info=True) }}

    {% do run_query(copy_sql) %}
{%endmacro%}