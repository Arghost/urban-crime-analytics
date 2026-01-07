# Urban Crime Analytics – Snowflake + dbt (Chicago)

End-to-end data pipeline and analytics model for Chicago crime data using **AWS + Snowflake + dbt**.

The goal of this project is to showcase solid data engineering and analytics architecture:

- Automated ingestion from a public API into **S3**.
- Loading into **Snowflake** with a medallion-style layout (RAW → STAGE → CORE → MART).
- **dbt** for transformations, tests, snapshots and hooks.
- Marts ready for BI tools (Snowsight / Tableau / Looker / Power BI) and future ML use cases.

---

## 1. High-level architecture

**Ingestion**

- Python / AWS Lambda pulls crime data from the **Chicago open data API**.
- Data is written as CSV into an S3 bucket:
  - Example key pattern: `chicago/chicago_crime_YYYYMMDD.csv`.

**Warehouse & modeling**

- Snowflake:
  - `RAW` schema for ingested data (Bronze).
  - `STAGE` schema for cleaned / conformed staging models (Silver).
  - `CORE` schema for reusable dimensions.
  - `MART` schema for business-ready fact and mart tables (Gold).
- dbt:
  - Organises transformations by layer.
  - Enforces data quality with tests.
  - Uses hooks for basic auditing and access control.
  - Uses snapshots for slowly changing dimensions.

---

## 2. Tech stack

- **Cloud & storage**
  - AWS Lambda, S3
- **Warehouse**
  - Snowflake (roles, warehouses, storage integrations, stages)
- **Transformations**
  - dbt Core (`snowflake` adapter)
- **Orchestration / CI**
  - GitHub Actions (CI workflow already configured in the repo)
- **BI**
  - Snowsight (native Snowflake charts) or any external BI (Tableau, Looker, Power BI)

---

## 3. Data model (Chicago)

### 3.1. Schemas and layers

- `CRIME_ANALYTICS_DEV.RAW`

  - `CRIME_EVENTS_CHICAGO_RAW`  
    Raw table loaded from S3 via Snowflake stage + `COPY INTO`.

- `CRIME_ANALYTICS_DEV.STAGE`

  - `STG_CRIME_CHICAGO`  
    Staging model:
    - Casts types (dates, booleans, floats).
    - Normalises column names.
    - Cleans nulls and obvious data issues.
    - Derives helper fields such as `incident_year`.

- `CRIME_ANALYTICS_DEV.CORE`

  - `DIM_CRIME_TYPE`  
    Crime categories, FBI codes, severity flags, violence/property indicators.
  - `DIM_LOCATION`  
    Police district, community area, ward and related information.
  - `DIM_DATE`  
    Calendar dimension generated from the fact date range  
    (year, month, day-of-week, ISO fields, year-quarter, etc.).

- `CRIME_ANALYTICS_DEV.MART`
  - `FCT_CRIME_EVENTS`  
    Grain: **one row per crime incident**.  
    Links to `DIM_DATE`, `DIM_LOCATION`, `DIM_CRIME_TYPE` via surrogate keys.
  - `MART_CRIME_DAILY_BY_AREA`  
    Daily metrics by date + district + area + crime type  
    (total incidents, violent incidents, arrests, rates).
  - `MART_CRIME_AREA_SNAPSHOT`  
    Yearly snapshot by area  
    (total incidents, arrests, domestic incidents, arrest_rate, domestic_rate).

### 3.2. Snapshots

- `CORE_HISTORY.CRIME_TYPE_SNAPSHOT` (dbt snapshot)
  - SCD-style history for `DIM_CRIME_TYPE`.
  - Tracks changes to description, severity and violence/property flags over time.

---

## 4. dbt project layout

```text
urban_crime_dbt/
  dbt_project.yml
  models/
    staging/
      stg_crime_chicago.sql
      staging.yml          # tests & docs for staging models
    core/
      dim_crime_type.sql
      dim_location.sql
      dim_date.sql
      core.yml             # tests & docs for core models
    mart/
      fct_crime_events.sql
      mart_crime_daily_by_area.sql
      mart_crime_area_snapshot.sql
      marts.yml            # tests & docs for marts
  snapshots/
    crime_type_snapshot.sql
  macros/
    ... (utility macros, hooks)
```

## 5. Tests used

- unique + not_null on dimension keys and fact keys.
- relationships tests:
  - fct_crime_events.date_key → dim_date.date_key
  - fct_crime_events.location_key → dim_location.location_key
  - fct_crime_events.crime_type_key → dim_crime_type.crime_type_key
  - Additional checks on important business columns (e.g. incident_id, incident_date).

## 6. Typical commands

```text
# Debug connection
dbt debug

# Build all models
dbt run

# Run tests
dbt test

# Run only staging for Chicago
dbt run  -s stg_crime_chicago
dbt test -s stg_crime_chicago

# Run fact + dependent models
dbt run -s +fct_crime_events

# Run only marts
dbt run  -s tag:mart
dbt test -s tag:mart
```

## 7. Quick demo ideas (Snowsight or any BI tool)

Once the marts are built, you can create simple but powerful views:

- Daily trends by district and crime type

From MART_CRIME_DAILY_BY_AREA:

- line chart: date vs total_incidents, coloured by police_district or crime_category.
- Area ranking by incidents and arrest_rate

From MART_CRIME_AREA_SNAPSHOT:

- bar chart: total_incidents by police_district, coloured by arrest_rate.
- Map of incidents

From FCT_CRIME_EVENTS:

- scatter or map using latitude / longitude, filtered by year and crime category.

## 8. Why this project is relevant

This repo demonstrates:

- A clean medallion-style layout (RAW/Stage/Core/Mart) implemented in Snowflake.
- Proper use of dbt for modular SQL, testing, hooks, snapshots and CI.
- Integration between AWS Lambda, S3, Snowflake and dbt in an end-to-end pipeline.
- Star schema and marts designed for real-world analytical questions around crime, risk and policing performance.

## 9. About de creator:

This code has been created and documented by Roberto Torres. More info at: www.robtorres.tech

```text
urban_crime_dbt/
  dbt_project.yml
  models/
    staging/
      stg_crime_chicago.sql
      staging.yml          # tests & docs for staging models
    core/
      dim_crime_type.sql
      dim_location.sql
      dim_date.sql
      core.yml             # tests & docs for core models
    mart/
      fct_crime_events.sql
      mart_crime_daily_by_area.sql
      mart_crime_area_snapshot.sql
      marts.yml            # tests & docs for marts
  snapshots/
    crime_type_snapshot.sql
  macros/
    ... (utility macros, hooks)
```

Tests used
• unique + not_null on dimension keys and fact keys.
• relationships tests:
• fct_crime_events.date_key → dim_date.date_key
• fct_crime_events.location_key → dim_location.location_key
• fct_crime_events.crime_type_key → dim_crime_type.crime_type_key
• Additional checks on important business columns (e.g. incident_id, incident_date).

Hooks
• Pre-/post-hooks on selected marts:
• Insert basic audit entries (model name, timestamp) into a metadata table.
• Grant SELECT on marts to a read-only analyst role (e.g. ROLE_CRIME_ANALYST).
