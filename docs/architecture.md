# Architecture – Urban Crime & Safety Analytics

## High-level flow

Open Data (UK Police, Chicago, NYC, LA)
→ AWS Lambda (one per source)
→ S3 (partitioned by city/country and date)
→ Snowflake (Snowpipe auto-ingest into RAW)
→ dbt (RAW → STAGE → CORE → MART, tests & snapshots)
→ Tableau (dashboards for crime risk & trends)

## Snowflake layout

- Database: CRIME_ANALYTICS_DEV
- Schemas:

  - RAW: permanent tables with ingested raw events
  - STAGE: transient tables for cleaned, harmonised events
  - CORE: permanent fact/dim schema
  - MART: business-facing tables and views for BI

- Warehouses:
  - WH_CRIME_ETL: used by dbt for transformations
  - WH_CRIME_BI: used by Tableau and other BI tools
