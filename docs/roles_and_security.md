# Roles & Security – Crime & Safety Analytics

## Project-specific roles

- ROLE_CRIME_INGEST

  - Owns storage integration, external stages and pipes.
  - Can SELECT/INSERT into RAW tables.

- ROLE_CRIME_TRANSFORM

  - Used by dbt.
  - Can SELECT from RAW and CREATE/ALTER/DROP tables in STAGE/CORE/MART.

- ROLE_CRIME_BI

  - Used by Tableau/BI.
  - Read-only access to CORE and MART schemas.

- ROLE_CRIME_ADMIN
  - Aggregates all project roles.
  - Used by the project owner for administration.

## Users

- USER_DBT_CRIME → default role: ROLE_CRIME_TRANSFORM
- USER_TABLEAU_CRIME (to be created later) → default role: ROLE_CRIME_BI
