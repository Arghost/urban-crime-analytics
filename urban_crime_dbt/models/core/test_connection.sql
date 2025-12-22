SELECT
  CURRENT_USER()      AS snowflake_user,
  CURRENT_ROLE()      AS snowflake_role,
  CURRENT_WAREHOUSE() AS snowflake_warehouse,
  CURRENT_DATABASE()  AS snowflake_database,
  CURRENT_SCHEMA()    AS snowflake_schema