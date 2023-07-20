{{
 config(materialized = 'table', file_format = 'delta')
}}
select 
  user_id,
  session_id,
  event_id,
  `date`,
  platform,
  action,
  url
from {{ var("uc_schema") }}.dbt_c360_bronze_events