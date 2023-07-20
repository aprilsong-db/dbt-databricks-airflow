
  
    
        create or replace table `asong_dev`.`dbdemos`.`dbt_c360_silver_events`
      
      
    using delta
      
      
      
      
      
      
      as
      

select 
  user_id,
  session_id,
  event_id,
  `date`,
  platform,
  action,
  url
from dbdemos.dbt_c360_bronze_events
  