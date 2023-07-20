
  
    
        create or replace table `asong_dev`.`dbdemos`.`dbt_c360_silver_orders`
      
      
    using delta
      
      
      
      
      
      
      as
      

--notes: order data cleaned and anonymized for analysis -- 
select
  cast(amount as int),
  `id` as order_id,
  user_id,
  cast(item_count as int),
  to_timestamp(transaction_date, "MM-dd-yyyy HH:mm:ss") as creation_date
from dbdemos.dbt_c360_bronze_orders
  