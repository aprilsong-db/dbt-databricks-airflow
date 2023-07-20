

-- notes: quarantine records and isolate them if the total sales amount is negative -- 
select 
 user_id,
 sum(amount) as total_amount 
from `asong_dev`.`dbdemos`.`dbt_c360_silver_orders`
group by 1 
having not (total_amount >= 0)