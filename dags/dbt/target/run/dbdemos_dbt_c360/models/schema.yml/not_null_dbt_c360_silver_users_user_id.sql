select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select user_id
from `asong_dev`.`dbdemos`.`dbt_c360_silver_users`
where user_id is null



      
    ) dbt_internal_test