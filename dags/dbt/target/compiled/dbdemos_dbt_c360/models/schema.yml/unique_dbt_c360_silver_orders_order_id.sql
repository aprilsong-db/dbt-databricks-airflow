
    
    

select
    order_id as unique_field,
    count(*) as n_records

from `asong_dev`.`dbdemos`.`dbt_c360_silver_orders`
where order_id is not null
group by order_id
having count(*) > 1


