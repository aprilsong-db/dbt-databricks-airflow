
    
    

with child as (
    select user_id as from_field
    from `asong_dev`.`dbdemos`.`dbt_c360_silver_orders`
    where user_id is not null
),

parent as (
    select user_id as to_field
    from `asong_dev`.`dbdemos`.`dbt_c360_silver_users`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


