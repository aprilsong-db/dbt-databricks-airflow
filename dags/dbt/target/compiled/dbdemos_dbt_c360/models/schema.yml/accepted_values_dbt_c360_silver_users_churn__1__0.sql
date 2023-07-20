
    
    

with all_values as (

    select
        churn as value_field,
        count(*) as n_records

    from `asong_dev`.`dbdemos`.`dbt_c360_silver_users`
    group by churn

)

select *
from all_values
where value_field not in (
    '1','0'
)


