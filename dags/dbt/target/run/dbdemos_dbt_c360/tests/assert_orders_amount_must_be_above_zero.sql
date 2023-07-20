select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `asong_dev`.`dbdemos_dbt_test__audit`.`assert_orders_amount_must_be_above_zero`
    
      
    ) dbt_internal_test