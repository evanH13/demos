# Project Tour

dags/
  ├─ cosmos_dbt_snowflake_fieldengineer.py
  ├─ cosmos_dbt_taskgroup_split_demo.py
  └─ dbt_jaffle_cosmos/
       ├─ dbt_project.yml
       ├─ profiles.yml
       ├─ seeds/
       ├─ models/
       └─ macros/

# DBT Project Tour

dbt/dbt_jaffle_cosmos/
 ├─ seeds/              # RAW_CUSTOMERS, RAW_ORDERS, RAW_PAYMENTS
 ├─ models/
 │   ├─ staging/        # stg_customers, stg_orders, stg_payments (views)
 │   └─ marts/          # dim_customers, fct_orders (tables)
 ├─ dbt_project.yml
 ├─ packages.yml
 └─ macros/             # (optional) generate_schema_name, alias, etc.