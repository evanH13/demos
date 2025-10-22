# DBT Project Tour

dags/dbt_jaffle_cosmos/
 ├─ seeds/              # RAW_CUSTOMERS, RAW_ORDERS, RAW_PAYMENTS
 ├─ models/
 │   ├─ staging/        # stg_customers, stg_orders, stg_payments (views)
 │   └─ marts/          # dim_customers, fct_orders (tables)
 ├─ dbt_project.yml
 ├─ packages.yml
 └─ macros/             # (optional) generate_schema_name, alias, etc.