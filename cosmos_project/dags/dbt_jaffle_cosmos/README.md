# jaffle_shop_cosmos (FIELDENGINEER-only)
All seeds and models build into SANDBOX.FIELDENGINEER.

After `dbt seed` you'll see: RAW_CUSTOMERS, RAW_ORDERS, RAW_PAYMENTS
After `dbt run` you'll see: STG_CUSTOMERS, STG_ORDERS, STG_PAYMENTS, DIM_CUSTOMERS, FCT_ORDERS

Trigger DAG: jaffle_shop_cosmos_snowflake_fieldengineer
