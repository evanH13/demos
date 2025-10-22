with payments as (
  select ORDER_ID, sum(AMOUNT) as TOTAL_AMOUNT
  from {{ ref('stg_payments') }}
  group by 1
), orders as (
  select * from {{ ref('stg_orders') }}
)
select
  o.ORDER_ID,
  o.CUSTOMER_ID,
  o.ORDER_DATE,
  o.STATUS,
  coalesce(p.TOTAL_AMOUNT, 0) as AMOUNT
from orders o
left join payments p using (ORDER_ID)
