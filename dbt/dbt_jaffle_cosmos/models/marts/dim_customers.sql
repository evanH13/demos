select
  c.CUSTOMER_ID,
  c.FULL_NAME,
  min(o.ORDER_DATE) as FIRST_ORDER_DATE,
  max(o.ORDER_DATE) as MOST_RECENT_ORDER_DATE,
  count(distinct o.ORDER_ID) as ORDER_COUNT
from {{ ref('stg_customers') }} c
left join {{ ref('stg_orders') }} o using (CUSTOMER_ID)