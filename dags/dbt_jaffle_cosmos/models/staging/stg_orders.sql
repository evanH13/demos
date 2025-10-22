with source as (
  select * from {{ ref('raw_orders') }}
),
renamed as (
  select
    ID as ORDER_ID,
    USER_ID as CUSTOMER_ID,
    to_date(ORDER_DATE) as ORDER_DATE,
    STATUS
  from source
)
select * from renamed
