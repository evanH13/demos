with source as (
  select * from {{ ref('raw_payments') }}
),
renamed as (
  select
    "ID" as PAYMENT_ID,
    "ORDER_ID",
    "PAYMENT_METHOD",
    try_to_decimal("AMOUNT") as AMOUNT
  from source
)
select * from renamed