with source as (
  select * from {{ ref('raw_customers') }}
),
renamed as (
  select
    ID as CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    lower(EMAIL) as EMAIL,
    FIRST_NAME || ' ' || LAST_NAME as FULL_NAME
  from source
)
select * from renamed
