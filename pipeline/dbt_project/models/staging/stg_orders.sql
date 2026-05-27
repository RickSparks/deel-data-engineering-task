with source as (
    select * from {{ source('raw', 'orders') }}
    where _op != 'd'
)

select
    order_id,
    order_date,
    delivery_date,
    customer_id,
    status,
    updated_at,
    created_at,
    _ingested_at
from source